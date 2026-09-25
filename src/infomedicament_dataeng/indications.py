"""Build application indication records from ANSM and Grist reference data."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import bindparam, text

from .config import PostgresConfig
from .db import VISIBLE_SPECIALITE_AVAILABILITIES, get_postgres_engine


@dataclass(frozen=True)
class IndicationBuildResult:
    inserted: int
    updated: int
    deleted: int


def _clinical_class_name(value: str | None) -> str:
    name = (value or "").strip()
    if name and re.match(r"[A-Z]", name[0]):
        return name[0] + name[1:].lower()
    return name


def _plan_indications(
    existing: list[dict[str, Any]],
    definitions: list[dict[str, Any]],
    pathologies: list[dict[str, Any]],
    clinical_classes: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[int]]:
    # Grist may contain both pathology-specific and class-level definitions.
    # Keep its row order to match the precedence used by the previous script.
    definitions_by_pathology: dict[int, dict[str, Any]] = {}
    definitions_by_class: dict[int, dict[str, Any]] = {}
    for definition in definitions:
        if definition["code_patho"] is not None:
            definitions_by_pathology.setdefault(definition["code_patho"], definition)
        if definition["code_classe_clinique"] is not None:
            definitions_by_class.setdefault(definition["code_classe_clinique"], definition)

    existing_pathologies: dict[tuple[int, str], dict[str, Any]] = {}
    existing_classes: dict[int, dict[str, Any]] = {}
    for indication in existing:
        if indication["code_patho"] is not None:
            existing_pathologies.setdefault((indication["code_patho"], indication["nom"]), indication)
        elif indication["code_classe_clinique"] is not None:
            existing_classes.setdefault(indication["code_classe_clinique"], indication)

    inserted: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    retained_ids: set[int] = set()

    # Pathology IDs remain stable only while both their code and official name
    # are unchanged, matching the historical aggregation behavior.
    for pathology in pathologies:
        code = pathology["code"]
        name = (pathology["nom"] or "").strip()
        definition = definitions_by_pathology.get(code)
        values = {
            "code_patho": code,
            "code_classe_clinique": definition["code_classe_clinique"] if definition else None,
            "nom": name,
            "definition": (definition["definition"] or None) if definition else None,
            "cis": pathology["cis"] or [],
        }
        previous = existing_pathologies.get((code, name))
        if previous:
            retained_ids.add(previous["id"])
            updated.append({"id": previous["id"], **values})
        else:
            inserted.append(values)

    for clinical_class in clinical_classes:
        code = clinical_class["code"]
        definition = definitions_by_class.get(code)
        # A class linked to a pathology is represented by that pathology rather
        # than duplicated as a standalone indication.
        if definition and definition["code_patho"]:
            continue
        values = {
            "code_patho": None,
            "code_classe_clinique": code,
            "nom": _clinical_class_name(clinical_class["libelle_court"]),
            "definition": (definition["definition"] or None) if definition else None,
            "cis": clinical_class["cis"] or [],
        }
        previous = existing_classes.get(code)
        if previous:
            retained_ids.add(previous["id"])
            updated.append({"id": previous["id"], **values})
        else:
            inserted.append(values)

    deleted = [indication["id"] for indication in existing if indication["id"] not in retained_ids]
    return inserted, updated, deleted


def build_indications(config: PostgresConfig | None = None) -> IndicationBuildResult:
    """Synchronize the derived ``indications`` table from current source tables."""
    engine = get_postgres_engine(config)
    availability_param = bindparam("availabilities", expanding=True)
    availability_values = {"availabilities": VISIBLE_SPECIALITE_AVAILABILITIES}

    with engine.begin() as conn:
        existing = list(
            conn.execute(
                text(
                    'SELECT id, "codePatho" AS code_patho, "codeClasseClinique" AS code_classe_clinique,'
                    ' nom, definition, "CIS" AS cis FROM indications ORDER BY id'
                )
            ).mappings()
        )
        definitions = list(
            conn.execute(
                text("SELECT code_patho, code_classe_clinique, definition FROM ref_pathologies ORDER BY id")
            ).mappings()
        )
        pathologies = list(
            conn.execute(
                text(
                    # A pathology inherits medicines through its related
                    # clinical classes; unavailable specialties are excluded.
                    "SELECT pathology.code, pathology.nom,"
                    " COALESCE(array_agg(DISTINCT specialty.cis ORDER BY specialty.cis)"
                    " FILTER (WHERE specialty.cis IS NOT NULL), ARRAY[]::varchar[]) AS cis"
                    " FROM ansm_pathologie pathology"
                    " LEFT JOIN ansm_classe_clinique_pathologie relation"
                    " ON relation.code_pathologie = pathology.code"
                    " LEFT JOIN ansm_specialite_classe_clinique specialty_class"
                    " ON specialty_class.code_classe_clinique = relation.code_classe_clinique"
                    " LEFT JOIN ansm_specialite specialty"
                    " ON specialty.cis = specialty_class.cis"
                    " AND specialty.disponibilite IN :availabilities"
                    " GROUP BY pathology.code, pathology.nom ORDER BY pathology.code"
                ).bindparams(availability_param),
                availability_values,
            ).mappings()
        )
        clinical_classes = list(
            conn.execute(
                text(
                    # Clinical classes retain their direct specialty links.
                    "SELECT clinical_class.code, clinical_class.libelle_court,"
                    " COALESCE(array_agg(DISTINCT specialty.cis ORDER BY specialty.cis)"
                    " FILTER (WHERE specialty.cis IS NOT NULL), ARRAY[]::varchar[]) AS cis"
                    " FROM ansm_classe_clinique clinical_class"
                    " LEFT JOIN ansm_specialite_classe_clinique specialty_class"
                    " ON specialty_class.code_classe_clinique = clinical_class.code"
                    " LEFT JOIN ansm_specialite specialty"
                    " ON specialty.cis = specialty_class.cis"
                    " AND specialty.disponibilite IN :availabilities"
                    " GROUP BY clinical_class.code, clinical_class.libelle_court ORDER BY clinical_class.code"
                ).bindparams(availability_param),
                availability_values,
            ).mappings()
        )

        inserted, updated, deleted = _plan_indications(existing, definitions, pathologies, clinical_classes)
        if updated:
            conn.execute(
                text(
                    'UPDATE indications SET "codePatho" = :code_patho,'
                    ' "codeClasseClinique" = :code_classe_clinique, nom = :nom,'
                    ' definition = :definition, "CIS" = :cis WHERE id = :id'
                ),
                updated,
            )
        if inserted:
            conn.execute(
                text(
                    'INSERT INTO indications ("codePatho", "codeClasseClinique", nom, definition, "CIS")'
                    " VALUES (:code_patho, :code_classe_clinique, :nom, :definition, :cis)"
                ),
                inserted,
            )
        if deleted:
            conn.execute(
                text("DELETE FROM indications WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": deleted},
            )

    return IndicationBuildResult(inserted=len(inserted), updated=len(updated), deleted=len(deleted))
