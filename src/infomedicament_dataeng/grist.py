"""Synchronize hand-maintained Grist reference data into PostgreSQL."""

from __future__ import annotations

import logging
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests
from sqlalchemy import text

from .config import PostgresConfig
from .db import get_postgres_engine

logger = logging.getLogger(__name__)

GRIST_SERVER = "https://grist.numerique.gouv.fr"

Record = dict[str, Any]
Mapper = Callable[[Record], dict[str, Any] | list[dict[str, Any]]]


@dataclass(frozen=True)
class TableSync:
    target: str
    source: str
    expected_fields: tuple[str, ...]
    mapper: Mapper


def _safe_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return ",".join(str(item) for item in value).strip()
    if isinstance(value, bool):
        return str(value).lower()
    return str(value).strip()


def _normalize_alias(value: Any) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").lower())
    return "".join(character for character in normalized if unicodedata.category(character) != "Mn").strip()


def _map_glossary(fields: Record) -> dict[str, Any]:
    return {
        "nom": fields.get("Nom_glossaire"),
        "definition": fields.get("Definition_glossaire"),
        "source": fields.get("Source") or None,
        "a_souligner": bool(fields.get("A_souligner")),
    }


def _map_article(fields: Record) -> dict[str, Any]:
    return {
        "titre": fields.get("Titre"),
        "source": fields.get("Source"),
        "contenu": fields.get("Contenu"),
        "theme": fields.get("Theme"),
        "lien": fields.get("Lien"),
        "metadescription": fields.get("Metadescription"),
        "homepage": bool(fields.get("Homepage")),
        "image": fields.get("ImageId"),
        "atc_classe": fields.get("Classes_ATC"),
        "substances": fields.get("Substances"),
        "specialites": fields.get("Specialites"),
        "pathologies": fields.get("Pathologies"),
    }


def _map_search_synonyms(fields: Record) -> list[dict[str, Any]]:
    canonical = _safe_string(fields.get("Canonical"))
    if not canonical:
        return []
    aliases = [_normalize_alias(alias) for alias in str(fields.get("Alias") or "").split(",")]
    return [{"alias": alias, "canonical": canonical} for alias in aliases if alias]


TABLE_SYNCS = (
    TableSync(
        "ref_glossaire",
        "Glossaire",
        ("Nom_glossaire", "Definition_glossaire", "Source", "A_souligner"),
        _map_glossary,
    ),
    TableSync(
        "ref_articles",
        "Articles",
        (
            "Titre",
            "Source",
            "Contenu",
            "Theme",
            "Lien",
            "Homepage",
            "ImageId",
            "Metadescription",
            "Classes_ATC",
            "Substances",
            "Specialites",
            "Pathologies",
        ),
        _map_article,
    ),
    TableSync(
        "ref_marr_url_cis",
        "MARR_URL_CIS",
        ("URL", "CIS"),
        lambda fields: {"url": fields.get("URL"), "cis": _safe_string(fields.get("CIS"))},
    ),
    TableSync(
        "ref_marr_url_pdf",
        "MARR_URL_PDF",
        ("URL_text", "Nom_document", "URL_document", "Type"),
        lambda fields: {
            "url": _safe_string(fields.get("URL_text")),
            "nom_document": fields.get("Nom_document"),
            "url_document": fields.get("URL_document"),
            "type": fields.get("Type"),
        },
    ),
    TableSync(
        "ref_pathologies",
        "Pathologies",
        ("codePatho", "Definition_pathologie"),
        lambda fields: {
            "code_patho": fields.get("codePatho") or None,
            "definition": fields.get("Definition_pathologie"),
            "code_classe_clinique": fields.get("codeClasClinique") or None,
        },
    ),
    TableSync(
        "ref_pediatrie",
        "Pediatrie",
        ("CIS", "contre_indication"),
        lambda fields: {
            "cis": _safe_string(fields.get("CIS")),
            "contre_indication": fields.get("contre_indication"),
        },
    ),
    TableSync(
        "ref_grossesse_substances_contre_indiquees",
        "Grossesse_substances_contre_indiquees",
        ("SubsId", "Lien_site_ANSM"),
        lambda fields: {
            "subs_id": _safe_string(fields.get("SubsId")),
            "lien_site_ansm": fields.get("Lien_site_ANSM"),
        },
    ),
    TableSync(
        "ref_grossesse_mention",
        "Grossesse_mention",
        ("CIS",),
        lambda fields: {"cis": _safe_string(fields.get("CIS"))},
    ),
    TableSync(
        "ref_substance_active_definitions",
        "Definitions_Substances_Actives",
        ("SubsId", "NomId", "SA", "Definition"),
        lambda fields: {
            "subs_id": _safe_string(fields.get("SubsId")),
            "nom_id": _safe_string(fields.get("NomId")),
            "sa": fields.get("SA"),
            "definition": fields.get("Definition"),
        },
    ),
    TableSync(
        "ref_atc_friendly_niveau_1",
        "ATC_friendly_1",
        ("Lettre_1_ATC_1", "Libelles_niveau_1", "Definition_Classe"),
        lambda fields: {
            "code": fields.get("Lettre_1_ATC_1"),
            "libelle": fields.get("Libelles_niveau_1"),
            "definition_classe": fields.get("Definition_Classe"),
        },
    ),
    TableSync(
        "ref_atc_friendly_niveau_2",
        "ATC_friendly_2",
        ("Lettre_2_ATC2", "libelle", "Libelles_niveau_2_Definition_sous_classe"),
        lambda fields: {
            "code": fields.get("Lettre_2_ATC2"),
            "libelle": fields.get("libelle"),
            "definition_sous_classe": fields.get("Libelles_niveau_2_Definition_sous_classe"),
        },
    ),
    TableSync(
        "search_synonyms",
        "SearchSynonyms",
        ("Alias", "Canonical"),
        _map_search_synonyms,
    ),
)


def _decode_list_fields(table: str, fields: Record) -> Record:
    decoded = dict(fields)
    list_fields = {
        "MARR_URL_CIS": {"Generiques"},
        "MARR_URL_PDF": {"Type"},
        "Articles": {"Classes_ATC", "Indications"},
    }.get(table, set())
    for field in list_fields:
        value = decoded.get(field)
        if isinstance(value, list) and value[:1] == ["L"]:
            decoded[field] = value[1:]
    return decoded


def _fetch_table(session: requests.Session, doc_id: str, api_key: str, spec: TableSync) -> list[Record]:
    response = session.get(
        f"{GRIST_SERVER}/api/docs/{doc_id}/tables/{spec.source}/records",
        params={"sort": "manualSort"},
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    body = response.json()
    if "error" in body:
        raise RuntimeError(f"Grist error: {body['error']}")
    records = body.get("records")
    if not isinstance(records, list):
        raise ValueError(f"Invalid Grist response for table {spec.source}")
    if records:
        fields = records[0].get("fields", {})
        missing = [field for field in spec.expected_fields if field not in fields]
        if missing:
            raise ValueError(f"Grist table {spec.source} is missing expected fields: {', '.join(missing)}")
    return [_decode_list_fields(spec.source, record.get("fields", {})) for record in records]


def _replace_table(engine, spec: TableSync, rows: list[dict[str, Any]]) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {spec.target}"))
        if not rows:
            return
        columns = tuple(rows[0])
        if any(tuple(row) != columns for row in rows):
            raise ValueError(f"Mapped rows for {spec.source} do not have consistent columns")
        placeholders = ", ".join(f":{column}" for column in columns)
        conn.execute(text(f"INSERT INTO {spec.target} ({', '.join(columns)}) VALUES ({placeholders})"), rows)


def sync_grist(doc_id: str, api_key: str, config: PostgresConfig | None = None) -> dict[str, int]:
    """Replace PostgreSQL reference tables with their current Grist contents."""
    if not doc_id:
        raise ValueError("GRIST_DOC_ID is required")
    if not api_key:
        raise ValueError("GRIST_API_KEY is required")

    engine = get_postgres_engine(config)
    results: dict[str, int] = {}
    with requests.Session() as session:
        for spec in TABLE_SYNCS:
            logger.info("Fetching %s from Grist document %s", spec.source, doc_id)
            records = _fetch_table(session, doc_id, api_key, spec)
            if not records:
                logger.warning("No data found for %s; leaving %s unchanged", spec.source, spec.target)
                continue

            rows: list[dict[str, Any]] = []
            for index, fields in enumerate(records):
                try:
                    mapped = spec.mapper(fields)
                    rows.extend(mapped if isinstance(mapped, list) else [mapped])
                except Exception:
                    logger.error("Error mapping line %d of table %s: %r", index, spec.source, fields)
                    raise

            logger.info("Updating %s (%d rows)", spec.target, len(rows))
            _replace_table(engine, spec, rows)
            results[spec.target] = len(rows)

    logger.info("Grist synchronization complete")
    return results
