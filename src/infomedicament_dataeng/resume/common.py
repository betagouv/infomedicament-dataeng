"""Shared data loading and formatting for resume-table builders."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import bindparam, text

from ..db import VISIBLE_SPECIALITE_AVAILABILITIES


@dataclass(frozen=True)
class Specialty:
    cis: str
    denomination: str
    procedure: str
    availability: str

    @property
    def status(self) -> int:
        if self.availability == "ALERTE":
            return 3
        if self.availability in {"PARTIELLE", "INDISPONIBLE"}:
            return 2
        return 1


@dataclass(frozen=True)
class Composant:
    cis: str
    element_number: int
    element_order: int
    nature: str
    composant_number: int
    composant_order: int
    substance_id: str
    name_id: str
    name: str


def load_specialties(conn) -> list[Specialty]:
    rows = conn.execute(
        text(
            "SELECT cis, denomination, procedure, disponibilite FROM ansm_specialite"
            " WHERE disponibilite IN :availabilities ORDER BY denomination"
        ).bindparams(bindparam("availabilities", expanding=True)),
        {"availabilities": VISIBLE_SPECIALITE_AVAILABILITIES},
    ).mappings()
    return [
        Specialty(
            cis=str(row["cis"]),
            denomination=row["denomination"] or "",
            procedure=row["procedure"] or "NON_COMMUNIQUEE",
            availability=row["disponibilite"] or "",
        )
        for row in rows
    ]


def group_name(denomination: str) -> str:
    match = re.match(r"^[^0-9,]+", denomination)
    return (match.group() if match else denomination).strip()


def format_name(value: str) -> str:
    return " ".join(
        word[0] + word[1:].lower() if word and re.match(r"[A-Z]", word[0]) else word for word in value.split(" ")
    )


def generic_dci(value: str) -> str:
    match = re.match(r"^[^-]+", value)
    return (match.group() if match else value).strip()


def normalize_letter(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.lower())
    return "".join(character for character in normalized if not "\u0300" <= character <= "\u036f").upper()


def sorted_letters(values: set[str]) -> list[str]:
    # JavaScript localeCompare places punctuation and digits before letters for
    # the single-character values produced by the previous script.
    return sorted(values, key=lambda value: (0 if not value.isalnum() else 1 if value.isdigit() else 2, value))


def replace_rows(conn, table: str, rows: list[dict[str, Any]]) -> None:
    conn.execute(text(f"DELETE FROM {table}"))
    if not rows:
        return
    columns = tuple(rows[0])
    if any(tuple(row) != columns for row in rows):
        raise ValueError(f"Rows for {table} do not have consistent columns")
    placeholders = ", ".join(f":{column}" for column in columns)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    conn.execute(text(f"INSERT INTO {table} ({quoted_columns}) VALUES ({placeholders})"), rows)


def replace_letters(conn, target: str, letters: set[str]) -> None:
    conn.execute(text("DELETE FROM letters WHERE type = :type"), {"type": target})
    conn.execute(
        text("INSERT INTO letters (type, letters) VALUES (:type, :letters)"),
        {"type": target, "letters": sorted_letters(letters)},
    )


def load_indications(conn) -> list[tuple[int, str, set[str]]]:
    rows = conn.execute(text('SELECT id, nom, "CIS" AS cis FROM indications')).mappings()
    return [(row["id"], row["nom"] or "", {str(cis) for cis in row["cis"] or []}) for row in rows]


def indications_for_cis(indications: list[tuple[int, str, set[str]]], cis_values: list[str]) -> list[tuple[int, str]]:
    selected_cis = set(cis_values)
    return [(identifier, name) for identifier, name, linked_cis in indications if linked_cis & selected_cis]


def load_atc_by_cis(conn) -> dict[str, str]:
    rows = conn.execute(
        text(
            "SELECT DISTINCT ON (specialty_atc.cis) specialty_atc.cis, atc.libelle_abr AS code"
            " FROM ansm_specialite_atc specialty_atc"
            " JOIN ansm_atc atc ON atc.code = specialty_atc.code_atc"
            " WHERE atc.libelle_abr IS NOT NULL"
            " ORDER BY specialty_atc.cis, specialty_atc.code_atc"
        )
    ).mappings()
    return {str(row["cis"]): row["code"] for row in rows}


def load_surveillance_cis(conn, today: date | None = None) -> set[str]:
    current_date = today or date.today()
    rows = conn.execute(
        text(
            "SELECT DISTINCT cis FROM ansm_specialite_evenement"
            " WHERE code_evenement = 83 AND :today > date_evenement AND :today < date_echeance"
        ),
        {"today": current_date},
    ).scalars()
    return {str(cis) for cis in rows}


def normalize_numeric_id(value: str) -> str | None:
    stripped = value.strip()
    if not stripped or not stripped.isascii() or not stripped.isdigit():
        return None
    normalized = stripped.lstrip("0")
    return normalized or None
