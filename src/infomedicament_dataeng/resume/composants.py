"""Map ANSM composition rows to the compatibility composant model."""

from __future__ import annotations

import unicodedata
from collections import defaultdict

from sqlalchemy import text

from .common import Composant


def _normalized_name(value: str | None) -> str:
    return unicodedata.normalize("NFC", (value or "").strip().lower())


def name_sort_key(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.casefold())
    return "".join(character for character in normalized if unicodedata.category(character) != "Mn")


def load_composants_by_cis(conn) -> dict[str, list[Composant]]:
    rows = list(
        conn.execute(
            text(
                "SELECT cis, numero_element, numero_composant, code_substance, substance, nature, ordre"
                " FROM ansm_composant"
            )
        ).mappings()
    )
    names = list(conn.execute(text("SELECT code_substance, code_nom, nom, type FROM ansm_substance_nom")).mappings())
    elements = conn.execute(text("SELECT cis, numero_element, ordre FROM ansm_element")).mappings()

    names_by_code: dict[str, list[dict]] = defaultdict(list)
    for name in names:
        names_by_code[str(name["code_substance"])].append(name)
    element_orders = {
        (str(element["cis"]), element["numero_element"]): (
            element["ordre"] if element["ordre"] is not None else element["numero_element"]
        )
        for element in elements
    }

    mapped: dict[str, list[Composant]] = defaultdict(list)
    for row in rows:
        cis = str(row["cis"])
        substance_id = (row["code_substance"] or "").strip()
        candidates = names_by_code.get(substance_id, [])
        exact = [name for name in candidates if _normalized_name(name["nom"]) == _normalized_name(row["substance"])]
        preferred = (
            next((name for name in exact if name["type"] == "CANONIQUE"), None)
            or (sorted(exact, key=lambda name: name["code_nom"])[0] if exact else None)
            or next((name for name in candidates if name["type"] == "CANONIQUE"), None)
            or (sorted(candidates, key=lambda name: name["code_nom"])[0] if candidates else None)
        )
        composant_number = row["ordre"] if row["ordre"] is not None else row["numero_composant"]
        mapped[cis].append(
            Composant(
                cis=cis,
                element_number=row["numero_element"],
                element_order=element_orders.get((cis, row["numero_element"]), row["numero_element"]),
                nature=(
                    "fraction"
                    if row["nature"] == "Fraction active"
                    else "substance"
                    if row["nature"] == "Substance active"
                    else "unknown"
                ),
                composant_number=composant_number,
                composant_order=composant_number,
                substance_id=substance_id,
                name_id=(preferred["code_nom"].strip() if preferred else substance_id),
                name=(row["substance"] or "").strip() or ((preferred["nom"] or "").strip() if preferred else ""),
            )
        )

    for composants in mapped.values():
        composants.sort(
            key=lambda composant: (
                composant.element_order,
                composant.element_number,
                composant.composant_order,
                composant.composant_number,
                name_sort_key(composant.name),
            )
        )
    return dict(mapped)


def load_main_names_by_subs_id(conn) -> dict[str, str]:
    rows = conn.execute(text("SELECT code_substance, code_nom, nom FROM ansm_substance_nom")).mappings()
    return {
        subs_id: (row["nom"] or "").strip()
        for row in rows
        if (subs_id := (row["code_substance"] or "").strip()) == (row["code_nom"] or "").strip()
    }


def simple_composants(composants: list[Composant]) -> list[Composant]:
    groups: dict[int, list[Composant]] = {}
    for composant in composants:
        groups.setdefault(composant.composant_number, []).append(composant)
    return [
        composant
        for group in groups.values()
        for composant in ([item for item in group if item.nature == "fraction"] or group)
    ]


def display_composants(composants: list[Composant]) -> str:
    return ", ".join(composant.name.strip() for composant in composants)
