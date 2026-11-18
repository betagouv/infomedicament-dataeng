"""Substance page titles, matching infomedicament PR #287.

Titles use main names only. Medicine-group usage affects page subtitles, not
these stored titles, so no dependency on other resume tables is needed.
"""

import re
from collections import defaultdict

from sqlalchemy import text

from .composants import clean_component_display_name


def clean_substance_name(value: str) -> str:
    entities = {
        "amp": "&",
        "apos": "'",
        "quot": '"',
        "lt": "<",
        "gt": ">",
        "nbsp": " ",
        "lsquo": "‘",
        "rsquo": "’",
        "Delta": "Δ",
    }
    value = re.sub(r"&#x([0-9a-f]+);", lambda match: chr(int(match[1], 16)), value, flags=re.IGNORECASE)
    value = re.sub(r"&#(\d+);", lambda match: chr(int(match[1])), value)
    value = re.sub(r"&([a-z]+);", lambda match: entities.get(match[1], match[0]), value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def _numeric_name_id_key(value: str) -> tuple:
    # Match localeCompare("en", { numeric: true }), including unpadded IDs.
    return tuple((0, int(part)) if part.isdigit() else (1, part) for part in re.split(r"(\d+)", value))


def load_titles_by_subs_id(conn) -> dict[str, str]:
    """Load all names once, choosing a main name and composition fallback per substance."""
    names_by_subs_id = defaultdict(list)
    for row in conn.execute(text("SELECT code_substance, code_nom, nom, type FROM ansm_substance_nom")).mappings():
        names_by_subs_id[(row["code_substance"] or "").strip()].append(row)

    titles = {}
    for subs_id, names in names_by_subs_id.items():
        usable = [row for row in names if not (row["nom"] or "").strip().startswith("(NE PAS UTILISER)")]
        candidates = usable or names
        main = next((row for row in candidates if row["type"] == "CANONIQUE"), None)
        if main is None:
            main = min(candidates, key=lambda row: _numeric_name_id_key((row["code_nom"] or "").strip()))
        titles[subs_id] = clean_substance_name(main["nom"] or "")

    # Match getSubstancesNames: the first nonempty composition label, sorted by
    # substance label, and only for substances with no ANSM name at all.
    labels = conn.execute(
        text("SELECT DISTINCT code_substance, substance FROM ansm_composant ORDER BY substance")
    ).mappings()
    for row in labels:
        subs_id = (row["code_substance"] or "").strip()
        label = (row["substance"] or "").strip()
        if subs_id and subs_id not in titles and label:
            titles[subs_id] = clean_substance_name(clean_component_display_name(label))
    return titles


def page_title(subs_ids: list[str], titles_by_subs_id: dict[str, str]) -> str | None:
    """Join main names in component order, returning null if any substance is missing."""
    ids = [subs_id.strip() for subs_id in subs_ids]
    if not ids or any(subs_id not in titles_by_subs_id for subs_id in ids):
        return None
    return ", ".join(titles_by_subs_id[subs_id] for subs_id in ids)
