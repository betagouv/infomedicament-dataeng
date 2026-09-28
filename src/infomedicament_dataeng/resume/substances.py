"""Build the active-substance browse summary."""

from collections import defaultdict

from .common import group_name, load_specialties, normalize_letter, replace_letters, replace_rows
from .composants import load_composants_by_cis, name_sort_key


def build(conn) -> int:
    specialties = load_specialties(conn)
    denomination_by_cis = {specialty.cis: specialty.denomination for specialty in specialties}
    composants_by_cis = load_composants_by_cis(conn)
    raw_rows = []
    seen: set[tuple[str, str, str]] = set()

    for cis, composants in composants_by_cis.items():
        composant_keys = {(composant.element_number, composant.composant_number) for composant in composants}
        if len(composant_keys) != 1 or cis not in denomination_by_cis:
            continue
        for composant in composants:
            key = (composant.substance_id, composant.name_id, denomination_by_cis[cis])
            if key not in seen:
                seen.add(key)
                raw_rows.append((composant, denomination_by_cis[cis]))

    raw_rows.sort(key=lambda value: name_sort_key(value[0].name))
    names_by_identity: dict[tuple[str, str], dict] = {}
    group_names_by_subs_id: dict[str, set[str]] = defaultdict(set)
    letters: set[str] = set()
    for composant, denomination in raw_rows:
        name_id = composant.name_id.strip()
        subs_id = composant.substance_id.strip()
        name = composant.name.strip()
        names_by_identity[(subs_id, name_id)] = {
            "SubsId": subs_id,
            "NomId": name_id,
            "NomLib": name,
            "specialites": 0,
        }
        group_names_by_subs_id[subs_id].add(group_name(denomination))
        letters.add(normalize_letter(name[:1]))

    rows = []
    for row in names_by_identity.values():
        row["specialites"] = len(group_names_by_subs_id[row["SubsId"]])
        if row["specialites"]:
            rows.append(row)
    replace_rows(conn, "resume_substances", rows)
    replace_letters(conn, "substances", letters)
    return len(rows)
