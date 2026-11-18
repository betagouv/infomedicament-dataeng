"""Build the active-substance browse summary."""

from collections import defaultdict

from .common import group_name, load_specialties, normalize_letter, replace_letters, replace_rows
from .composants import load_composants_by_cis, load_name_types_by_nom_id, name_sort_key, simple_composants
from .titles import clean_substance_name, load_titles_by_subs_id


def build(conn) -> int:
    specialties = load_specialties(conn)
    denomination_by_cis = {specialty.cis: specialty.denomination for specialty in specialties}
    composants_by_cis = load_composants_by_cis(conn)
    name_types_by_nom_id = load_name_types_by_nom_id(conn)
    titles_by_subs_id = load_titles_by_subs_id(conn)
    raw_rows = []
    seen: set[tuple[str, str, str]] = set()
    group_names_by_subs_id: dict[str, set[str]] = defaultdict(set)

    for cis, composants in composants_by_cis.items():
        if cis not in denomination_by_cis:
            continue
        composant_keys = {(composant.element_number, composant.composant_number) for composant in composants}
        substances = {composant.substance_id for composant in composants if composant.nature == "substance"}
        fractions = [composant for composant in composants if composant.nature == "fraction"]
        countable = (
            len(composant_keys) == 1
            and len(substances) <= 1
            and len({composant.substance_id for composant in fractions}) <= 1
        )
        # Keep names from combinations and kits too; only single-substance
        # specialties contribute to the browse count.
        for composant in simple_composants(composants):
            if countable:
                group_names_by_subs_id[composant.substance_id.strip()].add(group_name(denomination_by_cis[cis]))
            key = (composant.substance_id.strip(), composant.name_id.strip(), denomination_by_cis[cis])
            if key not in seen:
                seen.add(key)
                raw_rows.append((composant, denomination_by_cis[cis]))

    raw_rows.sort(key=lambda value: name_sort_key(value[0].name))
    names_by_identity: dict[tuple[str, str], dict] = {}
    letters: set[str] = set()
    for composant, _ in raw_rows:
        name_id = composant.name_id.strip()
        subs_id = composant.substance_id.strip()
        name = clean_substance_name(composant.name)
        if (subs_id, name_id) not in names_by_identity:
            names_by_identity[(subs_id, name_id)] = {
                "SubsId": subs_id,
                "NomId": name_id,
                "NomLib": name,
                "type": name_types_by_nom_id.get(name_id),
                "title": titles_by_subs_id.get(subs_id),
                "specialites": 0,
            }

    rows = []
    for row in names_by_identity.values():
        row["specialites"] = len(group_names_by_subs_id[row["SubsId"]])
        rows.append(row)
        if row["specialites"]:
            letters.add(normalize_letter(row["NomLib"][:1]))
    replace_rows(conn, "resume_substances", rows)
    replace_letters(conn, "substances", letters)
    return len(rows)
