"""Build grouped medicine summaries used by browse and search pages."""

from collections import defaultdict

from .common import (
    group_name,
    indications_for_cis,
    load_atc_by_cis,
    load_indications,
    load_specialties,
    load_surveillance_cis,
    normalize_letter,
    replace_letters,
    replace_rows,
)
from .composants import display_composants, load_composants_by_cis, simple_composants


def build(conn) -> int:
    specialties = load_specialties(conn)
    composants_by_cis = load_composants_by_cis(conn)
    indications = load_indications(conn)
    atc_by_cis = load_atc_by_cis(conn)
    surveillance_cis = load_surveillance_cis(conn)

    groups = defaultdict(list)
    for specialty in specialties:
        groups[group_name(specialty.denomination)].append(specialty)

    rows = []
    letters: set[str] = set()
    for name, group_specialties in groups.items():
        representative = group_specialties[0]
        composants = simple_composants(composants_by_cis.get(representative.cis, []))
        cis_values = [specialty.cis.strip() for specialty in group_specialties]
        indication_values = indications_for_cis(indications, cis_values)
        atc = atc_by_cis.get(representative.cis)
        letters.add(normalize_letter(name[:1]))
        specialty_entries = []
        for specialty in group_specialties:
            selected = simple_composants(composants_by_cis.get(specialty.cis, []))
            specialty_entries.append(
                [
                    specialty.cis.strip(),
                    specialty.denomination,
                    str(specialty.status),
                    specialty.procedure,
                    "true" if specialty.cis in surveillance_cis else "false",
                    display_composants(selected),
                    ",".join(composant.substance_id.strip() for composant in selected),
                    ",".join(composant.name_id.strip() for composant in selected),
                ]
            )
        rows.append(
            {
                "groupName": name,
                "composants": display_composants(composants),
                "indicationsIds": [value[0] for value in indication_values],
                "specialites": specialty_entries,
                "atc1Code": atc[:1] if atc else None,
                "atc2Code": atc[:3] if atc else None,
                "atc5Code": atc,
                "CISList": cis_values,
                "subsIds": [composant.substance_id.strip() for composant in composants],
                "subsNamesIds": [composant.name_id.strip() for composant in composants],
                "indicationsIdsNames": [[str(value[0]), value[1]] for value in indication_values],
            }
        )

    replace_rows(conn, "resume_medicaments", rows)
    replace_letters(conn, "medicaments", letters)
    return len(rows)
