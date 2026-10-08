"""Build one enriched resume row per visible specialty."""

from sqlalchemy import text

from .common import (
    group_name,
    indications_for_cis,
    load_atc_by_cis,
    load_indications,
    load_specialties,
    load_surveillance_cis,
    normalize_numeric_id,
    replace_rows,
)
from .composants import (
    display_composants,
    load_composants_by_cis,
    load_main_names_by_subs_id,
    simple_composants,
)


def _load_alerts(conn) -> tuple[set[str], set[str], dict[str, bool]]:
    pregnancy_plan = {
        normalized
        for value in conn.execute(text("SELECT subs_id FROM ref_grossesse_substances_contre_indiquees")).scalars()
        if value and (normalized := normalize_numeric_id(str(value)))
    }
    pregnancy_mentions = {
        str(value).strip()
        for value in conn.execute(text("SELECT cis FROM ref_grossesse_mention")).scalars()
        if value is not None
    }
    pediatric: dict[str, bool] = {}
    for row in conn.execute(text("SELECT cis, contre_indication FROM ref_pediatrie ORDER BY id")).mappings():
        if row["cis"] is not None:
            pediatric.setdefault(str(row["cis"]).strip(), bool(row["contre_indication"]))
    return pregnancy_plan, pregnancy_mentions, pediatric


def build(conn) -> int:
    specialties = load_specialties(conn)
    composants_by_cis = load_composants_by_cis(conn)
    main_names_by_subs_id = load_main_names_by_subs_id(conn)
    indications = load_indications(conn)
    atc_by_cis = load_atc_by_cis(conn)
    surveillance_cis = load_surveillance_cis(conn)
    pregnancy_plan, pregnancy_mentions, pediatric = _load_alerts(conn)

    rows = []
    for specialty in specialties:
        composants = simple_composants(composants_by_cis.get(specialty.cis, []))
        indication_values = indications_for_cis(indications, [specialty.cis])
        atc = atc_by_cis.get(specialty.cis)
        composant_names = [composant.name.strip() for composant in composants]
        main_names = [
            main_names_by_subs_id.get(composant.substance_id.strip(), composant.name.strip())
            for composant in composants
        ]
        composant_ids = {
            normalized for composant in composants if (normalized := normalize_numeric_id(composant.substance_id))
        }
        rows.append(
            {
                "specId": specialty.cis.strip(),
                "specName": specialty.denomination.strip(),
                "groupName": group_name(specialty.denomination),
                "composants": display_composants(composants),
                "subsIds": [composant.substance_id.strip() for composant in composants],
                "subsMainNames": ", ".join(main_names) if composant_names != main_names else None,
                "indicationsIds": [value[0] for value in indication_values],
                "indicationsIdsNames": [[str(value[0]), value[1]] for value in indication_values],
                "atc1Code": atc[:1] if atc else None,
                "atc2Code": atc[:3] if atc else None,
                "atc5Code": atc,
                "ProcId": specialty.procedure,
                "isSurveillanceRenforcee": specialty.cis in surveillance_cis,
                "StatutBdm": specialty.status,
                "isAlertPregnancyPlan": bool(composant_ids & pregnancy_plan),
                "isAlertPregnancyMention": specialty.cis in pregnancy_mentions,
                "isAlertPediatricContraindication": pediatric.get(specialty.cis, False),
            }
        )

    replace_rows(conn, "resume_specialites", rows)
    return len(rows)
