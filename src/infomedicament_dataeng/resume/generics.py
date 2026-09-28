"""Build the generic-group browse summary."""

from sqlalchemy import bindparam, text

from ..db import VISIBLE_SPECIALITE_AVAILABILITIES
from .common import format_name, generic_dci, normalize_letter, replace_letters, replace_rows


def build(conn) -> int:
    rows = conn.execute(
        text(
            "SELECT generic.code_groupe, generic.libelle"
            " FROM ansm_groupe_generique generic"
            " JOIN ansm_specialite_groupe_generique membership"
            " ON membership.code_groupe = generic.code_groupe"
            " JOIN ansm_specialite specialty ON specialty.cis = membership.cis"
            " WHERE specialty.disponibilite IN :availabilities"
            " AND (specialty.procedure IS NULL OR specialty.procedure != 'IMPORTATION_PARALLELE')"
            " ORDER BY generic.libelle, membership.rang"
        ).bindparams(bindparam("availabilities", expanding=True)),
        {"availabilities": VISIBLE_SPECIALITE_AVAILABILITIES},
    ).mappings()
    groups: dict[int, str] = {}
    for row in rows:
        groups.setdefault(row["code_groupe"], row["libelle"] or "")

    letters: set[str] = set()
    resume_rows = []
    for code, label in groups.items():
        name = format_name(generic_dci(label))
        letters.add(normalize_letter(name[:1]))
        resume_rows.append({"SpecId": str(code), "SpecName": name})

    replace_rows(conn, "resume_generiques", resume_rows)
    replace_letters(conn, "generiques", letters)
    return len(resume_rows)
