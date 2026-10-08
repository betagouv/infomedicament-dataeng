"""Build the indication browse summary."""

from sqlalchemy import text

from .common import load_specialties, normalize_letter, replace_letters, replace_rows


def build(conn) -> int:
    visible_cis = {specialty.cis for specialty in load_specialties(conn)}
    rows = conn.execute(text('SELECT id, nom, "CIS" AS cis FROM indications')).mappings()
    resume_rows = []
    letters: set[str] = set()
    for row in rows:
        name = row["nom"] or ""
        letters.add(normalize_letter(name[:1]))
        count = sum(1 for cis in row["cis"] or [] if str(cis) in visible_cis)
        if count:
            resume_rows.append({"idIndication": row["id"], "nomIndication": name, "specialites": count})

    replace_rows(conn, "resume_indications", resume_rows)
    replace_letters(conn, "indications", letters)
    return len(resume_rows)
