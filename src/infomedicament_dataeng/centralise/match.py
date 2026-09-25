"""Match a CIS to its presentation within a multi-presentation EMA PDF.

One EMA PDF bundles one SmPC/Notice per device (cartouche, KwikPen, Tempo Pen…).
Several CIS share the PDF, one per presentation. We pick, for each CIS, the
presentation whose denomination best matches the CIS's ``SpecDenom01`` using
Jaccard similarity over normalized tokens. Jaccard naturally prefers the tighter
match — e.g. a generic "…stylo prérempli" CIS maps to the KwikPen presentation
rather than "Tempo Pen …stylo prérempli", whose extra tokens lower the overlap.
"""

import re
import unicodedata

# Number spellings differ between PDBM and the EMA PDFs: "1000" vs "1 000" vs
# "1.000", and "2,5" vs "2.5". Normalize both sides before comparing.
_THOUSANDS_SPACE = re.compile(r"(?<=\d)\s+(?=\d{3}(?!\d))")
_THOUSANDS_DOT = re.compile(r"(?<=\d)\.(?=\d{3}(?!\d))")
_DECIMAL_COMMA = re.compile(r"(?<=\d),(?=\d)")


def _fold(text: str, merge_thousands: bool = False) -> str:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    if merge_thousands:
        folded = _THOUSANDS_SPACE.sub("", folded)
    folded = _THOUSANDS_DOT.sub("", folded)
    return _DECIMAL_COMMA.sub(".", folded)


def _tokens(text: str) -> set[str]:
    # Letters and digits tokenize apart so PDBM's "MIX 50" matches the PDF's "Mix50".
    return set(re.findall(r"[a-z]+|\d+(?:\.\d+)?", _fold(text)))


# A strength: a number followed by a dose unit ("100 unités/mL", "20 mg", "200 UI").
# Volumes (mL) are deliberately excluded — they are pack sizes, not strengths.
_STRENGTH = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:mg|mcg|ug|unites|unite|mmol|ui|g|u|%)(?![a-z0-9])")


def _canon(number: str) -> str:
    """ "3,0" -> "3", "1.10" -> "1.1", so spellings of one strength compare equal."""
    if "." in number:
        number = number.rstrip("0").rstrip(".")
    return number or "0"


def _strengths(text: str) -> set[str]:
    """Dose strengths in ``text``, under both readings of a space-separated number.

    "1 000 UI" is one number (1000), but "Humalog Mix 50 100 UI/mL" is a brand
    number followed by a strength (100) — and nothing in the text tells the two
    apart. So both readings are kept and Jaccard settles it; the alternative is
    dropping insulin mixes entirely. Groups such as the "000" of "1 000" are
    thousands artifacts, never strengths, and are discarded.
    """
    out = {_canon(m.group(1)) for m in _STRENGTH.finditer(_fold(text, merge_thousands=True))}
    for m in _STRENGTH.finditer(_fold(text)):
        head = m.group(1).split(".")[0]
        if not (len(head) > 1 and head.startswith("0")):
            out.add(_canon(m.group(1)))
    return out


_NUMBER = re.compile(r"\d+(?:\.\d+)?")


def _is_artifact(number: str) -> bool:
    """True for the "000" of a split "1 000" — a thousands group, never a real number."""
    head = number.split(".")[0]
    return len(head) > 1 and head.startswith("0")


def _numbers(text: str) -> set[str]:
    folded = _fold(text)
    return {_canon(m.group()) for m in _NUMBER.finditer(folded) if not _is_artifact(m.group())}


# A variant number stands on its own ("Comb 15") or is welded to the brand word
# ("Mix50"). One glued behind a hyphen or a dot is part of a name — "COVID-19",
# "JN.1", "13C-urée" — and never a variant.
_FREE_NUMBER = re.compile(r"(?:(?<=\s)|^)\d+(?:\.\d+)?")
_WELDED_NUMBER = re.compile(r"(?<=[a-z])\d+(?:\.\d+)?")


def _qualifiers(text: str) -> tuple[set[str], set[str]]:
    """Variant numbers — the "25" of "Humalog Mix 25 100 UI/ml" — as (free, welded).

    These name a variant of the product (the mix ratio), so they discriminate just
    as much as the strength: "Insuman Comb 15" and "Insuman Comb 25" are different
    medicines, even both at 40 UI/ml. A free-standing number is solid evidence; one
    welded to a word is weaker, since plenty of names simply contain digits.
    """
    folded = _fold(text)
    dosed = {folded[m.start(1) : m.end(1)] for m in _STRENGTH.finditer(folded)}

    def pick(pattern):
        return {
            _canon(m.group())
            for m in pattern.finditer(folded)
            if m.group() not in dosed and not _is_artifact(m.group())
        }

    return pick(_FREE_NUMBER), pick(_WELDED_NUMBER)


def _by_qualifier(spec_denom: str, docs: list[dict]) -> list[dict] | None:
    """Keep the docs carrying the CIS's variant numbers; None if a free one matches none.

    A free-standing variant number fails closed like the strength: a PDF covering
    Comb 25 and Comb 50 has nothing to say about Comb 15. A welded one only refines
    the shortlist, so a name that merely contains digits can never empty it. The
    candidate side is compared leniently, against every number it mentions, so a
    variant spelled "Mix25" still matches a CIS spelled "MIX 25".
    """
    free, welded = _qualifiers(spec_denom)
    if free:
        docs = [d for d in docs if free <= _numbers(d.get("denomination", ""))]
        if not docs:
            return None
    if welded:
        docs = [d for d in docs if welded <= _numbers(d.get("denomination", ""))] or docs
    return docs


def _by_strength(spec_denom: str, docs: list[dict]) -> list[dict] | None:
    """Keep only the docs whose strength matches the CIS's; None if none do.

    The strength is a hard constraint, not just another token: a 100 unités/mL CIS
    must never get the 200 unités/mL SmPC. Returning None makes the caller skip the
    CIS entirely — an EMA PDF that covers no matching strength (a strength since
    discontinued at EU level, or a wrong EMA product mapping upstream) has nothing to say
    about this presentation, and serving another strength's dosing is worse than
    serving nothing. The check is skipped when no strength is readable on either
    side, so a PDF whose denomination failed to extract is not silently dropped.
    """
    wanted = _strengths(spec_denom)
    if not wanted:
        return docs
    comparable = [d for d in docs if _strengths(d.get("denomination", ""))]
    if not comparable:
        return docs
    return [d for d in comparable if _strengths(d["denomination"]) & wanted] or None


def match_presentation(spec_denom: str, docs: list[dict]) -> dict | None:
    """Return the presentation doc best matching ``spec_denom``, or None.

    ``docs`` are the ``{"denomination": …, …}`` dicts from ``parse_pdf``. Candidates
    are first restricted to the CIS's strength (see ``_by_strength``); a single
    remaining presentation is returned unconditionally (unambiguous). With several
    presentations and no usable name (empty ``spec_denom``), returns None so the
    caller skips the CIS rather than publishing a possibly-wrong presentation.
    Ties keep the earliest doc.
    """
    if not docs:
        return None

    docs = _by_strength(spec_denom, docs)
    if not docs:
        return None
    docs = _by_qualifier(spec_denom, docs)
    if not docs:
        return None
    if len(docs) == 1:
        return docs[0]

    query = _tokens(spec_denom)
    if not query:
        return None  # no signal to disambiguate multiple presentations

    best, best_score = None, 0.0
    for doc in docs:
        candidate = _tokens(doc.get("denomination", ""))
        union = len(query | candidate)
        score = len(query & candidate) / union if union else 0.0
        if score > best_score:
            best, best_score = doc, score
    return best
