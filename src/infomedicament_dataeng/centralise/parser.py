"""Parse EMA QRD-template PDFs into render-ready semantic HTML.

PDF extraction is necessarily different from the legacy ANSM HTML path. This
module constructs semantic headings, paragraphs, lists, tables, and figures
directly, then shares only the sanitization, glossary annotation, and block-ID
pass with the HTML parser.
"""

import hashlib
import html
import re
from datetime import date
from typing import Iterable

import fitz

from ..config import get_config
from ..parsing.anchors import NOTICE_NUMBER_TO_ANCHOR, RCP_NUMBER_TO_ANCHOR
from ..parsing.semantic_parser import finalize_semantic_html
from .extract import Image, Table, TextLine, TextRun, extract_elements

# Heading patterns (applied only to bold lines). L2 is tried before L1.
_RCP_L2 = re.compile(r"^(\d{1,2}\.\d{1,2})\.?\s+(.+)$")
_RCP_L1 = re.compile(r"^(\d{1,2})\.\s+(.+)$")
_NOTICE_L1 = re.compile(r"^(\d)\.\s+(.+)$")
_BULLET = re.compile(r"^[-–•]\s+(.+)$")

# A revised/updated date, e.g. "09 septembre 2014" or "07/2020".
_MONTHS = "janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre"
_DATE_RE = re.compile(
    rf"\b\d{{1,2}}/\d{{1,2}}/\d{{4}}\b|\b\d{{1,2}}\s+(?:{_MONTHS})\s+\d{{4}}\b|\b\d{{2}}/\d{{4}}\b",
    re.I,
)
_MONTH_NUMBERS = {
    "janvier": 1,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
}

# Vertical gap (points) below which a line continues the current paragraph.
_PARA_GAP = 6.0

# Cap on the product-name lines collected under a notice header (before the INN).
_MAX_DENOM_LINES = 12


def _line_runs(line: TextLine) -> list[TextRun]:
    if line.runs is not None:
        return [TextRun(run.text, run.bold, run.underline) for run in line.runs]
    return [TextRun(line.text, bold=line.bold)]


def _merge_runs(runs: Iterable[TextRun]) -> list[TextRun]:
    merged: list[TextRun] = []
    for run in runs:
        if not run.text:
            continue
        if merged and (merged[-1].bold, merged[-1].underline) == (run.bold, run.underline):
            merged[-1].text += run.text
        else:
            merged.append(TextRun(run.text, run.bold, run.underline))
    return merged


def _join_runs(left: list[TextRun], right: list[TextRun]) -> list[TextRun]:
    already_separated = not left or not right or left[-1].text.endswith(" ") or right[0].text.startswith(" ")
    separator = [] if already_separated else [TextRun(" ")]
    return _merge_runs([*left, *separator, *right])


def _slice_runs(runs: list[TextRun], start: int) -> list[TextRun]:
    """Drop ``start`` plain-text characters while retaining run formatting."""
    sliced: list[TextRun] = []
    offset = 0
    for run in runs:
        end = offset + len(run.text)
        if end > start:
            sliced.append(TextRun(run.text[max(0, start - offset) :], run.bold, run.underline))
        offset = end
    return _merge_runs(sliced)


def _runs_html(runs: list[TextRun]) -> str:
    parts: list[str] = []
    for run in runs:
        content = html.escape(run.text)
        if run.underline:
            content = f"<u>{content}</u>"
        if run.bold:
            content = f"<strong>{content}</strong>"
        parts.append(content)
    return "".join(parts)


def _is_notice_header(text: str) -> bool:
    """Match the QRD notice header across wordings:
    'Notice: Information de l'utilisateur' / 'Notice : Information du patient'."""
    return text.startswith("Notice") and "information" in text.lower()


class _SemanticHtmlBuilder:
    """Accumulate ordered PDF elements into an HTML fragment."""

    def __init__(self, kind: str, cdn_base_url: str, image_prefix: str, images: dict[str, bytes]):
        self.kind = kind  # "rcp" | "notice"
        self.cdn_base_url = cdn_base_url  # e.g. ".../exports/images"
        self.image_prefix = image_prefix  # S3 key prefix, e.g. "exports/images/"
        self.images = images  # shared sha-keyed blob map, uploaded to the CDN by the caller
        self.blocks: list[str] = []
        self._para: list[list[TextRun]] = []
        self._bullets: list[list[TextRun]] = []
        self._prev_y1: float | None = None
        self._prev_page: int | None = None
        self.date_notif = ""
        self._await_date = False  # notice: next line after the "révisée" marker
        self._in_indication = False

    def _flush_para(self) -> None:
        runs: list[TextRun] = []
        for line_runs in self._para:
            runs = _join_runs(runs, line_runs)
        if runs:
            role = ' data-document-role="indication"' if self._in_indication else ""
            self.blocks.append(f"<p{role}>{_runs_html(runs)}</p>")
        self._para = []

    def _flush_bullets(self) -> None:
        if self._bullets:
            items = "".join(f"<li>{_runs_html(item)}</li>" for item in self._bullets)
            self.blocks.append(f"<ul>{items}</ul>")
        self._bullets = []

    def _flush(self) -> None:
        self._flush_para()
        self._flush_bullets()

    # -- heading handling --------------------------------------------------
    def _append_heading(self, tag_name: str, content: str, number: str) -> None:
        self._flush()
        anchor = self._anchor(number)
        identifier = f' id="{html.escape(anchor, quote=True)}"' if anchor else ""
        self.blocks.append(f"<{tag_name}{identifier}>{html.escape(content)}</{tag_name}>")
        self._in_indication = self.kind == "notice" and number == "1"

    def _anchor(self, number: str) -> str | None:
        table = RCP_NUMBER_TO_ANCHOR if self.kind == "rcp" else NOTICE_NUMBER_TO_ANCHOR
        return table.get(number)

    def _try_heading(self, text: str) -> bool:
        if self.kind == "notice" and _is_notice_header(text):
            self._append_heading("h2", text, "")
            return True
        if self.kind == "rcp":
            m2 = _RCP_L2.match(text)
            if m2 and m2.group(1) in RCP_NUMBER_TO_ANCHOR:
                self._append_heading("h3", text, m2.group(1))
                return True
            m1 = _RCP_L1.match(text)
            if m1 and m1.group(1) in RCP_NUMBER_TO_ANCHOR:
                self._append_heading("h2", text, m1.group(1))
                return True
        else:
            m = _NOTICE_L1.match(text)
            if m and m.group(1) in NOTICE_NUMBER_TO_ANCHOR:
                self._append_heading("h3", text, m.group(1))
                return True
        return False

    # -- element dispatch --------------------------------------------------
    def add(self, el) -> None:
        if isinstance(el, Table):
            self._flush()
            self.blocks.append(el.html)
            self._prev_y1, self._prev_page = el.y0, el.page
            return

        if isinstance(el, Image):
            self._flush()
            self.blocks.append(self._image_html(el))
            self._prev_y1, self._prev_page = el.y0, el.page
            return

        assert isinstance(el, TextLine)
        text = el.text
        gap = el.y0 - self._prev_y1 if (self._prev_page == el.page and self._prev_y1 is not None) else 1e9
        self._prev_y1, self._prev_page = el.y1, el.page

        if self._await_date:
            self._await_date = False
            m = _DATE_RE.search(text)
            if m:
                self.date_notif = m.group(0)
                return
        if text.startswith("La dernière date") and "révisée" in text:
            self._flush()
            self._await_date = True
            return

        if el.bold and self._try_heading(text):
            return

        mb = _BULLET.match(text)
        if mb:
            self._flush_para()
            self._bullets.append(_slice_runs(_line_runs(el), mb.start(1)))
            return

        # Plain body line: continue a bullet, continue a paragraph, or start one.
        if self._bullets and not self._para and gap < _PARA_GAP:
            self._bullets[-1] = _join_runs(self._bullets[-1], _line_runs(el))
        elif self._para and gap < _PARA_GAP:
            self._para.append(_line_runs(el))
        else:
            self._flush()
            self._para = [_line_runs(el)]

    def finish(self) -> str:
        self._flush()
        # RCP §10 body may carry the revision date inline.
        if self.kind == "rcp" and not self.date_notif:
            self._scan_section10_date()
        return "".join(self.blocks)

    def _scan_section10_date(self) -> None:
        soup_text = re.sub(r"<[^>]+>", " ", "".join(self.blocks))
        section10 = re.search(r"(?:^|\s)10\.\s.*", html.unescape(soup_text), re.S)
        match = _DATE_RE.search(section10.group(0)) if section10 else None
        if match:
            self.date_notif = match.group(0)

    def _image_html(self, el: Image) -> str:
        """Register the image blob and return its semantic HTML element."""
        sha = hashlib.sha256(el.data).hexdigest()
        ext = el.ext or "png"
        self.images[f"{self.image_prefix}centralise/{sha}.{ext}"] = el.data
        url = f"{self.cdn_base_url}/centralise/{sha}.{ext}"
        return f'<figure><img src="{html.escape(url, quote=True)}" alt=""/></figure>'


def _rcp_denomination(elements: list) -> str:
    """RCP denomination = the body text under section 1 (used to match a CIS).

    A single SmPC can cover several devices (e.g. KwikPen + Tempo Pen), so all
    of section 1's body lines are joined, not just the first.
    """
    parts: list[str] = []
    in_section_one = False
    for element in elements:
        if isinstance(element, TextLine) and element.bold:
            heading = _RCP_L1.match(element.text)
            if heading:
                if heading.group(1) == "1":
                    in_section_one = True
                    continue
                if in_section_one:
                    break
        if in_section_one and isinstance(element, TextLine):
            parts.append(element.text)
    return " ".join(parts).strip()


def _notice_denomination(elements: list) -> str:
    """Notice denomination = the product lines after the 'Notice: …' header.

    One notice can cover several strengths, each on its own line (Exelon 1,5 / 3,0 /
    4,5 / 6,0 mg), so all of them are joined — as ``_rcp_denomination`` does for
    section 1. The block ends at the INN/boilerplate that follows the names.
    """
    for i, el in enumerate(elements):
        if not (isinstance(el, TextLine) and _is_notice_header(el.text)):
            continue
        names: list[str] = []
        for nxt in elements[i + 1 :]:
            if not isinstance(nxt, TextLine):
                continue
            text = nxt.text.strip()
            if not text:
                continue
            if text.lower().startswith("veuillez lire") or _NOTICE_L1.match(text):
                break
            names.append(text)
            if len(names) >= _MAX_DENOM_LINES:
                break
        return " ".join(names)
    return ""


def _find_annexe1_range(doc: fitz.Document) -> range | None:
    """Page range of ANNEXE I (the RCP annexe), up to ANNEXE II."""
    start = end = None
    for pno in range(doc.page_count):
        for text, bold in _bold_lines(doc[pno]):
            if bold and text == "ANNEXE I" and start is None:
                start = pno
            elif bold and text == "ANNEXE II" and end is None:
                end = pno
    if start is None:
        return None
    return range(start, end if end is not None else doc.page_count)


def _find_notice_ranges(doc: fitz.Document) -> list[range]:
    """Page range of each ANNEXE III.B notice (one per presentation).

    A notice runs from its 'Notice: … information' header to just before the next
    such header (or the end of the document for the last one). This deliberately
    includes the illustrated 'Manuel d'utilisation' (instructions-for-use) that
    follows the notice text — its injection diagrams are part of the leaflet.
    The revision date inside the notice is still captured by the HTML builder.
    """
    b_notice = None
    headers: list[int] = []
    for pno in range(doc.page_count):
        for text, bold in _bold_lines(doc[pno]):
            if not bold:
                continue
            if text.startswith("B. NOTICE") and b_notice is None:
                b_notice = pno
            if b_notice is not None and _is_notice_header(text):
                headers.append(pno)

    ranges: list[range] = []
    for i, header in enumerate(headers):
        nxt = headers[i + 1] if i + 1 < len(headers) else doc.page_count
        ranges.append(range(header, nxt))
    return ranges


def _bold_lines(page: fitz.Page):
    """Yield (text, is_bold) for each non-empty line on the page."""
    for block in page.get_text("dict")["blocks"]:
        if "lines" not in block:
            continue
        for line in block["lines"]:
            spans = line["spans"]
            text = "".join(s["text"] for s in spans).strip()
            if text:
                yield text, bool(spans[0]["flags"] & 16)


def _split_smpcs(elements: list) -> list[list]:
    """Split an ANNEXE I element stream into one group per SmPC (at each §1)."""
    groups: list[list] = []
    current: list | None = None
    for el in elements:
        if isinstance(el, TextLine) and el.bold:
            m = _RCP_L1.match(el.text)
            if m and m.group(1) == "1":  # a new "1. DÉNOMINATION" starts a SmPC
                current = []
                groups.append(current)
        if current is not None:
            current.append(el)
    return groups


def _build_body(
    elements: list,
    kind: str,
    images: dict,
    cdn_base_url: str,
    image_prefix: str,
    glossary_terms: Iterable[str],
) -> tuple[str, str | None, str | None]:
    """Build and normalize one presentation; return HTML, ISO date, indication."""
    builder = _SemanticHtmlBuilder(kind, cdn_base_url, image_prefix, images)
    for el in elements:
        builder.add(el)
    document = finalize_semantic_html(
        builder.finish(),
        image_base_url=cdn_base_url,
        glossary_terms=glossary_terms,
    )
    return document.content_html, _normalise_date(builder.date_notif), document.indication


def _normalise_date(value: str) -> str | None:
    """Return a complete PDF revision date as ISO, without inventing a day."""
    value = " ".join(value.split()).casefold()
    if not value:
        return None
    numeric = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", value)
    if numeric:
        day, month, year = map(int, numeric.groups())
    else:
        textual = re.fullmatch(r"(\d{1,2})\s+([a-zéûô]+)\s+(\d{4})", value)
        if not textual or textual.group(2) not in _MONTH_NUMBERS:
            return None
        day = int(textual.group(1))
        month = _MONTH_NUMBERS[textual.group(2)]
        year = int(textual.group(3))
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def parse_pdf(pdf_bytes: bytes, *, glossary_terms: Iterable[str] = ()) -> dict:
    """Parse an EMA PI PDF into all its RCP and Notice presentations.

    Returns ``{"rcp": [doc, …], "notice": [doc, …], "images": {s3_key: bytes}}``
    where each document has ``denomination``, ISO ``date_notif``, plain-text
    ``indication``, and sanitized ``content_html``. One PDF bundles one
    presentation per device (cartouche, pen, …); the caller matches each CIS to
    its presentation (see ``match.py``) and uploads ``images`` to the CDN (they
    are content-addressed, so uploads are idempotent).
    """
    cfg = get_config()
    cdn_base_url, image_prefix = cfg.cdn_base_url, cfg.s3.image_prefix

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images: dict[str, bytes] = {}
    result: dict = {"rcp": [], "notice": [], "images": images}

    annexe1 = _find_annexe1_range(doc)
    if annexe1 is not None:
        for group in _split_smpcs(extract_elements(doc, annexe1)):
            content_html, date_notif, indication = _build_body(
                group, "rcp", images, cdn_base_url, image_prefix, glossary_terms
            )
            if content_html:
                result["rcp"].append(
                    {
                        "denomination": _rcp_denomination(group),
                        "date_notif": date_notif,
                        "indication": indication,
                        "content_html": content_html,
                    }
                )

    for rng in _find_notice_ranges(doc):
        elements = extract_elements(doc, rng)
        content_html, date_notif, indication = _build_body(
            elements, "notice", images, cdn_base_url, image_prefix, glossary_terms
        )
        if content_html:
            result["notice"].append(
                {
                    "denomination": _notice_denomination(elements),
                    "date_notif": date_notif,
                    "indication": indication,
                    "content_html": content_html,
                }
            )

    return result
