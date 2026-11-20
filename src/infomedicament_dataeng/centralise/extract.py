"""Extract ordered text lines and tables from an EMA QRD-template PDF.

PyMuPDF gives us per-page *visual* lines (spans with font metadata) and native
tables. This module turns a page range into a flat, reading-order stream of
``TextLine`` / ``Table`` elements that the semantic parser consumes.

Two PDF quirks are handled here so the segmenter can stay simple:
- QRD headings split the number and the title into two same-baseline "lines"
  (e.g. ``"1."`` then ``"DÉNOMINATION…"``); we merge lines at the same y.
- Table text is also returned as ordinary lines, so we drop any line inside a
  table's bounding box and emit a single ``Table`` element in its place.
"""

import re
from dataclasses import dataclass, field

import pymupdf as fitz

# Font size of the running page-number footer/header noise; anything smaller
# than a body line (11pt) is discarded.
_MIN_BODY_SIZE = 9.0
# Two "lines" whose top y differ by less than this are the same visual line.
_SAME_LINE_TOL = 3.0
_BOLD_FLAG = 16


@dataclass
class TextRun:
    """A contiguous piece of text sharing the same inline formatting."""

    text: str
    bold: bool = False
    underline: bool = False


@dataclass
class TextLine:
    text: str
    size: float
    bold: bool
    page: int
    y0: float
    y1: float
    runs: list[TextRun] | None = None
    x0: float = 0
    x1: float = 0


@dataclass
class Table:
    rows: list[list["TableCell"]]
    page: int
    y0: float


@dataclass
class TableCell:
    lines: list[TextLine]
    images: list["Image"] = field(default_factory=list)
    rowspan: int = 1
    colspan: int = 1
    header: bool = False


@dataclass
class Image:
    data: bytes  # displayed figure crop (uploaded to the CDN by the caller)
    ext: str  # "png" / "jpeg" / …
    page: int
    y0: float
    bbox: tuple[float, float, float, float] | None = None
    alt: str = ""


# Ignore tiny embedded bitmaps (icons, separators, soft masks); real QRD figures
# are far larger (the smallest injection-diagram in our fixtures is ~108px).
_MIN_IMAGE_PX = 32


def _merge_same_line(lines: list[TextLine]) -> list[TextLine]:
    """Merge consecutive lines sharing a baseline (number+title, bullet+text)."""
    merged: list[TextLine] = []
    for ln in lines:
        if merged and ln.page == merged[-1].page and abs(ln.y0 - merged[-1].y0) < _SAME_LINE_TOL:
            prev = merged[-1]
            combined_runs = (
                _merge_runs([*_line_runs(prev), TextRun(" "), *_line_runs(ln)])
                if prev.runs is not None or ln.runs is not None
                else None
            )
            # The longer fragment decides boldness (the title, not the "1." / "-").
            bold = ln.bold if len(ln.text.strip()) > len(prev.text.strip()) else prev.bold
            prev.text = f"{prev.text} {ln.text}".strip()
            prev.bold = bold
            prev.y1 = max(prev.y1, ln.y1)
            prev.x0 = min(prev.x0, ln.x0)
            prev.x1 = max(prev.x1, ln.x1)
            prev.runs = combined_runs
        else:
            merged.append(ln)
    return merged


def _line_runs(line: TextLine) -> list[TextRun]:
    if line.runs is not None:
        return line.runs
    return [TextRun(line.text, bold=line.bold)]


def _merge_runs(runs: list[TextRun]) -> list[TextRun]:
    merged: list[TextRun] = []
    for run in runs:
        if not run.text:
            continue
        if merged and (merged[-1].bold, merged[-1].underline) == (run.bold, run.underline):
            merged[-1].text += run.text
        else:
            merged.append(TextRun(run.text, run.bold, run.underline))
    return merged


def _underline_rects(page: fitz.Page) -> list[fitz.Rect]:
    """Return thin horizontal drawings that can represent PDF underlines."""
    return [
        fitz.Rect(drawing["rect"])
        for drawing in page.get_drawings()
        if drawing["rect"].width > 1 and drawing["rect"].height <= 1.5
    ]


def _char_is_underlined(char_bbox, underline_rects: list[fitz.Rect]) -> bool:
    char = fitz.Rect(char_bbox)
    if char.width <= 0:
        return False
    for underline in underline_rects:
        # Word-generated PDFs place the rule around 1.2pt above the bottom of
        # PyMuPDF's character box. Requiring horizontal character overlap keeps
        # nearby table borders and separators from becoming false positives.
        if not -2.5 <= underline.y0 - char.y1 <= 1.5:
            continue
        overlap = max(0.0, min(char.x1, underline.x1) - max(char.x0, underline.x0))
        if overlap >= char.width * 0.45:
            return True
    return False


def _extract_runs(spans: list[dict], underline_rects: list[fitz.Rect]) -> list[TextRun]:
    runs: list[TextRun] = []
    for span in spans:
        bold = bool(span["flags"] & _BOLD_FLAG)
        for char in span["chars"]:
            value = char["c"]
            underline = bool(value.strip()) and _char_is_underlined(char["bbox"], underline_rects)
            if runs and (runs[-1].bold, runs[-1].underline) == (bold, underline):
                runs[-1].text += value
            else:
                runs.append(TextRun(value, bold, underline))

    while runs:
        runs[0].text = runs[0].text.lstrip()
        if runs[0].text:
            break
        runs.pop(0)
    while runs:
        runs[-1].text = runs[-1].text.rstrip()
        if runs[-1].text:
            break
        runs.pop()
    return _merge_runs(runs)


def extract_elements(doc: fitz.Document, page_range: range) -> list:
    """Return an ordered ``TextLine``/``Table`` stream for the given pages."""
    elements: list = []
    for pno in page_range:
        page = doc[pno]
        drawings = page.get_drawings()
        blocks = page.get_text("rawdict", flags=fitz.TEXTFLAGS_RAWDICT & ~fitz.TEXT_PRESERVE_IMAGES)["blocks"]
        figures = _page_images(page, pno, blocks)
        tables = _find_tables(page, drawings, figures)
        table_rects = [fitz.Rect(t.bbox) for t in tables]
        underline_rects = _underline_rects(page)

        page_lines: list[TextLine] = []
        for block in blocks:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                spans = line["spans"]
                runs = _extract_runs(spans, underline_rects)
                text = "".join(run.text for run in runs)
                if not text:
                    continue
                span = max(spans, key=lambda s: len(s["chars"]))
                if span["size"] < _MIN_BODY_SIZE:
                    continue
                x0, y0, x1, y1 = line["bbox"]
                center = fitz.Point((x0 + x1) / 2, (y0 + y1) / 2)
                if any(center in r for r in table_rects) or any(center in fitz.Rect(i.bbox) for i in figures):
                    continue
                page_lines.append(
                    TextLine(
                        text=text,
                        size=span["size"],
                        bold=bool(span["flags"] & _BOLD_FLAG),
                        page=pno,
                        y0=y0,
                        y1=y1,
                        runs=runs,
                    )
                )

        page_elems: list = _merge_same_line(page_lines)
        for t, rect in zip(tables, table_rects):
            owned = [image for image in figures if rect.contains(fitz.Rect(image.bbox))]
            page_elems.append(_structured_table(page, t, owned, pno, blocks, underline_rects))

        page_elems.extend(image for image in figures if not any(r.contains(fitz.Rect(image.bbox)) for r in table_rects))

        page_elems.sort(key=lambda e: e.y0)
        elements.extend(page_elems)
    return elements


def _page_images(page: fitz.Page, pno: int, blocks: list) -> list:
    """Render displayed image regions, retaining masks and translated overlays.

    Raw embedded bytes can contain a different picture from the visible PDF:
    Afqlir, for example, covers English labels with French text and white boxes.
    """
    regions: list[fitz.Rect] = []
    labels = [
        (fitz.Rect(line["bbox"]), abs(line["dir"][0]) < 0.1)
        for block in blocks
        for line in block.get("lines", [])
        if any(char["c"].strip() for span in line["spans"] for char in span["chars"])
    ]
    for info in page.get_image_info():
        if info["width"] < _MIN_IMAGE_PX or info["height"] < _MIN_IMAGE_PX:
            continue
        rect = fitz.Rect(info["bbox"]) & page.rect
        if rect.is_empty:
            continue
        # Axes, legends and translated labels can be separate overlapping PDF
        # bitmaps. Render their union once, rather than exporting fragments.
        while True:
            previous = tuple(rect)
            for label, rotated in labels:
                axis_label = (
                    rotated
                    and min(label.y1, rect.y1) - max(label.y0, rect.y0) > label.height * 0.5
                    and label.x1 >= rect.x0 - 24
                    and label.x0 <= rect.x1 + 24
                )
                if (label.tl + label.br) / 2 in rect or axis_label:
                    rect |= label
            rect &= page.rect
            overlaps = [r for r in regions if (r & rect).get_area() > min(r.get_area(), rect.get_area()) * 0.1]
            if not overlaps and tuple(rect) == previous:
                break
            for region in overlaps:
                regions.remove(region)
                rect |= region
        regions.append(rect)
    return [_render_image(page, rect, pno, blocks) for rect in regions]


def _render_image(page, rect, pno, blocks):
    pixmap = page.get_pixmap(clip=rect, dpi=144, alpha=False, annots=False)
    labels = [
        "".join(char["c"] for span in line["spans"] for char in span["chars"]).strip()
        for block in blocks
        for line in block.get("lines", [])
        if (fitz.Rect(line["bbox"]).tl + fitz.Rect(line["bbox"]).br) / 2 in rect
    ]
    return Image(
        data=pixmap.tobytes("png"),
        ext="png",
        page=pno,
        y0=rect.y0,
        bbox=tuple(rect),
        alt=" ".join(" ".join(labels).split()),
    )


def _rules(drawings: list[dict]) -> tuple[list[dict], list[tuple], list[tuple]]:
    """Ignore filled text backgrounds; retain strokes and narrow border fills."""
    paths, horizontal, vertical = [], [], []
    for drawing in drawings:
        segments = []
        for item in drawing["items"]:
            if item[0] == "l":
                a, b = item[1:3]
                segments.append((a.x, a.y, b.x, b.y))
            elif item[0] == "re":
                r = item[1]
                if "s" in drawing["type"] or min(r.width, r.height) <= 2:
                    if r.height <= 2:
                        segments.append((r.x0, (r.y0 + r.y1) / 2, r.x1, (r.y0 + r.y1) / 2))
                    elif r.width <= 2:
                        segments.append(((r.x0 + r.x1) / 2, r.y0, (r.x0 + r.x1) / 2, r.y1))
                    else:
                        segments.extend(
                            [
                                (r.x0, r.y0, r.x1, r.y0),
                                (r.x0, r.y1, r.x1, r.y1),
                                (r.x0, r.y0, r.x0, r.y1),
                                (r.x1, r.y0, r.x1, r.y1),
                            ]
                        )
        if segments:
            paths.append(drawing)
        for x0, y0, x1, y1 in segments:
            if abs(y1 - y0) < 1 and abs(x1 - x0) > 10:
                horizontal.append((min(x0, x1), max(x0, x1), (y0 + y1) / 2))
            elif abs(x1 - x0) < 1 and abs(y1 - y0) > 2:
                vertical.append(((x0 + x1) / 2, min(y0, y1), max(y0, y1)))
    return paths, horizontal, vertical


def _cluster(values, tolerance=3):
    groups = []
    for value in sorted(values):
        if groups and value - groups[-1][-1] <= tolerance:
            groups[-1].append(value)
        else:
            groups.append([value])
    return [sum(group) / len(group) for group in groups]


def _find_tables(page, drawings, figures):
    """Recover ruled tables without treating label backgrounds as cell borders.

    Word's partially ruled statistical tables have column joints in horizontal
    rules even when interior vertical rules are absent. Repeated joints supply
    virtual columns, without product names or page-specific coordinates.
    """
    paths, horizontal, vertical = _rules(drawings)
    candidates = page.find_tables(paths=drawings, use_layout=False).tables
    result = []
    for candidate in candidates:
        rect = fitz.Rect(candidate.bbox)
        if any(fitz.Rect(image.bbox).contains(rect) for image in figures):
            continue
        sides = [
            sum(max(0, min(y1, rect.y1) - max(y0, rect.y0)) for x, y0, y1 in vertical if abs(x - edge) < 3)
            for edge in (rect.x0, rect.x1)
        ]
        if min(sides) < rect.height * 0.5:
            continue  # footnotes and diagram labels have no enclosing rules
        internal = [
            x
            for x in _cluster(x for x, _, _ in vertical if rect.x0 + 3 < x < rect.x1 - 3)
            if sum(max(0, min(y1, rect.y1) - max(y0, rect.y0)) for edge, y0, y1 in vertical if abs(edge - x) < 3)
            > rect.height * 0.3
        ]
        extra = []
        if not internal:
            edges = [
                (x, y)
                for x0, x1, y in horizontal
                if rect.y0 - 3 <= y <= rect.y1 + 3
                for x in (x0, x1)
                if rect.x0 + 3 < x < rect.x1 - 3
            ]
            for x in _cluster(x for x, _ in edges):
                levels = _cluster(y for edge, y in edges if abs(edge - x) < 3)
                if len(levels) >= 2:
                    extra.append(((x, rect.y0), (x, rect.y1)))
        finder = page.find_tables(paths=paths, clip=rect + (-1, -1, 1, 1), add_lines=extra, use_layout=False)
        result.extend(finder.tables)
    return result


def _cell_lines(blocks, rect, figures, pno, underline_rects):
    """Retain visual line breaks and inline emphasis within a cell."""
    lines = []
    underline_rects = [rule for rule in underline_rects if abs(rule.y0 - rect.y0) > 3 and abs(rule.y0 - rect.y1) > 3]
    for block in blocks:
        for line in block.get("lines", []):
            bounds = fitz.Rect(line["bbox"])
            center = (bounds.tl + bounds.br) / 2
            if center not in rect:
                continue
            if any(center in fitz.Rect(image.bbox) for image in figures):
                continue
            runs = _extract_runs(line["spans"], underline_rects)
            if runs:
                span = max(line["spans"], key=lambda span: len(span["chars"]))
                element = TextLine(
                    "".join(run.text for run in runs),
                    span["size"],
                    bool(span["flags"] & _BOLD_FLAG),
                    pno,
                    bounds.y0,
                    bounds.y1,
                    runs,
                    bounds.x0,
                    bounds.x1,
                )
                lines.append((bounds.y0, bounds.x0, element))
    lines.sort(key=lambda line: (line[0], line[1]))
    return _merge_same_line([line for _, _, line in lines])


def _structured_table(page, table, figures, pno, blocks, underline_rects):
    boxes = [fitz.Rect(cell) for row in table.rows for cell in row.cells if cell is not None]
    xs = _cluster(x for box in boxes for x in (box.x0, box.x1))
    ys = _cluster(y for box in boxes for y in (box.y0, box.y1))
    rows = [[] for _ in range(len(ys) - 1)]
    seen = set()
    for box in boxes:
        key = tuple(box)
        if key in seen:
            continue
        seen.add(key)
        col, end_col = [min(range(len(xs)), key=lambda i: abs(xs[i] - x)) for x in (box.x0, box.x1)]
        row, end_row = [min(range(len(ys)), key=lambda i: abs(ys[i] - y)) for y in (box.y0, box.y1)]
        owned = [image for image in figures if box.contains((fitz.Rect(image.bbox).tl + fitz.Rect(image.bbox).br) / 2)]
        lines = _cell_lines(blocks, box, owned, pno, underline_rects)
        # An image's transparent/white margin can extend over a table rule.
        # Keep that rule in the table rather than copying it into the picture.
        owned = [
            _render_image(page, fitz.Rect(image.bbox) & (box + (0.75, 0.75, -0.75, -0.75)), pno, blocks)
            for image in owned
        ]
        cell = TableCell(lines, owned, max(1, end_row - row), max(1, end_col - col))
        cell.header = row == 0 and bool(lines) and all(line.bold for line in lines)
        rows[row].append((col, cell))
    for index, row in enumerate(rows):
        cells = [cell for _, cell in sorted(row, key=lambda entry: entry[0])]
        if len(cells) < 2 or any(cell.images or cell.rowspan > 1 for cell in cells):
            continue
        if not cells[0].lines or any(cell.lines for cell in cells[1:]):
            continue
        whole = _cell_lines(blocks, fitz.Rect(xs[0], ys[index], xs[-1], ys[index + 1]), [], pno, underline_rects)
        if whole and all(line.bold for line in whole):
            rows[index] = [(0, TableCell(whole, colspan=len(xs) - 1))]
    return Table(
        rows=[
            subrow
            for row in rows
            for subrow in _measurement_rows([cell for _, cell in sorted(row, key=lambda entry: entry[0])], xs)
        ],
        page=pno,
        y0=table.bbox[1],
    )


_VALUE = re.compile(r"^(?:[\d(<≥≤>±−–-]|n\s*=)")


def _measurement_rows(cells, xs):
    """Split unruled statistical rows at aligned values, retaining wrapped labels.

    A paragraph of measurements inside a single PDF cell will lose its vertical
    alignment when HTML wraps. Shared value baselines identify logical rows;
    label-only continuation lines remain with the preceding measurement.
    """
    if len(cells) < 2 or any(cell.rowspan > 1 or cell.colspan > 1 or cell.images for cell in cells):
        return [cells]
    values = [line for cell in cells[1:] for line in cell.lines]
    numeric = [line for line in values if _VALUE.match(line.text)]
    if len(numeric) < 4 or len(numeric) < len(values) * 0.6 or len(cells[0].lines) < 3:
        return [cells]
    anchors = []
    for y in _cluster(line.y0 for line in numeric):
        aligned = sum(any(abs(line.y0 - y) < 3 for line in cell.lines) for cell in cells)
        if aligned >= 2:
            anchors.append(y)
    if len(anchors) < 3:
        return [cells]
    first_label = min(line.y0 for line in cells[0].lines)
    if first_label < anchors[0] - 3:
        anchors.insert(0, first_label)  # a heading precedes the first measurement
    rows = [[TableCell([]) for _ in cells] for _ in anchors]
    for col, cell in enumerate(cells):
        for line in cell.lines:
            index = max((i for i, y in enumerate(anchors) if y <= line.y0 + 3), default=0)
            rows[index][col].lines.append(line)
    for row in rows:
        # Shared estimates (hazard ratios, p-values) are centered across arms.
        # Keep the native line whole and merge the blank neighbor it overlaps.
        for col in range(len(row) - 1, -1, -1):
            cell = row[col]
            while cell.lines and col + 1 < len(row) and not row[col + 1].lines:
                edge = xs[col + cell.colspan]
                if not any(line.x0 < edge - 3 and line.x1 > edge + 3 for line in cell.lines):
                    break
                cell.colspan += row.pop(col + 1).colspan
    return rows
