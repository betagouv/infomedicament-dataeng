"""Convert legacy ANSM notice/RCP documents to render-ready semantic HTML.

Block identifiers are stored in ``data-block-id``. They are deterministic
within one parser run and deliberately sequential. They remain stable for
identical input, but may change when blocks are inserted or removed in a later
notice revision. Source ``id`` attributes are preserved separately.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

ADDRESSABLE_TAGS = {"h2", "h3", "h4", "h5", "h6", "p", "blockquote", "ul", "ol", "li", "table", "figure", "img"}
DEFAULT_IMAGE_BASE_URL = "https://cellar-c2.services.clever-cloud.com/info-medicaments/exports/images/"
ALERT_STYLE = "border:solid windowtext 1.0pt;padding:1.0pt 1.0pt 1.0pt 1.0pt"
DSFR_DIV_CLASSES = {"fr-alert", "fr-table", "fr-table__wrapper", "fr-table__container", "fr-table__content"}
ALLOWED_TAGS = {
    "div",
    "p",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "blockquote",
    "br",
    "strong",
    "em",
    "u",
    "sup",
    "sub",
    "ul",
    "ol",
    "li",
    "table",
    "caption",
    "thead",
    "tbody",
    "tfoot",
    "tr",
    "th",
    "td",
    "img",
    "figure",
    "figcaption",
    "span",
}
DANGEROUS_TAGS = {
    "script",
    "style",
    "link",
    "meta",
    "iframe",
    "object",
    "embed",
    "form",
    "input",
    "button",
    "svg",
    "math",
}
DOCUMENT_ROLES = {
    "holder-name",
    "holder-address",
    "composition",
    "indication",
}


@dataclass(frozen=True)
class SemanticDocument:
    """Metadata and sanitized HTML extracted from one ANSM document."""

    date_notif: date | None
    content_html: str
    indication: str | None = None


def parse_semantic_document(
    source: bytes | str,
    *,
    image_base_url: str = DEFAULT_IMAGE_BASE_URL,
    glossary_terms: Iterable[str] = (),
) -> SemanticDocument:
    """Parse one complete legacy ANSM notice or RCP HTML document."""
    html = _decode_html(source) if isinstance(source, bytes) else source
    soup = BeautifulSoup(html, "lxml")
    root = soup.body or soup
    date_notif = _extract_date(root)
    _remove_metadata_blocks(root)
    _remove_source_noise(root)
    _convert_alerts(root)
    _convert_lists(root)
    indication_blocks = _find_indication_blocks(root)
    indication = _extract_indication(indication_blocks)
    _convert_semantics(root, indication_blocks)
    return _finalize_semantic_root(
        root,
        date_notif=date_notif,
        indication=indication,
        image_base_url=image_base_url,
        glossary_terms=glossary_terms,
    )


def finalize_semantic_html(
    source: str,
    *,
    image_base_url: str = DEFAULT_IMAGE_BASE_URL,
    glossary_terms: Iterable[str] = (),
) -> SemanticDocument:
    """Sanitize and annotate HTML that already uses semantic elements.

    Unlike :func:`parse_semantic_document`, this entry point does not translate
    legacy ANSM classes. It is intended for parsers, such as the centralised PDF
    parser, that construct headings, paragraphs, lists, and emphasis directly.
    """
    soup = BeautifulSoup(source, "lxml")
    root = soup.body or soup
    indication_blocks = _find_indication_blocks(root)
    indication = _extract_indication(indication_blocks)
    return _finalize_semantic_root(
        root,
        date_notif=None,
        indication=indication,
        image_base_url=image_base_url,
        glossary_terms=glossary_terms,
    )


def _finalize_semantic_root(
    root: Tag,
    *,
    date_notif: date | None,
    indication: str | None,
    image_base_url: str,
    glossary_terms: Iterable[str],
) -> SemanticDocument:
    """Apply the normalization shared by legacy and already-semantic inputs."""
    _sanitize(root, image_base_url=image_base_url, preserve_block_ids=False)
    _strip_o_bullet_markers(root)
    _annotate_glossary_terms(root, glossary_terms)
    _remove_empty_blocks(root)
    _wrap_tables(root)
    _assign_block_ids(root)
    _sanitize(root, image_base_url=image_base_url, preserve_block_ids=True)
    _compact_whitespace(root)
    return SemanticDocument(
        date_notif=date_notif,
        indication=indication,
        content_html="".join(str(child) for child in root.contents),
    )


def _annotate_glossary_terms(root: Tag, glossary_terms: Iterable[str]) -> None:
    """Wrap glossary occurrences while preserving the document's original text."""
    canonical_terms: dict[str, str] = {}
    for term in glossary_terms:
        canonical = " ".join(str(term).split())
        if canonical:
            canonical_terms.setdefault(_glossary_key(canonical), canonical)

    if not canonical_terms:
        return

    alternatives = [_glossary_pattern(term) for term in canonical_terms.values()]
    alternatives.sort(key=len, reverse=True)
    matcher = re.compile(rf"(?<!\w)(?:{'|'.join(alternatives)})(?!\w)", re.IGNORECASE)

    for text_node in list(root.find_all(string=True)):
        if (
            not text_node.parent
            or text_node.parent.get("data-definition") is not None
            or any(parent.name in {"h1", "h2", "h3", "h4", "h5", "h6"} for parent in text_node.parents)
        ):
            continue
        matches = list(matcher.finditer(str(text_node)))
        if not matches:
            continue

        replacement: list[NavigableString | Tag] = []
        cursor = 0
        for match in matches:
            if match.start() > cursor:
                replacement.append(NavigableString(str(text_node)[cursor : match.start()]))
            span = BeautifulSoup("", "html.parser").new_tag("span")
            span["data-definition"] = canonical_terms[_glossary_key(match.group())]
            span.string = match.group()
            replacement.append(span)
            cursor = match.end()
        if cursor < len(text_node):
            replacement.append(NavigableString(str(text_node)[cursor:]))
        text_node.replace_with(*replacement)


def _glossary_key(value: str) -> str:
    return " ".join(value.replace("’", "'").split()).casefold()


def _glossary_pattern(term: str) -> str:
    normalized = term.replace("’", "'")
    escaped = re.escape(normalized)
    return escaped.replace(r"\ ", r"[\s\u00a0]+").replace("'", "['’]")


def _decode_html(source: bytes) -> str:
    declaration = re.search(rb"charset\s*=\s*[\"']?([A-Za-z0-9._-]+)", source[:4096], re.IGNORECASE)
    declared_encoding = declaration.group(1).decode("ascii") if declaration else None
    # HTML interprets ISO-8859-1 labels as Windows-1252. The legacy ANSM files
    # rely on this for curly quotes and otherwise produce invisible C1 controls.
    if declared_encoding and declared_encoding.lower().replace("_", "-") in {"iso-8859-1", "latin-1", "latin1"}:
        declared_encoding = "windows-1252"
    encodings = [declared_encoding] if declared_encoding else []
    encodings.extend(["utf-8", "windows-1252", "iso-8859-1"])
    for encoding in dict.fromkeys(encodings):
        try:
            return source.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return source.decode("windows-1252", errors="replace")


def _extract_date(root: Tag) -> date | None:
    date_tag = root.find(lambda tag: "datenotif" in _normalized_classes(tag))
    if not date_tag:
        return None
    match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", date_tag.get_text(" ", strip=True))
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%d/%m/%Y").date()
    except ValueError:
        return None


def _find_indication_blocks(root: Tag) -> list[Tag]:
    semantic_indication = root.find_all(attrs={"data-document-role": "indication"})
    if semantic_indication:
        return semantic_indication

    direct_indication = root.find(
        lambda tag: any(re.fullmatch(r"(?:amm)?ann3bquestceque\d*", name) for name in _normalized_classes(tag))
    )
    if direct_indication is not None:
        return [direct_indication]

    anchor = root.find(lambda tag: isinstance(tag.get("name"), str) and tag["name"].casefold() == "ann3bquestceque")
    if anchor is None:
        return []

    heading = anchor.find_parent(["p", "div", "h2", "h3", "h4", "h5", "h6"])
    if heading is None:
        return []

    blocks: list[Tag] = []
    for sibling in heading.next_siblings:
        if not isinstance(sibling, Tag):
            continue
        if _is_heading_element(sibling):
            break
        if sibling.name not in DANGEROUS_TAGS and _plain_text(sibling):
            blocks.append(sibling)
    return blocks


def _extract_indication(blocks: list[Tag]) -> str | None:
    paragraphs = [_plain_text(block) for block in blocks]
    return "\n\n".join(paragraphs) or None


def _convert_semantics(root: Tag, indication_blocks: list[Tag]) -> None:
    for tag in root.find_all(True):
        classes = _normalized_classes(tag)
        is_indication = tag.name in {"p", "div"} and any(tag is block for block in indication_blocks)
        # Roles are derived exclusively from recognized legacy classes.
        tag.attrs.pop("data-document-role", None)
        heading = _heading_tag(classes) if tag.name in {"p", "div"} else None
        if heading is not None:
            anchor_name = _named_anchor_id(tag)
            if anchor_name is not None and not tag.get("id"):
                tag["id"] = anchor_name
            tag.name = heading
        elif tag.name == "div" and is_indication:
            tag.name = "p"
        elif tag.name in {"p", "div"} and _is_body_block(classes, tag):
            tag.name = "p"

        document_role = "indication" if is_indication else _document_role(classes)
        if tag.name in {"p", "h2", "h3", "h4", "h5", "h6"} and document_role is not None:
            tag["data-document-role"] = document_role

        block_host = tag.name in {"p", "h2", "h3", "h4", "h5", "h6"}
        has_bold_class = any(
            re.fullmatch(r"(?:ammcorpstextegras(?:car)?|gras)\d*", class_name) for class_name in classes
        )
        has_italic_class = any(re.fullmatch(r"italique\d*", name) for name in classes)
        has_underline_class = any(re.fullmatch(r"souligne\d*", name) for name in classes)

        if tag.name in {"b", "strong"}:
            tag.name = "strong"
        elif has_bold_class:
            if block_host:
                _wrap_contents(tag, "strong")
            else:
                tag.name = "strong"
        elif tag.name in {"i", "em"}:
            tag.name = "em"
        elif has_italic_class:
            if block_host:
                _wrap_contents(tag, "em")
            else:
                tag.name = "em"
        elif tag.name == "u":
            tag.name = "u"
        elif has_underline_class:
            if block_host:
                _wrap_contents(tag, "u")
            else:
                tag.name = "u"
        elif "msofootnotereference" in classes:
            tag.name = "sup"

        if tag.name == "p" and "ammtableautitre1" in classes:
            _wrap_contents(tag, "strong")

    for anchor in root.find_all("a"):
        anchor.unwrap()
    for span in root.find_all("span"):
        span.unwrap()


def _heading_tag(classes: set[str]) -> str | None:
    for class_name in classes:
        if class_name == "ammannexetitre" or re.fullmatch(r"ammannexetitre1\d*", class_name):
            return "h2"
        if class_name == "ammnoticetitre1" or re.fullmatch(r"ammannexetitre2(?:bis|0*)", class_name):
            return "h3"
        if re.fullmatch(r"ammannexetitre3\d*", class_name):
            return "h4"
        if re.fullmatch(r"ammannexetitre4\d*", class_name):
            return "h5"
    return None


def _is_heading_element(tag: Tag) -> bool:
    return tag.name in {"h1", "h2", "h3", "h4", "h5", "h6"} or (
        tag.name in {"p", "div"} and _heading_tag(_normalized_classes(tag)) is not None
    )


def _named_anchor_id(tag: Tag) -> str | None:
    anchor = tag.find("a", attrs={"name": True})
    if anchor is None or not isinstance(anchor.get("name"), str):
        return None
    return anchor["name"].strip() or None


def _document_role(classes: set[str]) -> str | None:
    role_patterns = (
        (r"ammtitulairenom\d*", "holder-name"),
        (r"ammtitulaireadresse\d*", "holder-address"),
        (r"ammcomposition\d*", "composition"),
    )
    return next(
        (role for pattern, role in role_patterns if any(re.fullmatch(pattern, name) for name in classes)),
        None,
    )


def _is_body_block(classes: set[str], tag: Tag) -> bool:
    if tag.name == "div" and tag.find(["p", "div", "table", "ul", "ol"], recursive=False):
        return False
    return (
        any(
            re.match(
                r"^(?:ammcorpstexte|ammnoticecorpsdetexte|ammtitulaire(?:adresse|nom|cpvillepays)|"
                r"ammcomposition|ammdenomination|ammtableaucorpstexte|mso(?:body|footnote)text|tableparagraph|longtext)",
                class_name,
            )
            for class_name in classes
        )
        or "ammtableautitre1" in classes
    )


def _wrap_contents(tag: Tag, wrapper_name: str) -> None:
    if len(tag.contents) == 1 and isinstance(tag.contents[0], Tag) and tag.contents[0].name == wrapper_name:
        return
    wrapper = BeautifulSoup("", "html.parser").new_tag(wrapper_name)
    for child in list(tag.contents):
        wrapper.append(child.extract())
    tag.append(wrapper)


def _remove_source_noise(root: Tag) -> None:
    noise_classes = {"msopagenumber", "msocommentreference", "msocommenttext"}
    for tag in list(root.find_all(True)):
        if tag.parent is not None and _normalized_classes(tag) & noise_classes:
            tag.decompose()


def _convert_alerts(root: Tag) -> None:
    for tag in root.find_all("div"):
        if tag.get("style", "").strip().rstrip(";").casefold() == ALERT_STYLE:
            tag["class"] = ["fr-alert"]


def _remove_metadata_blocks(root: Tag) -> None:
    """Remove metadata blocks after their values have been extracted."""
    for tag in list(root.find_all(True)):
        anchor_name = _named_anchor_id(tag)
        if (
            tag.parent is not None
            and "ammannexetitre" in _normalized_classes(tag)
            and (anchor_name is None or anchor_name.casefold() == "ann3bnotice")
        ):
            tag.decompose()

    for tag in list(root.find_all(True)):
        if "datenotif" in _normalized_classes(tag):
            # The date is available through SemanticDocument.date_notif and
            # must not be duplicated in the renderable document body.
            tag.decompose()


def _convert_lists(root: Tag) -> None:
    for parent in [root, *root.find_all(True)]:
        children = list(parent.children)
        index = 0
        while index < len(children):
            child = children[index]
            if not isinstance(child, Tag) or _list_level(child) is None:
                index += 1
                continue

            run: list[Tag] = []
            cursor = index
            while cursor < len(children):
                candidate = children[cursor]
                if isinstance(candidate, Tag) and _list_level(candidate) is not None:
                    run.append(candidate)
                    cursor += 1
                    continue
                if not isinstance(candidate, Tag) and not str(candidate).strip():
                    cursor += 1
                    continue
                break

            ul = _build_list(run)
            run[0].insert_before(ul)
            for paragraph in run:
                paragraph.decompose()
            index = cursor


def _list_level(tag: Tag) -> int | None:
    if tag.name not in {"p", "div"}:
        return None
    for class_name in _normalized_classes(tag):
        if class_name.startswith("msolistparagraph"):
            return 1
        if "listepuce" in class_name:
            match = re.search(r"listepuces?(\d)", class_name)
            return int(match.group(1)) if match else 1
    return None


def _build_list(paragraphs: list[Tag]) -> Tag:
    root_list = BeautifulSoup("", "html.parser").new_tag("ul")
    first_source_level = _list_level(paragraphs[0]) or 1
    stack: list[tuple[int, Tag]] = [(first_source_level, root_list)]

    for paragraph in paragraphs:
        source_level = _list_level(paragraph) or 1
        if source_level > stack[-1][0]:
            parent_item = next(
                (child for child in reversed(stack[-1][1].contents) if isinstance(child, Tag) and child.name == "li"),
                None,
            )
            if parent_item is not None:
                nested_list = BeautifulSoup("", "html.parser").new_tag("ul")
                parent_item.append(nested_list)
                stack.append((source_level, nested_list))
        elif source_level < stack[-1][0]:
            while len(stack) > 1 and source_level < stack[-1][0]:
                stack.pop()

        _remove_bullet_marker(paragraph)
        item = BeautifulSoup("", "html.parser").new_tag("li")
        if paragraph.get("id") is not None:
            item["id"] = paragraph["id"]
        for content in list(paragraph.contents):
            item.append(content.extract())
        stack[-1][1].append(item)

    return root_list


def _remove_bullet_marker(tag: Tag) -> None:
    first = next((child for child in tag.contents if not (not isinstance(child, Tag) and not str(child).strip())), None)
    if isinstance(first, Tag) and (
        "font-family:symbol" in first.get("style", "").replace(" ", "").lower()
        or first.get_text(strip=True)[:1] in {"•", "·", "�"}
    ):
        first.decompose()


def _strip_o_bullet_markers(root: Tag) -> None:
    for item in root.find_all("li"):
        _strip_o_bullet_marker(item)


def _strip_o_bullet_marker(tag: Tag) -> None:
    text_nodes = list(tag.find_all(string=True))
    prefix = ""
    for text_node in text_nodes:
        prefix += str(text_node)
        match = re.match(r"^\s*o[ \t\u00a0]+", prefix)
        if match:
            remaining = match.end()
            for marker_node in text_nodes:
                marker_text = str(marker_node)
                if remaining >= len(marker_text):
                    remaining -= len(marker_text)
                    marker_node.extract()
                else:
                    marker_node.replace_with(marker_text[remaining:])
                    break
            return
        if not re.fullmatch(r"\s*o?", prefix):
            return


def _remove_empty_blocks(root: Tag) -> None:
    for tag in reversed(root.find_all(["strong", "em", "u", "sup", "sub"])):
        if not tag.get_text(strip=True) and not tag.find("img"):
            tag.decompose()

    empty_candidate_tags = (ADDRESSABLE_TAGS - {"img"}) | {
        "caption",
        "figcaption",
        "thead",
        "tbody",
        "tfoot",
        "tr",
    }
    for tag in reversed(root.find_all(empty_candidate_tags)):
        if not tag.get_text(strip=True) and not tag.find("img"):
            tag.decompose()


def _wrap_tables(root: Tag) -> None:
    for table in root.find_all("table"):
        parent = table.parent
        if isinstance(parent, Tag) and "fr-table__content" in parent.get("class", []):
            continue

        content = BeautifulSoup("", "html.parser").new_tag("div", attrs={"class": "fr-table__content"})
        container = BeautifulSoup("", "html.parser").new_tag("div", attrs={"class": "fr-table__container"})
        wrapper = BeautifulSoup("", "html.parser").new_tag("div", attrs={"class": "fr-table__wrapper"})
        outer = BeautifulSoup("", "html.parser").new_tag("div", attrs={"class": "fr-table"})
        table.wrap(content)
        content.wrap(container)
        container.wrap(wrapper)
        wrapper.wrap(outer)


def _compact_whitespace(root: Tag) -> None:
    """Remove source formatting whitespace without joining inline words."""
    structural_containers = {"ul", "ol", "table", "thead", "tbody", "tfoot", "tr", "figure"}
    block_tags = ADDRESSABLE_TAGS | {"caption", "thead", "tbody", "tfoot", "tr", "th", "td", "figcaption"}

    for text_node in list(root.find_all(string=True)):
        normalized = re.sub(r"[ \t\r\n\f]+", " ", str(text_node))
        if normalized == " " and _is_layout_whitespace(text_node, root, structural_containers, block_tags):
            text_node.extract()
        elif normalized != str(text_node):
            text_node.replace_with(normalized)


def _is_layout_whitespace(
    text_node: NavigableString,
    root: Tag,
    structural_containers: set[str],
    block_tags: set[str],
) -> bool:
    parent = text_node.parent
    if parent is None:
        return True
    if parent.name in structural_containers:
        return True
    if parent is not root:
        return False

    previous = text_node.previous_sibling
    following = text_node.next_sibling
    return (
        previous is None
        or following is None
        or isinstance(previous, Tag)
        and previous.name in block_tags
        or isinstance(following, Tag)
        and following.name in block_tags
    )


def _sanitize(root: Tag, *, image_base_url: str, preserve_block_ids: bool) -> None:
    _sanitize_images(root, image_base_url)

    for tag in list(root.find_all(DANGEROUS_TAGS)):
        tag.decompose()
    for tag in list(root.find_all(True)):
        if tag.parent is not None and tag.name not in ALLOWED_TAGS:
            tag.unwrap()

    allowed_attributes = {
        "div": {"class"},
        "p": {"data-document-date", "data-document-role"},
        "h2": {"data-document-role"},
        "h3": {"data-document-role"},
        "h4": {"data-document-role"},
        "h5": {"data-document-role"},
        "h6": {"data-document-role"},
        "span": {"data-definition"},
        "img": {"src", "alt", "title", "width", "height"},
        "td": {"rowspan", "colspan", "scope"},
        "th": {"rowspan", "colspan", "scope"},
    }
    for tag in root.find_all(True):
        keep = allowed_attributes.get(tag.name, set()) | {"id"}
        if preserve_block_ids:
            keep.add("data-block-id")
        for attribute in list(tag.attrs):
            if attribute not in keep:
                del tag.attrs[attribute]

        if (
            tag.name == "p"
            and tag.get("data-document-date")
            and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", tag["data-document-date"])
        ):
            del tag.attrs["data-document-date"]
        if tag.name in {"p", "h2", "h3", "h4", "h5", "h6"} and tag.get("data-document-role") not in DOCUMENT_ROLES:
            tag.attrs.pop("data-document-role", None)
        if tag.name in {"td", "th"}:
            _validate_table_attributes(tag)
        if tag.name == "img":
            for dimension in ("width", "height"):
                if tag.get(dimension) and not re.fullmatch(r"[1-9]\d{0,4}", tag[dimension]):
                    del tag.attrs[dimension]

        if tag.name == "div":
            classes = [class_name for class_name in tag.get("class", []) if class_name in DSFR_DIV_CLASSES]
            if classes:
                tag["class"] = classes
            else:
                tag.attrs.pop("class", None)

    for tag in list(root.find_all("div")):
        if not tag.get("class"):
            tag.unwrap()


def _sanitize_images(root: Tag, image_base_url: str) -> None:
    base = image_base_url.rstrip("/") + "/"
    parsed_base = urlparse(base)
    if parsed_base.scheme != "https" or not parsed_base.hostname or parsed_base.username or parsed_base.password:
        raise ValueError("image_base_url must be an HTTPS URL without credentials")

    for image in list(root.find_all("img")):
        source = image.get("src", "").strip()
        if not source:
            image.decompose()
            continue
        parsed_source = urlparse(source)
        if not parsed_source.scheme and not parsed_source.netloc:
            image_path = source.replace("\\", "/")
            if "images/" in image_path:
                image_path = image_path.split("images/", 1)[1]
            source = urljoin(base, image_path.lstrip("./"))
            image["src"] = source

        parsed_source = urlparse(source)
        if (
            parsed_source.scheme != "https"
            or parsed_source.hostname != parsed_base.hostname
            or parsed_source.port != parsed_base.port
            or parsed_source.username
            or parsed_source.password
        ):
            image.decompose()


def _validate_table_attributes(tag: Tag) -> None:
    for span in ("rowspan", "colspan"):
        if tag.get(span) and not re.fullmatch(r"[1-9]\d{0,3}", tag[span]):
            del tag.attrs[span]
    if tag.get("scope") not in {None, "row", "col", "rowgroup", "colgroup"}:
        del tag.attrs["scope"]


def _assign_block_ids(root: Tag) -> None:
    number = 1
    for tag in root.find_all(ADDRESSABLE_TAGS):
        if tag.name != "img" and not tag.get_text(strip=True) and not tag.find("img"):
            continue
        tag["data-block-id"] = f"document-b{number:04d}"
        number += 1


def _normalized_classes(tag: Tag) -> set[str]:
    return {class_name.casefold() for class_name in tag.get("class", [])} if tag.attrs is not None else set()


def _plain_text(tag: Tag) -> str:
    return " ".join(tag.get_text().split())
