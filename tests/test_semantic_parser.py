"""Behavior tests for sanitized semantic notice HTML."""

from datetime import date
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from infomedicament_dataeng.parsing.semantic_parser import parse_semantic_document

SAMPLES = Path(__file__).parents[1] / "tmp" / "html-test"
SAMPLES_AVAILABLE = all((SAMPLES / filename).exists() for filename in ("N0047820.htm", "N0051804.htm"))


@pytest.mark.skipif(not SAMPLES_AVAILABLE, reason="handoff corpus is not checked into the repository")
@pytest.mark.parametrize(
    ("filename", "expected_date"),
    [
        ("N0047820.htm", date(2002, 7, 4)),
        ("N0051804.htm", date(2003, 1, 21)),
    ],
)
def test_parse_semantic_document_processes_real_legacy_notices(filename, expected_date):
    document = parse_semantic_document((SAMPLES / filename).read_bytes())
    html = BeautifulSoup(document.content_html, "html.parser")

    assert document.date_notif == expected_date
    assert "�" not in document.content_html
    assert not any("\x80" <= character <= "\x9f" for character in document.content_html)
    assert not html.find_all(["script", "style", "link", "meta", "iframe", "object", "form"])
    assert all(block.get("data-block-id") for block in html.find_all(ADDRESSABLE_TAG_NAMES))
    block_ids = [tag["data-block-id"] for tag in html.select("[data-block-id]")]
    assert len(block_ids) == len(set(block_ids))


ADDRESSABLE_TAG_NAMES = [
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "p",
    "blockquote",
    "ul",
    "ol",
    "li",
    "table",
    "figure",
    "img",
]


def test_parse_semantic_document_decodes_metadata_and_keeps_the_complete_body():
    source = """
    <meta charset="iso-8859-1"><p>Contenu avant le titre</p>
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <p class="DateNotif">Mis à jour le : 21/01/2003</p>
    <p class="AmmNoticeTitre1">Dénomination du médicament</p>
    <p class="AmmDenomination">MÉDICAMENT TEST</p>
    <p class="AmmCorpsTexte">Médicament d’essai</p>
    <div><p><a name="RcpDenomination">RCP</a></p><p>Dosimétrie</p></div>
    """.encode("windows-1252")

    document = parse_semantic_document(source)
    html = BeautifulSoup(document.content_html, "html.parser")

    assert document.date_notif == date(2003, 1, 21)
    assert "Médicament d’essai" in document.content_html
    assert html.select_one("[data-document-date]") is None
    assert "Mis à jour le" not in document.content_html
    assert html.find(string="NOTICE") is None
    assert "Contenu avant le titre" in document.content_html
    assert "RCP" in document.content_html
    assert "Dosimétrie" in document.content_html
    assert "RcpDenomination" not in document.content_html
    assert "�" not in document.content_html
    assert not any("\x80" <= character <= "\x9f" for character in document.content_html)


def test_parse_semantic_document_extracts_and_marks_indication_from_class():
    source = """
    <div class="Ann3bQuestceque">Une indication avec <strong>du contenu</strong>.</div>
    """

    document = parse_semantic_document(source)
    html = BeautifulSoup(document.content_html, "html.parser")

    assert document.indication == "Une indication avec du contenu."
    indication = html.select_one('[data-document-role="indication"]')
    assert indication.name == "p"
    assert indication.get_text() == "Une indication avec du contenu."


def test_parse_semantic_document_extracts_and_marks_indication_from_legacy_anchor():
    source = """
    <p class="AmmAnnexeTitre1"><a name="Ann3bQuestceque">1. QU’EST-CE QUE CE MÉDICAMENT ?</a></p>
    <p class="AmmCorpsTexte">Première partie de la réponse.</p>
    <p class="AmmCorpsTexte">Deuxième partie de la réponse.</p>
    <p class="AmmAnnexeTitre1"><a name="Ann3bInfoNecessaires">2. INFORMATIONS À CONNAÎTRE</a></p>
    <p class="AmmCorpsTexte">Cette phrase appartient à la section suivante.</p>
    """

    document = parse_semantic_document(source)
    html = BeautifulSoup(document.content_html, "html.parser")

    assert document.indication == "Première partie de la réponse.\n\nDeuxième partie de la réponse."
    heading = html.find("h2", id="Ann3bQuestceque")
    assert heading.get("data-document-role") is None
    assert heading["id"] == "Ann3bQuestceque"
    assert [block.get_text() for block in html.select('[data-document-role="indication"]')] == [
        "Première partie de la réponse.",
        "Deuxième partie de la réponse.",
    ]


def test_parse_semantic_document_processes_an_rcp_body_and_metadata():
    source = """
    <p class="AmmAnnexeTitre">RÉSUMÉ DES CARACTÉRISTIQUES DU PRODUIT</p>
    <p class="DateNotif">ANSM - Mis à jour le : 17/07/2026</p>
    <p class="AmmAnnexeTitre1"><a name="RcpDenomination">1. DÉNOMINATION DU MÉDICAMENT</a></p>
    <p class="AmmAnnexeTitre2">1.1. Dénomination du médicament</p>
    <p class="AmmDenomination">RCP TEST 500 mg, comprimé</p>
    <p class="AmmAnnexeTitre1">2. COMPOSITION QUALITATIVE ET QUANTITATIVE</p>
    <p class="AmmCorpsTexte">Substance active : 500 mg.</p>
    """

    document = parse_semantic_document(source)
    html = BeautifulSoup(document.content_html, "html.parser")

    assert document.date_notif == date(2026, 7, 17)
    assert html.select_one("[data-document-date]") is None
    assert "Mis à jour le" not in html.get_text()
    assert "1. DÉNOMINATION DU MÉDICAMENT" in html.get_text()
    assert "Substance active : 500 mg." in html.get_text()
    assert not html.find_all("a")


def test_parse_semantic_document_matches_canonical_html_fixture():
    source = b'<p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p><p>Hello <b>world</b>.</p>'

    result = parse_semantic_document(source).content_html

    expected = (
        (Path(__file__).parent / "fixtures" / "semantic_document_expected.html")
        .read_text(encoding="utf-8")
        .removesuffix("\n")
    )
    assert result == expected


def test_parse_semantic_document_removes_layout_whitespace_but_preserves_inline_spaces():
    source = """

    <p class="AmmAnnexeTitre">NOTICE</p>

    <p class="DateNotif">Mis à jour le : 17/07/2026</p>

    <p class="AmmCorpsTexte">Hello <strong>dear</strong>
      <em>reader</em>.</p>

    <p class="AmmCorpsTexte">Second paragraph.</p>

    """

    result = parse_semantic_document(source).content_html

    assert "\n" not in result
    assert "</p><p" in result
    assert "Hello <strong>dear</strong> <em>reader</em>." in result


def test_parse_semantic_document_maps_structure_and_preserves_inline_scope():
    source = b"""
    <html><body>
      <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
      <p class="AmmAnnexeTitre1">Major section</p>
      <p class="AmmNoticeTitre1">Nested section</p>
      <p class="AmmCorpsTexte">plain <span class="gras">bold</span>,
        <i>italic</i>, <span class="souligne">underlined</span>,
        x<sup>2</sup> and H<sub>2</sub>O</p>
      <p class="AmmCorpsTexteGras">all bold</p>
      <p class="AmmCorpsTexte"><span style="font-size: 10pt"></span></p>
    </body></html>
    """

    result = parse_semantic_document(source).content_html
    html = BeautifulSoup(result, "html.parser")

    assert [heading.name for heading in html.find_all(["h2", "h3"])] == ["h2", "h3"]
    paragraph = next(tag for tag in html.find_all("p") if "plain" in tag.get_text())
    assert paragraph.find("strong").get_text() == "bold"
    assert paragraph.find("em").get_text() == "italic"
    assert paragraph.find("u").get_text() == "underlined"
    assert paragraph.find("sup").get_text() == "2"
    assert paragraph.find("sub").get_text() == "2"
    assert paragraph.find("strong").parent is paragraph
    assert html.find("p", string="all bold").strong.get_text() == "all bold"
    assert not html.find_all("span")

    blocks = html.find_all(["h2", "h3", "p"])
    assert [block["data-block-id"] for block in blocks] == [
        "document-b0001",
        "document-b0002",
        "document-b0003",
        "document-b0004",
    ]


def test_parse_semantic_document_groups_consecutive_word_bullets():
    source = b"""
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <p class="AmmListePuces"><span style="font-family: Symbol">&#8226; </span>first</p>
    <p class="AmmListePuces1"><span style="font-family: Symbol">&#8226; </span>second</p>
    <p class="AmmCorpsTexte">After the list</p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    lists = html.find_all("ul")
    assert len(lists) == 1
    assert [item.get_text(strip=True) for item in lists[0].find_all("li", recursive=False)] == ["first", "second"]
    assert "•" not in html.get_text()
    assert lists[0]["data-block-id"] == "document-b0001"
    assert [item["data-block-id"] for item in lists[0].find_all("li")] == [
        "document-b0002",
        "document-b0003",
    ]


def test_parse_semantic_document_preserves_source_ids_through_conversion():
    source = """
    <div id="section" class="AmmAnnexeTitre2Bis" data-block-id="source-value">Section</div>
    <p id="first-item" class="AmmListePuces">First item</p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    heading = html.find("h3")
    item = html.find("li")
    assert heading["id"] == "section"
    assert heading["data-block-id"] == "document-b0001"
    assert item["id"] == "first-item"
    assert item["data-block-id"] == "document-b0003"


def test_parse_semantic_document_promotes_named_heading_anchor_to_heading_id():
    source = """
    <p class="AmmAnnexeTitre"><a name="RcpTitre">RÉSUMÉ DES CARACTÉRISTIQUES DU PRODUIT</a></p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    heading = html.find("h2")
    assert heading["id"] == "RcpTitre"
    assert heading.get_text() == "RÉSUMÉ DES CARACTÉRISTIQUES DU PRODUIT"
    assert heading.find("a") is None


def test_parse_semantic_document_maps_common_generated_class_variants():
    source = b"""
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <div class="AmmAnnexeTitre2Bis">Level two bis</div>
    <p class="ammannexetitre30">Level three generated</p>
    <p class="AmmAnnexeTitre4">Level four</p>
    <div class="ammnoticecorpsdetexte">Notice body</div>
    <div class="ammcorpstexte0">Regular body</div>
    <div class="AmmTitulaireNom">Holder name</div>
    <div class="AmmTitulaireAdresse0">Holder address</div>
    <p class="AmmComposition">Active ingredient: 500 mg</p>
    <p class="AmmTableauTitre1">Table title</p>
    <p class="gras1">Bold paragraph alias</p>
    <p class="souligne">Underlined paragraph</p>
    <p class="AmmCorpsTexte">normal <span class="ammcorpstextegrascar0">bold variant</span>
      and <span class="gras1">bold alias</span></p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    assert [tag.name for tag in html.find_all(["h2", "h3", "h4", "h5"])] == ["h3", "h4", "h5"]
    assert [html.find(string=text).parent.name for text in ("Notice body", "Regular body", "Holder address")] == [
        "p",
        "p",
        "p",
    ]
    assert html.select_one('[data-document-role="holder-name"]').get_text() == "Holder name"
    assert html.select_one('[data-document-role="holder-address"]').get_text() == "Holder address"
    assert html.select_one('[data-document-role="composition"]').get_text() == "Active ingredient: 500 mg"
    assert html.find("p", string="Table title").strong.get_text() == "Table title"
    assert html.find("p", string="Bold paragraph alias").strong.get_text() == "Bold paragraph alias"
    assert html.find("p", string="Underlined paragraph").u.get_text() == "Underlined paragraph"
    assert [tag.get_text() for tag in html.find_all("strong")][-2:] == ["bold variant", "bold alias"]
    assert not any(tag.get("class") for tag in html.find_all(True))


def test_parse_semantic_document_recognizes_list_families_and_nesting():
    source = b"""
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <p class="listePuce"><span style="font-family: Symbol">&#8226; </span>first</p>
    <p class="AmmListePuces2"><span style="font-family: Symbol">&#8226; </span>nested</p>
    <p class="ammlistepuces20"><span style="font-family: Symbol">&#8226; </span>nested variant</p>
    <p class="AMMListePuces10"><span style="font-family: Symbol">&#8226; </span>back to root</p>
    <p class="AmmNoticeListePuces"><span style="font-family: Symbol">&#8226; </span>notice bullet</p>
    <p class="MsoListParagraphCxSpLast">Word bullet</p>
    <p class="MsoListParagraph">o plain o marker</p>
    <p class="MsoListParagraph"><span>o </span>inline o marker</p>
    <ul><li>o native o marker</li></ul>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")
    root_list = html.find("ul")

    assert [item.get_text(" ", strip=True) for item in root_list.find_all("li", recursive=False)] == [
        "first nested nested variant",
        "back to root",
        "notice bullet",
        "Word bullet",
        "plain o marker",
        "inline o marker",
    ]
    assert [item.get_text(strip=True) for item in root_list.find("ul").find_all("li", recursive=False)] == [
        "nested",
        "nested variant",
    ]
    assert "•" not in html.get_text()
    assert html.find("li", string="native o marker") is not None


def test_parse_semantic_document_adds_dsfr_table_and_alert_markup():
    source = """
    <div style="border:solid windowtext 1.0pt;padding:1.0pt 1.0pt 1.0pt 1.0pt">Important</div>
    <table><tbody><tr><td>Value</td></tr></tbody></table>
    <div class="untrusted">Regular content</div>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    alert = html.find("div", class_="fr-alert")
    assert alert.get_text(strip=True) == "Important"
    table = html.find("table")
    assert [table.parent["class"], table.parent.parent["class"], table.parent.parent.parent["class"]] == [
        ["fr-table__content"],
        ["fr-table__container"],
        ["fr-table__wrapper"],
    ]
    assert table.parent.parent.parent.parent["class"] == ["fr-table"]
    assert html.find("div", class_="untrusted") is None
    assert "Regular content" in html.get_text()


def test_parse_semantic_document_removes_word_noise_but_keeps_footnote_meaning():
    source = b"""
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <span class="MsoPageNumber">12</span>
    <span class="MsoCommentReference">comment marker</span>
    <p class="MsoCommentText">editorial comment</p>
    <p class="MsoFootnoteText"><span class="MsoFootnoteReference">1</span> Footnote content</p>
    <span class="MsoFootnoteReference"></span>
    <p><span class="msoIns">kept insertion</span><span class="apple-converted-space"> </span>text</p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    assert "12" not in html.get_text()
    assert "comment marker" not in html.get_text()
    assert "editorial comment" not in html.get_text()
    assert len(html.find_all("sup")) == 1
    assert html.find("sup").get_text() == "1"
    assert "Footnote content" in html.get_text()
    assert "kept insertion text" in html.get_text(" ", strip=True)


def test_parse_semantic_document_unwraps_links_and_removes_empty_blocks():
    source = """
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <p></p><h3> </h3><blockquote><span></span></blockquote>
    <table><tbody><tr><td></td></tr></tbody></table>
    <p>Keep <a href="https://example.test"><strong>linked text</strong></a>.</p>
    """

    html = BeautifulSoup(parse_semantic_document(source).content_html, "html.parser")

    assert not html.find_all("a")
    assert html.find("strong").get_text() == "linked text"
    assert "Keep linked text." in html.get_text()
    assert not html.find_all(["h3", "blockquote", "table"])
    assert all(tag.get_text(strip=True) or tag.find("img") for tag in html.find_all(["p", "li", "ul", "ol"]))


def test_parse_semantic_document_annotates_glossary_terms_with_canonical_names():
    source = """
    <p class="AmmCorpsTexte">Une substance active et une SUBSTANCE sont présentes.</p>
    <p class="AmmCorpsTexte">Cette procédure d’AMM nationale concerne l'AMM, pas GAMMA.</p>
    """

    result = parse_semantic_document(
        source,
        glossary_terms=["Substance", "Substance active", "Procédure d'AMM nationale", "AMM", " substance "],
    ).content_html
    html = BeautifulSoup(result, "html.parser")

    annotations = [(span.get_text(), span["data-definition"]) for span in html.find_all("span")]
    assert annotations == [
        ("substance active", "Substance active"),
        ("SUBSTANCE", "Substance"),
        ("procédure d’AMM nationale", "Procédure d'AMM nationale"),
        ("AMM", "AMM"),
    ]
    assert "GAMMA" in html.get_text()
    assert all(span.attrs.keys() == {"data-definition"} for span in html.find_all("span"))


def test_parse_semantic_document_does_not_annotate_glossary_terms_in_headings():
    source = """
    <p class="AmmAnnexeTitre1">Substance active</p>
    <h3><strong>Substance active</strong></h3>
    <p>Substance active</p>
    <ul><li>Substance active</li></ul>
    """

    html = BeautifulSoup(
        parse_semantic_document(source, glossary_terms=["Substance active"]).content_html,
        "html.parser",
    )

    assert all(heading.find("span", attrs={"data-definition": True}) is None for heading in html.find_all(["h2", "h3"]))
    assert [(span.parent.name, span.get_text()) for span in html.find_all("span")] == [
        ("p", "Substance active"),
        ("li", "Substance active"),
    ]


def test_parse_semantic_document_does_not_trust_source_definition_spans():
    source = '<p><span data-definition="injected" onclick="bad()">Pharmacovigilance</span></p>'

    html = BeautifulSoup(
        parse_semantic_document(source, glossary_terms=["Pharmacovigilance"]).content_html,
        "html.parser",
    )

    assert [(span.get_text(), span["data-definition"]) for span in html.find_all("span")] == [
        ("Pharmacovigilance", "Pharmacovigilance")
    ]
    assert "onclick" not in html.span.attrs


def test_parse_semantic_document_sanitizes_rich_content_and_is_deterministic():
    source = b"""
    <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
    <script>alert(1)</script><link rel="stylesheet" href="https://evil.test/x.css">
    <p class="AmmCorpsTexte" style="color: red" onclick="alert(1)" data-document-role="holder-name">
      <a href="javascript:alert(1)" title="unsafe">bad link</a>
      <a href="https://ansm.sante.fr/info" title="safe">safe link</a>
    </p>
    <table style="width: 100%"><tr><td rowspan="2" onclick="x()">A</td><td>B</td></tr>
      <tr><td colspan="3">C</td></tr></table>
    <p><img src="../images/photo.png" alt="photo" width="120" style="position: fixed" onerror="x()"></p>
    <img alt="missing">
    <img src="http://evil.test/tracker.png" alt="tracker">
    <iframe src="https://evil.test"></iframe>
    """

    first = parse_semantic_document(source, image_base_url="https://cdn.example.test/assets/").content_html
    second = parse_semantic_document(source, image_base_url="https://cdn.example.test/assets/").content_html
    html = BeautifulSoup(first, "html.parser")

    assert first == second
    assert not html.find_all(["script", "style", "link", "iframe"])
    assert not html.find_all("a")
    assert "bad link" in html.get_text()
    assert "safe link" in html.get_text()
    assert [[cell.get_text(strip=True) for cell in row.find_all(["td", "th"])] for row in html.find_all("tr")] == [
        ["A", "B"],
        ["C"],
    ]
    assert html.find("td", string="A")["rowspan"] == "2"
    assert html.find("td", string="C")["colspan"] == "3"
    image = html.find("img")
    assert image["src"] == "https://cdn.example.test/assets/photo.png"
    assert image.attrs.keys() == {"src", "alt", "width", "data-block-id"}
    assert "missing" not in first
    assert "tracker" not in first

    blocks = html.find_all(
        ["h2", "h3", "h4", "h5", "h6", "p", "blockquote", "ul", "ol", "li", "table", "figure", "img"]
    )
    block_ids = [block["data-block-id"] for block in blocks]
    assert len(block_ids) == len(set(block_ids))
    assert block_ids == [f"document-b{number:04d}" for number in range(1, len(block_ids) + 1)]
    assert all(not attr.startswith("on") for tag in html.find_all(True) for attr in tag.attrs)
    assert all("style" not in tag.attrs for tag in html.find_all(True))
    assert {class_name for tag in html.find_all(True) for class_name in tag.get("class", [])} == {
        "fr-table",
        "fr-table__wrapper",
        "fr-table__container",
        "fr-table__content",
    }
    assert html.find("p").get("data-document-role") is None
