"""Tests for the HTML parser."""

from bs4 import BeautifulSoup

from infomedicament_dataeng.parser import (
    extraire_styles,
    extraire_table_complete,
    html_vers_json,
    nettoyer_element_pour_texte,
    traiter_images_dans_html,
)


def test_traiter_images_dans_html_relative_to_absolute():
    """Test that relative image URLs are converted to absolute."""
    html = '<img src="../images/photo.png">'
    result = traiter_images_dans_html(html)
    assert "cellar-c2.services.clever-cloud.com" in result
    assert "../images/" not in result
    assert result.endswith(" />")


def test_traiter_images_dans_html_no_images():
    """Test that HTML without images is returned unchanged."""
    html = "<p>No images here</p>"
    result = traiter_images_dans_html(html)
    assert result == html


def test_traiter_images_dans_html_preserves_attributes():
    """Test that image attributes are preserved during URL conversion."""
    html = '<img class="photo" src="../images/med.png" alt="medication">'
    result = traiter_images_dans_html(html)
    assert 'class="photo"' in result or "class='photo'" in result or "class=photo" in result.lower()
    assert 'alt="medication"' in result
    assert "cellar-c2.services.clever-cloud.com/info-medicaments/exports/images/med.png" in result


def test_traiter_images_dans_html_multiple_images():
    """Test handling multiple images in one HTML string."""
    html = '<p><img src="../images/a.png"> text <img src="../images/b.png"></p>'
    result = traiter_images_dans_html(html)
    assert result.count("cellar-c2.services.clever-cloud.com") == 2
    assert "images/a.png" in result
    assert "images/b.png" in result


def test_traiter_images_dans_html_empty_input():
    """Test handling empty or None input."""
    assert traiter_images_dans_html("") == ""
    assert traiter_images_dans_html(None) is None


def test_nettoyer_element_pour_texte_superscript():
    """Test superscript conversion to Unicode."""
    soup = BeautifulSoup("<p>H<sup>2</sup>O</p>", "html.parser")
    element = soup.find("p")
    result = nettoyer_element_pour_texte(element)
    assert "²" in result.get_text()
    assert "<sup>" not in str(result)


def test_nettoyer_element_pour_texte_subscript():
    """Test subscript conversion to Unicode."""
    soup = BeautifulSoup("<p>CO<sub>2</sub></p>", "html.parser")
    element = soup.find("p")
    result = nettoyer_element_pour_texte(element)
    assert "₂" in result.get_text()
    assert "<sub>" not in str(result)


def test_nettoyer_element_pour_texte_letter_spacing_span():
    """Test removal of letter-spacing spans while preserving content."""
    soup = BeautifulSoup('<p>Hello<span style="letter-spacing: 2px"> </span>World</p>', "html.parser")
    element = soup.find("p")
    result = nettoyer_element_pour_texte(element)
    text = result.get_text()
    assert "Hello" in text
    assert "World" in text


def test_nettoyer_element_pour_texte_preserves_regular_text():
    """Test that regular text is preserved."""
    soup = BeautifulSoup("<p>Simple text without formatting</p>", "html.parser")
    element = soup.find("p")
    result = nettoyer_element_pour_texte(element)
    assert result.get_text() == "Simple text without formatting"


def test_nettoyer_element_pour_texte_multiple_superscripts():
    """Test multiple superscripts in one element."""
    soup = BeautifulSoup("<p>x<sup>2</sup> + y<sup>3</sup></p>", "html.parser")
    element = soup.find("p")
    result = nettoyer_element_pour_texte(element)
    text = result.get_text()
    assert "²" in text
    assert "³" in text


def test_extraire_styles_bold_class():
    """Test bold detection from class='gras'."""
    soup = BeautifulSoup('<p><span class="gras">Bold text</span></p>', "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "bold" in styles


def test_extraire_styles_bold_tag():
    """Test bold detection from <b> tag."""
    soup = BeautifulSoup("<p><b>Bold text</b></p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "bold" in styles


def test_extraire_styles_strong_tag():
    """Test bold detection from <strong> tag."""
    soup = BeautifulSoup("<p><strong>Bold text</strong></p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "bold" in styles


def test_extraire_styles_underline_class():
    """Test underline detection from class='souligne'."""
    soup = BeautifulSoup('<p><span class="souligne">Underlined</span></p>', "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "underline" in styles


def test_extraire_styles_underline_tag():
    """Test underline detection from <u> tag."""
    soup = BeautifulSoup("<p><u>Underlined</u></p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "underline" in styles


def test_extraire_styles_italic_tag():
    """Test italic detection from <em> tag."""
    soup = BeautifulSoup("<p><em>Italic text</em></p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "italic" in styles


def test_extraire_styles_multiple():
    """Test detection of multiple styles."""
    soup = BeautifulSoup("<p><b>Bold</b> and <em>italic</em></p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert "bold" in styles
    assert "italic" in styles


def test_extraire_styles_no_styles():
    """Test element with no styles."""
    soup = BeautifulSoup("<p>Plain text</p>", "html.parser")
    element = soup.find("p")
    styles = extraire_styles(element)
    assert styles == []


def test_extraire_table_complete_simple():
    """Test extraction of a simple table."""
    html = """
    <table class="AmmCorpsTexteTable">
        <tr><td>Cell 1</td><td>Cell 2</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    result = extraire_table_complete(table)

    assert result["type"] == "table"
    assert result["tag"] == "table"
    assert "children" in result
    assert len(result["children"]) > 0


def test_extraire_table_complete_with_thead():
    """Test extraction of table with thead."""
    html = """
    <table>
        <thead><tr><th>Header</th></tr></thead>
        <tbody><tr><td>Data</td></tr></tbody>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    result = extraire_table_complete(table)

    # Should have thead and tbody as children
    child_tags = [c["tag"] for c in result["children"]]
    assert "thead" in child_tags
    assert "tbody" in child_tags


def test_extraire_table_complete_with_images():
    """Test that images in tables are processed."""
    html = """
    <table>
        <tr><td><img src="../images/icon.png"></td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    result = extraire_table_complete(table)

    # The HTML should contain the absolute URL
    assert "cellar-c2.services.clever-cloud.com" in result["html"]


def test_html_vers_json_returns_list(sample_notice_html):
    """Test that parsing returns a list of nodes."""
    result = html_vers_json(sample_notice_html)
    assert isinstance(result, list)


def test_html_vers_json_extracts_titles(sample_notice_html):
    """Test that section titles are extracted."""
    result = html_vers_json(sample_notice_html)
    # Should have at least one top-level element
    assert len(result) > 0
    # Check that we have title types
    types = [node.get("type") for node in result]
    title_types = {"AmmAnnexeTitre", "AmmNoticeTitre1"}
    assert any(t in title_types for t in types), f"Expected title types, got: {types}"


def test_html_vers_json_titre1():
    """Test AmmAnnexeTitre1 extraction."""
    html = '<p class="AmmAnnexeTitre1">Section 1</p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmAnnexeTitre1"
    assert result[0]["content"] == "Section 1"
    assert "children" in result[0]


def test_html_vers_json_notice_titre1():
    """Test AmmNoticeTitre1 extraction."""
    html = '<p class="AmmNoticeTitre1">Notice Section</p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmNoticeTitre1"
    assert result[0]["content"] == "Notice Section"


def test_html_vers_json_titre2_nested():
    """Test AmmAnnexeTitre2 is nested under Titre1."""
    html = """
    <p class="AmmAnnexeTitre1">Main Section</p>
    <p class="AmmAnnexeTitre2">Subsection</p>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmAnnexeTitre1"
    assert len(result[0]["children"]) == 1
    assert result[0]["children"][0]["type"] == "AmmAnnexeTitre2"
    assert result[0]["children"][0]["content"] == "Subsection"


def test_html_vers_json_titre2_without_titre1():
    """Test AmmAnnexeTitre2 without parent Titre1 is skipped."""
    html = '<p class="AmmAnnexeTitre2">Orphan Subsection</p>'
    result = html_vers_json(html)

    # Should be empty since Titre2 needs a parent Titre1
    assert len(result) == 0


def test_html_vers_json_bullet_list():
    """Test bullet list extraction."""
    html = """
    <p class="AmmListePuces1">Item 1</p>
    <p class="AmmListePuces1">Item 2</p>
    <p class="AmmListePuces1">Item 3</p>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "listePuce"
    assert result[0]["content"] == ["Item 1", "Item 2", "Item 3"]


def test_html_vers_json_bullet_list_nested_in_section():
    """Test bullet list nested in a section."""
    html = """
    <p class="AmmAnnexeTitre1">Section</p>
    <p class="AmmListePuces1">Bullet 1</p>
    <p class="AmmListePuces1">Bullet 2</p>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmAnnexeTitre1"
    assert len(result[0]["children"]) == 1
    assert result[0]["children"][0]["type"] == "listePuce"
    assert len(result[0]["children"][0]["content"]) == 2


def test_html_vers_json_multiple_bullet_levels():
    """Test different bullet list levels are separate."""
    html = """
    <p class="AmmListePuces1">Level 1</p>
    <p class="AmmListePuces2">Level 2</p>
    """
    result = html_vers_json(html)

    # Should be two separate lists since they have different classes
    assert len(result) == 2
    assert result[0]["type"] == "listePuce"
    assert result[1]["type"] == "listePuce"


def test_html_vers_json_corps_texte():
    """Test AmmCorpsTexte extraction."""
    html = '<p class="AmmCorpsTexte">Regular body text.</p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmCorpsTexte"
    assert result[0]["content"] == "Regular body text."
    assert "html" in result[0]


def test_html_vers_json_corps_texte_gras():
    """Test AmmCorpsTexteGras includes bold style."""
    html = '<p class="AmmCorpsTexteGras">Bold body text.</p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmCorpsTexteGras"
    assert "bold" in result[0]["styles"]


def test_html_vers_json_corps_texte_nested_in_section():
    """Test body text nested in sections."""
    html = """
    <p class="AmmAnnexeTitre1">Section</p>
    <p class="AmmCorpsTexte">Body text in section.</p>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert len(result[0]["children"]) == 1
    assert result[0]["children"][0]["type"] == "AmmCorpsTexte"


def test_html_vers_json_anchor_extraction():
    """Test anchor extraction from <a name='...'> tags."""
    html = '<p class="AmmAnnexeTitre1"><a name="section1">Section 1</a></p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["anchor"] == "section1"


def test_html_vers_json_anchor_in_body_text():
    """Test anchor extraction in body text."""
    html = '<p class="AmmCorpsTexte"><a name="para1">Paragraph with anchor</a></p>'
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["anchor"] == "para1"


def test_html_vers_json_table():
    """Test table extraction with AmmCorpsTexteTable class."""
    html = """
    <table class="AmmCorpsTexteTable">
        <tr><td>Cell 1</td><td>Cell 2</td></tr>
    </table>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "table"


def test_html_vers_json_table_nested_in_section():
    """Test table nested in a section."""
    html = """
    <p class="AmmAnnexeTitre1">Section with Table</p>
    <table class="AmmCorpsTexteTable">
        <tr><td>Data</td></tr>
    </table>
    """
    result = html_vers_json(html)

    assert len(result) == 1
    assert result[0]["type"] == "AmmAnnexeTitre1"
    assert len(result[0]["children"]) == 1
    assert result[0]["children"][0]["type"] == "table"


def test_html_vers_json_complex_structure():
    """Test a complex document with multiple nested elements."""
    html = """
    <p class="AmmAnnexeTitre1"><a name="sec1">Section 1</a></p>
    <p class="AmmCorpsTexte">Intro text.</p>
    <p class="AmmAnnexeTitre2">Subsection 1.1</p>
    <p class="AmmCorpsTexte">Subsection text.</p>
    <p class="AmmListePuces1">Bullet A</p>
    <p class="AmmListePuces1">Bullet B</p>
    <p class="AmmAnnexeTitre1">Section 2</p>
    <p class="AmmCorpsTexteGras">Bold intro.</p>
    """
    result = html_vers_json(html)

    # Should have 2 top-level sections
    assert len(result) == 2

    # First section
    sec1 = result[0]
    assert sec1["type"] == "AmmAnnexeTitre1"
    assert sec1["anchor"] == "sec1"
    assert len(sec1["children"]) >= 2  # At least intro text and subsection

    # Check subsection exists with its children
    subsection = next((c for c in sec1["children"] if c["type"] == "AmmAnnexeTitre2"), None)
    assert subsection is not None
    assert len(subsection["children"]) >= 1  # Has text and bullets

    # Second section
    sec2 = result[1]
    assert sec2["type"] == "AmmAnnexeTitre1"
    assert len(sec2["children"]) >= 1


def test_html_vers_json_real_notice_structure(sample_notice_html):
    """Test that real notice has expected structure."""
    result = html_vers_json(sample_notice_html)

    # Should have multiple sections
    assert len(result) > 5

    # Check for common section types in a medication notice
    all_types = []

    def collect_types(nodes):
        for node in nodes:
            all_types.append(node.get("type"))
            if "children" in node:
                collect_types(node["children"])

    collect_types(result)

    # Should have titles and body text
    assert "AmmAnnexeTitre1" in all_types or "AmmNoticeTitre1" in all_types
    assert "AmmCorpsTexte" in all_types or "AmmCorpsTexteGras" in all_types


def test_html_vers_json_real_notice_has_bullet_lists(sample_notice_html):
    """Test that real notice contains bullet lists."""
    result = html_vers_json(sample_notice_html)

    all_types = []

    def collect_types(nodes):
        for node in nodes:
            all_types.append(node.get("type"))
            if "children" in node:
                collect_types(node["children"])

    collect_types(result)

    assert "listePuce" in all_types, "Notice should contain bullet lists"
