"""Regressions from CIS 63409366 (Afqlir) and 60345905 (Tecentriq).

The PDF excerpts preserve the original page contents: Afqlir pages 30, 31, 22, 51 and
Tecentriq pages 4, 42, 64. Exercise extraction through semantic HTML rendering.
"""

import re
from collections import Counter
from pathlib import Path

import pymupdf
import pytest
from bs4 import BeautifulSoup

from infomedicament_dataeng.centralise.extract import Image, extract_elements
from infomedicament_dataeng.centralise.parser import _SemanticHtmlBuilder
from infomedicament_dataeng.parsing.semantic_parser import finalize_semantic_html

FIXTURES = Path(__file__).parent / "fixtures"


def render_page(name, page):
    images = {}
    with pymupdf.open(FIXTURES / f"{name}-rcp-tables.pdf") as document:
        builder = _SemanticHtmlBuilder("rcp", "https://cdn.example/images", "images/", images)
        for element in extract_elements(document, range(page, page + 1)):
            builder.add(element)
    html = finalize_semantic_html(builder.finish(), image_base_url="https://cdn.example/images").content_html
    return BeautifulSoup(html, "html.parser"), images


def test_afqlir_pictograms_and_instructions_stay_in_their_cells():
    first, blobs = render_page("afqlir", 0)
    tables = first.find_all("table")
    assert len(tables) == 2  # the syringe labels are a figure, not a third table
    assert len(tables[0].select("td img")) == 4
    assert "Conserver Afqlir" in tables[0].get_text()
    assert blobs

    second, _ = render_page("afqlir", 1)
    table = second.find("table")
    assert len(table.select("td img")) == 4
    assert len(second.find_all("img")) == 4  # no duplicate sibling figures
    rows = table.find_all("tr")
    assert rows[0].find("img") is not None
    assert "capuchon" in rows[0].get_text()
    assert "Fixer fermement" in rows[1].get_text()
    assert rows[1].find("img") is not None


def test_tecentriq_efficacy_values_keep_their_treatment_arm():
    soup, _ = render_page("tecentriq", 1)
    tables = soup.find_all("table")
    assert len(tables) == 1  # the numbered footnotes must stay ordinary text
    rows = tables[0].find_all("tr")
    header = rows[0].find_all(["td", "th"])
    assert len(header) == 4
    assert ["Bras " + arm in cell.get_text() for arm, cell in zip("ABC", header[1:])] == [True] * 3
    result = next(row for row in rows if "330 (82,1 %)" in row.get_text())
    cells = result.find_all(["td", "th"])
    assert len(cells) == 4
    assert "330 (82,1 %)" in cells[1].get_text()
    assert "291 (72,8 %)" in cells[2].get_text()
    assert "355 (88,8 %)" in cells[3].get_text()
    assert cells[0].get_text().strip() == "Nombre d’événements (%)"
    assert cells[1].get_text().strip() == "330 (82,1 %)"


def test_afqlir_syringe_uses_the_visible_french_composition():
    soup, blobs = render_page("afqlir", 0)
    image = soup.find("img", alt=lambda text: text and "Capuchon de la seringue" in text)
    assert image is not None
    key = "images/" + image["src"].split("/images/")[1]
    actual = pymupdf.Pixmap(blobs[key])
    with pymupdf.open(FIXTURES / "afqlir-rcp-tables.pdf") as document:
        page = document[0]
        region = next(info["bbox"] for info in page.get_image_info() if info["width"] == 752)
        # Compare to the displayed PDF, which overlays French labels onto an
        # English bitmap. Returning the embedded bytes cannot pass this check.
        region = pymupdf.Rect(region) | page.search_for("Capuchon de la seringue")[0]
        expected = page.get_pixmap(clip=region, dpi=144, alpha=False, annots=False)
    assert actual.samples == expected.samples


def test_afqlir_overlay_label_outside_bitmap_is_not_clipped():
    with pymupdf.open(FIXTURES / "afqlir-rcp-tables.pdf") as document:
        page = document[2]
        label = page.search_for("+17.1")[0]
        elements = extract_elements(document, range(2, 3))
        assert len([element for element in elements if isinstance(element, Image)]) == 1
        picture = next(element for element in elements if isinstance(element, Image) and "+17.1" in element.alt)
        assert pymupdf.Rect(picture.bbox).contains(label)


def test_afqlir_rotated_label_survives_after_merging_figure_regions():
    with pymupdf.open(FIXTURES / "afqlir-rcp-tables.pdf") as document:
        labels = document[3].search_for("(lettres)")
        assert labels
        elements = extract_elements(document, range(3, 4))
        pictures = [pymupdf.Rect(element.bbox) for element in elements if isinstance(element, Image)]
        assert all(any(picture.contains(label) for picture in pictures) for label in labels)


def test_tecentriq_headers_keep_merged_cells():
    soup, _ = render_page("tecentriq", 0)
    table = soup.find_all("table")[-1]
    spanning = table.find(attrs={"colspan": "3"})
    assert spanning is not None
    assert "Tecentriq en monothérapie" in spanning.get_text()


def test_tecentriq_shaded_header_does_not_create_extra_columns():
    soup, _ = render_page("tecentriq", 2)
    table = soup.find("table")
    header = table.find("tr").find_all(["td", "th"])
    assert len(header) == 3
    assert "Atezolizumab" in header[1].get_text()
    assert "Placebo" in header[2].get_text()
    shared = next(row for row in table.find_all("tr") if "0,62" in row.get_text())
    estimate = shared.find_all(["td", "th"])[1]
    assert estimate["colspan"] == "2"
    assert estimate.get_text().strip() == "0,62 (0,49 ; 0,78)"
    p_values = [row.find_all(["td", "th"])[1] for row in table.find_all("tr") if "Valeur de p" in row.get_text()]
    assert len(p_values) == 2
    assert all(cell.get("colspan") == "2" and cell.get_text().strip() == "< 0,0001" for cell in p_values)


@pytest.mark.parametrize("name,pages", [("afqlir", [30, 31, 22, 51]), ("tecentriq", [4, 42, 64])])
def test_source_words_and_numbers_survive_once(name, pages):
    """Catch clipped/duplicated values and labels even when the grid looks right."""
    with pymupdf.open(FIXTURES / f"{name}-rcp-tables.pdf") as document:
        for index, source_page in enumerate(pages):
            soup, _ = render_page(name, index)
            actual = (
                soup.get_text(" ", strip=True) + " " + " ".join(image.get("alt", "") for image in soup.select("img"))
            )
            expected = Counter(re.findall(r"\w+", document[index].get_text().casefold()))
            expected.subtract({str(source_page): 1})  # intentionally omitted footer
            assert Counter(re.findall(r"\w+", actual.casefold())) == +expected
