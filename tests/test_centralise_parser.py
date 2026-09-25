"""Tests for the centralised EMA PDF parser (Abasaglar golden fixture)."""

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from infomedicament_dataeng.centralise.extract import TextLine, _merge_same_line, _table_to_html
from infomedicament_dataeng.centralise.match import match_presentation
from infomedicament_dataeng.centralise.parser import (
    _is_notice_header,
    _normalise_date,
    _SemanticHtmlBuilder,
    parse_pdf,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ABASAGLAR_PDF = FIXTURES_DIR / "abasaglar-epar-product-information_fr.pdf"

# Real PDBM SpecDenom01 for the three CIS sharing the Abasaglar PDF.
CIS_NAMES = {
    "cartouche": "ABASAGLAR 100 unités/ml, solution injectable en cartouche",
    "generic_pen": "ABASAGLAR 100 unités/ml, solution injectable en stylo prérempli",
    "tempo_pen": "ABASAGLAR 100 unités/mL TEMPO PEN, solution injectable en stylo prérempli",
}


@pytest.fixture(scope="module")
def parsed() -> dict:
    return parse_pdf(ABASAGLAR_PDF.read_bytes())


@pytest.fixture(scope="module")
def rcp(parsed) -> BeautifulSoup:
    return BeautifulSoup(parsed["rcp"][0]["content_html"], "html.parser")  # cartouche SmPC is first


@pytest.fixture(scope="module")
def notice(parsed) -> BeautifulSoup:
    return BeautifulSoup(parsed["notice"][0]["content_html"], "html.parser")  # cartouche notice is first


def _headings(document, tag):
    return [heading.get_text(" ", strip=True) for heading in document.find_all(tag)]


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------


class TestExtractHelpers:
    def test_merge_same_line_joins_number_and_title(self):
        lines = [
            TextLine("1.", 11.0, True, 0, 57.0, 69.0),
            TextLine("DÉNOMINATION DU MÉDICAMENT", 11.0, True, 0, 57.1, 69.0),
        ]
        merged = _merge_same_line(lines)
        assert len(merged) == 1
        assert merged[0].text == "1. DÉNOMINATION DU MÉDICAMENT"
        assert merged[0].bold is True

    def test_merge_keeps_distinct_y_separate(self):
        lines = [
            TextLine("first line", 11.0, False, 0, 100.0, 112.0),
            TextLine("next paragraph", 11.0, False, 0, 130.0, 142.0),
        ]
        assert len(_merge_same_line(lines)) == 2

    def test_table_to_html_escapes_and_wraps(self):
        html = _table_to_html([["a & b", "c"], ["d", None]])
        assert html.startswith("<table><tbody>")
        assert "<td>a &amp; b</td>" in html
        assert "<td></td>" in html  # None cell

    def test_pdf_builder_constructs_semantic_elements_directly(self):
        builder = _SemanticHtmlBuilder("rcp", "https://cdn.example.test/images", "images/", {})
        builder.add(TextLine("1. DÉNOMINATION DU MÉDICAMENT", 11, True, 0, 10, 20))
        builder.add(TextLine("Example medicine", 11, False, 0, 30, 40))
        builder.add(TextLine("- First item", 11, False, 0, 50, 60))
        builder.add(TextLine("2. COMPOSITION QUALITATIVE ET QUANTITATIVE", 11, True, 0, 70, 80))

        fragment = builder.finish()

        assert fragment.startswith('<h2 id="RcpDenomination">1. DÉNOMINATION DU MÉDICAMENT</h2>')
        assert "<p>Example medicine</p>" in fragment
        assert "<ul><li>First item</li></ul>" in fragment
        assert 'class="' not in fragment


# ---------------------------------------------------------------------------
# Presentations (one SmPC/Notice per device)
# ---------------------------------------------------------------------------


class TestPresentations:
    def test_rcp_has_cartouche_and_pen_smpcs(self, parsed):
        denoms = [d["denomination"].lower() for d in parsed["rcp"]]
        assert len(denoms) == 2
        assert any("cartouche" in d for d in denoms)
        assert any("stylo prérempli" in d for d in denoms)

    def test_notice_has_three_device_presentations(self, parsed):
        denoms = [d["denomination"].lower() for d in parsed["notice"]]
        assert len(denoms) == 3
        assert any("cartouche" in d for d in denoms)
        assert any("kwikpen" in d for d in denoms)
        assert any("tempo" in d for d in denoms)


# ---------------------------------------------------------------------------
# Presentation matching (Jaccard)
# ---------------------------------------------------------------------------


class TestMatching:
    def test_each_cis_matches_its_notice(self, parsed):
        notices = parsed["notice"]
        assert "cartouche" in match_presentation(CIS_NAMES["cartouche"], notices)["denomination"].lower()
        # generic "stylo prérempli" resolves to KwikPen, not Tempo Pen
        assert "kwikpen" in match_presentation(CIS_NAMES["generic_pen"], notices)["denomination"].lower()
        assert "tempo" in match_presentation(CIS_NAMES["tempo_pen"], notices)["denomination"].lower()

    def test_cartouche_cis_matches_cartouche_smpc(self, parsed):
        matched = match_presentation(CIS_NAMES["cartouche"], parsed["rcp"])
        assert "cartouche" in matched["denomination"].lower()

    def test_dosage_matching_prefers_exact_strength(self):
        # Brintellix: 20 mg tablet must beat "20 mg/ml solution" despite the shared "20"
        docs = [
            {"denomination": "Brintellix 20 mg comprimé pelliculé"},
            {"denomination": "Brintellix 20 mg/ml solution buvable en gouttes"},
        ]
        matched = match_presentation("BRINTELLIX 20 mg, comprimé pelliculé", docs)
        assert "comprimé" in matched["denomination"]

    def test_strength_beats_token_overlap(self):
        # Humalog: the 100 U/mL SmPC bundles three devices, so its long denomination
        # loses on Jaccard to the standalone 200 U/mL one. The strength is decisive.
        docs = [
            {
                "denomination": (
                    "Humalog 100 unités/mL solution injectable en flacon "
                    "Humalog 100 unités/mL solution injectable en cartouche "
                    "Humalog 100 unités/mL KwikPen solution injectable en stylo pré-rempli"
                )
            },
            {"denomination": "Humalog 200 unités/mL KwikPen solution injectable en stylo pré-rempli"},
        ]
        name = "HUMALOG 100 UI/ml KWIKPEN, solution injectable en stylo pré-rempli"
        assert "100 unités/mL KwikPen" in match_presentation(name, docs)["denomination"]

    def test_pack_volume_is_not_a_strength(self):
        # A shared "3 mL" pack volume must not rescue the wrong strength.
        docs = [
            {"denomination": "Humalog 100 unités/mL KwikPen 3 mL"},
            {"denomination": "Humalog 200 unités/mL KwikPen 3 mL stylo pré-rempli"},
        ]
        matched = match_presentation("HUMALOG 100 UI/ml KWIKPEN 3 ml, stylo pré-rempli", docs)
        assert "100 unités/mL" in matched["denomination"]

    def test_thousands_separator_spellings_are_equivalent(self):
        # PDBM writes "1000 UI"; the EMA PDFs write "1 000 UI" or "1.000 UI".
        docs = [
            {"denomination": "Esperoct 500 UI, poudre et solvant pour solution injectable"},
            {"denomination": "Esperoct 1 000 UI, poudre et solvant pour solution injectable"},
            {"denomination": "Ceprotin 1.000 UI/10 ml, poudre et solvant pour solution injectable"},
        ]
        assert "1 000 UI" in match_presentation("ESPEROCT 1000 UI, poudre et solvant", docs)["denomination"]
        assert "1.000 UI" in match_presentation("CEPROTIN 1000 UI/10 ml, poudre et solvant", docs)["denomination"]

    def test_decimal_separator_spellings_are_equivalent(self):
        docs = [{"denomination": "Volibris 5 mg comprimés"}, {"denomination": "Volibris 2.5 mg comprimés"}]
        assert "2.5 mg" in match_presentation("VOLIBRIS 2,5 mg, comprimés pelliculés", docs)["denomination"]

    def test_trailing_zero_decimals_are_equivalent(self):
        # PDBM writes "EXELON 3 mg"; the PDF writes "Exelon 3,0 mg".
        docs = [{"denomination": "Exelon 1,5 mg gélule"}, {"denomination": "Exelon 3,0 mg gélule"}]
        assert "3,0 mg" in match_presentation("EXELON 3 mg, gélule", docs)["denomination"]

    def test_insulin_mix_brand_number_is_not_a_strength(self):
        # "Mix 50 100 UI/ml" = brand number 50 + strength 100, not "50100".
        docs = [
            {"denomination": "Humalog Mix25 100 unités/mL KwikPen suspension injectable en stylo pré-rempli"},
            {"denomination": "Humalog Mix50 100 unités/mL KwikPen suspension injectable en stylo pré-rempli"},
        ]
        matched = match_presentation("HUMALOG MIX 50 100 UI/ml KWIKPEN, suspension injectable", docs)
        assert matched is not None and "Mix50" in matched["denomination"]

    def test_mix_number_discriminates_at_equal_strength(self):
        # Insuman Comb 15 and Comb 25 are different medicines at the same 40 UI/ml.
        docs = [
            {"denomination": "Insuman Comb 25 40 UI/ml suspension injectable en flacon"},
            {"denomination": "Insuman Comb 15 40 UI/ml suspension injectable en flacon"},
        ]
        matched = match_presentation("INSUMAN COMB 15 40 UI/ml, suspension injectable en flacon", docs)
        assert "Comb 15" in matched["denomination"]

    def test_absent_mix_number_skips_the_cis(self):
        # The EU PDF covers Comb 25/50 at 40 UI/ml but not Comb 15: skip, don't guess.
        docs = [
            {"denomination": "Insuman Comb 25 40 UI/ml suspension injectable en flacon"},
            {"denomination": "Insuman Comb 50 40 UI/ml suspension injectable en flacon"},
        ]
        assert match_presentation("INSUMAN COMB 15 40 UI/ml, suspension injectable en flacon", docs) is None

    def test_welded_mix_number_still_discriminates(self):
        # PDBM spells it "MIX50", the PDF "Mix50" — no space, still a variant number.
        docs = [
            {"denomination": "Humalog Mix25 100 unités/mL suspension injectable en flacon"},
            {"denomination": "Humalog Mix50 100 unités/mL suspension injectable en flacon"},
        ]
        matched = match_presentation("HUMALOG MIX50 100 UI/ml, suspension injectable en flacon", docs)
        assert "Mix50" in matched["denomination"]

    def test_welded_number_never_empties_the_shortlist(self):
        # "B12" is part of the name; it must refine at most, never skip the CIS.
        docs = [{"denomination": "Cyano 100 microgrammes, solution injectable"}]
        assert match_presentation("CYANO B12 100 microgrammes, solution injectable", docs) is docs[0]

    def test_number_welded_into_a_word_is_not_a_variant(self):
        # "COVID-19" must not be read as a variant number and skip the CIS.
        docs = [
            {"denomination": "Spikevax 0,1 mg/ml, dispersion injectable"},
            {"denomination": "Spikevax 0,2 mg/mL, dispersion injectable"},
        ]
        name = "SPIKEVAX 0,2 mg/mL, dispersion injectable. Vaccin à ARNm contre la COVID-19"
        assert "0,2 mg" in match_presentation(name, docs)["denomination"]

    def test_no_matching_strength_skips_the_cis(self):
        # Celsentri 25 mg: the EU PDF only covers 150/300 mg. Serving those is worse
        # than serving nothing, so the CIS is skipped.
        docs = [
            {"denomination": "CELSENTRI 150 mg comprimés pelliculés"},
            {"denomination": "CELSENTRI 300 mg comprimés pelliculés"},
        ]
        assert match_presentation("CELSENTRI 25 mg, comprimé pelliculé", docs) is None

    def test_single_presentation_of_wrong_strength_is_skipped(self):
        docs = [{"denomination": "Foscan 1 mg/mL, solution injectable"}]
        assert match_presentation("FOSCAN 4 mg/ml, solution injectable", docs) is None

    def test_unextractable_denomination_is_not_dropped(self):
        # Section 1 failed to parse: no strength on the candidate side, so the
        # strength check stands down rather than silently discarding the content.
        docs = [{"denomination": "", "content": ["…"]}]
        assert match_presentation("TEPMETKO 225 mg comprimés pelliculés", docs) is docs[0]

    def test_unreadable_strength_falls_back_to_overlap(self):
        docs = [{"denomination": "Vaxo 1 mL seringue"}, {"denomination": "Vaxo 0,5 mL seringue préremplie"}]
        matched = match_presentation("VAXO 0,5 ml, suspension injectable en seringue préremplie", docs)
        assert "0,5 mL" in matched["denomination"]

    def test_single_doc_returned_unconditionally(self):
        only = {"denomination": "whatever", "content_html": "", "date_notif": None}
        assert match_presentation("unrelated name", [only]) is only

    def test_empty_name_with_multiple_presentations_skips(self):
        docs = [{"denomination": "5 mg comprimé"}, {"denomination": "10 mg comprimé"}]
        assert match_presentation("", docs) is None

    def test_empty_docs_returns_none(self):
        assert match_presentation("x", []) is None


class TestNoticeHeaderDetection:
    def test_matches_both_qrd_wordings(self):
        assert _is_notice_header("Notice: Information de l’utilisateur")  # Abasaglar
        assert _is_notice_header("Notice : Information du patient")  # Brintellix

    def test_rejects_non_headers(self):
        assert not _is_notice_header("Que contient cette notice?")
        assert not _is_notice_header("Gardez cette notice.")


# ---------------------------------------------------------------------------
# Annexe segmentation (golden, on the cartouche presentation)
# ---------------------------------------------------------------------------


class TestAnnexeSplit:
    def test_rcp_has_ten_top_level_sections(self, rcp):
        numbers = [h.split(".")[0] for h in _headings(rcp, "h2")]
        assert numbers == [str(i) for i in range(1, 11)]

    def test_notice_has_six_sections(self, notice):
        numbers = [h.split(".")[0] for h in _headings(notice, "h3")]
        assert numbers == [str(i) for i in range(1, 7)]

    def test_rcp_section4_has_subsections_4_1_to_4_9(self, rcp):
        subs = [heading for heading in _headings(rcp, "h3") if heading.startswith("4.")]
        assert [s.split()[0] for s in subs] == [f"4.{i}" for i in range(1, 10)]


# ---------------------------------------------------------------------------
# Anchor mapping
# ---------------------------------------------------------------------------


class TestAnchors:
    def test_rcp_subsection_anchors(self, rcp):
        anchors = {
            heading.get_text(" ", strip=True).split()[0]: heading["id"]
            for heading in rcp.find_all("h3")
            if heading.get_text(strip=True).startswith("4.")
        }
        assert anchors["4.1"] == "RcpIndicTherap"
        assert anchors["4.8"] == "RcpEffetsIndesirables"

    def test_notice_section_anchors(self, notice):
        anchors = {heading.get_text(strip=True).split(".")[0]: heading["id"] for heading in notice.find_all("h3")}
        assert anchors["1"] == "Ann3bQuestceque"
        assert anchors["2"] == "Ann3bInfoNecessaires"
        assert anchors["4"] == "Ann3bEffetsIndesirables"


# ---------------------------------------------------------------------------
# Metadata + tables + contract
# ---------------------------------------------------------------------------


class TestSemanticContract:
    def test_rcp_denomination_is_cartouche(self, parsed):
        assert "cartouche" in parsed["rcp"][0]["denomination"].lower()

    def test_records_expose_semantic_html_not_content_trees(self, parsed):
        for kind in ("rcp", "notice"):
            for document in parsed[kind]:
                assert "content" not in document
                assert document["content_html"].startswith("<")
                assert 'data-block-id="document-b0001"' in document["content_html"]
                html = BeautifulSoup(document["content_html"], "html.parser")
                assert {class_name for tag in html.find_all(True) for class_name in tag.get("class", [])} <= {
                    "fr-table",
                    "fr-table__wrapper",
                    "fr-table__container",
                    "fr-table__content",
                }

    def test_notice_has_document_heading_above_numbered_sections(self, notice):
        assert notice.h2.get_text(" ", strip=True).startswith("Notice:")
        assert notice.h3.get_text(" ", strip=True).startswith("1.")

    def test_table_rendered_in_section_4_8(self, rcp):
        heading = rcp.find("h3", id="RcpEffetsIndesirables")
        assert heading is not None
        table = heading.find_next("table")
        assert table is not None
        assert table.find("td").get_text(strip=True)

    def test_notice_indication_is_extracted_and_marked(self, parsed):
        document = parsed["notice"][0]
        assert document["indication"]
        html = BeautifulSoup(document["content_html"], "html.parser")
        assert html.find(attrs={"data-document-role": "indication"})

    def test_glossary_terms_use_the_shared_semantic_annotation(self):
        result = parse_pdf(ABASAGLAR_PDF.read_bytes(), glossary_terms=["insuline"])
        assert '<span data-definition="insuline">insuline</span>' in result["rcp"][0]["content_html"]

    def test_complete_dates_are_normalised_without_inventing_partial_dates(self):
        assert _normalise_date("09 septembre 2014") == "2014-09-09"
        assert _normalise_date("09/09/2014") == "2014-09-09"
        assert _normalise_date("07/2020") is None

    def test_mixed_inline_bold_is_preserved(self, notice):
        emphasis = notice.find("strong", string="Prenez contact avec un médecin rapidement")

        assert emphasis is not None
        assert emphasis.parent.name in {"p", "li"}
        assert "Dans la plupart des cas" in emphasis.parent.get_text()

    def test_pdf_underlines_are_preserved(self, rcp):
        underline = rcp.find("u", string="Posologie")

        assert underline is not None
        assert underline.parent.name == "p"


def _image_nodes(parsed):
    return [
        image
        for kind in ("rcp", "notice")
        for document in parsed[kind]
        for image in BeautifulSoup(document["content_html"], "html.parser").find_all("img")
    ]


class TestImages:
    def test_blobs_extracted_and_content_addressed(self, parsed):
        images = parsed["images"]
        assert images  # the Abasaglar notices carry injection diagrams
        for key in images:
            assert key.startswith("exports/images/centralise/")  # default image_prefix

    def test_image_nodes_reference_uploaded_blobs(self, parsed):
        nodes = _image_nodes(parsed)
        assert nodes
        keys = parsed["images"]
        for image in nodes:
            assert image.parent.name == "figure"
            sha = image["src"].split("/centralise/")[1].rsplit(".", 1)[0]
            assert any(sha in k for k in keys), "image URL must point at an uploaded blob"
