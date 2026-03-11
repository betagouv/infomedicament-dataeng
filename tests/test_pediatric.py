"""Tests for pediatric classification module."""

from infomedicament_dataeng.pediatric import (
    classify,
    extract_section_texts,
    find_pediatric_keywords_in_text,
    is_adult_reserved,
    load_ground_truth,
    matches_negative_pattern,
)


# test helper functions

class TestFindPediatricKeywords:
    def test_finds_simple_keyword(self):
        assert "enfant" in find_pediatric_keywords_in_text("chez l'enfant de plus de 6 ans")

    def test_finds_multiple_keywords(self):
        text = "nourrissons et enfants de moins de 12 ans"
        keywords = find_pediatric_keywords_in_text(text)
        assert "nourrisson" in keywords or "nourrissons" in keywords
        assert "enfant" in keywords or "enfants" in keywords

    def test_finds_age_pattern(self):
        keywords = find_pediatric_keywords_in_text("patients âgés de moins de 12 ans")
        assert any("ans" in kw for kw in keywords)

    def test_finds_age_months_over_17(self):
        keywords = find_pediatric_keywords_in_text("enfants âgés de moins de 24 mois")
        assert any("mois" in kw for kw in keywords)

    def test_finds_age_months_36(self):
        keywords = find_pediatric_keywords_in_text("nourrissons âgés de 36 mois")
        assert any("mois" in kw for kw in keywords)

    def test_no_match_adult_only(self):
        assert find_pediatric_keywords_in_text("réservé à l'adulte") == []

    def test_empty_text(self):
        assert find_pediatric_keywords_in_text("") == []


class TestMatchesNegativePattern:
    def test_matches_ne_doit_pas(self):
        text = "Ce médicament ne doit pas être utilisé chez les enfants"
        assert matches_negative_pattern(text) is not None

    def test_matches_securite_efficacite(self):
        text = "La sécurité et l'efficacité n'ont pas été étudiées chez les enfants"
        assert matches_negative_pattern(text) is not None

    def test_matches_pas_recommande(self):
        text = "L'utilisation n'est pas recommandée chez l'enfant"
        assert matches_negative_pattern(text) is not None

    def test_matches_sans_objet(self):
        assert matches_negative_pattern("Sans objet") is not None

    def test_matches_aucune_donnee(self):
        text = "Aucune donnée n'est disponible en pédiatrie"
        assert matches_negative_pattern(text) is not None

    def test_no_match_positive(self):
        text = "Ce médicament est indiqué chez l'enfant de plus de 6 ans"
        assert matches_negative_pattern(text) is None


class TestIsAdultReserved:
    def test_matches(self):
        assert is_adult_reserved("Ce médicament est réservé à l'adulte")

    def test_no_match(self):
        assert not is_adult_reserved("Ce médicament est indiqué chez l'enfant")


# classify tests

class TestClassify:
    def test_positive_indication(self, make_rcp):
        """Keyword in 4.1 without negative pattern → A=True."""
        rcp = make_rcp(sections={
            "4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"],
        })
        result = classify(rcp)
        assert result.condition_a is True
        assert len(result.matches_41_42) > 0
        assert "keyword positif en 4.1/4.2" in result.a_reasons

    def test_negative_pattern_gives_c(self, make_rcp):
        """Keyword + negative pattern in 4.2 → C=True, A=False."""
        rcp = make_rcp(sections={
            "4.2": ["La sécurité et l'efficacité n'ont pas été étudiées chez les enfants"],
        })
        result = classify(rcp)
        assert result.condition_a is False
        assert result.a_reasons == []
        assert result.condition_c is True

    def test_keyword_without_indication_gives_c(self, make_rcp, monkeypatch):
        """Keyword present but no indication phrase → C=True, A=False."""
        monkeypatch.setattr("infomedicament_dataeng.pediatric_config.REQUIRE_POSITIVE_INDICATION", True)
        rcp = make_rcp(sections={
            "4.1": ["Posologie chez l'enfant de plus de 6 ans : 10 mg/jour"],
        })
        result = classify(rcp)
        assert result.condition_a is False
        assert result.condition_c is True
        assert "keyword sans indication explicite en 4.1/4.2" in result.c_reasons

    def test_no_keyword_gives_c(self, make_rcp):
        """No pediatric keyword at all → C=True."""
        rcp = make_rcp(sections={
            "4.1": ["Traitement de l'hypertension artérielle"],
        })
        result = classify(rcp)
        assert result.condition_a is False
        assert result.condition_c is True
        assert "pas de mention pédiatrique en 4.1/4.2" in result.c_reasons

    def test_contraindication_in_43(self, make_rcp):
        """Keyword in 4.3 → B=True."""
        rcp = make_rcp(sections={
            "4.1": ["Ce médicament est indiqué chez l'enfant"],
            "4.3": ["Contre-indiqué chez le nourrisson de moins de 3 mois"],
        })
        result = classify(rcp)
        assert result.condition_b is True
        assert len(result.matches_43) > 0
        assert "mention pédiatrique en 4.3" in result.b_reasons

    def test_adult_reserved(self, make_rcp):
        """'réservé à l'adulte' in 4.1 → C=True."""
        rcp = make_rcp(sections={
            "4.1": ["Ce médicament est réservé à l'adulte"],
        })
        result = classify(rcp)
        assert result.condition_c is True
        assert "réservé à l'adulte" in result.c_reasons

    def test_contraceptive_atc(self, make_rcp):
        """ATC G03A → C=True."""
        rcp = make_rcp(sections={
            "4.1": ["Contraception orale"],
        })
        result = classify(rcp, atc_code="G03AA07")
        assert result.condition_c is True
        assert "contraceptif (ATC G03)" in result.c_reasons

    def test_a_and_b_together(self, make_rcp):
        """Indication in 4.1 + contraindication in 4.3 → A=True and B=True."""
        rcp = make_rcp(sections={
            "4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"],
            "4.3": ["Contre-indiqué chez l'enfant de moins de 6 ans"],
        })
        result = classify(rcp)
        assert result.condition_a is True
        assert result.condition_b is True
        assert "keyword positif en 4.1/4.2" in result.a_reasons
        assert "mention pédiatrique en 4.3" in result.b_reasons

    def test_c_overrides_a(self, make_rcp, monkeypatch):
        """C overrides A, but B overrides C."""
        monkeypatch.setattr("infomedicament_dataeng.pediatric_config.TIE_BREAKER_PRIORITY", {"AC": "C", "BC": "B", "ABC": "B"})
        rcp = make_rcp(sections={
            "4.1": [
                "Ce médicament est indiqué chez l'enfant de plus de 6 ans",
                "La sécurité et l'efficacité n'ont pas été étudiées chez les enfants",
            ],
            "4.3": ["Contre-indiqué chez le nourrisson"],
        })
        result = classify(rcp)
        assert result.condition_a is False  # C overrode A
        assert result.condition_b is True   # B overrides C
        assert result.condition_c is False  # B overrode C
        # Reasons/matches are still populated for traceability
        assert "keyword positif en 4.1/4.2" in result.a_reasons
        assert "mention pédiatrique en 4.3" in result.b_reasons
        assert "phrases négatives en 4.1/4.2" in result.c_reasons

    def test_empty_rcp(self, make_rcp):
        """Empty RCP → C=True (no keywords)."""
        rcp = make_rcp(sections={})
        result = classify(rcp)
        assert result.condition_a is False
        assert result.condition_b is False
        assert result.condition_c is True
        assert result.a_reasons == []
        assert result.b_reasons == []


# extract_section_texts tests

class TestExtractSectionTexts:
    def test_extracts_section(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Indiqué chez l'adulte"]})
        texts = extract_section_texts(rcp, "4.1")
        assert texts == ["Indiqué chez l'adulte"]

    def test_extracts_multiple_texts(self, make_rcp):
        rcp = make_rcp(sections={
            "4.2": ["Posologie chez l'adulte", "Posologie chez l'enfant"],
        })
        texts = extract_section_texts(rcp, "4.2")
        assert len(texts) == 2

    def test_nonexistent_section(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Some text"]})
        assert extract_section_texts(rcp, "99.99") == []

    def test_heading_only_titles_skipped(self):
        """Generic subsection headings like 'Population pédiatrique' are skipped."""
        rcp = {
            "source": {"cis": "12345"},
            "content": [{
                "type": "AmmAnnexeTitre1",
                "children": [{
                    "type": "AmmAnnexeTitre2",
                    "content": "4.2 Posologie",
                    "children": [
                        {"type": "AmmAnnexeTitre3", "content": "Population pédiatrique"},
                        {"type": "AmmCorpsTexte", "content": "Sans objet"},
                    ],
                }],
            }],
        }
        texts = extract_section_texts(rcp, "4.2")
        assert "Population pédiatrique" not in texts
        assert "Sans objet" in texts


# ground truth loading

class TestLoadGroundTruth:
    def test_loads_correctly(self, ground_truth_csv):
        gt = load_ground_truth(str(ground_truth_csv))
        assert len(gt) == 2
        assert gt["12345"]["A"] is True
        assert gt["12345"]["B"] is False
        assert gt["67890"]["C"] is True
