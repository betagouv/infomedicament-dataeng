"""Tests for pediatric classification module."""

from infomedicament_dataeng.pediatric import (
    _is_heading_label,
    classify,
    extract_section_texts,
    find_pediatric_keywords_in_text,
    is_adult_reserved,
    load_ground_truth,
    matches_negative_pattern,
    parse_explicit_age_range,
    text_block_covers_age,
)

# test helper functions


class TestIsHeadingLabel:
    # --- should be filtered out ---
    def test_population_pediatrique(self):
        assert _is_heading_label("Population pédiatrique")

    def test_population_pediatrique_plural(self):
        assert _is_heading_label("Populations pédiatriques")

    def test_populations_particulieres(self):
        assert _is_heading_label("Populations particulières")

    def test_posologie(self):
        assert _is_heading_label("Posologie")

    def test_mode_administration(self):
        assert _is_heading_label("Mode d'administration")

    def test_duree_traitement(self):
        assert _is_heading_label("Durée du traitement")

    def test_age_subgroup_less_than_with_weight(self):
        assert _is_heading_label("Enfants de moins de 6 ans et de moins de 20 kg")

    def test_age_subgroup_range_with_weight(self):
        assert _is_heading_label("Enfants de 6 à 11 ans pesant au moins 20 kg")

    def test_adults_and_adolescents(self):
        assert _is_heading_label("Adultes et adolescents (12 ans et plus)")

    def test_children_and_adolescents_range(self):
        assert _is_heading_label("Enfants et adolescents (7-17 ans)")

    def test_enfants_ages_moins_de(self):
        assert _is_heading_label("Enfants âgés de moins de 7 ans")

    def test_case_insensitive(self):
        assert _is_heading_label("POPULATION PÉDIATRIQUE")

    # --- should NOT be filtered out ---
    def test_clinical_sentence_not_matched(self):
        assert not _is_heading_label(
            "Aucune donnée n'étant disponible avec le bisoprolol en pédiatrie, "
            "son utilisation ne peut donc être recommandée chez les patients pédiatriques."
        )

    def test_clinical_heading_not_matched(self):
        assert not _is_heading_label("Réservé au nourrisson et à l'enfant de plus de 3 mois")

    def test_partial_match_not_enough(self):
        assert not _is_heading_label("Voir rubrique Population pédiatrique ci-dessous")


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
        rcp = make_rcp(
            sections={
                "4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is True
        assert len(result.matches_41_42) > 0
        assert "keyword positif en 4.1/4.2" in result.a_reasons

    def test_negative_pattern_gives_c(self, make_rcp):
        """Keyword + negative pattern in 4.2 → C=True, A=False."""
        rcp = make_rcp(
            sections={
                "4.2": ["La sécurité et l'efficacité n'ont pas été étudiées chez les enfants"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is False
        assert result.a_reasons == []
        assert result.condition_c is True

    def test_keyword_without_indication_gives_c(self, make_rcp, monkeypatch):
        """Keyword present but no indication phrase → C=True, A=False."""
        monkeypatch.setattr("infomedicament_dataeng.pediatric_config.REQUIRE_POSITIVE_INDICATION", True)
        rcp = make_rcp(
            sections={
                "4.1": ["Posologie chez l'enfant de plus de 6 ans : 10 mg/jour"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is False
        assert result.condition_c is True
        assert "keyword sans indication explicite en 4.1/4.2" in result.c_reasons

    def test_no_keyword_gives_c(self, make_rcp):
        """No pediatric keyword at all → C=True."""
        rcp = make_rcp(
            sections={
                "4.1": ["Traitement de l'hypertension artérielle"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is False
        assert result.condition_c is True
        assert "pas de mention pédiatrique en 4.1/4.2" in result.c_reasons

    def test_contraindication_in_43(self, make_rcp):
        """Keyword in 4.3 → B=True."""
        rcp = make_rcp(
            sections={
                "4.1": ["Ce médicament est indiqué chez l'enfant"],
                "4.3": ["Contre-indiqué chez le nourrisson de moins de 3 mois"],
            }
        )
        result = classify(rcp)
        assert result.condition_b is True
        assert len(result.matches_43) > 0
        assert "mention pédiatrique en 4.3" in result.b_reasons

    def test_adult_reserved(self, make_rcp):
        """'réservé à l'adulte' in 4.1 → C=True."""
        rcp = make_rcp(
            sections={
                "4.1": ["Ce médicament est réservé à l'adulte"],
            }
        )
        result = classify(rcp)
        assert result.condition_c is True
        assert "réservé à l'adulte" in result.c_reasons

    def test_contraceptive_atc(self, make_rcp):
        """ATC G03A → C=True."""
        rcp = make_rcp(
            sections={
                "4.1": ["Contraception orale"],
            }
        )
        result = classify(rcp, atc_code="G03AA07")
        assert result.condition_c is True
        assert "contraceptif (ATC G03)" in result.c_reasons

    def test_a_and_b_together(self, make_rcp):
        """Indication in 4.1 + contraindication in 4.3 → A=True and B=True."""
        rcp = make_rcp(
            sections={
                "4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"],
                "4.3": ["Contre-indiqué chez l'enfant de moins de 6 ans"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is True
        assert result.condition_b is True
        assert "keyword positif en 4.1/4.2" in result.a_reasons
        assert "mention pédiatrique en 4.3" in result.b_reasons

    def test_c_overrides_a(self, make_rcp, monkeypatch):
        """C overrides A, but B overrides C."""
        monkeypatch.setattr(
            "infomedicament_dataeng.pediatric_config.TIE_BREAKER_PRIORITY", {"AC": "C", "BC": "B", "ABC": "B"}
        )
        rcp = make_rcp(
            sections={
                "4.1": [
                    "Ce médicament est indiqué chez l'enfant de plus de 6 ans",
                    "La sécurité et l'efficacité n'ont pas été étudiées chez les enfants",
                ],
                "4.3": ["Contre-indiqué chez le nourrisson"],
            }
        )
        result = classify(rcp)
        assert result.condition_a is False  # C overrode A
        assert result.condition_b is True  # B overrides C
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
        rcp = make_rcp(
            sections={
                "4.2": ["Posologie chez l'adulte", "Posologie chez l'enfant"],
            }
        )
        texts = extract_section_texts(rcp, "4.2")
        assert len(texts) == 2

    def test_nonexistent_section(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Some text"]})
        assert extract_section_texts(rcp, "99.99") == []

    def test_heading_only_titles_skipped(self):
        """Generic subsection headings like 'Population pédiatrique' are skipped."""
        rcp = {
            "source": {"cis": "12345"},
            "content": [
                {
                    "type": "AmmAnnexeTitre1",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.2 Posologie",
                            "children": [
                                {"type": "AmmAnnexeTitre3", "content": "Population pédiatrique"},
                                {"type": "AmmCorpsTexte", "content": "Sans objet"},
                            ],
                        }
                    ],
                }
            ],
        }
        texts = extract_section_texts(rcp, "4.2")
        assert "Population pédiatrique" not in texts
        assert "Sans objet" in texts

    def test_heading_label_in_corps_texte_gras_skipped(self):
        """'Population pédiatrique' as AmmCorpsTexteGras (bold label) is also skipped."""
        rcp = {
            "source": {"cis": "12345"},
            "content": [
                {
                    "type": "AmmAnnexeTitre1",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.2 Posologie",
                            "children": [
                                {"type": "AmmCorpsTexteGras", "content": "Population pédiatrique"},
                                {
                                    "type": "AmmCorpsTexte",
                                    "content": "Aucune donnée n'étant disponible, l'utilisation n'est pas recommandée.",
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        texts = extract_section_texts(rcp, "4.2")
        assert "Population pédiatrique" not in texts
        assert "Aucune donnée n'étant disponible, l'utilisation n'est pas recommandée." in texts


class TestParseExplicitAgeRange:
    def test_moins_de_N_ans(self):
        assert parse_explicit_age_range("moins de 6 ans") == (0, 5)

    def test_moins_de_N_ans_in_sentence(self):
        assert parse_explicit_age_range("patients âgés de moins de 12 ans") == (0, 11)

    def test_a_partir_de_N_ans(self):
        assert parse_explicit_age_range("à partir de 12 ans") == (12, 17)

    def test_plus_de_N_ans(self):
        assert parse_explicit_age_range("plus de 15 ans") == (15, 17)

    def test_N_ans_et_plus(self):
        assert parse_explicit_age_range("12 ans et plus") == (12, 17)

    def test_de_N_a_M_ans(self):
        assert parse_explicit_age_range("de 6 à 11 ans") == (6, 11)

    def test_N_a_M_ans(self):
        assert parse_explicit_age_range("6 à 11 ans") == (6, 11)

    def test_N_tiret_M_ans(self):
        assert parse_explicit_age_range("7-17 ans") == (7, 17)

    def test_lt_N_ans(self):
        assert parse_explicit_age_range("< 6 ans") == (0, 5)

    def test_lte_N_ans(self):
        assert parse_explicit_age_range("<= 6 ans") == (0, 6)

    def test_gte_N_ans(self):
        assert parse_explicit_age_range(">= 12 ans") == (12, 17)

    def test_moins_de_N_mois(self):
        assert parse_explicit_age_range("moins de 6 mois") == (0, 0)

    def test_moins_de_24_mois(self):
        assert parse_explicit_age_range("moins de 24 mois") == (0, 0)

    def test_no_match_keyword_only(self):
        assert parse_explicit_age_range("indiqué chez l'enfant") is None

    def test_no_match_empty(self):
        assert parse_explicit_age_range("") is None


class TestTextBlockCoversAge:
    def test_explicit_range_covers(self):
        assert text_block_covers_age("indiqué de 6 à 11 ans", 8)

    def test_explicit_range_excludes(self):
        assert not text_block_covers_age("indiqué de 6 à 11 ans", 12)

    def test_explicit_range_overrides_broad_keyword(self):
        # "enfant" alone would cover 0-17, but explicit range narrows it
        assert not text_block_covers_age("enfants de 6 à 11 ans : 10 mg/jour", 4)

    def test_keyword_nourrisson_covers(self):
        assert text_block_covers_age("chez le nourrisson", 1)

    def test_keyword_nourrisson_excludes(self):
        assert not text_block_covers_age("chez le nourrisson", 5)

    def test_keyword_adolescent_covers(self):
        assert text_block_covers_age("chez l'adolescent", 14)

    def test_keyword_adolescent_excludes(self):
        assert not text_block_covers_age("chez l'adolescent", 10)

    def test_keyword_enfant_covers_all_pediatric(self):
        assert text_block_covers_age("chez l'enfant", 5)

    def test_keyword_nouveau_ne_covers_neonate(self):
        assert text_block_covers_age("chez le nouveau-né", 0)

    def test_keyword_nouveau_ne_excludes_older(self):
        assert not text_block_covers_age("chez le nouveau-né", 1)

    def test_no_pediatric_signal(self):
        assert not text_block_covers_age("traitement de l'hypertension artérielle", 5)


class TestClassifyWithAge:
    def test_age_filters_in_matching_block(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["enfants de 6 à 11 ans : 10 mg/jour"]})
        assert classify(rcp, age=8).condition_a is True

    def test_age_filters_out_non_matching_block(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["enfants de 6 à 11 ans : 10 mg/jour"]})
        result = classify(rcp, age=12)
        assert result.condition_a is False
        assert result.condition_c is True

    def test_age_none_keeps_existing_behavior(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Ce médicament est indiqué chez l'enfant de plus de 6 ans"]})
        assert classify(rcp, age=None).condition_a is True

    def test_age_filters_43_in_range(self, make_rcp):
        rcp = make_rcp(sections={"4.3": ["Contre-indiqué chez le nourrisson"]})
        assert classify(rcp, age=0).condition_b is True

    def test_age_filters_43_out_of_range(self, make_rcp):
        rcp = make_rcp(sections={"4.3": ["Contre-indiqué chez le nourrisson"]})
        result = classify(rcp, age=5)
        assert result.condition_b is False

    def test_adolescent_keyword_covers_matching_age(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Ce médicament est indiqué chez l'adolescent"]})
        assert classify(rcp, age=14).condition_a is True

    def test_adolescent_keyword_excludes_young_child(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["Ce médicament est indiqué chez l'adolescent"]})
        result = classify(rcp, age=8)
        assert result.condition_a is False
        assert result.condition_c is True

    def test_neonate_age_0(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["indiqué chez le nouveau-né"]})
        assert classify(rcp, age=0).condition_a is True

    def test_neonate_keyword_excludes_age_2(self, make_rcp):
        rcp = make_rcp(sections={"4.1": ["indiqué chez le nouveau-né"]})
        result = classify(rcp, age=2)
        assert result.condition_a is False
        assert result.condition_c is True


# ground truth loading
class TestLoadGroundTruth:
    def test_loads_correctly(self, ground_truth_csv):
        gt = load_ground_truth(str(ground_truth_csv))
        assert len(gt) == 2
        assert gt["12345"]["A"] is True
        assert gt["12345"]["B"] is False
        assert gt["67890"]["C"] is True
