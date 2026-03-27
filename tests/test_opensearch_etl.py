from infomedicament_dataeng.opensearch.sections import _extract_text, _iter_section_docs


class TestExtractText:
    def test_flat_string_content(self):
        block = {"content": "Contre-indications"}
        assert _extract_text(block) == "Contre-indications"

    def test_list_content(self):
        block = {"content": ["· Ligne 1", "· Ligne 2"]}
        assert _extract_text(block) == "· Ligne 1 · Ligne 2"

    def test_nested_children(self):
        block = {
            "content": "Section titre",
            "children": [
                {"content": "Paragraphe enfant"},
                {"content": ["Élément A", "Élément B"]},
            ],
        }
        result = _extract_text(block)
        assert "Section titre" in result
        assert "Paragraphe enfant" in result
        assert "Élément A" in result
        assert "Élément B" in result

    def test_empty_block(self):
        assert _extract_text({}) == ""

    def test_empty_children_ignored(self):
        block = {"content": "Texte", "children": [{"content": ""}, {}]}
        assert _extract_text(block) == "Texte"

    def test_deeply_nested(self):
        block = {
            "content": "Niveau 1",
            "children": [
                {
                    "content": "Niveau 2",
                    "children": [{"content": "Niveau 3"}],
                }
            ],
        }
        result = _extract_text(block)
        assert "Niveau 1" in result
        assert "Niveau 2" in result
        assert "Niveau 3" in result


class TestIterSectionDocs:
    def _make_record(self, cis, blocks):
        return {"source": {"cis": cis}, "content": blocks}

    def test_skips_amm_annexe_titre(self):
        record = self._make_record(
            "123",
            [
                {"type": "AmmAnnexeTitre", "content": "NOTICE"},
                {"type": "AmmNoticeTitre1", "content": "Contre-indications", "anchor": "Ann3bContreIndic"},
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {"123": "DOLIPRANE"}))
        assert len(docs) == 1
        assert docs[0]["section_type"] == "AmmNoticeTitre1"

    def test_captures_date_notif_as_metadata(self):
        record = self._make_record(
            "123",
            [
                {"type": "DateNotif", "content": "ANSM - Mis à jour le : 06/03/2013"},
                {"type": "AmmNoticeTitre1", "content": "Posologie", "anchor": "Ann3bPosologie"},
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {"123": "DOLIPRANE"}))
        assert len(docs) == 1
        assert docs[0]["date_notif"] == "ANSM - Mis à jour le : 06/03/2013"

    def test_date_notif_not_yielded_as_doc(self):
        record = self._make_record(
            "123",
            [{"type": "DateNotif", "content": "ANSM - Mis à jour le : 06/03/2013"}],
        )
        docs = list(_iter_section_docs(record, "notice", {}))
        assert docs == []

    def test_yields_one_doc_per_section(self):
        record = self._make_record(
            "456",
            [
                {"type": "AmmNoticeTitre1", "content": "Section 1", "anchor": "s1"},
                {"type": "AmmNoticeTitre1", "content": "Section 2", "anchor": "s2"},
                {"type": "AmmNoticeTitre1", "content": "Section 3", "anchor": "s3"},
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {"456": "ADVIL"}))
        assert len(docs) == 3

    def test_enriches_spec_name_from_lookup(self):
        record = self._make_record(
            "789",
            [{"type": "AmmNoticeTitre1", "content": "Posologie", "anchor": "x"}],
        )
        docs = list(_iter_section_docs(record, "notice", {"789": "PARACETAMOL 1g"}))
        assert docs[0]["spec_name"] == "PARACETAMOL 1g"

    def test_unknown_cis_empty_spec_name(self):
        record = self._make_record(
            "999",
            [{"type": "AmmNoticeTitre1", "content": "Posologie", "anchor": "x"}],
        )
        docs = list(_iter_section_docs(record, "notice", {}))
        assert docs[0]["spec_name"] == ""

    def test_doc_fields_present(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmNoticeTitre1",
                    "content": "Contre-indications",
                    "anchor": "Ann3bContreIndic",
                    "children": [{"content": "Ne pas utiliser en cas d'allergie."}],
                }
            ],
        )
        doc = list(_iter_section_docs(record, "notice", {"123": "IBUPROFENE"}))[0]
        assert doc["cis_code"] == "123"
        assert doc["doc_type"] == "notice"
        assert doc["section_anchor"] == "Ann3bContreIndic"
        assert doc["section_title"] == "Contre-indications"
        assert "allergie" in doc["text_content"]

    def test_skips_blocks_with_no_text(self):
        record = self._make_record(
            "123",
            [
                {"type": "AmmNoticeTitre1", "content": "", "anchor": "empty"},
                {"type": "AmmNoticeTitre1", "content": "Posologie", "anchor": "pos"},
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {}))
        assert len(docs) == 1
        assert docs[0]["section_anchor"] == "pos"

    def test_rcp_yields_subsections_instead_of_parent(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4. DONNEES CLINIQUES",
                    "anchor": "RcpDonneesCliniques",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.2. Posologie",
                            "anchor": "RcpPosoAdmin",
                            "children": [{"content": "Adulte : 1g"}],
                        },
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.3. Contre-indications",
                            "anchor": "RcpContreindications",
                            "children": [{"content": "Allergie connue"}],
                        },
                    ],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {"123": "DOLIPRANE"}))
        assert len(docs) == 2
        anchors = {d["section_anchor"] for d in docs}
        assert anchors == {"RcpPosoAdmin", "RcpContreindications"}

    def test_rcp_subsection_title_from_child(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4. DONNEES CLINIQUES",
                    "anchor": "RcpDonneesCliniques",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.2. Posologie",
                            "anchor": "RcpPosoAdmin",
                            "children": [{"content": "Adulte : 1g"}],
                        },
                    ],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_title"] == "4.2. Posologie"

    def test_rcp_subsection_text_is_child_only(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4. DONNEES CLINIQUES",
                    "anchor": "RcpDonneesCliniques",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.2. Posologie",
                            "anchor": "RcpPosoAdmin",
                            "children": [{"content": "posologie text"}],
                        },
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "4.3. Contre-indications",
                            "anchor": "RcpContreindications",
                            "children": [{"content": "contreindication text"}],
                        },
                    ],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        poso_doc = next(d for d in docs if d["section_anchor"] == "RcpPosoAdmin")
        assert "posologie text" in poso_doc["text_content"]
        assert "contreindication text" not in poso_doc["text_content"]

    def test_parent_without_subsection_children_yields_normally(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "Dénomination",
                    "anchor": "RcpDenomination",
                    "children": [{"type": "AmmCorpsTexte", "content": "DOLIPRANE 1g"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert len(docs) == 1
        assert docs[0]["section_anchor"] == "RcpDenomination"
        assert "DOLIPRANE 1g" in docs[0]["text_content"]

    def test_anchor_alias_normalized(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "Contre-indications",
                    "anchor": "RcpContreIndic",
                    "children": [{"content": "Allergie"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "RcpContreindications"

    def test_toc_anchor_normalized_via_section_number(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4.1. Indications thérapeutiques",
                    "anchor": "_Toc999",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "RcpIndicTherap"

    def test_toc_anchor_normalized_with_encoding_artifacts(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4.1.ÂÂÂÂÂ Indications thérapeutiques",
                    "anchor": "_Toc999",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "RcpIndicTherap"

    def test_toc_anchor_body_text_title_unchanged(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "DANS LA PRESENTE ANNEXE LES TERMES...",
                    "anchor": "_Toc999",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "_Toc999"

    def test_toc_anchor_not_normalized_for_notice(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmNoticeTitre1",
                    "content": "4.1. Indications thérapeutiques",
                    "anchor": "_Toc999",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        # Notices don't use numbered sections — no RCP number mapping should apply
        docs = list(_iter_section_docs(record, "notice", {}))
        assert docs[0]["section_anchor"] != "RcpIndicTherap"

    def test_toc_anchor_normalized_for_notice_by_title(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmNoticeTitre1",
                    "content": "Posologie",
                    "anchor": "_Toc999",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {}))
        assert docs[0]["section_anchor"] == "Ann3bPosologie"

    def test_hlk_anchor_normalized_same_as_toc(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4.8. Effets indésirables",
                    "anchor": "_Hlk160213897",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "RcpEffetsIndesirables"

    def test_toc_anchor_normalized_for_notice_by_number(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "2. QUELLES SONT LES INFORMATIONS A CONNAITRE AVANT DE PRENDRE CE MEDICAMENT ?",
                    "anchor": "_Toc142279004",
                    "children": [{"content": "texte"}],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "notice", {}))
        assert docs[0]["section_anchor"] == "Ann3bInfoNecessaires"

    def test_subsection_anchor_alias_normalized(self):
        record = self._make_record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "content": "4. DONNEES CLINIQUES",
                    "anchor": "RcpDonneesCliniques",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "content": "Grossesse",
                            "anchor": "RcpGrossAllait",
                            "children": [{"content": "Déconseillé"}],
                        },
                    ],
                }
            ],
        )
        docs = list(_iter_section_docs(record, "rcp", {}))
        assert docs[0]["section_anchor"] == "RcpFertGrossAllait"
