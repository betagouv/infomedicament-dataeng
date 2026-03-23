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
