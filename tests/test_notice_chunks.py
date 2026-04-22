from infomedicament_dataeng.opensearch.notice_chunks import _iter_notice_chunks, _make_embed_text, _node_text


def _record(cis, content):
    return {"source": {"cis": cis}, "content": content}


class TestNodeText:
    def test_string(self):
        assert _node_text("texte") == "texte"

    def test_strips_whitespace(self):
        assert _node_text("  texte  ") == "texte"

    def test_list_joined(self):
        assert _node_text(["a", "b", "c"]) == "a b c"

    def test_none_returns_empty(self):
        assert _node_text(None) == ""

    def test_empty_string_returns_empty(self):
        assert _node_text("") == ""

    def test_empty_list_returns_empty(self):
        assert _node_text([]) == ""


class TestMakeEmbedText:
    def test_with_sub_header(self):
        assert (
            _make_embed_text("Section 2", "Avec de l'alcool", "Sans objet.")
            == "Section 2 > Avec de l'alcool: Sans objet."
        )

    def test_without_sub_header(self):
        assert (
            _make_embed_text("Section 1", "", "Ce médicament est un antibiotique.")
            == "Section 1: Ce médicament est un antibiotique."
        )


class TestIterNoticeChunks:
    # top-level skip types

    def test_skips_amm_annexe_titre(self):
        assert list(_iter_notice_chunks(_record("1", [{"type": "AmmAnnexeTitre", "content": "NOTICE"}]))) == []

    def test_skips_date_notif(self):
        assert list(_iter_notice_chunks(_record("1", [{"type": "DateNotif", "content": "06/03/2013"}]))) == []

    def test_amm_notice_titre1_without_children_yields_nothing(self):
        assert (
            list(
                _iter_notice_chunks(
                    _record(
                        "1",
                        [{"type": "AmmNoticeTitre1", "content": "Que contient cette notice ?", "anchor": "Ann3bSomm"}],
                    )
                )
            )
            == []
        )

    def test_amm_notice_titre1_with_children_yields_chunks(self):
        record = _record(
            "1",
            [
                {
                    "type": "AmmNoticeTitre1",
                    "content": "Contre-indications",
                    "anchor": "Ann3bContreIndic",
                    "children": [{"type": "AmmCorpsTexte", "content": "Ne pas utiliser en cas d'allergie."}],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 1
        assert chunks[0]["section_title"] == "Contre-indications"
        assert "allergie" in chunks[0]["text"]

    # anchor skips

    def test_skips_ann3b_emballage(self):
        record = _record(
            "1",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bEmballage",
                    "content": "Contenu de l'emballage",
                    "children": [{"type": "AmmCorpsTexte", "content": "Informations administratives."}],
                }
            ],
        )
        assert list(_iter_notice_chunks(record)) == []

    # flat section : basic chunk output

    def test_flat_section_no_bold_yields_one_chunk(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [{"type": "AmmCorpsTexte", "content": "Conserver en dessous de 25°C."}],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 1
        assert chunks[0]["section_title"] == "5. Conservation"
        assert chunks[0]["sub_header"] == ""
        assert "25°C" in chunks[0]["text"]

    def test_flat_section_bold_before_body_creates_two_chunks(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [
                        {"type": "AmmCorpsTexte", "content": "Texte avant le premier gras."},
                        {"type": "AmmCorpsTexteGras", "content": "Avec de l'alcool"},
                        {"type": "AmmCorpsTexte", "content": "Sans objet."},
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 2
        assert chunks[0]["sub_header"] == ""
        assert "Texte avant" in chunks[0]["text"]
        assert chunks[1]["sub_header"] == "Avec de l'alcool"
        assert "Sans objet" in chunks[1]["text"]

    def test_flat_section_multiple_bold_headers(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [
                        {"type": "AmmCorpsTexteGras", "content": "Avec de l'alcool"},
                        {"type": "AmmCorpsTexte", "content": "Sans objet."},
                        {"type": "AmmCorpsTexteGras", "content": "Avec de la nourriture"},
                        {"type": "AmmCorpsTexte", "content": "Avec ou sans nourriture."},
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 2
        assert chunks[0]["sub_header"] == "Avec de l'alcool"
        assert chunks[1]["sub_header"] == "Avec de la nourriture"

    def test_amm_annexe_titre3_also_splits_chunk(self):
        """AmmAnnexeTitre3 must behave like AmmCorpsTexteGras as a chunk boundary."""
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bEffetsIndesirables",
                    "content": "4. Effets indésirables",
                    "children": [
                        {"type": "AmmAnnexeTitre3", "content": "Effets fréquents"},
                        {"type": "AmmCorpsTexte", "content": "Nausées."},
                        {"type": "AmmAnnexeTitre3", "content": "Effets rares"},
                        {"type": "AmmCorpsTexte", "content": "Éruptions cutanées."},
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 2
        assert chunks[0]["sub_header"] == "Effets fréquents"
        assert chunks[1]["sub_header"] == "Effets rares"

    def test_bullet_list_text_included_in_chunk(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bContreIndic",
                    "content": "Contre-indications",
                    "children": [
                        {"type": "AmmCorpsTexteGras", "content": "Ne pas prendre si"},
                        {"type": "listePuce", "content": ["allergie au principe actif", "insuffisance rénale sévère"]},
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 1
        assert "allergie" in chunks[0]["text"]
        assert "insuffisance" in chunks[0]["text"]

    def test_bold_header_with_no_body_is_dropped(self):
        """A bold header followed immediately by another bold (or end of section) produces no text → skipped."""
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [{"type": "AmmCorpsTexteGras", "content": "Titre sans corps"}],
                }
            ],
        )
        assert list(_iter_notice_chunks(record)) == []

    def test_empty_children_yields_nothing(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [],
                }
            ],
        )
        assert list(_iter_notice_chunks(record)) == []

    # AmmAnnexeTitre2 case

    def test_titre2_children_each_become_one_chunk(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [
                        {
                            "type": "AmmAnnexeTitre2",
                            "anchor": "sub1",
                            "content": "Grossesse",
                            "children": [{"type": "AmmCorpsTexte", "content": "Déconseillé."}],
                        },
                        {
                            "type": "AmmAnnexeTitre2",
                            "anchor": "sub2",
                            "content": "Allaitement",
                            "children": [{"type": "AmmCorpsTexte", "content": "Non recommandé."}],
                        },
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 2
        assert {c["sub_header"] for c in chunks} == {"Grossesse", "Allaitement"}

    def test_titre2_non_titre2_siblings_skipped(self):
        """When a section has AmmAnnexeTitre2 children, non-titre2 siblings are ignored."""
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [
                        {"type": "AmmCorpsTexte", "content": "Texte orphelin ignoré."},
                        {
                            "type": "AmmAnnexeTitre2",
                            "anchor": "sub1",
                            "content": "Grossesse",
                            "children": [{"type": "AmmCorpsTexte", "content": "Déconseillé."}],
                        },
                    ],
                }
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 1
        assert chunks[0]["sub_header"] == "Grossesse"

    def test_titre2_empty_body_is_skipped(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bInfoNecessaires",
                    "content": "2. Informations",
                    "children": [
                        {"type": "AmmAnnexeTitre2", "anchor": "sub1", "content": "Vide", "children": []},
                    ],
                }
            ],
        )
        assert list(_iter_notice_chunks(record)) == []

    # chunk metadata

    def test_chunk_has_required_fields(self):
        record = _record(
            "999",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [{"type": "AmmCorpsTexte", "content": "Conserver au frais."}],
                }
            ],
        )
        chunk = list(_iter_notice_chunks(record))[0]
        for field in (
            "_id",
            "cis",
            "section_anchor",
            "section_title",
            "sub_header",
            "text",
            "embed_text",
            "html_snippets",
        ):
            assert field in chunk, f"Missing field: {field}"

    def test_embed_text_contains_section_title_sub_header_and_body(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [
                        {"type": "AmmCorpsTexteGras", "content": "Au réfrigérateur"},
                        {"type": "AmmCorpsTexte", "content": "Entre 2 et 8°C."},
                    ],
                }
            ],
        )
        chunk = list(_iter_notice_chunks(record))[0]
        assert "5. Conservation" in chunk["embed_text"]
        assert "Au réfrigérateur" in chunk["embed_text"]
        assert "2 et 8°C" in chunk["embed_text"]

    def test_html_snippets_collected_from_body_nodes(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [{"type": "AmmCorpsTexte", "content": "Conserver.", "html": "<p>Conserver.</p>"}],
                }
            ],
        )
        chunk = list(_iter_notice_chunks(record))[0]
        assert "<p>Conserver.</p>" in chunk["html_snippets"]

    def test_chunk_id_is_deterministic(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [{"type": "AmmCorpsTexte", "content": "Conserver."}],
                }
            ],
        )
        ids_a = [c["_id"] for c in _iter_notice_chunks(record)]
        ids_b = [c["_id"] for c in _iter_notice_chunks(record)]
        assert ids_a == ids_b

    def test_different_content_different_id(self):
        def make(body):
            return _record(
                "123",
                [
                    {
                        "type": "AmmAnnexeTitre1",
                        "anchor": "Ann3bConservation",
                        "content": "5.",
                        "children": [{"type": "AmmCorpsTexte", "content": body}],
                    }
                ],
            )

        id1 = list(_iter_notice_chunks(make("A.")))[0]["_id"]
        id2 = list(_iter_notice_chunks(make("B.")))[0]["_id"]
        assert id1 != id2

    #  multi-section record

    def test_multiple_sections_all_yielded(self):
        record = _record(
            "123",
            [
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bConservation",
                    "content": "5. Conservation",
                    "children": [{"type": "AmmCorpsTexte", "content": "Conserver."}],
                },
                {
                    "type": "AmmAnnexeTitre1",
                    "anchor": "Ann3bEffetsIndesirables",
                    "content": "4. Effets",
                    "children": [{"type": "AmmCorpsTexte", "content": "Nausées."}],
                },
            ],
        )
        chunks = list(_iter_notice_chunks(record))
        assert len(chunks) == 2
        assert {c["section_anchor"] for c in chunks} == {"Ann3bConservation", "Ann3bEffetsIndesirables"}
