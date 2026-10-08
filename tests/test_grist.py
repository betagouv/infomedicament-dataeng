"""Tests for Grist reference-data synchronization."""

from unittest.mock import MagicMock

import pytest

from infomedicament_dataeng import grist
from infomedicament_dataeng.grist import TABLE_SYNCS, TableSync, _fetch_table, _replace_table, _safe_string


def _spec(source: str) -> TableSync:
    return next(spec for spec in TABLE_SYNCS if spec.source == source)


@pytest.mark.parametrize(
    ("source", "fields", "expected"),
    [
        (
            "Glossaire",
            {"Nom_glossaire": "AMM", "Definition_glossaire": "Definition", "Source": "", "A_souligner": 1},
            {"nom": "AMM", "definition": "Definition", "source": None, "a_souligner": True},
        ),
        (
            "Articles",
            {
                "Titre": "Title",
                "Source": "Source",
                "Contenu": "Body",
                "Theme": "Theme",
                "Lien": "https://example.test",
                "Homepage": 1,
                "ImageId": "42",
                "Metadescription": "Description",
                "Classes_ATC": "M01, N02",
                "Substances": "00037",
                "Specialites": "61234567",
                "Pathologies": "62",
            },
            {
                "titre": "Title",
                "source": "Source",
                "contenu": "Body",
                "theme": "Theme",
                "lien": "https://example.test",
                "metadescription": "Description",
                "homepage": True,
                "image": "42",
                "atc_classe": "M01, N02",
                "substances": "00037",
                "specialites": "61234567",
                "pathologies": "62",
            },
        ),
        ("MARR_URL_CIS", {"URL": "entry", "CIS": 61234567}, {"url": "entry", "cis": "61234567"}),
        (
            "MARR_URL_PDF",
            {"URL_text": 123, "Nom_document": "Document", "URL_document": "https://example.test", "Type": "Patient"},
            {"url": "123", "nom_document": "Document", "url_document": "https://example.test", "type": "Patient"},
        ),
        (
            "Pathologies",
            {"codePatho": 62, "Definition_pathologie": "Definition", "codeClasClinique": 7},
            {"code_patho": 62, "definition": "Definition", "code_classe_clinique": 7},
        ),
        (
            "Pediatrie",
            {"CIS": 61234567, "contre_indication": False},
            {"cis": "61234567", "contre_indication": False},
        ),
        (
            "Grossesse_substances_contre_indiquees",
            {"SubsId": 42, "Lien_site_ANSM": "https://example.test"},
            {"subs_id": "42", "lien_site_ansm": "https://example.test"},
        ),
        ("Grossesse_mention", {"CIS": 61234567}, {"cis": "61234567"}),
        (
            "Definitions_Substances_Actives",
            {"SubsId": 42, "NomId": 7, "SA": "Substance", "Definition": "Definition"},
            {"subs_id": "42", "nom_id": "7", "sa": "Substance", "definition": "Definition"},
        ),
        (
            "ATC_friendly_1",
            {"Lettre_1_ATC_1": "A", "Libelles_niveau_1": "Label", "Definition_Classe": "Definition"},
            {"code": "A", "libelle": "Label", "definition_classe": "Definition"},
        ),
        (
            "ATC_friendly_2",
            {"Lettre_2_ATC2": "A01", "libelle": "Label", "Libelles_niveau_2_Definition_sous_classe": "Definition"},
            {"code": "A01", "libelle": "Label", "definition_sous_classe": "Definition"},
        ),
        (
            "SearchSynonyms",
            {"Alias": "Mal de tête, MIGRAINE,", "Canonical": "Céphalées"},
            [
                {"alias": "mal de tete", "canonical": "Céphalées"},
                {"alias": "migraine", "canonical": "Céphalées"},
            ],
        ),
    ],
)
def test_mappings_match_previous_sync(source, fields, expected):
    assert _spec(source).mapper(fields) == expected


def test_safe_string_matches_javascript_string_conversion():
    assert _safe_string(None) is None
    assert _safe_string(123) == "123"
    assert _safe_string(True) == "true"
    assert _safe_string(["one", "two"]) == "one,two"


def test_fetch_table_uses_fixed_server_manual_order_and_decodes_grist_lists():
    response = MagicMock()
    response.json.return_value = {
        "records": [
            {
                "id": 1,
                "fields": {
                    "URL_text": "entry",
                    "Nom_document": "Document",
                    "URL_document": "https://example.test",
                    "Type": ["L", "Patients", "Professionnels"],
                },
            }
        ]
    }
    session = MagicMock()
    session.get.return_value = response

    records = _fetch_table(session, "doc-id", "secret", _spec("MARR_URL_PDF"))

    assert records[0]["Type"] == ["Patients", "Professionnels"]
    session.get.assert_called_once_with(
        "https://grist.numerique.gouv.fr/api/docs/doc-id/tables/MARR_URL_PDF/records",
        params={"sort": "manualSort"},
        headers={"Authorization": "Bearer secret", "Accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status.assert_called_once()


def test_fetch_table_rejects_missing_expected_fields():
    response = MagicMock()
    response.json.return_value = {"records": [{"id": 1, "fields": {"CIS": 61234567}}]}
    session = MagicMock()
    session.get.return_value = response

    with pytest.raises(ValueError, match="contre_indication"):
        _fetch_table(session, "doc-id", "secret", _spec("Pediatrie"))


def test_replace_table_deletes_and_bulk_inserts_in_one_transaction():
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    spec = TableSync("target_table", "Source", ("Name",), lambda fields: fields)
    rows = [{"name": "one"}, {"name": "two"}]

    _replace_table(engine, spec, rows)

    assert conn.execute.call_count == 2
    assert str(conn.execute.call_args_list[0].args[0]) == "DELETE FROM target_table"
    assert "INSERT INTO target_table (name) VALUES (:name)" == str(conn.execute.call_args_list[1].args[0])
    assert conn.execute.call_args_list[1].args[1] == rows


def test_sync_leaves_target_unchanged_when_grist_source_is_empty(monkeypatch):
    spec = TableSync("target_table", "Source", ("Name",), lambda fields: fields)
    engine = MagicMock()
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    monkeypatch.setattr(grist, "TABLE_SYNCS", (spec,))
    monkeypatch.setattr(grist.requests, "Session", lambda: session)
    monkeypatch.setattr(grist, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(grist, "_fetch_table", lambda *args: [])

    assert grist.sync_grist("doc-id", "secret", "postgres-config") == {}
    engine.begin.assert_not_called()


def test_sync_replaces_table_even_when_mapping_produces_no_rows(monkeypatch):
    spec = TableSync("search_synonyms", "SearchSynonyms", ("Alias", "Canonical"), lambda fields: [])
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    monkeypatch.setattr(grist, "TABLE_SYNCS", (spec,))
    monkeypatch.setattr(grist.requests, "Session", lambda: session)
    monkeypatch.setattr(grist, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(grist, "_fetch_table", lambda *args: [{"Alias": "", "Canonical": ""}])

    assert grist.sync_grist("doc-id", "secret") == {"search_synonyms": 0}
    assert str(conn.execute.call_args.args[0]) == "DELETE FROM search_synonyms"


def test_sync_requires_grist_credentials():
    with pytest.raises(ValueError, match="GRIST_DOC_ID"):
        grist.sync_grist("", "secret")
    with pytest.raises(ValueError, match="GRIST_API_KEY"):
        grist.sync_grist("doc-id", "")
