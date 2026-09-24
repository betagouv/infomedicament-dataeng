"""Tests for the local semantic-parser CLI workflow."""

import json
import logging
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from infomedicament_dataeng import cli


def test_verbose_pins_opensearchpy_logger(monkeypatch):
    configured_levels = {}
    get_logger = logging.getLogger

    def capture_logger(name=None):
        if name is None:
            return get_logger()
        return SimpleNamespace(setLevel=lambda level: configured_levels.__setitem__(name, level))

    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "traiter_dossier_semantic_local", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli.logging, "getLogger", capture_logger)
    monkeypatch.setattr(sys, "argv", ["infomedicament-dataeng", "--verbose", "semantic-local", "html-files"])

    cli.main()

    assert configured_levels["opensearchpy"] == logging.INFO
    assert "opensearch" not in configured_levels


def test_traiter_fichier_semantic_local_returns_render_ready_record(tmp_path):
    source = tmp_path / "N0000001.htm"
    source.write_bytes(
        """
        <meta charset="iso-8859-1">
        <p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>
        <p class="DateNotif">Mis à jour le : 16/07/2026</p>
        <p class="AmmDenomination">MÉDICAMENT TEST</p>
        <p class="AmmCorpsTexte">Contenu patient.</p>
        """.encode("windows-1252")
    )

    result = cli.traiter_fichier_semantic_local((str(source), "https://cdn.example.test/assets/"))

    assert result == {
        "source": {"filename": "N0000001.htm"},
        "date_notif": "2026-07-16",
        "indication": None,
        "content_html": result["content_html"],
    }
    assert "Mis à jour le" not in result["content_html"]
    assert "data-document-date" not in result["content_html"]
    assert '<p data-block-id="document-b0002">Contenu patient.</p>' in result["content_html"]


def test_main_routes_semantic_local_arguments(monkeypatch, tmp_path):
    output = tmp_path / "semantic.jsonl"
    calls = []

    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "traiter_dossier_semantic_local", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "infomedicament-dataeng",
            "semantic-local",
            "html-files",
            "--output",
            str(output),
            "--limit",
            "2",
            "--image-base-url",
            "https://cdn.example.test/assets/",
        ],
    )

    cli.main()

    assert calls == [
        (
            ("html-files",),
            {
                "fichier_sortie": str(output),
                "limite": 2,
                "pattern": "all",
                "image_base_url": "https://cdn.example.test/assets/",
            },
        )
    ]


def test_traiter_dossier_semantic_local_loads_notice_and_rcp_files(tmp_path):
    (tmp_path / "N0000001.htm").write_text(
        '<p class="AmmAnnexeTitre">NOTICE</p><p class="AmmDenomination">NOTICE TEST</p>', encoding="utf-8"
    )
    (tmp_path / "R0000001.htm").write_text(
        '<p class="AmmAnnexeTitre">RCP</p><p class="AmmDenomination">RCP TEST</p>', encoding="utf-8"
    )
    output = tmp_path / "documents.jsonl"

    cli.traiter_dossier_semantic_local(str(tmp_path), fichier_sortie=str(output))

    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [record["source"]["filename"] for record in records] == ["N0000001.htm", "R0000001.htm"]
    assert all("title" not in record for record in records)


@pytest.mark.parametrize(
    ("pattern", "key", "filename", "cis", "table"),
    [
        ("N", "imports/notice/staging/N0000001.htm", "N0000001.htm", "61234567", "notices"),
        ("R", "imports/rcp/staging/R0000001.htm", "R0000001.htm", "67654321", "rcp"),
    ],
)
def test_import_semantic_documents_from_staging_does_not_move_sources(monkeypatch, pattern, key, filename, cis, table):
    class FakeS3Client:
        def list_staging_html_files(self, requested_pattern):
            assert requested_pattern == pattern
            return iter([key])

        def download_file_content(self, requested_key):
            assert requested_key == key
            return (
                b'<p class="DateNotif">Mis a jour le : 17/07/2026</p>'
                b'<p class="AmmDenomination">NOTICE TEST</p><p class="AmmCorpsTexte">Body</p>'
            )

    imported = []
    client = FakeS3Client()
    monkeypatch.setattr(cli, "make_s3_client", lambda: client)
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(postgres="postgres-config"))
    monkeypatch.setattr(cli, "get_authorized_cis", lambda: {cis})
    monkeypatch.setattr(cli, "get_filename_to_cis_mapping", lambda: {filename: int(cis)})
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: ["Body"])

    def capture_import(records, selected_table, config):
        imported.extend(records)
        assert selected_table == table
        assert config == "postgres-config"
        return len(imported), 0

    monkeypatch.setattr(cli, "import_semantic_documents", capture_import)

    cli.import_semantic_documents_from_s3(pattern=pattern, staging=True)

    assert imported[0]["cis"] == cis
    assert imported[0]["filename"] == filename
    assert imported[0]["date_notif"] == "2026-07-17"
    assert imported[0]["indication"] is None
    assert "<p" in imported[0]["content_html"]
    assert '<span data-definition="Body">Body</span>' in imported[0]["content_html"]


def test_import_semantic_documents_from_s3_can_target_one_cis(monkeypatch):
    class FakeS3Client:
        def list_html_files(self, requested_pattern):
            assert requested_pattern == "N"
            return iter(["imports/notice/N0000001.htm", "imports/notice/N0000002.htm"])

        def download_file_content(self, requested_key):
            assert requested_key == "imports/notice/N0000002.htm"
            return b'<p class="AmmDenomination">TARGET NOTICE</p><p class="AmmCorpsTexte">Body</p>'

    imported = []
    monkeypatch.setattr(cli, "make_s3_client", FakeS3Client)
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(postgres="postgres-config"))
    monkeypatch.setattr(cli, "get_authorized_cis", lambda: {"61234567", "67654321"})
    monkeypatch.setattr(
        cli,
        "get_filename_to_cis_mapping",
        lambda: {"N0000001.htm": 61234567, "N0000002.htm": 67654321},
    )
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: [])

    def capture_import(records, table, config):
        imported.extend(records)
        return len(imported), 0

    monkeypatch.setattr(cli, "import_semantic_documents", capture_import)

    cli.import_semantic_documents_from_s3(pattern="N", cis="67654321")

    assert [record["cis"] for record in imported] == ["67654321"]


def test_main_routes_semantic_s3_import_arguments(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "import_semantic_documents_from_s3", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "infomedicament-dataeng",
            "semantic-s3-import",
            "--pattern",
            "R",
            "--cis",
            "67654321",
            "--staging",
            "--limit",
            "2",
            "--image-base-url",
            "https://cdn.example.test/assets/",
        ],
    )

    cli.main()

    assert calls == [
        {
            "pattern": "R",
            "limite": 2,
            "staging": True,
            "image_base_url": "https://cdn.example.test/assets/",
            "cis": "67654321",
        }
    ]


def test_import_semantic_documents_from_db_reads_only_selected_s3_documents(monkeypatch):
    cutoff = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
    worklist = [
        {
            "cis": "61234567",
            "denomination": "MEDICAMENT TEST",
            "procedure": "NATIONALE",
            "documents": {
                "notice": "https://ansm.example/documents/N0000001.htm",
                "rcp": "https://ansm.example/documents/R0000001.htm",
            },
        }
    ]

    class FakeS3Client:
        def __init__(self):
            self.downloads = []

        def download_file_content(self, key):
            self.downloads.append(key)
            return b'<p class="AmmDenomination">TEST</p><p class="AmmCorpsTexte">Body</p>'

    client = FakeS3Client()
    config = SimpleNamespace(
        postgres="postgres-config",
        s3=SimpleNamespace(notice_prefix="imports/notice/", rcp_prefix="imports/rcp/"),
    )
    worklist_calls = []
    imports = []
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(cli, "make_s3_client", lambda: client)
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: [])
    monkeypatch.setattr(
        cli,
        "get_semantic_import_worklist",
        lambda since, config, cis, limit: worklist_calls.append((since, config, cis, limit)) or worklist,
    )
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_urls", lambda cis_codes: {})
    monkeypatch.setattr(
        cli,
        "import_semantic_documents",
        lambda records, table, config: imports.append((table, list(records))) or (len(records), 0),
    )

    cli.import_semantic_documents_from_db(since=cutoff, cis="61234567", limite=1)

    assert worklist_calls == [(cutoff, "postgres-config", "61234567", 1)]
    assert client.downloads == ["imports/notice/N0000001.htm", "imports/rcp/R0000001.htm"]
    assert [table for table, _ in imports] == ["rcp", "notices"]
    assert imports[0][1][0]["cis"] == "61234567"
    assert imports[1][1][0]["filename"] == "N0000001.htm"


def test_main_routes_semantic_db_full_import(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "import_semantic_documents_from_db", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        sys,
        "argv",
        ["infomedicament-dataeng", "semantic-db-import", "--full", "--cis", "61234567", "--limit", "1"],
    )

    cli.main()

    assert calls == [
        {
            "since": None,
            "full": True,
            "cis": "61234567",
            "limite": 1,
            "batch_size": 500,
            "image_base_url": cli.DEFAULT_IMAGE_BASE_URL,
        }
    ]
