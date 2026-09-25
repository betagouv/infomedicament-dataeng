"""Tests for the local semantic-parser CLI workflow."""

import json
import logging
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from infomedicament_dataeng import cli


def test_verbose_pins_noisy_dependency_loggers(monkeypatch):
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

    assert configured_levels["boto3"] == logging.INFO
    assert configured_levels["urllib3"] == logging.INFO


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


def test_import_semantic_documents_from_db_reads_selected_document_urls(monkeypatch):
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
        pass

    client = FakeS3Client()
    config = SimpleNamespace(
        postgres="postgres-config",
        s3=SimpleNamespace(notice_prefix="imports/notice/", rcp_prefix="imports/rcp/"),
    )
    worklist_calls = []
    downloads = []
    imports = []
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(cli, "make_s3_client", lambda: client)
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: [])
    monkeypatch.setattr(
        cli,
        "_download_document",
        lambda url: downloads.append(url) or b'<p class="AmmDenomination">TEST</p><p class="AmmCorpsTexte">Body</p>',
    )
    monkeypatch.setattr(
        cli,
        "get_semantic_import_worklist",
        lambda since, config, cis, limit: worklist_calls.append((since, config, cis, limit)) or worklist,
    )
    monkeypatch.setattr(
        "infomedicament_dataeng.db.get_centralised_specialties",
        lambda *args, **kwargs: pytest.fail("centralised specialties should not be queried"),
    )
    monkeypatch.setattr(
        cli,
        "import_semantic_documents",
        lambda records, table, config: imports.append((table, list(records))) or (len(records), 0),
    )

    cli.import_semantic_documents_from_db(
        since=cutoff,
        cis="61234567",
        limite=1,
        non_centralised_only=True,
    )

    assert worklist_calls == [(cutoff, "postgres-config", "61234567", None)]
    assert downloads == [
        "https://ansm.example/documents/N0000001.htm",
        "https://ansm.example/documents/R0000001.htm",
    ]
    assert [table for table, _ in imports] == ["rcp", "notices"]
    assert imports[0][1][0]["cis"] == "61234567"
    assert imports[1][1][0]["filename"] == "N0000001.htm"


def test_download_document_rejects_non_https_url():
    with pytest.raises(ValueError, match="must be an HTTPS URL"):
        cli._download_document("http://annexes.example/notice.html")


def test_document_image_base_url_uses_export_image_root():
    assert (
        cli._document_image_base_url("https://annexes.example/notice/20/60003620.html")
        == "https://annexes.example/images/"
    )


def test_ema_document_updated_since_uses_report_timestamp():
    cutoff = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)

    assert cli._ema_document_updated_since({"last_updated_date": "2026-09-23T12:00:00Z"}, cutoff)
    assert not cli._ema_document_updated_since({"last_updated_date": "2026-09-23T11:59:59Z"}, cutoff)


def test_db_import_selects_centralised_specialty_updated_only_at_ema(monkeypatch):
    from infomedicament_dataeng.centralise import acquire

    cutoff = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
    specialty = {
        "cis": "61234567",
        "denomination": "EMA TEST",
        "code_ema": "EMEA/H/C/009999",
    }
    ema_index = {
        "EMEA/H/C/009999": {
            "french_url": "https://ema.example/product-information_fr.pdf",
            "last_updated_date": "2026-09-24T08:00:00Z",
        }
    }
    selected = []
    config = SimpleNamespace(postgres="postgres-config")
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(
        cli,
        "get_semantic_import_worklist",
        lambda *args, **kwargs: [
            {
                "cis": "60000000",
                "denomination": "ANSM TEST",
                "procedure": "NATIONALE",
                "documents": {"rcp": "https://ansm.example/R0000001.htm"},
            }
        ],
    )
    monkeypatch.setattr(cli, "_download_document", lambda url: pytest.fail("ANSM documents should not be downloaded"))
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_specialties", lambda config, cis=None: [specialty])
    monkeypatch.setattr(acquire, "fetch_ema_document_report", lambda: {"data": []})
    monkeypatch.setattr(acquire, "build_product_information_index", lambda report: ema_index)
    monkeypatch.setattr(cli, "_get_ema_worklist", lambda specialties, index: selected.extend(specialties) or {})
    monkeypatch.setattr(cli, "make_s3_client", lambda: object())
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: [])
    monkeypatch.setattr(cli, "import_semantic_documents", lambda records, table, config: (0, 0))

    cli.import_semantic_documents_from_db(since=cutoff, centralised_only=True)

    assert selected == [{**specialty, "procedure": "CENTRALISEE", "documents": {}}]


def test_main_routes_semantic_db_full_import(monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO"))
    monkeypatch.setattr(cli, "import_semantic_documents_from_db", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "infomedicament-dataeng",
            "semantic-db-import",
            "--full",
            "--cis",
            "61234567",
            "--limit",
            "1",
            "--centralised-only",
        ],
    )

    cli.main()

    assert calls == [
        {
            "since": None,
            "full": True,
            "cis": "61234567",
            "limite": 1,
            "batch_size": 500,
            "centralised_only": True,
            "non_centralised_only": False,
        }
    ]


def test_main_rejects_conflicting_semantic_db_source_flags(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "infomedicament-dataeng",
            "semantic-db-import",
            "--centralised-only",
            "--non-centralised-only",
        ],
    )

    with pytest.raises(SystemExit, match="2"):
        cli.main()


def test_main_routes_pediatric_classification_from_postgres(monkeypatch, tmp_path):
    output = tmp_path / "predictions.csv"
    records = iter([{"cis": "61234567", "content_html": "<p>RCP</p>", "atc_code": "A01"}])
    iterator_calls = []
    runner_calls = []
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(log_level="INFO", postgres="postgres-config"))
    monkeypatch.setattr(
        cli,
        "iter_pediatric_rcps",
        lambda config, **kwargs: iterator_calls.append((config, kwargs)) or records,
    )
    monkeypatch.setattr(
        cli,
        "run_pediatric_classification",
        lambda selected, truth, output_path, debug: runner_calls.append((selected, truth, output_path, debug)),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "infomedicament-dataeng",
            "classify-pediatric",
            "--cis",
            "61234567",
            "--limit",
            "10",
            "--batch-size",
            "25",
            "--output",
            str(output),
            "--debug",
        ],
    )

    cli.main()

    assert iterator_calls == [("postgres-config", {"cis": "61234567", "limit": 10, "batch_size": 25})]
    assert runner_calls == [(records, None, str(output), True)]
