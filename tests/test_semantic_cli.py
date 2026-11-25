"""Tests for the local semantic-parser CLI workflow."""

import json
import logging
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from infomedicament_dataeng import cli
from infomedicament_dataeng.semantic_report import DocumentFailure


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
    monkeypatch.setattr(cli, "get_latest_ansm_document_modification", lambda config: cutoff)
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
        lambda since, config, cis, limit, retry_cis: worklist_calls.append((since, config, cis, limit)) or worklist,
    )
    monkeypatch.setattr(
        "infomedicament_dataeng.db.get_centralised_specialties",
        lambda *args, **kwargs: pytest.fail("centralised specialties should not be queried"),
    )
    monkeypatch.setattr(
        cli,
        "import_semantic_documents",
        lambda records, table, config, **kwargs: imports.append((table, list(records))) or (len(records), 0),
    )

    watermark = cli.import_semantic_documents_from_db(
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
    assert watermark.watermark == cutoff


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

    watermark = cli.import_semantic_documents_from_db(since=cutoff, centralised_only=True)

    assert selected == [{**specialty, "procedure": "CENTRALISEE", "documents": {}}]
    assert watermark.watermark == datetime(2026, 9, 24, 8, tzinfo=timezone.utc)


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


def test_full_db_import_reconciles_specialty_metadata(monkeypatch):
    synced = []
    config = SimpleNamespace(postgres="postgres-config")
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(
        cli,
        "get_latest_ansm_document_modification",
        lambda config: datetime(2026, 9, 25, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(cli, "sync_specialites_metadata_from_db", lambda config: synced.append(config))
    monkeypatch.setattr(cli, "get_semantic_import_worklist", lambda *args, **kwargs: [])
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_specialties", lambda config, cis=None: [])

    cli.import_semantic_documents_from_db(full=True)

    assert synced == ["postgres-config"]


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


def test_main_routes_grist_sync(monkeypatch):
    calls = []
    config = SimpleNamespace(
        log_level="INFO",
        postgres="postgres-config",
        grist=SimpleNamespace(doc_id="doc-id", api_key="api-key"),
    )
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(cli, "sync_grist", lambda *args: calls.append(args))
    monkeypatch.setattr(sys, "argv", ["infomedicament-dataeng", "sync-grist"])

    cli.main()

    assert calls == [("doc-id", "api-key", "postgres-config")]


def test_main_routes_indications_build(monkeypatch):
    calls = []
    config = SimpleNamespace(log_level="INFO", postgres="postgres-config")
    result = SimpleNamespace(inserted=1, updated=2, deleted=3)
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(cli, "build_indications", lambda postgres: calls.append(postgres) or result)
    monkeypatch.setattr(sys, "argv", ["infomedicament-dataeng", "build-indications"])

    cli.main()

    assert calls == ["postgres-config"]


def test_main_routes_resume_build(monkeypatch):
    calls = []
    config = SimpleNamespace(log_level="INFO", postgres="postgres-config")
    monkeypatch.setattr(cli, "get_config", lambda: config)
    monkeypatch.setattr(cli, "build_resume", lambda *args: calls.append(args) or {"specialites": 10})
    monkeypatch.setattr(
        sys,
        "argv",
        ["infomedicament-dataeng", "build-resume", "--target", "specialites"],
    )

    cli.main()

    assert calls == [("specialites", "postgres-config")]


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


@pytest.fixture
def semantic_environment(monkeypatch):
    """Real document import with external systems replaced by deterministic fixtures."""
    monkeypatch.setattr(cli, "get_config", lambda: SimpleNamespace(postgres="postgres"))
    monkeypatch.setattr(cli, "make_s3_client", lambda: object())
    monkeypatch.setattr(cli, "get_glossary_terms", lambda config: [])
    monkeypatch.setattr(
        cli, "get_latest_ansm_document_modification", lambda config: datetime(2026, 11, 23, tzinfo=timezone.utc)
    )
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_specialties", lambda *args, **kwargs: [])
    specialty = {
        "cis": "61234567",
        "denomination": "TEST",
        "procedure": "NATIONALE",
        "code_ema": "",
        "documents": {"notice": "https://ansm.example/notice.html", "rcp": "https://ansm.example/rcp.html"},
    }
    monkeypatch.setattr(cli, "get_semantic_import_worklist", lambda *args, **kwargs: [specialty])
    monkeypatch.setattr(cli, "_download_document", lambda url: b"document")
    monkeypatch.setattr(
        cli,
        "parse_semantic_document",
        lambda *args, **kwargs: SimpleNamespace(date_notif=None, indication=None, content_html="<p>content</p>"),
    )
    monkeypatch.setattr(cli, "import_semantic_documents", lambda records, *args, **kwargs: (len(records), 0))
    return specialty


def test_partial_import_preserves_failed_document_and_reports_counts(monkeypatch, semantic_environment, caplog):
    def download(url):
        if "notice" in url:
            raise RuntimeError("source unavailable")
        return b"rcp"

    monkeypatch.setattr(cli, "_download_document", download)
    with caplog.at_level(logging.INFO):
        result = cli.import_semantic_documents_from_db()

    assert result.imported == {"rcp": 1, "notice": 0}
    assert result.watermark == datetime(2026, 11, 23, tzinfo=timezone.utc)
    assert result.failed_documents == [
        DocumentFailure(
            "61234567", "notice", "https://ansm.example/notice.html", "download-or-parse", "source unavailable"
        )
    ]
    assert "cis=61234567 type=notice url=https://ansm.example/notice.html" in caplog.text
    assert "rcp_imported=1 notice_imported=0 failed_documents=1" in caplog.text


def test_retry_selects_old_document_and_uses_current_url(monkeypatch, semantic_environment):
    old_failure = DocumentFailure("61234567", "notice", "https://old.example/notice", "download-or-parse", "offline")
    cutoff = datetime(2026, 11, 22, tzinfo=timezone.utc)
    selections = []
    downloads = []

    def select(since, config, **kwargs):
        selections.append((since, kwargs["retry_cis"]))
        return [semantic_environment] if "61234567" in kwargs["retry_cis"] else []

    monkeypatch.setattr(cli, "get_semantic_import_worklist", select)
    monkeypatch.setattr(cli, "_download_document", lambda url: downloads.append(url) or b"document")
    result = cli.import_semantic_documents_from_db(since=cutoff, pending_documents=[old_failure])

    assert selections == [(cutoff, ["61234567"])]
    assert "https://ansm.example/notice.html" in downloads
    assert result.failed_documents == []
    assert result.imported == {"rcp": 1, "notice": 1}


def test_missing_retry_remains_pending_without_advancing_it_out_of_history(monkeypatch, semantic_environment):
    pending = DocumentFailure("60000000", "notice", "https://old.example/notice", "download-or-parse", "offline")
    monkeypatch.setattr(cli, "get_semantic_import_worklist", lambda *args, **kwargs: [])
    result = cli.import_semantic_documents_from_db(pending_documents=[pending])
    assert len(result.failed_documents) == 1
    assert result.failed_documents[0].cis == "60000000"
    assert result.failed_documents[0].stage == "resolve"


def test_individual_database_errors_are_retained_for_retry(monkeypatch, semantic_environment):
    def write(records, table, config, on_error):
        if table == "notices":
            on_error(records[0], RuntimeError("invalid record"))
            return 0, 1
        return len(records), 0

    monkeypatch.setattr(cli, "import_semantic_documents", write)
    result = cli.import_semantic_documents_from_db()
    assert result.imported == {"rcp": 1, "notice": 0}
    assert result.failed_documents[0].stage == "database"
    assert result.failed_documents[0].url == "https://ansm.example/notice.html"


@pytest.mark.parametrize("failure_stage", ["download-or-parse", "match", "resolve"])
def test_ema_failures_identify_each_document_and_retry_old_specialties(
    monkeypatch, semantic_environment, failure_stage
):
    from infomedicament_dataeng.centralise import acquire, match, parser

    specialty = {"cis": "61234567", "denomination": "EMA TEST", "code_ema": "EMEA/TEST"}
    index = {"EMEA/TEST": {"french_url": "https://ema.example/product.pdf", "last_updated_date": "2026-01-01"}}
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_specialties", lambda *args, **kwargs: [specialty])
    monkeypatch.setattr(cli, "get_semantic_import_worklist", lambda *args, **kwargs: [])
    monkeypatch.setattr(acquire, "fetch_ema_document_report", lambda: {})
    monkeypatch.setattr(acquire, "build_product_information_index", lambda report: index)
    monkeypatch.setattr(acquire, "get_ema_pdf", lambda *args, **kwargs: b"pdf")
    monkeypatch.setattr(parser, "parse_pdf", lambda *args, **kwargs: {"images": [], "rcp": [], "notice": []})
    monkeypatch.setattr(cli, "_upload_images", lambda *args: None)
    monkeypatch.setattr(match, "match_presentation", lambda *args: None)
    if failure_stage == "download-or-parse":

        def download(*args, **kwargs):
            raise RuntimeError("EMA unavailable")

        monkeypatch.setattr(acquire, "get_ema_pdf", download)
    elif failure_stage == "resolve":
        index["EMEA/TEST"]["french_url"] = None
    pending = DocumentFailure("61234567", "notice", None, "resolve", "previous error")
    result = cli.import_semantic_documents_from_db(
        since=datetime(2026, 11, 22, tzinfo=timezone.utc), pending_documents=[pending]
    )

    assert {(failure.cis, failure.document_type) for failure in result.failed_documents} == {
        ("61234567", "notice"),
        ("61234567", "rcp"),
    }
    assert all(failure.stage == failure_stage for failure in result.failed_documents)
    if failure_stage != "resolve":
        assert all(failure.url == "https://ema.example/product.pdf" for failure in result.failed_documents)


def test_repeated_failure_survives_multiple_watermark_advances(monkeypatch, semantic_environment):
    def download(url):
        if "notice" in url:
            raise RuntimeError("still offline")
        return b"rcp"

    monkeypatch.setattr(cli, "_download_document", download)
    first = cli.import_semantic_documents_from_db()
    selections = []

    def select(since, config, **kwargs):
        selections.append(kwargs["retry_cis"])
        return [semantic_environment] if semantic_environment["cis"] in kwargs["retry_cis"] else []

    monkeypatch.setattr(cli, "get_semantic_import_worklist", select)
    for day in (26, 29):
        first = cli.import_semantic_documents_from_db(
            since=datetime(2026, 11, day, tzinfo=timezone.utc), pending_documents=first.failed_documents
        )
        assert len(first.failed_documents) == 1
    assert selections == [["61234567"], ["61234567"]]


def test_ema_successful_retry_clears_pending_documents(monkeypatch, semantic_environment):
    from infomedicament_dataeng.centralise import acquire, match, parser

    specialty = {"cis": "61234567", "denomination": "EMA TEST", "code_ema": "EMEA/TEST"}
    index = {"EMEA/TEST": {"french_url": "https://ema.example/product.pdf", "last_updated_date": "2026-01-01"}}
    monkeypatch.setattr("infomedicament_dataeng.db.get_centralised_specialties", lambda *args, **kwargs: [specialty])
    monkeypatch.setattr(cli, "get_semantic_import_worklist", lambda *args, **kwargs: [])
    monkeypatch.setattr(acquire, "fetch_ema_document_report", lambda: {})
    monkeypatch.setattr(acquire, "build_product_information_index", lambda report: index)
    fetches = []

    def pdf_bytes(url, client, *, refresh=False):
        fetches.append((url, refresh))
        return b"corrected" if refresh else b"broken-cached"

    def parse_pdf(pdf, **kwargs):
        assert pdf == b"corrected"
        return {"images": [], "rcp": [], "notice": []}

    monkeypatch.setattr(acquire, "get_ema_pdf", pdf_bytes)
    monkeypatch.setattr(parser, "parse_pdf", parse_pdf)
    monkeypatch.setattr(cli, "_upload_images", lambda *args: None)
    monkeypatch.setattr(
        match, "match_presentation", lambda *args: {"date_notif": None, "indication": None, "content_html": "<p>ok</p>"}
    )
    pending = DocumentFailure("61234567", "notice", "https://ema.example/product.pdf", "match", "missing")
    result = cli.import_semantic_documents_from_db(
        since=datetime(2026, 11, 22, tzinfo=timezone.utc), pending_documents=[pending]
    )
    assert fetches == [("https://ema.example/product.pdf", True)]
    assert result.failed_documents == []
    assert result.imported == {"rcp": 1, "notice": 1}


def test_empty_document_does_not_overwrite_content_or_clear_retry(monkeypatch, semantic_environment):
    monkeypatch.setattr(
        cli,
        "parse_semantic_document",
        lambda *args, **kwargs: SimpleNamespace(date_notif=None, indication=None, content_html="  "),
    )
    writes = []
    monkeypatch.setattr(cli, "import_semantic_documents", lambda *args, **kwargs: writes.append(args))
    pending = DocumentFailure("61234567", "notice", "https://ansm.example/notice.html", "download-or-parse", "offline")
    result = cli.import_semantic_documents_from_db(pending_documents=[pending])
    assert writes == []
    assert len(result.failed_documents) == 2
    assert all("no semantic HTML" in failure.error for failure in result.failed_documents)
