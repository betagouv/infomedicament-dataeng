"""Tests for the sequential daily-sync orchestration."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from infomedicament_dataeng import orchestration
from infomedicament_dataeng.indications import IndicationBuildResult


def test_sync_datagouv_imports_every_resource(monkeypatch, tmp_path):
    first = SimpleNamespace(postgresql_table="first")
    second = SimpleNamespace(postgresql_table="second")
    monkeypatch.setattr(orchestration, "load_datasets", lambda path: {"first": first, "second": second})
    imported = []
    monkeypatch.setattr(orchestration, "import_dataset", lambda dataset: imported.append(dataset) or 12)

    result = orchestration.sync_datagouv_config(tmp_path / "sources.yml")

    assert imported == [first, second]
    assert result == {"first": 12, "second": 12}


def test_daily_sync_uses_overlapped_semantic_watermark(monkeypatch):
    previous_watermark = datetime(2026, 9, 27, tzinfo=timezone.utc)
    next_watermark = datetime(2026, 9, 30, tzinfo=timezone.utc)
    run_id = uuid4()
    connection = MagicMock()
    connection.execute.return_value.scalar_one.return_value = True
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    ledger = MagicMock()
    ledger.previous_semantic_watermark.return_value = previous_watermark
    ledger.start_run.return_value = run_id
    monkeypatch.setattr(orchestration, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(orchestration, "RunLedger", lambda conn: ledger)

    calls = []
    monkeypatch.setattr(
        orchestration,
        "sync_datagouv_config",
        lambda path: calls.append(path.name) or {},
    )
    monkeypatch.setattr(orchestration, "sync_grist", lambda *args: calls.append("grist") or {})
    monkeypatch.setattr(
        orchestration,
        "build_indications",
        lambda config: calls.append("indications") or IndicationBuildResult(inserted=1, updated=2, deleted=3),
    )
    monkeypatch.setattr(orchestration, "build_resume", lambda *args: calls.append("resume") or {})

    def semantic_importer(**kwargs):
        calls.append(("semantic", kwargs))
        return next_watermark

    config = SimpleNamespace(postgres="postgres", grist=SimpleNamespace(doc_id="doc", api_key="key"))
    result = orchestration.run_daily_sync(config, semantic_importer)

    assert result == run_id
    assert calls == [
        "ansm.yml",
        "has.yml",
        "grist",
        ("semantic", {"since": datetime(2026, 9, 26, tzinfo=timezone.utc), "full": False}),
        "indications",
        "resume",
    ]
    ledger.record_semantic_watermark.assert_called_once_with(run_id, next_watermark)
    ledger.start_run.assert_called_once()
    assert ledger.start_run.call_args.args[0] == "manual"
    ledger.finish_run.assert_called_once()
    assert ledger.finish_run.call_args.args == (run_id, "success")


def test_daily_sync_runs_full_semantic_import_without_watermark(monkeypatch):
    run_id = uuid4()
    next_watermark = datetime(2026, 9, 30, tzinfo=timezone.utc)
    connection = MagicMock()
    connection.execute.return_value.scalar_one.return_value = True
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    ledger = MagicMock()
    ledger.previous_semantic_watermark.return_value = None
    ledger.start_run.return_value = run_id
    monkeypatch.setattr(orchestration, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(orchestration, "RunLedger", lambda conn: ledger)
    monkeypatch.setattr(orchestration, "sync_datagouv_config", lambda path: {})
    monkeypatch.setattr(orchestration, "sync_grist", lambda *args: {})
    monkeypatch.setattr(
        orchestration,
        "build_indications",
        lambda config: IndicationBuildResult(inserted=0, updated=0, deleted=0),
    )
    monkeypatch.setattr(orchestration, "build_resume", lambda *args: {})
    semantic_importer = MagicMock(return_value=next_watermark)
    config = SimpleNamespace(postgres="postgres", grist=SimpleNamespace(doc_id="doc", api_key="key"))

    orchestration.run_daily_sync(config, semantic_importer)

    semantic_importer.assert_called_once_with(since=None, full=True)
    ledger.record_semantic_watermark.assert_called_once_with(run_id, next_watermark)


def test_daily_sync_stops_and_records_failure(monkeypatch):
    run_id = uuid4()
    connection = MagicMock()
    connection.execute.return_value.scalar_one.return_value = True
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    ledger = MagicMock()
    ledger.previous_semantic_watermark.return_value = None
    ledger.start_run.return_value = run_id
    monkeypatch.setattr(orchestration, "get_postgres_engine", lambda config: engine)
    monkeypatch.setattr(orchestration, "RunLedger", lambda conn: ledger)
    monkeypatch.setattr(
        orchestration,
        "sync_datagouv_config",
        lambda *args: (_ for _ in ()).throw(RuntimeError("source unavailable")),
    )
    sync_grist = MagicMock()
    monkeypatch.setattr(orchestration, "sync_grist", sync_grist)
    config = SimpleNamespace(postgres="postgres", grist=SimpleNamespace(doc_id="doc", api_key="key"))

    with pytest.raises(RuntimeError, match="source unavailable"):
        orchestration.run_daily_sync(config, MagicMock())

    sync_grist.assert_not_called()
    assert ledger.finish_run.call_args.args == (run_id, "failure")
    assert ledger.finish_run.call_args.kwargs["failed_step"] == "import-ansm"
    assert "source unavailable" in ledger.finish_run.call_args.kwargs["error"]


def test_daily_sync_rejects_overlapping_run(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.scalar_one.return_value = False
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr(orchestration, "get_postgres_engine", lambda config: engine)

    with pytest.raises(orchestration.PipelineAlreadyRunning):
        orchestration.run_daily_sync(SimpleNamespace(postgres="postgres"), MagicMock())
