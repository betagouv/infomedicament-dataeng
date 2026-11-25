"""Tests for semantic document and specialty metadata writes."""

from unittest.mock import MagicMock

from infomedicament_dataeng.db import (
    VISIBLE_SPECIALITE_AVAILABILITIES,
    _upsert_semantic_document,
    sync_specialites_metadata,
)


def test_notice_upserts_specialty_metadata_from_catalog():
    conn = MagicMock()

    _upsert_semantic_document(
        conn,
        "notices",
        {
            "cis": "61234567",
            "content_html": "<p>Notice</p>",
            "date_notif": "2026-09-25",
            "indication": "Indication text",
        },
    )

    assert conn.execute.call_count == 2
    statement, params = conn.execute.call_args_list[1].args
    sql = str(statement)
    assert "INSERT INTO specialites_metadata" in sql
    assert "FROM ansm_specialite" in sql
    assert "ON CONFLICT" in sql
    assert params == {
        "cis_text": "61234567",
        "description": "Indication text",
        "availabilities": VISIBLE_SPECIALITE_AVAILABILITIES,
    }


def test_rcp_does_not_write_specialty_metadata():
    conn = MagicMock()

    _upsert_semantic_document(
        conn,
        "rcp",
        {"cis": "61234567", "content_html": "<p>RCP</p>", "date_notif": None},
    )

    assert conn.execute.call_count == 1


def test_sync_specialty_metadata_preserves_descriptions_and_removes_obsolete_rows():
    conn = MagicMock()

    sync_specialites_metadata(conn)

    assert conn.execute.call_count == 2
    insert_sql = str(conn.execute.call_args_list[0].args[0])
    delete_sql = str(conn.execute.call_args_list[1].args[0])
    assert "INSERT INTO specialites_metadata" in insert_sql
    assert "DO UPDATE SET title = EXCLUDED.title" in insert_sql
    assert "description = EXCLUDED" not in insert_sql
    assert "DELETE FROM specialites_metadata" in delete_sql


def test_worklist_includes_pending_cis_beyond_incremental_cutoff(monkeypatch):
    from datetime import datetime

    from sqlalchemy import create_engine, text

    from infomedicament_dataeng import db

    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE ansm_specialite (cis TEXT, denomination TEXT, procedure TEXT, code_ema TEXT)")
        )
        connection.execute(text("CREATE TABLE ansm_document (cis TEXT, type TEXT, url TEXT, date_modification TEXT)"))
        connection.execute(text("INSERT INTO ansm_specialite VALUES ('61234567', 'TEST', 'NATIONALE', '')"))
        connection.execute(
            text(
                "INSERT INTO ansm_document VALUES "
                "('61234567', 'notice', 'https://ansm.example/current.html', '2026-01-01')"
            )
        )
    monkeypatch.setattr(db, "get_postgres_engine", lambda config: engine)
    cutoff = datetime(2026, 11, 22)
    assert db.get_semantic_import_worklist(cutoff, config="postgres", limit=10) == []
    selected = db.get_semantic_import_worklist(cutoff, config="postgres", limit=10, retry_cis=["61234567"])
    assert [item["cis"] for item in selected] == ["61234567"]
    assert selected[0]["documents"] == {"notice": "https://ansm.example/current.html"}


def test_document_write_failure_calls_retry_handler_and_continues(monkeypatch):
    from infomedicament_dataeng import db

    connection = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr(db, "get_postgres_engine", lambda config: engine)
    first = {"cis": "61234567", "source_url": "https://ansm.example/broken.html"}
    second = {"cis": "60000000", "source_url": "https://ansm.example/good.html"}
    written = []

    def upsert(connection, table, record):
        if record is first:
            raise ValueError("invalid record")
        written.append(record)

    monkeypatch.setattr(db, "_upsert_semantic_document", upsert)
    failed = []
    imported, errors = db.import_semantic_documents(
        [first, second],
        "notices",
        config="postgres",
        on_error=lambda record, error: failed.append((record, str(error))),
    )
    assert (imported, errors) == (1, 1)
    assert failed == [(first, "invalid record")]
    assert written == [second]
    connection.rollback.assert_called_once()
    connection.commit.assert_called_once()
