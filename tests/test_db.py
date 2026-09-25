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
