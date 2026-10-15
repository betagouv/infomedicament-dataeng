"""Recovery reimports current sources and refuses unresolved corruption."""

import sys
from unittest.mock import MagicMock

import pytest

from scripts import reimport_encoding as recovery


@pytest.mark.parametrize("apply", [False, True])
def test_recovery_deduplicates_sources_and_repairs_stored_records(monkeypatch, apply):
    url = "https://example.test/N123.htm"
    engine = MagicMock()
    monkeypatch.setattr(recovery, "get_postgres_engine", lambda: engine)
    monkeypatch.setattr(
        recovery,
        "recovery_worklist",
        lambda *args: [{"cis": cis, "url": url, "table": "notices"} for cis in ("60000001", "60000002")],
    )
    monkeypatch.setattr(recovery, "get_glossary_terms", lambda: [])
    download = MagicMock(return_value='<meta charset="utf-8"><p>l\x92enfant et le c\x9cur</p>'.encode())
    monkeypatch.setattr(recovery, "_download_document", download)
    imported = []

    def write(records, table):
        imported.extend(dict(record) for record in records)
        assert table == "notices"
        return len(records), 0

    monkeypatch.setattr(recovery, "import_semantic_documents", write)
    monkeypatch.setattr(sys, "argv", ["reimport_encoding", *(["--apply"] if apply else [])])
    recovery.main()
    assert download.call_count == int(apply)
    assert len(imported) == (2 if apply else 0)
    for record in imported:
        assert "l’enfant et le cœur" in record["content_html"]
        assert record["filename"] == "N123.htm"


def test_recovery_does_not_overwrite_when_source_has_undefined_controls(monkeypatch):
    monkeypatch.setattr(recovery, "get_postgres_engine", MagicMock())
    monkeypatch.setattr(
        recovery,
        "recovery_worklist",
        lambda *args: [{"cis": "60000001", "url": "https://example.test/N.htm", "table": "notices"}],
    )
    monkeypatch.setattr(recovery, "get_glossary_terms", lambda: [])
    monkeypatch.setattr(recovery, "_download_document", lambda url: b'<meta charset="utf-8"><p>bad\xc2\x81</p>')
    write = MagicMock()
    monkeypatch.setattr(recovery, "import_semantic_documents", write)
    monkeypatch.setattr(sys, "argv", ["reimport_encoding", "--apply"])
    with pytest.raises(SystemExit, match="1"):
        recovery.main()
    write.assert_not_called()
