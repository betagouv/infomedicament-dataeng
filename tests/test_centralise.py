"""Tests for the centralised EMA PDF acquisition (S3 cache)."""

import logging

import pytest

from infomedicament_dataeng.centralise import acquire
from infomedicament_dataeng.centralise.acquire import (
    EMA_DOCUMENT_REPORT_URL,
    build_french_product_information_index,
    build_product_information_index,
    fetch_ema_document_report,
    get_ema_pdf,
    pdf_cache_key,
)

URL = "https://www.ema.europa.eu/fr/documents/product-information/abasaglar-epar-product-information_fr.pdf"
EXPECTED_KEY = "imports/ema_pdf/abasaglar-epar-product-information_fr.pdf"


class FakeS3Client:
    """Minimal fake S3Client tracking uploads/downloads for the cache tests."""

    def __init__(self, exists=False, stored=b""):
        self._exists = exists
        self._stored = stored
        self.uploads = {}  # key -> content
        self.downloaded = []

    def object_exists(self, key):
        return self._exists

    def download_file_content(self, key):
        self.downloaded.append(key)
        return self._stored

    def upload_file_content(self, key, content, content_type="application/json"):
        self.uploads[key] = content


class TestPdfCacheKey:
    def test_from_url_slug(self):
        assert pdf_cache_key(URL) == EXPECTED_KEY

    def test_rejects_empty_slug(self):
        with pytest.raises(ValueError):
            pdf_cache_key("")


class TestGetEmaPdf:
    def test_cache_hit_serves_from_s3(self, monkeypatch):
        def _boom(url):
            raise AssertionError("should not fetch from EMA on cache hit")

        monkeypatch.setattr(acquire, "_fetch_from_ema", _boom)
        s3 = FakeS3Client(exists=True, stored=b"%PDF-cached")

        assert get_ema_pdf(URL, s3) == b"%PDF-cached"
        assert s3.downloaded == [EXPECTED_KEY]
        assert s3.uploads == {}

    def test_cache_hit_uses_status_callback_without_info_log(self, monkeypatch, caplog):
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: pytest.fail("unexpected EMA fetch"))
        s3 = FakeS3Client(exists=True, stored=b"%PDF-cached")
        statuses = []

        with caplog.at_level(logging.INFO, logger=acquire.__name__):
            get_ema_pdf(URL, s3, on_cache_hit=statuses.append)

        assert statuses == [EXPECTED_KEY]
        assert "Cache hit:" not in caplog.text

    def test_cache_miss_fetches_and_uploads_with_sidecar(self, monkeypatch):
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: b"%PDF-fresh")
        s3 = FakeS3Client(exists=False)

        assert get_ema_pdf(URL, s3) == b"%PDF-fresh"
        assert set(s3.uploads) == {EXPECTED_KEY, f"{EXPECTED_KEY}.sha256"}
        assert s3.uploads[EXPECTED_KEY] == b"%PDF-fresh"

    def test_refresh_bypasses_cache(self, monkeypatch):
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: b"%PDF-new")
        s3 = FakeS3Client(exists=True, stored=b"%PDF-old")

        assert get_ema_pdf(URL, s3, refresh=True) == b"%PDF-new"
        assert s3.downloaded == []
        assert EXPECTED_KEY in s3.uploads


class TestEmaDocumentReport:
    def test_fetches_fresh_report_from_ema(self, monkeypatch):
        calls = []
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: calls.append(url) or b'{"data": []}')

        assert fetch_ema_document_report() == {"data": []}
        assert calls == [EMA_DOCUMENT_REPORT_URL]

    def test_indexes_only_french_product_information(self):
        report = {
            "data": [
                {
                    "type": "product-information",
                    "ema_product_number": "EMEA/H/C/002835 ",
                    "last_updated_date": "2026-01-01T00:00:00Z",
                    "translations": {"fr": URL},
                },
                {
                    "type": "product-information",
                    "ema_product_number": "EMEA/H/C/999999",
                    "translations": {"de": "https://example/de.pdf"},
                },
                {
                    "type": "overview",
                    "ema_product_number": "EMEA/H/C/000001",
                    "translations": {"fr": "https://example/overview-fr.pdf"},
                },
            ]
        }

        assert build_french_product_information_index(report) == {
            "EMEA/H/C/002835": URL,
            "EMEA/H/C/999999": None,
        }
        assert build_product_information_index(report)["EMEA/H/C/002835"] == {
            "french_url": URL,
            "last_updated_date": "2026-01-01T00:00:00Z",
        }

    def test_worklist_reports_missing_french_translation(self, monkeypatch, caplog):
        from infomedicament_dataeng import cli

        monkeypatch.setattr(acquire, "fetch_ema_document_report", lambda: {"data": []})
        monkeypatch.setattr(
            acquire,
            "build_product_information_index",
            lambda report: {"EMEA/H/C/999999": {"french_url": None, "last_updated_date": "2026-09-24"}},
        )

        with caplog.at_level(logging.ERROR, logger=cli.__name__):
            result = cli._get_ema_worklist(
                [{"cis": "61234567", "denomination": "TEST", "code_ema": "EMEA/H/C/999999"}]
            )

        assert result == {}
        assert "French EMA product-information translation missing for CIS 61234567" in caplog.text


def _doc(denom, tag):
    return {
        "denomination": denom,
        "date_notif": None,
        "indication": f"indication-{tag}",
        "content_html": f"<p>{tag}</p>",
    }


class TestParseFanOut:
    """`centralise parse` imports one semantic document per matched CIS."""

    def test_fans_out_matches_and_imports_directly(self, monkeypatch):
        from infomedicament_dataeng import cli, db
        from infomedicament_dataeng.centralise import acquire as acq
        from infomedicament_dataeng.centralise import parser

        worklist = {
            URL: [
                ("111", "ABASAGLAR 100 unités/ml solution injectable en cartouche"),
                ("222", "ABASAGLAR 100 unités/ml solution injectable en stylo prérempli"),
            ]
        }
        parsed = {
            "rcp": [_doc("… en cartouche", "rcp-cartouche"), _doc("… KwikPen … stylo prérempli", "rcp-pen")],
            "notice": [_doc("… en cartouche", "notice-cartouche"), _doc("… KwikPen … stylo prérempli", "notice-pen")],
            "images": {},
        }

        postfixes = []

        class FakeTqdm:
            def __init__(self, iterable, **kwargs):
                self.iterable = iterable

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def __iter__(self):
                return iter(self.iterable)

            def set_postfix_str(self, value):
                postfixes.append(value)

        def get_cached_pdf(url, s3=None, *, on_cache_hit=None, **kwargs):
            assert on_cache_hit is not None
            on_cache_hit(EXPECTED_KEY)
            return b"%PDF"

        monkeypatch.setattr(cli, "tqdm", FakeTqdm)
        monkeypatch.setattr(cli, "make_s3_client", lambda: object())
        monkeypatch.setattr(cli, "get_glossary_terms", lambda config: ["solution"])
        monkeypatch.setattr(acq, "get_ema_pdf", get_cached_pdf)
        specialties = [
            {"cis": cis, "denomination": denomination, "code_ema": "EMEA/H/C/002835"}
            for cis, denomination in worklist[URL]
        ]
        monkeypatch.setattr(db, "get_centralised_specialties", lambda config, cis=None: specialties)
        monkeypatch.setattr(cli, "_get_ema_worklist", lambda rows: {URL: rows})
        monkeypatch.setattr(parser, "parse_pdf", lambda b, **kwargs: parsed)
        imports = []
        monkeypatch.setattr(
            cli,
            "import_semantic_documents",
            lambda records, table, config: imports.append((table, list(records))) or (len(records), 0),
        )

        cli.run_centralise_parse()

        assert [table for table, _ in imports] == ["rcp", "notices"]
        rcp_records = imports[0][1]
        notice_records = imports[1][1]
        by_cis = {record["cis"]: record for record in rcp_records}
        assert set(by_cis) == {"111", "222"}
        assert by_cis["111"]["content_html"] == "<p>rcp-cartouche</p>"
        assert by_cis["222"]["content_html"] == "<p>rcp-pen</p>"
        assert "content" not in by_cis["111"]
        assert by_cis["111"]["cis"] == "111"
        assert by_cis["111"]["indication"] == "indication-rcp-cartouche"
        assert {record["cis"] for record in notice_records} == {"111", "222"}
        assert postfixes == ["cache: abasaglar-epar-product-information_fr.pdf"]
