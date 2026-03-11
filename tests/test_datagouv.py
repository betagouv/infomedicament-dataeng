"""Tests for datagouv data fetching and import."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from infomedicament_dataeng.datagouv import (
    ColumnDef,
    CsvSource,
    DataGouvDataset,
    fetch_csv,
    import_dataset,
    load_datasets,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

SAMPLE_CSV = "col_a|col_b|col_c\nval1|val2|val3\nval4|val5|val6\n"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_dataset() -> DataGouvDataset:
    return DataGouvDataset(
        datagouv_dataset_id="abc-123",
        postgresql_table="test_table",
        source=CsvSource(delimiter="|", encoding="utf-8"),
        columns=[
            ColumnDef(name="col_a", type="str"),
            ColumnDef(name="col_b", type="str"),
            ColumnDef(name="col_c", type="str"),
        ],
    )


# ---------------------------------------------------------------------------
# load_datasets
# ---------------------------------------------------------------------------


class TestLoadDatasets:
    def test_loads_dataset_from_yaml(self):
        datasets = load_datasets(FIXTURES_DIR / "test_datagouv.yml")
        assert "test_dataset" in datasets

    def test_parses_fields(self):
        ds = load_datasets(FIXTURES_DIR / "test_datagouv.yml")["test_dataset"]
        assert ds.datagouv_dataset_id == "abc-123"
        assert ds.postgresql_table == "test_table"
        assert ds.source.delimiter == "|"
        assert ds.source.encoding == "utf-8"
        assert [c.name for c in ds.columns] == ["col_a", "col_b", "col_c"]
        assert all(c.type == "str" for c in ds.columns)

    def test_raises_on_unknown_source_type(self, tmp_path: Path):
        bad_yaml = (FIXTURES_DIR / "test_datagouv.yml").read_text().replace("type: csv", "type: json")
        config_file = tmp_path / "bad.yml"
        config_file.write_text(bad_yaml, encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported source type"):
            load_datasets(config_file)


# ---------------------------------------------------------------------------
# fetch_csv
# ---------------------------------------------------------------------------


class TestFetchCsv:
    def _mock_urlopen(self, content: str, encoding: str = "utf-8"):
        mock_response = MagicMock()
        mock_response.read.return_value = content.encode(encoding)
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        return patch("infomedicament_dataeng.datagouv.urllib.request.urlopen", return_value=mock_response)

    def test_skips_header_row(self, sample_dataset: DataGouvDataset):
        with self._mock_urlopen(SAMPLE_CSV):
            rows = fetch_csv(sample_dataset)
        assert ["col_a", "col_b", "col_c"] not in rows

    def test_parses_pipe_delimited_rows(self, sample_dataset: DataGouvDataset):
        with self._mock_urlopen(SAMPLE_CSV):
            rows = fetch_csv(sample_dataset)
        assert rows == [["val1", "val2", "val3"], ["val4", "val5", "val6"]]

    def test_uses_dataset_encoding(self, sample_dataset: DataGouvDataset):
        latin1_content = "col_a|col_b\néàü|xyz\n"
        sample_dataset.source.encoding = "latin-1"
        with self._mock_urlopen(latin1_content, encoding="latin-1"):
            rows = fetch_csv(sample_dataset)
        assert rows[0][0] == "éàü"

    def test_respects_custom_quotechar(self, sample_dataset: DataGouvDataset):
        dollar_quoted_csv = "$col_a$;$col_b$\n$val;1$;$val2$\n"
        sample_dataset.source.delimiter = ";"
        sample_dataset.source.quotechar = "$"
        with self._mock_urlopen(dollar_quoted_csv):
            rows = fetch_csv(sample_dataset)
        assert rows == [["val;1", "val2"]]

    def test_builds_correct_url(self, sample_dataset: DataGouvDataset):
        with self._mock_urlopen(SAMPLE_CSV) as mock_urlopen:
            fetch_csv(sample_dataset)
        mock_urlopen.assert_called_once_with("https://www.data.gouv.fr/api/1/datasets/r/abc-123")


# ---------------------------------------------------------------------------
# import_dataset
# ---------------------------------------------------------------------------


class TestImportDataset:
    def _mock_psycopg2(self):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return patch("infomedicament_dataeng.datagouv.psycopg2.connect", return_value=mock_conn), mock_conn, mock_cursor

    def test_truncates_before_insert(self, sample_dataset: DataGouvDataset):
        mock_conn_patch, mock_conn, mock_cursor = self._mock_psycopg2()
        with mock_conn_patch, patch("infomedicament_dataeng.datagouv.fetch_csv", return_value=[["a", "b", "c"]]):
            import_dataset(sample_dataset)
        truncate_call = mock_cursor.execute.call_args_list[0]
        assert "TRUNCATE" in truncate_call.args[0].upper()
        assert "test_table" in truncate_call.args[0]

    def test_inserts_all_rows(self, sample_dataset: DataGouvDataset):
        rows = [["val1", "val2", "val3"], ["val4", "val5", "val6"]]
        mock_conn_patch, mock_conn, mock_cursor = self._mock_psycopg2()
        with mock_conn_patch, patch("infomedicament_dataeng.datagouv.fetch_csv", return_value=rows):
            import_dataset(sample_dataset)
        mock_cursor.executemany.assert_called_once()
        _, insert_rows = mock_cursor.executemany.call_args.args
        assert insert_rows == rows

    def test_returns_row_count(self, sample_dataset: DataGouvDataset):
        rows = [["a", "b", "c"]] * 42
        mock_conn_patch, mock_conn, mock_cursor = self._mock_psycopg2()
        with mock_conn_patch, patch("infomedicament_dataeng.datagouv.fetch_csv", return_value=rows):
            count = import_dataset(sample_dataset)
        assert count == 42

    def test_commits_transaction(self, sample_dataset: DataGouvDataset):
        mock_conn_patch, mock_conn, mock_cursor = self._mock_psycopg2()
        with mock_conn_patch, patch("infomedicament_dataeng.datagouv.fetch_csv", return_value=[]):
            import_dataset(sample_dataset)
        mock_conn.commit.assert_called_once()
