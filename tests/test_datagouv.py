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
from infomedicament_dataeng.datapackage_importer import LOAD_ORDER

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
        assert ds.base_url == "https://www.data.gouv.fr/api/1/datasets/r/"

    def test_parses_base_url_override(self, tmp_path: Path):
        config = (FIXTURES_DIR / "test_datagouv.yml").read_text()
        config_file = tmp_path / "demo.yml"
        config_file.write_text(f"base_url: https://demo.data.gouv.fr/api/1/datasets/r\n{config}", encoding="utf-8")

        ds = load_datasets(config_file)["test_dataset"]

        assert ds.base_url == "https://demo.data.gouv.fr/api/1/datasets/r"

    def test_raises_on_unknown_source_type(self, tmp_path: Path):
        bad_yaml = (FIXTURES_DIR / "test_datagouv.yml").read_text().replace("type: csv", "type: json")
        config_file = tmp_path / "bad.yml"
        config_file.write_text(bad_yaml, encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported source type"):
            load_datasets(config_file)

    def test_ansm_config_covers_all_package_resources(self):
        datasets = load_datasets(Path("data_sources/ansm.yml"))

        assert list(datasets) == LOAD_ORDER
        assert all(dataset.postgresql_table == f"ansm_{name}" for name, dataset in datasets.items())
        assert all(dataset.base_url == "https://demo.data.gouv.fr/api/1/datasets/r/" for dataset in datasets.values())


# ---------------------------------------------------------------------------
# fetch_csv
# ---------------------------------------------------------------------------


class TestFetchCsv:
    def _mock_urlopen(self, content: str, encoding: str = "utf-8"):
        mock_response = MagicMock()
        mock_response.read.return_value = content.encode(encoding)
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        return patch("infomedicament_dataeng.datagouv.importer.urllib.request.urlopen", return_value=mock_response)

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

    def test_keeps_first_row_when_no_header(self, sample_dataset: DataGouvDataset):
        headerless_csv = "val1|val2|val3\nval4|val5|val6\n"
        sample_dataset.source.has_header = False
        with self._mock_urlopen(headerless_csv):
            rows = fetch_csv(sample_dataset)
        assert rows == [["val1", "val2", "val3"], ["val4", "val5", "val6"]]

    def test_builds_correct_url(self, sample_dataset: DataGouvDataset):
        with self._mock_urlopen(SAMPLE_CSV) as mock_urlopen:
            fetch_csv(sample_dataset)
        mock_urlopen.assert_called_once_with("https://www.data.gouv.fr/api/1/datasets/r/abc-123")

    def test_uses_base_url_override(self, sample_dataset: DataGouvDataset):
        sample_dataset.base_url = "https://demo.data.gouv.fr/api/1/datasets/r/"
        with self._mock_urlopen(SAMPLE_CSV) as mock_urlopen:
            fetch_csv(sample_dataset)
        mock_urlopen.assert_called_once_with("https://demo.data.gouv.fr/api/1/datasets/r/abc-123")


# ---------------------------------------------------------------------------
# import_dataset
# ---------------------------------------------------------------------------


class TestImportDataset:
    def _mock_engine(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return (
            patch("infomedicament_dataeng.datagouv.importer.get_postgres_engine", return_value=mock_engine),
            mock_engine,
            mock_conn,
        )

    def test_truncates_before_insert(self, sample_dataset: DataGouvDataset):
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with (
            mock_engine_patch,
            patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=[["a", "b", "c"]]),
        ):
            import_dataset(sample_dataset)
        truncate_call = mock_conn.execute.call_args_list[0]
        sql = str(truncate_call.args[0])
        assert "TRUNCATE" in sql.upper()
        assert "test_table" in sql

    def test_copies_all_rows(self, sample_dataset: DataGouvDataset):
        rows = [["val1", "val2", "val3"], ["val4", "val5", "val6"]]
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with mock_engine_patch, patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=rows):
            import_dataset(sample_dataset)
        cur = mock_conn.connection.dbapi_connection.cursor.return_value.__enter__.return_value
        copy_sql, buf = cur.copy_expert.call_args.args
        assert copy_sql.upper().startswith("COPY")
        assert "test_table" in copy_sql
        assert "col_a, col_b, col_c" in copy_sql
        assert buf.getvalue() == '"val1","val2","val3"\n"val4","val5","val6"\n'

    def test_ignores_surplus_trailing_empty_csv_field(self, sample_dataset: DataGouvDataset):
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with (
            mock_engine_patch,
            patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=[["val1", "val2", "val3", ""]]),
        ):
            count = import_dataset(sample_dataset)

        cur = mock_conn.connection.dbapi_connection.cursor.return_value.__enter__.return_value
        _, buf = cur.copy_expert.call_args.args
        assert count == 1
        assert buf.getvalue() == '"val1","val2","val3"\n'

    def test_serializes_typed_nulls_and_arrays(self, sample_dataset: DataGouvDataset):
        sample_dataset.columns = [
            ColumnDef(name="col_a", type="int"),
            ColumnDef(name="col_b", type="array"),
            ColumnDef(name="col_c", type="str"),
        ]
        rows = [["", '["one", "two"]', ""]]
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with mock_engine_patch, patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=rows):
            import_dataset(sample_dataset)

        cur = mock_conn.connection.dbapi_connection.cursor.return_value.__enter__.return_value
        _, buf = cur.copy_expert.call_args.args
        assert buf.getvalue() == ',"{""one"",""two""}",""\n'

    def test_returns_row_count(self, sample_dataset: DataGouvDataset):
        rows = [["a", "b", "c"]] * 42
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with mock_engine_patch, patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=rows):
            count = import_dataset(sample_dataset)
        assert count == 42

    def test_commits_transaction(self, sample_dataset: DataGouvDataset):
        mock_engine_patch, mock_engine, mock_conn = self._mock_engine()
        with mock_engine_patch, patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=[]):
            import_dataset(sample_dataset)
        mock_engine.begin.assert_called_once()

    def test_specialty_import_synchronizes_metadata(self, sample_dataset: DataGouvDataset):
        sample_dataset.postgresql_table = "ansm_specialite"
        mock_engine_patch, _, mock_conn = self._mock_engine()
        with (
            mock_engine_patch,
            patch("infomedicament_dataeng.datagouv.importer.fetch_csv", return_value=[]),
            patch("infomedicament_dataeng.datagouv.importer.sync_specialites_metadata") as sync_metadata,
        ):
            import_dataset(sample_dataset)

        sync_metadata.assert_called_once_with(mock_conn)
