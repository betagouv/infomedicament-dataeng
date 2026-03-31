"""Tests for the SQL to CSV converter."""

import csv
from pathlib import Path

from infomedicament_dataeng.convert.sql_to_csv import extract_value, sql_to_csv


class TestExtractValue:
    """Tests for the extract_value helper function."""

    def test_extract_null_returns_none(self):
        """Test that SQL NULL is converted to Python None."""
        from sqlglot import exp

        val = exp.Null()
        assert extract_value(val) is None

    def test_extract_int_literal(self):
        """Test integer literal extraction."""
        from sqlglot import exp

        val = exp.Literal.number(42)
        result = extract_value(val)
        assert result == 42
        assert isinstance(result, int)

    def test_extract_float_literal(self):
        """Test float literal extraction."""
        from sqlglot import exp

        val = exp.Literal.number(3.14)
        result = extract_value(val)
        assert result == 3.14
        assert isinstance(result, float)

    def test_extract_string_literal(self):
        """Test string literal extraction."""
        from sqlglot import exp

        val = exp.Literal.string("hello")
        result = extract_value(val)
        assert result == "hello"

    def test_extract_national_string(self):
        """Test N'...' national string extraction (T-SQL)."""
        from sqlglot import exp

        val = exp.National(this="Mépolizumab")
        result = extract_value(val)
        assert result == "Mépolizumab"


class TestSqlToCsv:
    """Tests for the sql_to_csv function."""

    def test_converts_sql_to_csv(self, sample_atc_sql: Path, tmp_csv: Path):
        """Test basic SQL to CSV conversion."""
        result = sql_to_csv(sample_atc_sql, tmp_csv)

        assert tmp_csv.exists()
        assert result["table"] == "ClasseATC"
        assert result["rows"] == 10
        assert result["columns"] == 13

    def test_csv_has_correct_headers(self, sample_atc_sql: Path, tmp_csv: Path):
        """Test that CSV has correct column headers."""
        sql_to_csv(sample_atc_sql, tmp_csv)

        with open(tmp_csv, encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

        expected_headers = [
            "codeTerme",
            "codeTermePere",
            "libAbr",
            "libCourt",
            "libLong",
            "libLongAnglais",
            "libRech",
            "numOrdreEdit",
            "dateCreationTerme",
            "dateModifTerme",
            "dateInactivTerme",
            "textSourceRef",
            "remTerme",
        ]
        assert headers == expected_headers

    def test_csv_has_correct_data(self, sample_atc_sql: Path, tmp_csv: Path):
        """Test that CSV contains correct data values."""
        sql_to_csv(sample_atc_sql, tmp_csv)

        with open(tmp_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Check first row
        assert rows[0]["codeTerme"] == "0"
        assert rows[0]["libAbr"] == "R03DX09"
        assert rows[0]["libLongAnglais"] == "mepolizumab"

        # Check a row with NULL values
        assert rows[1]["codeTermePere"] == ""  # NULL converted to empty string

    def test_default_output_path(self, sample_atc_sql: Path, tmp_path: Path):
        """Test that default output path uses same name with .csv extension."""
        # Copy fixture to temp directory to avoid polluting fixtures
        temp_sql = tmp_path / "test.sql"
        temp_sql.write_bytes(sample_atc_sql.read_bytes())

        sql_to_csv(temp_sql)

        expected_csv = tmp_path / "test.csv"
        assert expected_csv.exists()

    def test_handles_escaped_quotes(self, sample_atc_sql: Path, tmp_csv: Path):
        """Test that escaped quotes in SQL are handled correctly."""
        sql_to_csv(sample_atc_sql, tmp_csv)

        with open(tmp_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Row with "Fluorure d'étain" (single quote in French)
        etain_row = next(r for r in rows if "A01AA04" in r["libAbr"])
        assert "'" in etain_row["libCourt"]  # Should have the quote

    def test_empty_sql_returns_zero_stats(self, tmp_path: Path):
        """Test handling of SQL file with no INSERT statements."""
        empty_sql = tmp_path / "empty.sql"
        empty_sql.write_text("-- Just a comment\nSELECT 1;", encoding="utf-8")

        result = sql_to_csv(empty_sql, encoding="utf-8")

        assert result["table"] is None
        assert result["rows"] == 0
        assert result["columns"] == 0

    def test_output_is_utf8(self, sample_atc_sql: Path, tmp_csv: Path):
        """Test that output CSV is UTF-8 encoded."""
        sql_to_csv(sample_atc_sql, tmp_csv)

        # Should be readable as UTF-8 without errors
        content = tmp_csv.read_text(encoding="utf-8")
        assert "VOIES DIGESTIVES" in content

    def test_non_insert_statement_logs_warning_and_continues(self, tmp_path: Path, caplog):
        """Test that non-INSERT statements log a warning but don't stop processing."""
        import logging

        mixed_sql = tmp_path / "mixed.sql"
        mixed_sql.write_text(
            "SELECT 1;\nINSERT INTO test (id, name) VALUES (1, 'foo'), (2, 'bar');\nCREATE TABLE other (x INT);\n",
            encoding="utf-8",
        )

        with caplog.at_level(logging.WARNING):
            result = sql_to_csv(mixed_sql, encoding="utf-8")

        # Should have logged warnings for SELECT and CREATE
        assert any("non-insert" in record.message.lower() for record in caplog.records)

        # But should still have processed the INSERT
        assert result["rows"] == 2
        assert result["table"] == "test"
