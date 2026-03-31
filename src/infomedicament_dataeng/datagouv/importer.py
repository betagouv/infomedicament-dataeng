"""Fetch and import open data from data.gouv.fr."""

import csv
import io
import logging
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml
from sqlalchemy import text

from ..config import PostgresConfig, get_config
from ..db import get_postgres_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://www.data.gouv.fr/api/1/datasets/r/"

# Map YAML type strings to Python SQL types (extensible for future types)
_SQL_TYPES = {"str": "text"}


@dataclass
class ColumnDef:
    name: str
    type: str  # YAML type string, e.g. "str"


@dataclass
class CsvSource:
    delimiter: str
    encoding: str
    quotechar: str = '"'


@dataclass
class DataGouvDataset:
    datagouv_dataset_id: str
    postgresql_table: str
    source: CsvSource
    columns: list[ColumnDef]


def load_datasets(config_path: Path) -> dict[str, DataGouvDataset]:
    """Load dataset descriptors from a YAML config file."""
    with config_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    datasets = {}
    for name, d in raw["datasets"].items():
        src = d["source"]
        if src["type"] != "csv":
            raise ValueError(f"Unsupported source type {src['type']!r} for dataset {name!r}")
        datasets[name] = DataGouvDataset(
            datagouv_dataset_id=d["datagouv_dataset_id"],
            postgresql_table=d["postgresql_table"],
            source=CsvSource(delimiter=src["delimiter"], encoding=src["encoding"], quotechar=src.get("quotechar", '"')),
            columns=[ColumnDef(name=c["name"], type=c["type"]) for c in d["columns"]],
        )
    return datasets


def fetch_csv(dataset: DataGouvDataset) -> list[list[str]]:
    """Fetch the dataset CSV from data.gouv.fr, returning rows without the header."""
    url = BASE_URL + dataset.datagouv_dataset_id
    with urllib.request.urlopen(url) as response:
        content = response.read().decode(dataset.source.encoding)
    reader = csv.reader(io.StringIO(content), delimiter=dataset.source.delimiter, quotechar=dataset.source.quotechar)
    rows = list(reader)
    return rows[1:]  # skip header


def import_dataset(dataset: DataGouvDataset, config: PostgresConfig | None = None) -> int:
    """Truncate the target table and insert all rows fetched from data.gouv.fr.

    Returns:
        Number of rows imported.
    """
    if config is None:
        config = get_config().postgres

    rows = fetch_csv(dataset)
    expected_cols = len(dataset.columns)
    valid_rows = [row for row in rows if len(row) == expected_cols]
    if len(valid_rows) < len(rows):
        logger.warning(
            f"Skipped {len(rows) - len(valid_rows)} rows with unexpected column count (expected {expected_cols})"
        )
    rows = valid_rows
    logger.info(f"Fetched {len(rows)} rows for table '{dataset.postgresql_table}'")

    col_keys = [c.name for c in dataset.columns]
    col_names = ", ".join(col_keys)
    placeholders = ", ".join(f":{k}" for k in col_keys)
    rows_as_dicts = [dict(zip(col_keys, row)) for row in rows]

    engine = get_postgres_engine(config)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {dataset.postgresql_table}"))
        if rows_as_dicts:
            conn.execute(
                text(f"INSERT INTO {dataset.postgresql_table} ({col_names}) VALUES ({placeholders})"),
                rows_as_dicts,
            )
    logger.info(f"Imported {len(rows)} rows into '{dataset.postgresql_table}'")
    return len(rows)
