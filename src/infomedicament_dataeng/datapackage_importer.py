"""Import the ANSM frictionless datapackage into PostgreSQL."""

import io
import logging
import os
import tempfile
import urllib.request
from datetime import date, datetime

from frictionless import Package
from sqlalchemy import text

from .config import PostgresConfig, get_config
from .db import get_postgres_engine

logger = logging.getLogger(__name__)

# Respects FK dependency order: parents before children.
LOAD_ORDER = [
    "specialite",
    "atc",
    "classe_clinique",
    "pathologie",
    "delivrance",
    "groupe_substance",
    "classe_interaction",
    "substance_nom",
    "presentation",
    "presentation_evenement",
    "element",
    "specialite_atc",
    "specialite_classe_clinique",
    "classe_clinique_pathologie",
    "specialite_delivrance",
    "specialite_evenement",
    "classe_groupe_substance",
    "substance_groupe_substance",
    "interaction",
    "groupe_generique",
    "specialite_groupe_generique",
    "composant",
    "recipient",
    "dispositif",
    "document",
    "caracteristique",
    "specialite_titulaire",
]

TABLE_PREFIX = "ansm_"


def import_datapackage(
    package_path: str,
    resource_name: str | None = None,
    config: PostgresConfig | None = None,
) -> dict[str, int]:
    """Load resources from a frictionless datapackage into PostgreSQL.

    Tables must already exist (created by Kysely migrations in the infomedicament repo).
    Each table is truncated then fully reloaded from the CSV.

    Args:
        package_path: Path or URL to a datapackage.json or a zip containing one.
        resource_name: If given, load only this resource; otherwise load all in
                       dependency order.
        config: PostgreSQL connection config (defaults to env).

    Returns:
        Dict mapping table name to number of rows inserted.
    """
    if config is None:
        config = get_config().postgres

    logger.info(f"Loading package from: {package_path}")
    tmp_path = None
    if package_path.startswith("http://") or package_path.startswith("https://"):
        logger.info("Remote URL detected — downloading to a temporary file first ...")
        fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        urllib.request.urlretrieve(package_path, tmp_path)
        logger.info(f"Downloaded to {tmp_path} ({os.path.getsize(tmp_path):,} bytes)")
        local_path = tmp_path
    else:
        local_path = package_path

    try:
        package = Package(local_path)
        available = {r.name for r in package.resources}
        logger.info(f"Package contains {len(available)} resource(s): {sorted(available)}")

        if resource_name is not None:
            if resource_name not in available:
                raise ValueError(f"Resource '{resource_name}' not found in package. Available: {sorted(available)}")
            resources_to_load = [resource_name]
            logger.info(f"Single-resource mode: will load '{resource_name}' only")
        else:
            resources_to_load = [name for name in LOAD_ORDER if name in available]
            skipped = available - set(LOAD_ORDER)
            if skipped:
                logger.warning(f"Resources in package but not in LOAD_ORDER (will be skipped): {sorted(skipped)}")
            missing = set(LOAD_ORDER) - available
            if missing:
                logger.warning(f"Resources in LOAD_ORDER but not in package (will be skipped): {sorted(missing)}")
            logger.info(f"Will load {len(resources_to_load)} resource(s) in dependency order: {resources_to_load}")

        engine = get_postgres_engine(config)
        results = {}

        for name in resources_to_load:
            table_name = TABLE_PREFIX + name
            results[table_name] = _load_resource(package, name, table_name, engine)

        logger.info(f"Done. Loaded {len(results)} table(s): { {t: f'{n:,}' for t, n in results.items()} }")
        return results
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
            logger.debug(f"Removed temporary file {tmp_path}")


def _pg_array_literal(items) -> str:
    """Render a Python list as a PostgreSQL array literal, e.g. {"a","b"}.

    Every element is double-quoted (with backslash-escaping of \\ and ") so the
    literal is safe for any text content; None elements become an unquoted NULL.
    """
    parts = []
    for el in items:
        if el is None:
            parts.append("NULL")
        else:
            s = str(el).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{s}"')
    return "{" + ",".join(parts) + "}"


def _to_text(value) -> str | None:
    """Convert a frictionless cell to its COPY text form, or None for SQL NULL."""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return _pg_array_literal(value)
    return str(value)


def _csv_field(text_value: str | None) -> str:
    """CSV-encode one already-stringified cell for COPY ... FORMAT csv.

    None -> bare empty field (the default CSV NULL marker).
    Everything else is always quoted, so an empty string round-trips as "" (an
    empty string, distinct from NULL) and embedded commas/quotes/newlines are safe.
    """
    if text_value is None:
        return ""
    return '"' + text_value.replace('"', '""') + '"'


def _load_resource(package: Package, resource_name: str, table_name: str, engine) -> int:
    """Truncate `table_name` and COPY all rows from `resource_name`.

    Returns the number of rows inserted.
    """
    logger.info(f"[{resource_name}] Reading rows from package ...")
    resource = package.get_resource(resource_name)
    rows = resource.read_rows()
    logger.info(f"[{resource_name}] Read {len(rows):,} rows")

    if not rows:
        logger.warning(f"[{resource_name}] No rows to insert — truncating '{table_name}' and leaving it empty")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        return 0

    field_names = list(rows[0].keys())
    col_names = ", ".join(field_names)

    buf = io.StringIO()
    for row in rows:
        buf.write(",".join(_csv_field(_to_text(row[k])) for k in field_names))
        buf.write("\n")
    buf.seek(0)

    logger.info(f"[{resource_name}] Truncating '{table_name}' and COPYing {len(rows):,} rows ...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        raw = conn.connection.dbapi_connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {table_name} ({col_names}) FROM STDIN WITH (FORMAT csv)",
                buf,
            )

    logger.info(f"[{resource_name}] ✓ {len(rows):,} rows inserted into '{table_name}'")
    return len(rows)
