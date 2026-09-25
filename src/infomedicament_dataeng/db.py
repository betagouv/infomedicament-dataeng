"""PostgreSQL operations for semantic document imports."""

import logging
from collections.abc import Iterator
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

from .config import PostgresConfig, get_config

logger = logging.getLogger(__name__)


def get_postgres_engine(config: PostgresConfig | None = None) -> Engine:
    """Create a SQLAlchemy engine for PostgreSQL (postgresql+psycopg2)."""
    if config is None:
        config = get_config().postgres
    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database,
        )
    )


def get_glossary_terms(config: PostgresConfig | None = None) -> list[str]:
    """Return the distinct glossary names marked for annotation."""
    engine = get_postgres_engine(config)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT DISTINCT btrim(nom) AS nom "
                "FROM ref_glossaire "
                "WHERE a_souligner IS TRUE AND btrim(nom) <> '' "
                "ORDER BY nom"
            )
        )
        return list(result.scalars())


def iter_pediatric_rcps(
    config: PostgresConfig | None = None,
    cis: str | None = None,
    limit: int | None = None,
    batch_size: int = 500,
) -> Iterator[dict[str, str]]:
    """Stream semantic RCP documents and their ATC code from PostgreSQL."""
    engine = get_postgres_engine(config)
    query = text(
        """
        SELECT r."codeCIS"::text AS cis,
               r.content_html,
               (
                   SELECT ca.code_terme_atc
                   FROM cis_atc ca
                   WHERE ca.code_cis::text = r."codeCIS"::text
                   ORDER BY ca.code_terme_atc
                   LIMIT 1
               ) AS atc_code
        FROM rcp r
        WHERE r.content_html IS NOT NULL
          AND btrim(r.content_html) <> ''
          AND (:cis IS NULL OR r."codeCIS"::text = :cis)
        ORDER BY r."codeCIS"
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execution_options(yield_per=batch_size).execute(query, {"cis": cis, "limit": limit}).mappings()
        for row in rows:
            yield {
                "cis": str(row["cis"]),
                "content_html": row["content_html"],
                "atc_code": row["atc_code"] or "",
            }


def get_semantic_import_worklist(
    since: datetime | None,
    config: PostgresConfig | None = None,
    cis: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Return specialties and their ANSM documents for a DB-driven semantic import.

    A specialty is selected when one of its documents changed since the cutoff.
    ``since=None`` selects the full catalog.
    """
    engine = get_postgres_engine(config)
    query = text(
        """
        WITH selected AS (
            SELECT s.cis, s.denomination, s.procedure, s.code_ema
            FROM ansm_specialite s
            WHERE (:since IS NULL
                   OR EXISTS (
                       SELECT 1 FROM ansm_document changed
                       WHERE changed.cis = s.cis AND changed.date_modification >= :since
                   ))
              AND (:cis IS NULL OR s.cis = :cis)
            ORDER BY s.cis
            LIMIT :limit
        )
        SELECT selected.cis, selected.denomination, selected.procedure, selected.code_ema,
               d.type AS document_type, d.url
        FROM selected
        LEFT JOIN ansm_document d ON d.cis = selected.cis
        ORDER BY selected.cis, d.type
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"since": since, "cis": cis, "limit": limit}).mappings()
        specialties: dict[str, dict] = {}
        for row in rows:
            item = specialties.setdefault(
                str(row["cis"]),
                {
                    "cis": str(row["cis"]),
                    "denomination": row["denomination"] or "",
                    "procedure": row["procedure"] or "",
                    "code_ema": row["code_ema"] or "",
                    "documents": {},
                },
            )
            if row["document_type"] in {"notice", "rcp"} and row["url"]:
                item["documents"][row["document_type"]] = row["url"]
        return list(specialties.values())


def get_centralised_specialties(config: PostgresConfig | None = None, cis: str | None = None) -> list[dict[str, str]]:
    """Return centralised specialties and their EMA product number from PostgreSQL."""
    engine = get_postgres_engine(config)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT cis, denomination, code_ema FROM ansm_specialite"
                " WHERE procedure = 'CENTRALISEE' AND (:cis IS NULL OR cis = :cis)"
                " ORDER BY cis"
            ),
            {"cis": cis},
        ).mappings()
        return [
            {
                "cis": str(row["cis"]),
                "denomination": row["denomination"] or "",
                "code_ema": row["code_ema"] or "",
            }
            for row in rows
        ]


def _upsert_semantic_document(conn, table: str, record: dict) -> None:
    """Upsert semantic HTML and extracted metadata for one document."""
    if table not in {"notices", "rcp"}:
        raise ValueError(f"Unsupported semantic document table: {table}")

    cis = record.get("cis")
    if not cis:
        raise ValueError("Semantic document record is missing its CIS code")

    conn.execute(
        text(
            f'INSERT INTO {table} ("codeCIS", content_html, "dateNotif")'
            " VALUES (:cis, :content_html, :date_notif)"
            ' ON CONFLICT ("codeCIS") DO UPDATE'
            " SET content_html = EXCLUDED.content_html,"
            ' "dateNotif" = EXCLUDED."dateNotif"'
        ),
        {
            "cis": int(cis),
            "content_html": record["content_html"],
            "date_notif": record.get("date_notif"),
        },
    )

    if table == "notices":
        conn.execute(
            text('UPDATE specialites_metadata SET description = :description WHERE "CIS" = :cis'),
            {
                "cis": int(cis),
                "description": record.get("indication") or "",
            },
        )


def import_semantic_documents(
    records,
    table: str,
    config: PostgresConfig | None = None,
    fail_fast: bool = False,
) -> tuple[int, int]:
    """Upsert semantic HTML into PostgreSQL, committing each document independently.

    Args:
        fail_fast: Re-raise on the first failing record instead of counting it.
    """
    engine = get_postgres_engine(config)
    imported = 0
    errors = 0
    with engine.connect() as conn:
        for record in records:
            cis = record.get("cis", "?")
            try:
                _upsert_semantic_document(conn, table, record)
                conn.commit()
                imported += 1
            except Exception as e:
                conn.rollback()
                if fail_fast:
                    raise
                logger.error("CIS %s failed: %s", cis, str(e).split("\n")[0])
                logger.debug("CIS %s full traceback", cis, exc_info=True)
                errors += 1
    return imported, errors
