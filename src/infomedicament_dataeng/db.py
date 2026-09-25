"""Database operations for CIS mapping."""

import logging
import os
import re
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

from .config import DatabaseConfig, PostgresConfig, get_config

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


def get_mysql_engine(config: DatabaseConfig | None = None) -> Engine:
    """Create a SQLAlchemy engine for MySQL (mysql+pymysql)."""
    if config is None:
        config = get_config().database
    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            database=config.database,
        )
    )


def get_cis_atc_mapping(config: PostgresConfig | None = None) -> dict[str, str]:
    """Get CIS → ATC code mapping from PostgreSQL."""
    engine = get_postgres_engine(config)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT code_cis, code_terme_atc FROM cis_atc"))
        return {str(row[0]): row[1] for row in result.fetchall()}


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


def get_filename_to_cis_mapping(config: DatabaseConfig | None = None) -> dict[str, str]:
    """Retrieve the filename → CIS mapping from MySQL."""
    engine = get_mysql_engine(config)
    mapping = {}
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT sd.SpecId AS cis, d.DocPath AS filename
                FROM Spec_Doc sd
                JOIN Document d ON sd.DocId = d.DocId
            """)
        )
        for row in result.mappings():
            mapping[os.path.basename(row["filename"])] = row["cis"]
    return mapping


def get_authorized_cis(config: DatabaseConfig | None = None) -> set[str]:
    """Return SpecId of all specialties where isBdm is true."""
    engine = get_mysql_engine(config)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT SpecId FROM Specialite WHERE isBdm"))
        return {str(row[0]) for row in result.fetchall()}


def get_centralised_specialties(
    config: PostgresConfig | None = None, cis: str | None = None
) -> list[dict[str, str]]:
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


def check_sequences(tables: list[str], fix: bool = False, config: PostgresConfig | None = None) -> list[tuple]:
    """Report tables whose id sequence lags MAX(id), which makes every INSERT fail.

    Happens when rows arrive with explicit ids (dump restore, seed) without a
    matching setval. Returns (table, last_value, max_id, drifted) per table.
    """
    engine = get_postgres_engine(config)
    rows = []
    with engine.connect() as conn:
        for table in tables:
            seq = conn.execute(text("SELECT pg_get_serial_sequence(:t, 'id')"), {"t": table}).scalar()
            if not seq:
                logger.warning("%s: no serial sequence on id", table)
                continue
            last_value, is_called = conn.execute(text(f"SELECT last_value, is_called FROM {seq}")).one()
            max_id = int(conn.execute(text(f"SELECT COALESCE(MAX(id), 0) FROM {table}")).scalar() or 0)
            # The next id nextval() will hand out; is_called=false means last_value itself is next.
            next_id = int(last_value) + (1 if is_called else 0)
            drifted = next_id <= max_id
            rows.append((table, next_id, max_id, drifted))
            if drifted and fix:
                conn.execute(text("SELECT setval(:s, :v, false)"), {"s": seq, "v": max_id + 1})
                conn.commit()
                logger.info("%s: sequence reset to %d", table, max_id + 1)
    return rows


def get_clean_html(html: str) -> str:
    """Remove <a name="...">...</a> tags while preserving their content."""
    return re.sub(r"<a name=[^>]*>(.*?)</a>", r"\1", html, flags=re.DOTALL)


def _delete_content_tree(conn, content_table: str, ids: list[int]) -> None:
    """Recursively delete a content tree bottom-up (children before parents)."""
    if not ids:
        return
    result = conn.execute(
        text(f"SELECT children FROM {content_table} WHERE id = ANY(:ids)"),
        {"ids": ids},
    )
    nested = []
    for (children,) in result.fetchall():
        if children:
            nested.extend(children)
    if nested:
        _delete_content_tree(conn, content_table, nested)
    conn.execute(
        text(f"DELETE FROM {content_table} WHERE id = ANY(:ids)"),
        {"ids": ids},
    )


def _insert_content_blocks(conn, content_table: str, blocks: list) -> list[int]:
    """Recursively insert content blocks, returning their inserted IDs."""
    ids = []
    for block in blocks:
        if not (block.get("content") or block.get("children") or block.get("text")):
            continue

        is_table = block.get("type") == "table"

        children_ids = []
        if block.get("children") and not is_table:
            children_ids = _insert_content_blocks(conn, content_table, block["children"])

        content_val = block.get("content")
        if isinstance(content_val, str):
            content_val = [content_val]

        styles_val = block.get("styles")
        if isinstance(styles_val, str):
            styles_val = [styles_val]

        html_val = block.get("html") or None
        if html_val and not is_table:
            html_val = get_clean_html(html_val)

        result = conn.execute(
            text(
                f"INSERT INTO {content_table}"
                " (type, styles, anchor, content, children, tag, rowspan, colspan, html)"
                " VALUES (:type, :styles, :anchor, :content, :children, :tag, :rowspan, :colspan, :html)"
                " RETURNING id"
            ),
            {
                "type": block.get("type") or None,
                "styles": styles_val or None,
                "anchor": block.get("anchor") or None,
                "content": content_val or None,
                "children": children_ids or None,
                "tag": block.get("tag") or None,
                "rowspan": block.get("rowspan"),
                "colspan": block.get("colspan"),
                "html": html_val,
            },
        )
        row = result.fetchone()
        if row:
            ids.append(row[0])
    return ids


def _import_one_record(conn, main_table: str, content_table: str, record: dict) -> None:
    """Insert or update one parsed JSONL record. Caller is responsible for commit/rollback."""
    source = record.get("source", {})
    cis = source.get("cis")
    if not cis:
        raise ValueError(f"record has no source.cis (source={source!r})")

    code_cis = int(cis)
    content_blocks = record.get("content") or []

    title = ""
    date_notif = ""
    real_content = []
    for block in content_blocks:
        btype = block.get("type", "")
        if btype == "DateNotif":
            val = block.get("content", "")
            date_notif = val[0] if isinstance(val, list) else val
        elif btype == "AmmAnnexeTitre":
            val = block.get("content", "")
            title = val[0] if isinstance(val, list) else val
        elif block.get("content") or block.get("children"):
            real_content.append(block)

    result = conn.execute(
        text(f'SELECT children FROM {main_table} WHERE "codeCIS" = :cis'),
        {"cis": code_cis},
    )
    existing = result.fetchone()
    if existing and existing[0]:
        _delete_content_tree(conn, content_table, existing[0])

    children_ids = _insert_content_blocks(conn, content_table, real_content)

    conn.execute(
        text(
            f'INSERT INTO {main_table} ("codeCIS", title, "dateNotif", children)'
            " VALUES (:cis, :title, :date, :children)"
            f' ON CONFLICT ("codeCIS") DO UPDATE'
            " SET title = EXCLUDED.title,"
            ' "dateNotif" = EXCLUDED."dateNotif",'
            " children = EXCLUDED.children"
        ),
        {
            "cis": code_cis,
            "title": title or None,
            "date": date_notif or None,
            "children": children_ids or None,
        },
    )


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


def import_to_postgres(
    records,
    main_table: str,
    content_table: str,
    config: PostgresConfig | None = None,
    fail_fast: bool = False,
) -> tuple[int, int]:
    """Import parsed JSONL records into PostgreSQL.

    Args:
        fail_fast: Re-raise on the first failing record instead of counting it.

    Returns:
        Tuple of (imported_count, error_count).
    """
    engine = get_postgres_engine(config)
    imported = 0
    errors = 0
    with engine.connect() as conn:
        for record in records:
            cis = (record.get("source") or {}).get("cis", "?")
            try:
                _import_one_record(conn, main_table, content_table, record)
                conn.commit()
                imported += 1
            except Exception as e:
                conn.rollback()
                if fail_fast:
                    raise
                # Only the root cause: the full record is megabytes of content blocks.
                logger.error("CIS %s failed: %s", cis, str(e).split("\n")[0])
                logger.debug("CIS %s full traceback", cis, exc_info=True)
                errors += 1
    return imported, errors
