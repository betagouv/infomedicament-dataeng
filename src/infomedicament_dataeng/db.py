"""Database operations for CIS mapping."""

import os
import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

from .config import DatabaseConfig, PostgresConfig, get_config


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
        return

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


def import_to_postgres(
    records,
    main_table: str,
    content_table: str,
    config: PostgresConfig | None = None,
) -> tuple[int, int]:
    """Import parsed JSONL records into PostgreSQL.

    Returns:
        Tuple of (imported_count, error_count).
    """
    engine = get_postgres_engine(config)
    imported = 0
    errors = 0
    with engine.connect() as conn:
        for record in records:
            try:
                _import_one_record(conn, main_table, content_table, record)
                conn.commit()
                imported += 1
            except Exception:
                conn.rollback()
                errors += 1
    return imported, errors
