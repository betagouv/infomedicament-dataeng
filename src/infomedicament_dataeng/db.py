"""Database operations for CIS mapping."""

import os
import re

import psycopg2
import pymysql
import pymysql.cursors

from .config import DatabaseConfig, PostgresConfig, get_config


def get_cis_atc_mapping(config: PostgresConfig | None = None) -> dict[str, str]:
    """Get CIS → ATC code mapping from PostgreSQL.

    Args:
        config: PostgreSQL configuration. If None, uses config from environment.

    Returns:
        Dict mapping CIS codes to ATC codes.
    """
    import psycopg2

    if config is None:
        config = get_config().postgres

    conn = psycopg2.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        dbname=config.database,
        port=config.port,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT code_cis, code_terme_atc FROM cis_atc")
            return {str(row[0]): row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def get_filename_to_cis_mapping(config: DatabaseConfig | None = None) -> dict[str, str]:
    """
    Retrieve the filename -> CIS mapping from the database.

    Args:
        config: Database configuration. If None, uses config from environment.

    Returns:
        Dict mapping filenames to CIS codes.
    """
    if config is None:
        config = get_config().database

    connexion = pymysql.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port,
        cursorclass=pymysql.cursors.DictCursor,
    )
    mapping = {}
    try:
        with connexion.cursor() as cursor:
            cursor.execute("""
                SELECT
                  sd.SpecId AS cis,
                  d.DocPath AS filename
                FROM
                  Spec_Doc sd
                JOIN
                  Document d ON sd.DocId = d.DocId
            """)
            for row in cursor.fetchall():
                basename = os.path.basename(row["filename"])
                mapping[basename] = row["cis"]
    finally:
        connexion.close()
    return mapping


def get_clean_html(html: str) -> str:
    """Remove <a name="...">...</a> tags while preserving their content."""
    return re.sub(r"<a name=[^>]*>(.*?)</a>", r"\1", html, flags=re.DOTALL)


def _delete_content_tree(cur, content_table: str, ids: list[int]) -> None:
    """Recursively delete a content tree bottom-up (children before parents)."""
    if not ids:
        return
    cur.execute(f'SELECT children FROM {content_table} WHERE id = ANY(%s)', (ids,))
    nested = []
    for (children,) in cur.fetchall():
        if children:
            nested.extend(children)
    if nested:
        _delete_content_tree(cur, content_table, nested)
    cur.execute(f'DELETE FROM {content_table} WHERE id = ANY(%s)', (ids,))


def _insert_content_blocks(cur, content_table: str, blocks: list) -> list[int]:
    """Recursively insert content blocks, returning their inserted IDs."""
    ids = []
    for block in blocks:
        if not (block.get("content") or block.get("children") or block.get("text")):
            continue

        is_table = block.get("type") == "table"

        children_ids = []
        if block.get("children") and not is_table:
            children_ids = _insert_content_blocks(cur, content_table, block["children"])

        content_val = block.get("content")
        if isinstance(content_val, str):
            content_val = [content_val]

        styles_val = block.get("styles")
        if isinstance(styles_val, str):
            styles_val = [styles_val]

        html_val = block.get("html") or None
        if html_val and not is_table:
            html_val = get_clean_html(html_val)

        cur.execute(
            f"INSERT INTO {content_table} (type, styles, anchor, content, children, tag, rowspan, colspan, html)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (
                block.get("type") or None,
                styles_val or None,
                block.get("anchor") or None,
                content_val or None,
                children_ids or None,
                block.get("tag") or None,
                block.get("rowspan"),
                block.get("colspan"),
                html_val,
            ),
        )
        row = cur.fetchone()
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

    with conn.cursor() as cur:
        cur.execute(f'SELECT children FROM {main_table} WHERE "codeCIS" = %s', (code_cis,))
        existing = cur.fetchone()
        if existing and existing[0]:
            _delete_content_tree(cur, content_table, existing[0])

        children_ids = _insert_content_blocks(cur, content_table, real_content)

        cur.execute(
            f'INSERT INTO {main_table} ("codeCIS", title, "dateNotif", children) VALUES (%s, %s, %s, %s)'
            f' ON CONFLICT ("codeCIS") DO UPDATE SET title = EXCLUDED.title,'
            f' "dateNotif" = EXCLUDED."dateNotif", children = EXCLUDED.children',
            (code_cis, title or None, date_notif or None, children_ids or None),
        )


def import_to_postgres(
    records: list[dict],
    main_table: str,
    content_table: str,
    config: PostgresConfig | None = None,
) -> tuple[int, int]:
    """Import parsed JSONL records into PostgreSQL.

    Args:
        records: Parsed JSONL records to import.
        main_table: Target table ("notices" or "rcp").
        content_table: Content table ("notices_content" or "rcp_content").
        config: PostgreSQL config. If None, uses config from environment.

    Returns:
        Tuple of (imported_count, error_count).
    """
    if config is None:
        config = get_config().postgres

    conn = psycopg2.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        dbname=config.database,
        port=config.port,
    )
    imported = 0
    errors = 0
    try:
        for record in records:
            try:
                _import_one_record(conn, main_table, content_table, record)
                conn.commit()
                imported += 1
            except Exception:
                conn.rollback()
                errors += 1
    finally:
        conn.close()
    return imported, errors


def get_authorized_cis(config: DatabaseConfig | None = None) -> set[str]:
    """
    Retrieve the list of authorized CIS codes from the database.

    Returns the SpecId of all specialties where isBdm is true.

    Args:
        config: Database configuration. If None, uses config from environment.

    Returns:
        Set of authorized CIS codes.
    """
    if config is None:
        config = get_config().database

    connexion = pymysql.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port,
        cursorclass=pymysql.cursors.DictCursor,
    )
    cis_set = set()
    try:
        with connexion.cursor() as cursor:
            cursor.execute("SELECT SpecId FROM Specialite WHERE isBdm")
            for row in cursor.fetchall():
                cis_set.add(str(row["SpecId"]))
    finally:
        connexion.close()
    return cis_set
