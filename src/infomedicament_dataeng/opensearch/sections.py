"""ETL: index parsed Notice/RCP sections into OpenSearch (one document per section)."""

import json
import logging
from collections.abc import Iterable, Iterator
from datetime import date

import pymysql
import pymysql.cursors
from opensearchpy import OpenSearch, helpers

from ..config import DatabaseConfig, OpenSearchConfig, get_config
from .client import create_or_update_index, get_opensearch_client

logger = logging.getLogger(__name__)

DEFAULT_INDEX = "specialite_sections"

INDEX_MAPPING = {
    "settings": {
        "analysis": {
            "filter": {
                "french_elision": {
                    "type": "elision",
                    "articles_case": True,
                    "articles": ["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"],
                },
                "french_stop": {"type": "stop", "stopwords": "_french_"},
                "french_stemmer": {"type": "stemmer", "language": "light_french"},
            },
            "analyzer": {
                "french": {
                    "tokenizer": "standard",
                    "filter": ["french_elision", "lowercase", "asciifolding", "french_stop", "french_stemmer"],
                }
            },
        }
    },
    "mappings": {
        "properties": {
            "cis_code": {"type": "keyword"},
            "spec_name": {
                "type": "text",
                "analyzer": "french",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "doc_type": {"type": "keyword"},
            "section_type": {"type": "keyword"},
            "section_anchor": {"type": "keyword"},
            "section_title": {"type": "text", "analyzer": "french"},
            "text_content": {"type": "text", "analyzer": "french"},
            "date_notif": {"type": "keyword"},
        }
    },
}

_SKIP_TYPES = {"AmmAnnexeTitre"}
_DATE_NOTIF_TYPE = "DateNotif"
_BANNED_ANCHORS = {
    "Ann3bSomm",  # "Que contient cette notice ?" — table of contents, no search value
}


def load_cis_names(config: DatabaseConfig | None = None) -> dict[str, str]:
    """Load CIS code → specialité name mapping from MySQL.

    Returns:
        Dict mapping CIS code strings to SpecDenom01 (specialité name).
    """
    if config is None:
        config = get_config().database

    conn = pymysql.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT SpecId, SpecDenom01 FROM Specialite WHERE isBdm")
            return {str(row["SpecId"]): row["SpecDenom01"] for row in cur.fetchall()}
    finally:
        conn.close()


def _extract_text(block: dict) -> str:
    """Recursively extract plain text from a content block."""
    parts = []

    content = block.get("content")
    if isinstance(content, list):
        parts.extend(c for c in content if isinstance(c, str))
    elif isinstance(content, str):
        parts.append(content)

    for child in block.get("children") or []:
        child_text = _extract_text(child)
        if child_text:
            parts.append(child_text)

    return " ".join(p.strip() for p in parts if p.strip())


def _iter_section_docs(record: dict, doc_type: str, cis_names: dict[str, str]) -> Iterator[dict]:
    """Yield one OpenSearch document per section from a parsed JSONL record.

    Skips AmmAnnexeTitre and banned anchors. Uses DateNotif as metadata
    propagated to all section documents.
    """
    source = record.get("source", {})
    cis = str(source.get("cis", ""))
    spec_name = cis_names.get(cis, "")
    date_notif = ""

    for block in record.get("content") or []:
        block_type = block.get("type", "")

        if block_type == _DATE_NOTIF_TYPE:
            content = block.get("content", "")
            date_notif = content[0] if isinstance(content, list) else content
            continue

        if block_type in _SKIP_TYPES:
            continue

        if block.get("anchor") in _BANNED_ANCHORS:
            continue

        text = _extract_text(block)
        if not text:
            continue

        section_title = block.get("content", "")
        if isinstance(section_title, list):
            section_title = " ".join(section_title)

        yield {
            "cis_code": cis,
            "spec_name": spec_name,
            "doc_type": doc_type,
            "section_type": block_type,
            "section_anchor": block.get("anchor") or "",
            "section_title": section_title.strip(),
            "text_content": text,
            "date_notif": date_notif,
        }


def index_records(
    records: Iterable[dict],
    index_name: str,
    doc_type: str,
    cis_names: dict[str, str],
    client: OpenSearch,
) -> int:
    """Index an iterable of parsed JSONL records into OpenSearch.

    Documents use a deterministic ID ({cis}_{anchor}_{doc_type}), so
    re-indexing is idempotent.

    Returns the number of documents successfully indexed.
    """

    def _actions() -> Iterator[dict]:
        for record in records:
            for doc in _iter_section_docs(record, doc_type, cis_names):
                doc_id = f"{doc['cis_code']}_{doc['section_anchor']}_{doc_type}"
                yield {"_index": index_name, "_id": doc_id, "_source": doc}

    success, failed = helpers.bulk(client, _actions(), raise_on_error=False, stats_only=True)
    if failed:
        logger.warning(f"{failed} documents failed to index")
    return success


def index_from_local(
    path: str,
    index_name: str,
    doc_type: str,
    limite: int | None = None,
    config: OpenSearchConfig | None = None,
    db_config: DatabaseConfig | None = None,
) -> int:
    """Index a local JSONL file into OpenSearch."""
    client = get_opensearch_client(config)
    create_or_update_index(client, index_name, INDEX_MAPPING)
    cis_names = load_cis_names(db_config)
    logger.info(f"Loaded {len(cis_names)} CIS names from MySQL")

    def _read_records() -> Iterator[dict]:
        count = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if limite is not None and count >= limite:
                    break
                try:
                    yield json.loads(line)
                    count += 1
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse line: {e}")

    total = index_records(_read_records(), index_name, doc_type, cis_names, client)
    logger.info(f"Indexed {total} section documents from {path}")
    return total


def index_from_s3(
    pattern: str,
    index_name: str,
    doc_type: str,
    since: date | None = None,
    limite: int | None = None,
    config: OpenSearchConfig | None = None,
    db_config: DatabaseConfig | None = None,
) -> int:
    """Index parsed JSONL files from S3 into OpenSearch.

    Args:
        pattern: "N" for Notices, "R" for RCP.
        since: If provided, only index JSONL files dated on or after this date.
    """
    from ..s3 import S3Client

    app_config = get_config()
    if not app_config.s3.is_configured():
        raise RuntimeError("S3 credentials not configured. Set S3_KEY_ID and S3_KEY_SECRET.")

    s3_client = S3Client(app_config.s3)
    client = get_opensearch_client(config)
    create_or_update_index(client, index_name, INDEX_MAPPING)
    cis_names = load_cis_names(db_config)
    logger.info(f"Loaded {len(cis_names)} CIS names from MySQL")

    jsonl_keys = list(s3_client.list_parsed_files(pattern, since=since))
    logger.info(f"Found {len(jsonl_keys)} JSONL files to index from S3")

    total = 0
    for key in jsonl_keys:
        content = s3_client.download_file_content(key)
        lines = [line for line in content.decode("utf-8").split("\n") if line.strip()]

        if limite is not None:
            remaining = limite - total
            if remaining <= 0:
                break
            lines = lines[: remaining * 40]  # rough cap (avg ~40 sections per record)

        records = []
        for line in lines:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse line in {key}: {e}")

        indexed = index_records(records, index_name, doc_type, cis_names, client)
        total += indexed
        logger.info(f"{key}: {indexed} sections indexed")

        if limite is not None and total >= limite:
            break

    logger.info(f"S3 indexing complete: {total} section documents indexed")
    return total
