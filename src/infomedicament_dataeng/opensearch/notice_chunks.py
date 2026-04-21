"""ETL: index parsed Notice sections into OpenSearch as fine-grained chunks with vector embeddings."""

import gzip
import hashlib
import io
import json
import logging
import time
from collections.abc import Iterable, Iterator
from datetime import date

import openai
from openai import OpenAI
from opensearchpy import OpenSearch, helpers

from ..config import OpenSearchConfig, S3Config, get_config
from ..s3 import S3Client
from .client import create_or_update_index, get_opensearch_client
from .sections import _extract_text, _normalize_anchor

logger = logging.getLogger(__name__)

DEFAULT_INDEX = "notice_chunks"

# Node types that trigger a chunk boundary within a flat section
_FLAT_HEADER_TYPES = {"AmmCorpsTexteGras", "AmmAnnexeTitre3"}

# Top-level node types carrying no patient-relevant content
_SKIP_TOP_TYPES = {"AmmAnnexeTitre", "DateNotif", "AmmNoticeTitre1"}

# Section anchors to skip entirely
_SKIP_ANCHORS = {"Ann3bEmballage"}  # section 6: administrative / packaging info

INDEX_MAPPING = {
    "settings": {
        "index": {
            "knn": True,
            "knn.algo_param.ef_search": 100,
        },
        "analysis": {
            "filter": {
                "french_elision": {
                    "type": "elision",
                    "articles_case": True,
                    "articles": [
                        "l",
                        "m",
                        "t",
                        "qu",
                        "n",
                        "s",
                        "j",
                        "d",
                        "c",
                        "jusqu",
                        "quoiqu",
                        "lorsqu",
                        "puisqu",
                    ],
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
        },
    },
    "mappings": {
        "properties": {
            "cis": {"type": "keyword"},
            "section_anchor": {"type": "keyword"},
            "section_title": {"type": "text", "analyzer": "french"},
            "sub_header": {"type": "text", "analyzer": "french"},
            "text": {"type": "text", "analyzer": "french"},
            "embed_text": {"type": "text", "index": False},  # stored, not BM25-indexed
            "html_snippets": {"type": "object", "enabled": False},  # stored for UI highlighting
            "embedding": {
                "type": "knn_vector",
                "dimension": 1024,  # bge-m3 output dimension
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "engine": "nmslib",
                },
            },
        }
    },
}


def _node_text(value) -> str:
    if isinstance(value, list):
        return " ".join(value).strip()
    return (value or "").strip()


def _make_embed_text(section_title: str, sub_header: str, body: str) -> str:
    # bge-m3 does not require query/passage prefixes
    if sub_header:
        return f"{section_title} > {sub_header}: {body}"
    return f"{section_title}: {body}"


def _collect_html(nodes: list[dict]) -> list[str]:
    return [n["html"] for n in nodes if n.get("html")]


def _make_chunk(
    cis: str,
    section_anchor: str,
    section_title: str,
    sub_header: str,
    body_nodes: list[dict],
) -> dict | None:
    body_text = " ".join(_extract_text(n) for n in body_nodes).strip()
    if not body_text:
        return None
    embed_text = _make_embed_text(section_title, sub_header, body_text)
    doc_id = f"{cis}_{section_anchor}_{hashlib.md5(embed_text.encode()).hexdigest()[:8]}"
    return {
        "_id": doc_id,
        "cis": cis,
        "section_anchor": section_anchor,
        "section_title": section_title,
        "sub_header": sub_header,
        "text": body_text,
        "embed_text": embed_text,
        "html_snippets": _collect_html(body_nodes),
    }


def _iter_notice_chunks(record: dict) -> Iterator[dict]:
    """Yield fine-grained chunks from a parsed notice record.

    Each chunk is a logical block: a bold sub-header (AmmCorpsTexteGras /
    AmmAnnexeTitre3) followed by its body paragraphs and bullet lists, or
    an AmmAnnexeTitre2 sub-section with its children.

    Sections 1 to 5 are indexed; section 6 (emballage / admin) is skipped.
    """
    cis = str(record.get("source", {}).get("cis", ""))

    for section in record.get("content", []):
        if section.get("type") in _SKIP_TOP_TYPES:
            continue

        raw_anchor = section.get("anchor") or ""
        section_title = _node_text(section.get("content"))

        anchor = _normalize_anchor(raw_anchor, title=section_title, doc_type="notice")
        if anchor in _SKIP_ANCHORS:
            continue

        children = section.get("children") or []
        if not children:
            continue

        has_titre2 = any(c.get("type") == "AmmAnnexeTitre2" for c in children)

        if has_titre2:
            # Each AmmAnnexeTitre2 sub-section is its own chunk
            for child in children:
                if child.get("type") != "AmmAnnexeTitre2":
                    continue
                sub_title = _node_text(child.get("content"))
                child_anchor = _normalize_anchor(
                    child.get("anchor") or anchor,
                    title=sub_title,
                    doc_type="notice",
                )
                chunk = _make_chunk(cis, child_anchor, section_title, sub_title, child.get("children") or [])
                if chunk:
                    yield chunk
        else:
            # Flat children: use AmmCorpsTexteGras / AmmAnnexeTitre3 as chunk boundaries
            current_sub_header = ""
            current_body_nodes: list[dict] = []

            for child in children:
                if child.get("type") in _FLAT_HEADER_TYPES:
                    chunk = _make_chunk(cis, anchor, section_title, current_sub_header, current_body_nodes)
                    if chunk:
                        yield chunk
                    current_sub_header = _node_text(child.get("content"))
                    current_body_nodes = []
                else:
                    current_body_nodes.append(child)

            # Flush the last accumulated chunk
            chunk = _make_chunk(cis, anchor, section_title, current_sub_header, current_body_nodes)
            if chunk:
                yield chunk


def _content_hash(record: dict) -> str:
    """SHA1 of the raw notice content — used to detect source text changes for cache invalidation."""
    content = json.dumps(record.get("content", []), sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(content.encode()).hexdigest()


def _cache_key(s3_client: S3Client, cis: str) -> str:
    return f"{s3_client.config.output_prefix}embeddings/notices/{cis}.jsonl.gz"


def _save_embedding_cache(
    s3_client: S3Client, cis: str, content_hash: str, pairs: list[tuple[dict, list[float]]]
) -> None:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write((json.dumps({"version": 1, "cis": cis, "content_hash": content_hash}) + "\n").encode())
        for chunk, emb in pairs:
            gz.write((json.dumps({"_id": chunk["_id"], "embedding": emb}) + "\n").encode())
    s3_client.upload_file_content(_cache_key(s3_client, cis), buf.getvalue(), content_type="application/gzip")
    logger.debug(f"Saved embedding cache for CIS {cis}")


def _try_load_cache(s3_client: S3Client, cis: str, record: dict) -> list[tuple[dict, list[float]]] | None:
    key = _cache_key(s3_client, cis)
    if not s3_client.object_exists(key):
        return None
    try:
        lines = gzip.decompress(s3_client.download_file_content(key)).decode().splitlines()
        header = json.loads(lines[0])
        if header.get("content_hash") != _content_hash(record):
            logger.info(f"Cache stale for CIS {cis}, re-embedding")
            return None
        current_chunks = {c["_id"]: c for c in _iter_notice_chunks(record)}
        result = []
        for line in lines[1:]:
            entry = json.loads(line)
            chunk = current_chunks.get(entry["_id"])
            if chunk is None:
                # Should not happen when content_hash matched, but guard anyway
                logger.warning(f"Cache chunk ID mismatch for CIS {cis}, re-embedding")
                return None
            result.append((chunk, entry["embedding"]))
        return result or None
    except Exception as e:
        logger.warning(f"Failed to load embedding cache for CIS {cis}: {e}")
        return None


def _get_albert_client(config=None):
    albert = config or get_config().albert
    if not albert.is_configured():
        raise RuntimeError("ALBERT_API_KEY is not set.")
    return OpenAI(api_key=albert.api_key, base_url=albert.base_url), albert.model


def _embed_texts(texts: list[str], client, model: str) -> list[list[float]]:
    response = client.embeddings.create(model=model, input=texts, encoding_format="float")
    return [e.embedding for e in response.data]


def _embed_with_retry(texts: list[str], client, model: str, max_retries: int = 3) -> list[list[float]]:
    for attempt in range(max_retries - 1):
        try:
            return _embed_texts(texts, client, model)
        except (openai.RateLimitError, openai.APIStatusError) as e:
            wait = 2**attempt
            logger.warning(f"Albert API error ({e}), retrying in {wait}s...")
            time.sleep(wait)
    return _embed_texts(texts, client, model)  # last attempt: exceptions propagate


def index_notice_chunks(
    records: Iterable[dict],
    embed_client,
    embed_model: str,
    os_client: OpenSearch,
    index_name: str = DEFAULT_INDEX,
    chunk_batch_size: int = 64,  # AlbertAPI limit
    requests_per_minute: int = 500,
    s3_client: S3Client | None = None,
    save_embeddings: bool = False,
    load_embeddings: bool = False,
) -> int:
    """Chunk, embed via Albert API, and index an iterable of parsed notice records.

    Returns the total number of chunks successfully indexed.
    """
    create_or_update_index(os_client, index_name, INDEX_MAPPING)
    min_interval = 60.0 / requests_per_minute
    last_embed_call = 0.0
    total = 0

    # Each entry: (chunk, cis, content_hash) — cis + hash needed for cache save
    pending: list[tuple[dict, str, str]] = []

    def _bulk_index(pairs: list[tuple[dict, list[float]]]) -> int:
        actions = [
            {
                "_index": index_name,
                "_id": chunk["_id"],
                "_source": {k: v for k, v in chunk.items() if k != "_id"} | {"embedding": emb},
            }
            for chunk, emb in pairs
        ]
        success, failed = helpers.bulk(os_client, actions, raise_on_error=False, stats_only=True)
        if failed:
            logger.warning(f"{failed} chunks failed to index")
        return success

    def _flush() -> int:
        nonlocal last_embed_call
        if not pending:
            return 0
        chunks = [p[0] for p in pending]
        elapsed = time.monotonic() - last_embed_call
        if last_embed_call and elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        embeddings = _embed_with_retry([c["embed_text"] for c in chunks], embed_client, embed_model)
        last_embed_call = time.monotonic()

        if save_embeddings and s3_client:
            by_cis: dict[str, tuple[str, list[tuple[dict, list[float]]]]] = {}
            for (chunk, cis, content_hash), emb in zip(pending, embeddings):
                if cis not in by_cis:
                    by_cis[cis] = (content_hash, [])
                by_cis[cis][1].append((chunk, emb))
            for cis, (content_hash, pairs) in by_cis.items():
                _save_embedding_cache(s3_client, cis, content_hash, pairs)

        count = _bulk_index(list(zip(chunks, embeddings)))
        pending.clear()
        return count

    for record in records:
        cis = str(record.get("source", {}).get("cis", ""))

        if load_embeddings and s3_client:
            cached = _try_load_cache(s3_client, cis, record)
            if cached is not None:
                total += _bulk_index(cached)
                continue

        content_hash = _content_hash(record)
        for chunk in _iter_notice_chunks(record):
            pending.append((chunk, cis, content_hash))
            if len(pending) >= chunk_batch_size:
                total += _flush()

    total += _flush()
    return total


def index_from_local(
    path: str,
    index_name: str = DEFAULT_INDEX,
    limite: int | None = None,
    os_config: OpenSearchConfig | None = None,
    albert_config=None,
    chunk_batch_size: int = 512,
    requests_per_minute: int = 500,
) -> int:
    """Index a local parsed notice JSONL file into OpenSearch via Albert API embeddings."""
    os_client = get_opensearch_client(os_config)
    embed_client, embed_model = _get_albert_client(albert_config)
    logger.info(f"Using Albert API embedding model: {embed_model}")

    def _records() -> Iterator[dict]:
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

    total = index_notice_chunks(
        _records(),
        embed_client,
        embed_model,
        os_client,
        index_name,
        chunk_batch_size=chunk_batch_size,
        requests_per_minute=requests_per_minute,
    )
    logger.info(f"Indexed {total} chunks from {path} into '{index_name}'")
    return total


def index_from_s3(
    index_name: str = DEFAULT_INDEX,
    limite: int | None = None,
    os_config: OpenSearchConfig | None = None,
    albert_config=None,
    s3_config: S3Config | None = None,
    since: str | None = None,
    chunk_batch_size: int = 512,
    requests_per_minute: int = 500,
    save_embeddings: bool = False,
    load_embeddings: bool = False,
) -> int:
    """Index parsed notice JSONL files from S3 into OpenSearch via Albert API embeddings."""
    os_client = get_opensearch_client(os_config)
    embed_client, embed_model = _get_albert_client(albert_config)
    s3 = S3Client(s3_config or get_config().s3)
    since_date = date.fromisoformat(since) if since else None
    logger.info(f"Using Albert API embedding model: {embed_model}")

    def _records() -> Iterator[dict]:
        count = 0
        for key in s3.list_parsed_files("N", since=since_date):
            content = s3.download_file_content(key)
            for line in content.decode("utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                if limite is not None and count >= limite:
                    return
                try:
                    yield json.loads(line)
                    count += 1
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse line in {key}: {e}")

    total = index_notice_chunks(
        _records(),
        embed_client,
        embed_model,
        os_client,
        index_name,
        chunk_batch_size=chunk_batch_size,
        requests_per_minute=requests_per_minute,
        s3_client=s3,
        save_embeddings=save_embeddings,
        load_embeddings=load_embeddings,
    )
    logger.info(f"Indexed {total} chunks from S3 into '{index_name}'")
    return total
