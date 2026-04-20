"""ETL: index parsed Notice sections into OpenSearch as fine-grained chunks with vector embeddings."""

import hashlib
import json
import logging
from collections.abc import Iterable, Iterator

from opensearchpy import OpenSearch, helpers

from ..config import OpenSearchConfig
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
                "dimension": 1024,  # multilingual-e5-large output dimension
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "engine": "nmslib",
                },
            },
        }
    },
}


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
        section_title = section.get("content") or ""
        if isinstance(section_title, list):
            section_title = " ".join(section_title)
        section_title = section_title.strip()

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
                sub_title = child.get("content") or ""
                if isinstance(sub_title, list):
                    sub_title = " ".join(sub_title)
                sub_title = sub_title.strip()
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
                    sub_header_text = child.get("content") or ""
                    if isinstance(sub_header_text, list):
                        sub_header_text = " ".join(sub_header_text)
                    current_sub_header = sub_header_text.strip()
                    current_body_nodes = []
                else:
                    current_body_nodes.append(child)

            # Flush the last accumulated chunk
            chunk = _make_chunk(cis, anchor, section_title, current_sub_header, current_body_nodes)
            if chunk:
                yield chunk


def _get_albert_client(config=None):
    from openai import OpenAI

    from ..config import get_config

    albert = config or get_config().albert
    if not albert.is_configured():
        raise RuntimeError("ALBERT_API_KEY is not set.")
    return OpenAI(api_key=albert.api_key, base_url=albert.base_url), albert.model


def _embed_texts(texts: list[str], client, model: str) -> list[list[float]]:
    response = client.embeddings.create(model=model, input=texts)
    return [e.embedding for e in response.data]


def index_notice_chunks(
    records: Iterable[dict],
    embed_client,
    embed_model: str,
    os_client: OpenSearch,
    index_name: str = DEFAULT_INDEX,
) -> int:
    """Chunk, embed via Albert API, and index an iterable of parsed notice records.

    Returns the total number of chunks successfully indexed.
    """
    create_or_update_index(os_client, index_name, INDEX_MAPPING)

    total = 0
    for record in records:
        chunks = list(_iter_notice_chunks(record))
        if not chunks:
            continue

        embeddings = _embed_texts([c["embed_text"] for c in chunks], embed_client, embed_model)
        actions = [
            {
                "_index": index_name,
                "_id": chunk["_id"],
                "_source": {k: v for k, v in chunk.items() if k != "_id"} | {"embedding": emb},
            }
            for chunk, emb in zip(chunks, embeddings)
        ]
        success, failed = helpers.bulk(os_client, actions, raise_on_error=False, stats_only=True)
        if failed:
            cis = record.get("source", {}).get("cis", "?")
            logger.warning(f"CIS {cis}: {failed} chunks failed to index")
        total += success

    return total


def index_from_local(
    path: str,
    index_name: str = DEFAULT_INDEX,
    limite: int | None = None,
    os_config: OpenSearchConfig | None = None,
    albert_config=None,
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

    total = index_notice_chunks(_records(), embed_client, embed_model, os_client, index_name)
    logger.info(f"Indexed {total} chunks from {path} into '{index_name}'")
    return total
