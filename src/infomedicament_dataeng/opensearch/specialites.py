"""ETL: index specialités into OpenSearch (one document per CIS code)."""

import logging
from collections.abc import Iterator

from opensearchpy import helpers
from sqlalchemy import text

from ..config import OpenSearchConfig, PostgresConfig, get_config
from ..db import get_postgres_engine
from .client import create_or_update_index, get_opensearch_client

logger = logging.getLogger(__name__)

DEFAULT_INDEX = "specialites"

# ATC code lengths corresponding to hierarchy levels (1=class, 3=subclass, etc.)
_ATC_LEVELS = [1, 3, 4, 5, 7]

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
            "substances": {"type": "text", "analyzer": "french"},
            "pathologies": {"type": "text", "analyzer": "french"},
            "atc_labels": {
                "type": "text",
                "analyzer": "french",
                "fields": {"keyword": {"type": "keyword"}},
            },
        }
    },
}


def _atc_ancestor_codes(atc5_code: str) -> set[str]:
    """Return all ancestor ATC codes for a given level-5 code, including itself."""
    return {atc5_code[:n] for n in _ATC_LEVELS if n <= len(atc5_code)}


def _load_reference_data(conn) -> tuple[dict, dict, dict]:
    """Load substance, pathology, and ATC label lookup maps from PostgreSQL.

    Returns:
        (substance_map, pathology_map, atc_label_map) where atc_label_map maps
        ATC code → set of labels (technical and friendly).
    """
    result = conn.execute(text('SELECT "NomId", "NomLib" FROM resume_substances'))
    substance_map = {str(row["NomId"]).strip(): row["NomLib"] for row in result.mappings()}

    result = conn.execute(text('SELECT "codePatho", "NomPatho" FROM resume_pathologies'))
    pathology_map = {str(row["codePatho"]).strip(): row["NomPatho"] for row in result.mappings()}

    atc_label_map: dict[str, set[str]] = {}

    result = conn.execute(text("SELECT code, label_court FROM atc WHERE code IS NOT NULL AND label_court IS NOT NULL"))
    for row in result.mappings():
        atc_label_map.setdefault(row["code"], set()).add(row["label_court"])

    for table in ("ref_atc_friendly_niveau_1", "ref_atc_friendly_niveau_2"):
        result = conn.execute(text(f"SELECT code, libelle FROM {table} WHERE code IS NOT NULL AND libelle IS NOT NULL"))
        for row in result.mappings():
            atc_label_map.setdefault(row["code"], set()).add(row["libelle"])

    return substance_map, pathology_map, atc_label_map


def _iter_specialite_docs(
    conn,
    substance_map: dict,
    pathology_map: dict,
    atc_label_map: dict,
) -> Iterator[dict]:
    """Yield one OpenSearch document per CIS code from resume_medicaments."""
    result = conn.execute(
        text('SELECT "groupName", "specialites", "subsIds", "pathosCodes", "atc5Code" FROM resume_medicaments')
    )
    for group in result.mappings():
        substances = [substance_map[sid.strip()] for sid in (group["subsIds"] or []) if sid.strip() in substance_map]
        pathologies = [
            pathology_map[code.strip()] for code in (group["pathosCodes"] or []) if code.strip() in pathology_map
        ]

        atc_labels: list[str] = []
        if group["atc5Code"]:
            for code in _atc_ancestor_codes(group["atc5Code"]):
                atc_labels.append(code)
                atc_labels.extend(atc_label_map.get(code, []))

        for spec in group["specialites"] or []:
            cis_code, spec_name = spec[0], spec[1]
            yield {
                "cis_code": str(cis_code),
                "spec_name": spec_name,
                "substances": substances,
                "pathologies": pathologies,
                "atc_labels": atc_labels,
            }


def index_specialites(
    index_name: str,
    limite: int | None = None,
    config: OpenSearchConfig | None = None,
    pg_config: PostgresConfig | None = None,
) -> int:
    """Index all specialités from PostgreSQL into OpenSearch.

    Returns the number of documents successfully indexed.
    """
    if pg_config is None:
        pg_config = get_config().postgres

    client = get_opensearch_client(config)
    create_or_update_index(client, index_name, INDEX_MAPPING)

    engine = get_postgres_engine(pg_config)
    with engine.connect() as conn:
        substance_map, pathology_map, atc_label_map = _load_reference_data(conn)
        logger.info(
            f"Loaded {len(substance_map)} substances, {len(pathology_map)} pathologies, {len(atc_label_map)} ATC codes"
        )

        docs = _iter_specialite_docs(conn, substance_map, pathology_map, atc_label_map)
        if limite is not None:
            from itertools import islice

            docs = islice(docs, limite)

        def _actions() -> Iterator[dict]:
            for doc in docs:
                yield {"_index": index_name, "_id": doc["cis_code"], "_source": doc}

        success, failed = helpers.bulk(client, _actions(), raise_on_error=False, stats_only=True)

    if failed:
        logger.warning(f"{failed} documents failed to index")
    logger.info(f"Indexed {success} specialités into '{index_name}'")
    return success
