"""Shared OpenSearch client utilities."""

import logging
from urllib.parse import urlparse

from opensearchpy import OpenSearch

from ..config import OpenSearchConfig, get_config

logger = logging.getLogger(__name__)


def get_opensearch_client(config: OpenSearchConfig | None = None) -> OpenSearch:
    """Build an OpenSearch client from config.

    Parses credentials from the URL if present (Scalingo format:
    http://user:password@host:port).
    """
    if config is None:
        config = get_config().opensearch

    parsed = urlparse(config.url)
    http_auth = None
    if parsed.username and parsed.password:
        http_auth = (parsed.username, parsed.password)

    netloc_no_auth = parsed.hostname
    if parsed.port:
        netloc_no_auth = f"{netloc_no_auth}:{parsed.port}"
    clean_url = parsed._replace(netloc=netloc_no_auth).geturl()

    return OpenSearch(hosts=[clean_url], http_auth=http_auth, use_ssl=parsed.scheme == "https")


def create_or_update_index(client: OpenSearch, index_name: str, mapping: dict) -> None:
    """Create the index with the given mapping. No-op if it already exists."""
    client.indices.create(index=index_name, body=mapping, ignore=400)
    logger.info(f"Index '{index_name}' ready")
