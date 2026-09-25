"""Acquire EMA product-information PDFs, cached on S3 to avoid re-scraping EMA."""

import hashlib
import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable

from ..config import get_config
from ..s3 import S3Client, make_s3_client

logger = logging.getLogger(__name__)

EMA_DOCUMENT_REPORT_URL = (
    "https://www.ema.europa.eu/en/documents/report/documents-output-epar_documents_json-report_en.json"
)

# EMA serves the PDF fine with a plain UA; set one to avoid default-urllib blocks.
_USER_AGENT = "infomedicament-dataeng/0.1 (+https://info-medicaments.fr)"

# EMA rate-limits scraping (HTTP 429). Retry with exponential backoff, honoring
# the Retry-After header when present.
_MAX_RETRIES = 5
_BACKOFF_BASE = 5.0  # seconds; doubled each retry


def _retry_after_seconds(err: urllib.error.HTTPError, attempt: int) -> float:
    """How long to wait before the next retry.

    Always at least the exponential backoff; a Retry-After header is honored only
    when it asks for *longer* (EMA sends ``Retry-After: 0``, which is useless).
    """
    backoff = _BACKOFF_BASE * (2**attempt)
    header = err.headers.get("Retry-After") if err.headers else None
    if header:
        try:
            return max(float(header), backoff)
        except ValueError:
            pass  # HTTP-date form is rare here; fall through to backoff
    return backoff


def pdf_cache_key(url: str) -> str:
    """S3 key for a cached EMA PDF, derived from the URL's filename slug.

    e.g. ``.../abasaglar-epar-product-information_fr.pdf`` →
    ``imports/ema_pdf/abasaglar-epar-product-information_fr.pdf``. The slug is
    stable and dedups the many-CIS-share-one-PDF case.
    """
    slug = url.rstrip("/").split("/")[-1]
    if not slug:
        raise ValueError(f"Cannot derive a cache key from URL: {url!r}")
    return f"{get_config().s3.ema_pdf_prefix}{slug}"


def _fetch_from_ema(url: str) -> bytes:
    logger.info(f"Fetching from EMA: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    for attempt in range(_MAX_RETRIES):
        try:
            with urllib.request.urlopen(req) as response:
                return response.read()
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == _MAX_RETRIES - 1:
                raise
            wait = _retry_after_seconds(err, attempt)
            logger.warning(f"EMA returned 429; retrying in {wait:.0f}s ({attempt + 1}/{_MAX_RETRIES}): {url}")
            time.sleep(wait)
    raise RuntimeError("unreachable")  # loop either returns or raises


def fetch_ema_document_report() -> dict:
    """Download and decode the current EMA EPAR document report without caching it."""
    logger.info("Downloading current EMA EPAR document report")
    report = json.loads(_fetch_from_ema(EMA_DOCUMENT_REPORT_URL))
    if not isinstance(report, dict) or not isinstance(report.get("data"), list):
        raise ValueError("EMA EPAR document report has an unexpected structure: missing data array")
    return report


def build_product_information_index(report: dict) -> dict[str, dict]:
    """Map EMA product numbers to their latest product-information metadata."""
    index: dict[str, dict] = {}
    for document in report.get("data", []):
        if not isinstance(document, dict) or document.get("type") != "product-information":
            continue
        product_number = str(document.get("ema_product_number") or "").strip()
        if not product_number:
            continue
        last_updated = str(document.get("last_updated_date") or "")
        if product_number in index and last_updated < index[product_number]["last_updated_date"]:
            continue
        translations = document.get("translations")
        french_url = translations.get("fr") if isinstance(translations, dict) else None
        index[product_number] = {
            "french_url": french_url if isinstance(french_url, str) and french_url.strip() else None,
            "last_updated_date": last_updated,
        }
    return index


def build_french_product_information_index(report: dict) -> dict[str, str | None]:
    """Map EMA product numbers to French product-information PDF URLs.

    A present key with a ``None`` value means that the product-information
    record exists but EMA does not provide a French translation.
    """
    return {code: document["french_url"] for code, document in build_product_information_index(report).items()}


def get_ema_pdf(
    url: str,
    s3_client: S3Client | None = None,
    *,
    refresh: bool = False,
    on_cache_hit: Callable[[str], None] | None = None,
) -> bytes:
    """Return the PDF bytes for an EMA PI URL, using the S3 cache.

    Serves from S3 when the PDF is already cached, unless ``refresh`` is set. On
    a cache miss (or refresh) fetches from EMA, uploads the PDF plus a
    ``.sha256`` sidecar (for parse-step idempotency), and returns the bytes.

    EMA is hit at most once per distinct PDF, ever, unless ``refresh`` is passed.
    ``on_cache_hit`` can update an interactive display without emitting a log
    line for every cached PDF.
    """
    if s3_client is None:
        s3_client = make_s3_client()

    key = pdf_cache_key(url)

    if not refresh and s3_client.object_exists(key):
        logger.debug(f"Cache hit: {key}")
        if on_cache_hit is not None:
            on_cache_hit(key)
        return s3_client.download_file_content(key)

    pdf_bytes = _fetch_from_ema(url)
    s3_client.upload_file_content(key, pdf_bytes, content_type="application/pdf")
    digest = hashlib.sha256(pdf_bytes).hexdigest()
    s3_client.upload_file_content(f"{key}.sha256", digest, content_type="text/plain")
    logger.info(f"Cached {len(pdf_bytes)} bytes to {key} (sha256={digest[:12]}…)")
    return pdf_bytes
