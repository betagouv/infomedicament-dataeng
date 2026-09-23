"""Import presentation prices from the public medicines database (BDPM)."""

import csv
import io
import logging
import re
import urllib.request

from sqlalchemy import text

from .config import PostgresConfig, get_config
from .db import get_postgres_engine

logger = logging.getLogger(__name__)

CEPS_PRICE_URL = "https://base-donnees-publique.medicaments.gouv.fr/download/file/CIS_CIP_bdpm.txt"
CEPS_PRICE_TABLE = "ceps_price"
CNAM_AGREMENT_TABLE = "cnam_agrement_collectivite"
EXPECTED_COLUMN_COUNT = 13
_PRICE_PATTERN = re.compile(r"\d+(?:,\d{3})*,\d{2}")
_REIMBURSEMENT_RATE_PATTERN = re.compile(r"(\d+)\s*%")

PriceRow = tuple[str, tuple[int, ...] | None, int | None, int | None, int | None]
AgrementRow = tuple[str, bool | None]


def _parse_price_cents(value: str, *, line_number: int, column_name: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    if not _PRICE_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {column_name} on line {line_number}: {value!r}")
    return int(value.replace(",", ""))


def _parse_reimbursement_rates(value: str, *, line_number: int) -> tuple[int, ...] | None:
    value = value.strip()
    if not value:
        return None

    rates = []
    for raw_rate in value.split(";"):
        match = _REIMBURSEMENT_RATE_PATTERN.fullmatch(raw_rate.strip())
        if not match or not 0 <= (rate := int(match.group(1))) <= 100:
            raise ValueError(f"Invalid reimbursement rate on line {line_number}: {value!r}")
        rates.append(rate)
    return tuple(rates)


def _fetch_presentation_rows(url: str) -> list[tuple[int, list[str]]]:
    with urllib.request.urlopen(url) as response:
        content = response.read().decode("utf-8-sig")

    rows = []
    seen_cips: set[str] = set()
    reader = csv.reader(io.StringIO(content), delimiter="\t")
    for line_number, row in enumerate(reader, start=1):
        if len(row) != EXPECTED_COLUMN_COUNT:
            raise ValueError(
                f"Unexpected column count on line {line_number}: expected {EXPECTED_COLUMN_COUNT}, got {len(row)}"
            )

        cip = row[6].strip()
        if len(cip) != 13 or not cip.isdigit():
            raise ValueError(f"Invalid CIP13 on line {line_number}: {cip!r}")
        if cip in seen_cips:
            raise ValueError(f"Duplicate CIP13 on line {line_number}: {cip}")
        seen_cips.add(cip)
        rows.append((line_number, row))

    if not rows:
        raise ValueError("The BDPM download is empty; refusing to truncate a table")
    return rows


def fetch_ceps_prices(url: str = CEPS_PRICE_URL) -> list[PriceRow]:
    """Download and validate priced presentation rows from CIS_CIP_bdpm.txt."""
    prices: list[PriceRow] = []
    partial_price_rows = 0
    for line_number, row in _fetch_presentation_rows(url):
        cip = row[6].strip()

        reimbursement_rates = _parse_reimbursement_rates(row[8], line_number=line_number)
        price_values = (
            _parse_price_cents(row[9], line_number=line_number, column_name="medicine price"),
            _parse_price_cents(row[10], line_number=line_number, column_name="public price"),
            _parse_price_cents(row[11], line_number=line_number, column_name="dispensing fee"),
        )
        if reimbursement_rates is None and all(value is None for value in price_values):
            continue
        if any(value is None for value in price_values):
            partial_price_rows += 1
        prices.append((cip, reimbursement_rates, *price_values))

    if not prices:
        raise ValueError("The BDPM download contains no presentation price information; refusing to truncate the table")
    if partial_price_rows:
        logger.warning(f"Found {partial_price_rows} presentation(s) with incomplete published price information")
    return prices


def fetch_cnam_agrements(url: str = CEPS_PRICE_URL) -> list[AgrementRow]:
    """Download agrément aux collectivités values keyed by CIP13."""
    value_map = {"oui": True, "non": False, "inconnu": None}
    agrements = []
    for line_number, row in _fetch_presentation_rows(url):
        raw_value = row[7].strip().lower()
        if raw_value not in value_map:
            raise ValueError(f"Invalid agrément aux collectivités on line {line_number}: {row[7]!r}")
        agrements.append((row[6].strip(), value_map[raw_value]))
    return agrements


def import_ceps_prices(config: PostgresConfig | None = None, *, url: str = CEPS_PRICE_URL) -> int:
    """Replace ``ceps_price`` with the current validated BDPM price data."""
    if config is None:
        config = get_config().postgres

    rows = fetch_ceps_prices(url)
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(
        (cip, "{" + ",".join(map(str, rates)) + "}" if rates is not None else None, medicine, public, fee)
        for cip, rates, medicine, public, fee in rows
    )
    buf.seek(0)

    engine = get_postgres_engine(config)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {CEPS_PRICE_TABLE}"))
        raw = conn.connection.dbapi_connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {CEPS_PRICE_TABLE} "
                "(cip, reimbursement_rates, medicine_price_cents, public_price_cents, dispensing_fee_cents) "
                "FROM STDIN WITH (FORMAT csv)",
                buf,
            )

    logger.info(f"Imported {len(rows)} presentation prices into '{CEPS_PRICE_TABLE}'")
    return len(rows)


def import_cnam_agrements(config: PostgresConfig | None = None, *, url: str = CEPS_PRICE_URL) -> int:
    """Replace ``cnam_agrement_collectivite`` with the current BDPM data."""
    if config is None:
        config = get_config().postgres

    rows = fetch_cnam_agrements(url)
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(rows)
    buf.seek(0)

    engine = get_postgres_engine(config)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {CNAM_AGREMENT_TABLE}"))
        raw = conn.connection.dbapi_connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {CNAM_AGREMENT_TABLE} (cip, agrement_collectivite) FROM STDIN WITH (FORMAT csv)",
                buf,
            )

    logger.info(f"Imported {len(rows)} presentation approvals into '{CNAM_AGREMENT_TABLE}'")
    return len(rows)
