"""Command-line interface for Info Medicament data workflows."""

import argparse
import csv
import glob
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from tqdm import tqdm

from .config import get_config
from .convert import sql_to_csv
from .datagouv import import_dataset, load_datasets
from .datapackage_importer import import_datapackage
from .db import (
    get_glossary_terms,
    get_semantic_import_worklist,
    import_semantic_documents,
    iter_pediatric_rcps,
    sync_specialites_metadata_from_db,
)
from .grist import sync_grist
from .parsing import DEFAULT_IMAGE_BASE_URL, parse_semantic_document
from .s3 import make_s3_client

logger = logging.getLogger(__name__)

_DOCUMENT_USER_AGENT = "infomedicament-dataeng/0.1 (+https://info-medicaments.fr)"


def _download_document(url: str) -> bytes:
    """Download an ANSM document from the HTTPS object URL stored in PostgreSQL."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError(f"ANSM document URL must be an HTTPS URL without credentials: {url!r}")
    request = Request(url, headers={"User-Agent": _DOCUMENT_USER_AGENT})
    with urlopen(request, timeout=60) as response:
        return response.read()


def _document_image_base_url(url: str) -> str:
    """Return the ANSM export's root image directory for a document URL."""
    parsed = urlparse(url)
    return parsed._replace(path="/images/", params="", query="", fragment="").geturl()


def traiter_fichier_semantic_local(fichier_data: tuple) -> dict | None:
    """
    Process a local notice or RCP HTML file into sanitized semantic HTML.

    Args:
        fichier_data: Tuple containing (file_path, image_base_url)

    Returns:
        Render-ready notice record or None if error
    """
    fichier, image_base_url = fichier_data

    try:
        base = os.path.basename(fichier)
        document = parse_semantic_document(Path(fichier).read_bytes(), image_base_url=image_base_url)

        return {
            "source": {"filename": base},
            "date_notif": document.date_notif.isoformat() if document.date_notif else None,
            "indication": document.indication,
            "content_html": document.content_html,
        }

    except Exception as e:
        logger.error(f"Error processing {fichier}: {e}")
        return None


def traiter_dossier_semantic_local(
    dossier_html: str,
    fichier_sortie: str = "semantic_output.jsonl",
    limite: int | None = None,
    pattern: str = "all",
    image_base_url: str = DEFAULT_IMAGE_BASE_URL,
) -> None:
    """
    Process a local folder of notices and/or RCPs into semantic HTML JSONL.

    Args:
        dossier_html: Path to the folder containing HTML files
        fichier_sortie: Output JSONL file
        limite: Limit number of files to process
        pattern: Document filename prefix: "N", "R", or "all"
        image_base_url: Base URL used to rewrite relative image paths
    """
    if pattern not in {"N", "R", "all"}:
        raise ValueError('pattern must be "N", "R", or "all"')
    filename_pattern = "[NR]*.htm" if pattern == "all" else f"{pattern}*.htm"
    fichiers = sorted(glob.glob(os.path.join(dossier_html, filename_pattern)))
    if limite is not None:
        fichiers = fichiers[:limite]

    logger.info(f"{len(fichiers)} HTML files found")
    logger.info("Semantic local mode")

    fichiers_data = [(fichier, image_base_url) for fichier in fichiers]

    with open(fichier_sortie, "w", encoding="utf-8") as f_out:
        pass

    files_processed = 0
    files_failed = 0

    with tqdm(total=len(fichiers_data), desc="Processing", unit="file") as pbar:
        for fichier_data in fichiers_data:
            result = traiter_fichier_semantic_local(fichier_data)
            if result is not None:
                with open(fichier_sortie, "a", encoding="utf-8") as f_out:
                    f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                files_processed += 1
            else:
                files_failed += 1
            pbar.set_postfix(processed=files_processed, failed=files_failed)
            pbar.update(1)

    logger.info(f"Semantic processing complete: {files_processed} processed, {files_failed} failed")
    logger.info(f"Output: {fichier_sortie}")


def import_semantic_documents_from_db(
    since: datetime | None = None,
    full: bool = False,
    cis: str | None = None,
    limite: int | None = None,
    batch_size: int = 500,
    centralised_only: bool = False,
    non_centralised_only: bool = False,
) -> None:
    """Import Notice/RCP content for specialties selected from PostgreSQL."""
    from .centralise.acquire import get_ema_pdf, pdf_cache_key
    from .centralise.match import match_presentation
    from .centralise.parser import parse_pdf

    if full and since is not None:
        raise ValueError("--full and --since are mutually exclusive")
    if centralised_only and non_centralised_only:
        raise ValueError("--centralised-only and --non-centralised-only are mutually exclusive")
    cutoff = None if full else since or datetime.now(timezone.utc) - timedelta(hours=24)
    config = get_config()
    if full:
        sync_specialites_metadata_from_db(config.postgres)
    worklist = get_semantic_import_worklist(cutoff, config.postgres, cis=cis, limit=None)

    from .centralise.acquire import build_product_information_index, fetch_ema_document_report
    from .db import get_centralised_specialties

    if centralised_only:
        worklist = [item for item in worklist if item["procedure"] == "CENTRALISEE"]
    elif non_centralised_only:
        worklist = [item for item in worklist if item["procedure"] != "CENTRALISEE"]

    all_centralised = [] if non_centralised_only else get_centralised_specialties(config.postgres, cis=cis)
    ema_index = build_product_information_index(fetch_ema_document_report()) if all_centralised else {}
    selected_by_cis = {item["cis"]: item for item in worklist}
    for specialty in all_centralised:
        document = ema_index.get(specialty["code_ema"].strip())
        if cutoff is None or (document and _ema_document_updated_since(document, cutoff)):
            selected_by_cis.setdefault(
                specialty["cis"],
                {**specialty, "procedure": "CENTRALISEE", "documents": {}},
            )
    worklist = sorted(selected_by_cis.values(), key=lambda item: item["cis"])
    if limite is not None:
        worklist = worklist[:limite]
    logger.info(
        "%d specialties selected%s",
        len(worklist),
        " for a full import" if cutoff is None else f" since {cutoff.isoformat()}",
    )
    if not worklist:
        return

    s3_client = make_s3_client()
    glossary_terms = get_glossary_terms(config.postgres)
    records = {"rcp": [], "notice": []}
    parse_errors = 0
    total_imported = {"rcp": 0, "notice": 0}
    total_db_errors = 0

    def flush() -> None:
        nonlocal total_db_errors
        for doc_type, table in (("rcp", "rcp"), ("notice", "notices")):
            if not records[doc_type]:
                continue
            imported, errors = import_semantic_documents(records[doc_type], table, config.postgres)
            total_imported[doc_type] += imported
            total_db_errors += errors
            records[doc_type].clear()

    centralised = [item for item in worklist if item["procedure"] == "CENTRALISEE"]
    ema_worklist = _get_ema_worklist(centralised, ema_index) if centralised else {}

    for item in tqdm(worklist, desc="ANSM documents", unit="specialty"):
        if item["procedure"] == "CENTRALISEE":
            continue
        for doc_type, url in item["documents"].items():
            try:
                filename = os.path.basename(unquote(urlparse(url).path))
                if not filename:
                    raise ValueError(f"document URL has no filename: {url!r}")
                document = parse_semantic_document(
                    _download_document(url),
                    image_base_url=_document_image_base_url(url),
                    glossary_terms=glossary_terms,
                )
                records[doc_type].append(
                    {
                        "cis": item["cis"],
                        "filename": filename,
                        "date_notif": document.date_notif.isoformat() if document.date_notif else None,
                        "indication": document.indication,
                        "content_html": document.content_html,
                    }
                )
                if len(records[doc_type]) >= batch_size:
                    flush()
            except Exception as e:
                logger.error("Failed to download or parse %s for CIS %s from %s: %s", doc_type, item["cis"], url, e)
                parse_errors += 1

    for url, items in tqdm(ema_worklist.items(), desc="EMA PDFs", unit="pdf"):
        try:
            parsed = parse_pdf(get_ema_pdf(url, s3_client), glossary_terms=glossary_terms)
            _upload_images(s3_client, parsed["images"])
            filename = pdf_cache_key(url).rsplit("/", 1)[-1]
            for item in items:
                for doc_type in ("rcp", "notice"):
                    document = match_presentation(item["denomination"], parsed[doc_type])
                    if document:
                        records[doc_type].append(
                            {
                                "cis": item["cis"],
                                "filename": filename,
                                "date_notif": document["date_notif"],
                                "indication": document["indication"],
                                "content_html": document["content_html"],
                            }
                        )
                        if len(records[doc_type]) >= batch_size:
                            flush()
        except Exception as e:
            logger.error("Failed to parse EMA PDF %s: %s", url, e)
            parse_errors += 1

    flush()
    logger.info(
        "DB-driven import complete: %d RCP + %d Notice imported, %d parse errors, %d database errors",
        total_imported["rcp"],
        total_imported["notice"],
        parse_errors,
        total_db_errors,
    )
    if total_db_errors:
        raise RuntimeError(f"Database import failed for {total_db_errors} document(s)")


def telecharger_html_depuis_s3(
    dossier_sortie: str,
    limite: int | None = None,
    pattern: str = "N",
    staging: bool = False,
) -> None:
    """
    Download raw HTML files from S3 into a local folder for parser testing.

    Args:
        dossier_sortie: Local output directory
        limite: Maximum number of files to download
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
        staging: If True, download files from the staging prefix
    """
    s3_client = make_s3_client()
    output_dir = Path(dossier_sortie)
    output_dir.mkdir(parents=True, exist_ok=True)

    keys = s3_client.list_staging_html_files(pattern) if staging else s3_client.list_html_files(pattern)
    total_downloaded = 0

    logger.info(f"Downloading HTML files for pattern '{pattern}' into {output_dir}")
    if limite is not None:
        logger.info(f"Limit: {limite} file(s)")

    with tqdm(total=limite, desc="Downloading", unit="file") as pbar:
        for key in keys:
            if limite is not None and total_downloaded >= limite:
                break

            filename = s3_client.get_filename_from_key(key)
            destination = output_dir / filename
            content = s3_client.download_file_content(key)
            destination.write_bytes(content)

            total_downloaded += 1
            pbar.update(1)
            pbar.set_postfix(file=filename)

    logger.info(f"Downloaded {total_downloaded} file(s) to {output_dir}")


def run_pediatric_classification(
    records,
    truth_path: str | None,
    output_path: str,
    debug: bool = False,
) -> None:
    """Classify semantic RCP records streamed from PostgreSQL."""
    from .pediatric import (
        classify,
        compute_metrics,
        extract_section_texts,
        format_metrics,
        load_ground_truth,
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ground_truth = load_ground_truth(truth_path) if truth_path else {}
    header = ["cis", "pred_A", "pred_B", "pred_C"]
    if ground_truth:
        header += ["truth_A", "truth_B", "truth_C", "match_A", "match_B", "match_C"]
    header += [
        "a_reasons",
        "b_reasons",
        "c_reasons",
        "keywords_41_42",
        "keywords_43",
        "evidence_41_42",
        "evidence_43",
    ]

    debug_path = os.path.join(os.path.dirname(output_path) or ".", "debug_sections.jsonl") if debug else None
    predictions = []
    seen_cis: set[str] = set()

    with open(output_path, "w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(header)
        debug_file = open(debug_path, "w", encoding="utf-8") if debug_path else None
        try:
            for record in tqdm(records, desc="RCPs", unit="rcp"):
                cis = record["cis"]
                content_html = record["content_html"]
                atc_code = record.get("atc_code", "")
                prediction = classify(cis, content_html, atc_code=atc_code)
                predictions.append(prediction)
                seen_cis.add(cis)

                if debug_file:
                    debug_file.write(
                        json.dumps(
                            {
                                "cis": cis,
                                "atc_code": atc_code,
                                "raw_41": "\n".join(extract_section_texts(content_html, "4.1")),
                                "raw_42": "\n".join(extract_section_texts(content_html, "4.2")),
                                "raw_43": "\n".join(extract_section_texts(content_html, "4.3")),
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                truth = ground_truth.get(cis, {})
                row = [cis, int(prediction.condition_a), int(prediction.condition_b), int(prediction.condition_c)]
                if ground_truth:
                    truth_values = [truth.get(label, "") for label in ("A", "B", "C")]
                    row += [int(value) if isinstance(value, bool) else "" for value in truth_values]
                    row += [
                        int(predicted == expected) if isinstance(expected, bool) else ""
                        for predicted, expected in zip(
                            (prediction.condition_a, prediction.condition_b, prediction.condition_c),
                            truth_values,
                            strict=True,
                        )
                    ]
                keywords_41_42 = [keyword for match in prediction.matches_41_42 for keyword in match.keywords]
                keywords_43 = [keyword for match in prediction.matches_43 for keyword in match.keywords]
                row += [
                    " | ".join(prediction.a_reasons),
                    " | ".join(prediction.b_reasons),
                    " | ".join(prediction.c_reasons),
                    " | ".join(dict.fromkeys(keywords_41_42)),
                    " | ".join(dict.fromkeys(keywords_43)),
                    " ||| ".join(match.text[:200] for match in prediction.matches_41_42),
                    " ||| ".join(match.text[:200] for match in prediction.matches_43),
                ]
                writer.writerow(row)

            for cis in ground_truth.keys() - seen_cis:
                truth = ground_truth[cis]
                writer.writerow(
                    [
                        cis,
                        "",
                        "",
                        "",
                        int(truth["A"]),
                        int(truth["B"]),
                        int(truth["C"]),
                        "",
                        "",
                        "",
                        "",
                        "",
                        "RCP manquant",
                        "",
                        "",
                        "",
                        "",
                    ]
                )
        finally:
            if debug_file:
                debug_file.close()

    logger.info("Classified %d RCPs; predictions written to %s", len(seen_cis), output_path)
    if ground_truth:
        print(format_metrics(compute_metrics(predictions, ground_truth)))


def run_import_datagouv(config_path: Path, dataset_name: str | None = None) -> None:
    """Import one or all datasets defined in a data.gouv.fr YAML config file."""
    datasets = load_datasets(config_path)

    to_import = {dataset_name: datasets[dataset_name]} if dataset_name else datasets

    for name, dataset in to_import.items():
        logger.info(f"Importing dataset '{name}' into '{dataset.postgresql_table}'...")
        count = import_dataset(dataset)
        logger.info(f"Done: {count} rows imported into '{dataset.postgresql_table}'")


def _ema_document_updated_since(document: dict, cutoff: datetime) -> bool:
    """Return whether EMA's product-information timestamp reaches the cutoff."""
    value = document.get("last_updated_date")
    if not value:
        return False
    try:
        updated = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        logger.error("Invalid EMA product-information last_updated_date: %r", value)
        return False
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    return updated >= cutoff


def _get_ema_worklist(specialties: list[dict], document_index: dict[str, dict] | None = None) -> dict[str, list[dict]]:
    """Resolve centralised specialties to current French EMA PI URLs."""
    from .centralise.acquire import build_product_information_index, fetch_ema_document_report

    if not specialties:
        return {}
    index = (
        document_index if document_index is not None else build_product_information_index(fetch_ema_document_report())
    )
    worklist: dict[str, list[dict]] = {}
    for specialty in specialties:
        cis = specialty["cis"]
        code_ema = str(specialty.get("code_ema") or "").strip()
        if not code_ema:
            logger.error("EMA product information missing for CIS %s: ansm_specialite.code_ema is empty", cis)
            continue
        if code_ema not in index:
            logger.error(
                "EMA product information missing for CIS %s: no product-information record for EMA code %s",
                cis,
                code_ema,
            )
            continue
        french_url = index[code_ema]["french_url"]
        if not french_url:
            logger.error(
                "French EMA product-information translation missing for CIS %s (EMA code %s)",
                cis,
                code_ema,
            )
            continue
        worklist.setdefault(french_url, []).append(specialty)
    return worklist


def run_centralise_fetch(cis: str | None = None, refresh: bool = False, limite: int | None = None) -> None:
    """Download and cache EMA product-information PDFs on S3 (acquisition step).

    With ``cis`` set, fetches only that CIS's PDF, so the full pipeline can be
    prototyped on one PDF without an expensive initial parse run.
    """
    from .centralise.acquire import get_ema_pdf, pdf_cache_key
    from .db import get_centralised_specialties

    config = get_config()
    s3_client = make_s3_client()
    worklist = _get_ema_worklist(get_centralised_specialties(config.postgres, cis=cis))
    if not worklist:
        logger.warning(f"No EMA PDFs found in worklist{f' for CIS {cis}' if cis else ''}")
        return

    urls = list(worklist.keys())
    if limite is not None:
        urls = urls[:limite]
    logger.info(f"{len(urls)} distinct PDF(s) to acquire (refresh={refresh})")

    acquired = 0
    with tqdm(urls, desc="PDFs", unit="pdf") as pbar:
        for url in pbar:
            try:
                # Warming the cache only needs a cheap existence check, not the bytes.
                key = pdf_cache_key(url)
                if not refresh and s3_client.object_exists(key):
                    pbar.set_postfix_str(f"cache: {key.rsplit('/', 1)[-1]}")
                    acquired += 1
                    continue
                pdf = get_ema_pdf(url, s3_client, refresh=refresh)
                logger.info(f"{url}: {len(pdf)} bytes, shared by {len(worklist[url])} CIS")
                acquired += 1
                time.sleep(1.0)  # polite gap between real EMA hits to avoid 429s
            except Exception as e:
                logger.error(f"Failed to acquire {url}: {e}")

    logger.info(f"Acquisition complete: {acquired}/{len(urls)} PDFs cached")


def _load_processed_slugs(path: str | None) -> set[str]:
    """Read the set of already-parsed PDF slugs from a plain-text file (one per line)."""
    if not path or not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _append_processed_slugs(path: str | None, slugs: list[str]) -> None:
    """Append newly-parsed PDF slugs to the processed-list file (after their batch is durable)."""
    if not path or not slugs:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(slugs) + "\n")


def _upload_images(s3_client, images: dict) -> int:
    """Upload content-addressed images to the CDN prefix, skipping ones already there."""
    uploaded = 0
    for key, data in images.items():
        if s3_client.object_exists(key):
            continue
        ext = key.rsplit(".", 1)[-1].lower()
        content_type = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
        s3_client.upload_file_content(key, data, content_type=content_type)
        uploaded += 1
    return uploaded


def run_centralise_parse(
    cis: str | None = None,
    pdf_path: str | None = None,
    limite: int | None = None,
    batch_size: int = 500,
    processed_file: str | None = None,
) -> None:
    """Parse centralised EMA PDFs and upsert semantic HTML directly into PostgreSQL.

    A PDF bundles one presentation per device (cartouche, pen, …); worklist mode
    (default) fetches each distinct PDF via the S3 cache, parses all its
    presentations once, and matches each CIS to its own via ``SpecDenom01``.
    ``--pdf`` parses a local file, matches the requested ``--cis``, and imports
    that presentation directly.

    Records are imported every ``batch_size`` matched documents. Each database
    document commits independently. With ``processed_file`` set, PDF slugs are
    recorded only after their whole batch imports without database errors.
    """
    from .centralise.match import match_presentation
    from .centralise.parser import parse_pdf
    from .db import get_centralised_specialties

    def record(doc: dict, filename: str, cis_code: str) -> dict:
        return {
            "cis": cis_code,
            "filename": filename,
            "date_notif": doc["date_notif"],
            "indication": doc["indication"],
            "content_html": doc["content_html"],
        }

    config = get_config()
    s3_client = make_s3_client()
    glossary_terms = get_glossary_terms(config.postgres)
    logger.info("Loaded %d glossary terms to annotate", len(glossary_terms))

    if pdf_path:
        if not cis:
            raise ValueError("--pdf requires --cis to match and import the correct presentation")
        cis_row = next(iter(get_centralised_specialties(config.postgres, cis=cis)), None)
        if cis_row is None:
            raise ValueError(f"No centralised medicine found for CIS {cis}")
        denomination = cis_row["denomination"]
        res = parse_pdf(Path(pdf_path).read_bytes(), glossary_terms=glossary_terms)
        uploaded = _upload_images(s3_client, res["images"])
        filename = os.path.basename(pdf_path)
        rcp_doc = match_presentation(denomination, res["rcp"])
        notice_doc = match_presentation(denomination, res["notice"])
        if rcp_doc is None and notice_doc is None:
            raise ValueError(f"No RCP or Notice presentation matched CIS {cis}")
        rcp_records = [record(rcp_doc, filename, cis)] if rcp_doc else []
        notice_records = [record(notice_doc, filename, cis)] if notice_doc else []
        rcp_imported, rcp_errors = (
            import_semantic_documents(rcp_records, "rcp", config.postgres) if rcp_records else (0, 0)
        )
        notice_imported, notice_errors = (
            import_semantic_documents(notice_records, "notices", config.postgres) if notice_records else (0, 0)
        )
        if rcp_errors or notice_errors:
            raise RuntimeError(f"Database import failed for {rcp_errors + notice_errors} document(s)")
        logger.info(
            "Imported %d RCP + %d Notice document(s) for CIS %s; uploaded %d new image(s)",
            rcp_imported,
            notice_imported,
            cis,
            uploaded,
        )
        return

    from .centralise.acquire import get_ema_pdf, pdf_cache_key

    worklist = _get_ema_worklist(get_centralised_specialties(config.postgres, cis=cis))
    urls = list(worklist)
    if limite is not None:
        urls = urls[:limite]

    already_done = _load_processed_slugs(processed_file)
    if already_done:
        logger.info(f"{len(already_done)} PDF(s) already processed (from {processed_file}); skipping those")

    rcp_records: list[dict] = []
    notice_records: list[dict] = []
    pending_slugs: list[str] = []
    batch_num = 0
    total_rcp = total_notice = total_images = total_db_errors = 0

    def flush() -> None:
        nonlocal batch_num, total_rcp, total_notice, total_db_errors
        nonlocal rcp_records, notice_records, pending_slugs
        if not rcp_records and not notice_records:
            return
        batch_num += 1
        rcp_imported, rcp_errors = (
            import_semantic_documents(rcp_records, "rcp", config.postgres) if rcp_records else (0, 0)
        )
        notice_imported, notice_errors = (
            import_semantic_documents(notice_records, "notices", config.postgres) if notice_records else (0, 0)
        )
        batch_errors = rcp_errors + notice_errors
        total_rcp += rcp_imported
        total_notice += notice_imported
        total_db_errors += batch_errors
        if batch_errors:
            logger.error(
                "Batch %d had %d database error(s); its PDF slugs were not marked processed",
                batch_num,
                batch_errors,
            )
        else:
            _append_processed_slugs(processed_file, pending_slugs)
        rcp_records, notice_records, pending_slugs = [], [], []

    todo = [u for u in urls if pdf_cache_key(u).split("/")[-1] not in already_done]
    logger.info(f"{len(todo)} distinct PDF(s) to parse ({len(urls) - len(todo)} skipped)")

    with tqdm(todo, desc="PDFs", unit="pdf") as pbar:
        for url in pbar:
            try:
                res = parse_pdf(
                    get_ema_pdf(
                        url,
                        s3_client,
                        on_cache_hit=lambda key: pbar.set_postfix_str(f"cache: {key.rsplit('/', 1)[-1]}"),
                    ),
                    glossary_terms=glossary_terms,
                )
                total_images += _upload_images(s3_client, res["images"])  # before records reference them
                filename = pdf_cache_key(url).split("/")[-1]
                for specialty in worklist[url]:
                    cis_code = specialty["cis"]
                    denom = specialty["denomination"]
                    rcp_doc = match_presentation(denom, res["rcp"])
                    notice_doc = match_presentation(denom, res["notice"])
                    if rcp_doc:
                        rcp_records.append(record(rcp_doc, filename, cis_code))
                    if notice_doc:
                        notice_records.append(record(notice_doc, filename, cis_code))
                pending_slugs.append(filename)
                if len(rcp_records) >= batch_size or len(notice_records) >= batch_size:
                    flush()
            except Exception as e:
                logger.error(f"Failed to parse {url}: {e}")

    flush()  # final partial batch
    logger.info(
        f"Imported {total_rcp} RCP + {total_notice} Notice document(s) in {batch_num} batch(es); "
        f"uploaded {total_images} new image(s); {total_db_errors} database error(s)"
    )
    if total_db_errors:
        raise RuntimeError(f"Centralised import completed with {total_db_errors} database error(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Parse ANSM medication HTML documents (Notices and RCPs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import changed ANSM and EMA documents from the PostgreSQL catalog
  infomedicament-dataeng semantic-db-import

  # Re-import the full document catalog
  infomedicament-dataeng semantic-db-import --full

  # Test the semantic parser against local HTML
  infomedicament-dataeng semantic-local ./html_files --limit 10

        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Processing mode")

    # Local semantic HTML mode
    semantic_parser = subparsers.add_parser(
        "semantic-local", help="Process local notices and RCPs into semantic HTML JSONL"
    )
    semantic_parser.add_argument("dossier_html", help="Folder containing N*.htm and/or R*.htm files")
    semantic_parser.add_argument("--output", "-o", default="semantic_output.jsonl", help="Output JSONL file")
    semantic_parser.add_argument("--limit", type=int, help="Limit number of files to process")
    semantic_parser.add_argument(
        "--pattern", default="all", choices=["N", "R", "all"], help="Documents to process (default: all)"
    )
    semantic_parser.add_argument(
        "--image-base-url",
        default=DEFAULT_IMAGE_BASE_URL,
        help="Base URL used to rewrite relative image paths",
    )

    semantic_db_parser = subparsers.add_parser(
        "semantic-db-import",
        help="Import Notice/RCP documents for specialties selected from the database",
    )
    semantic_db_cutoff = semantic_db_parser.add_mutually_exclusive_group()
    semantic_db_cutoff.add_argument(
        "--since",
        type=datetime.fromisoformat,
        metavar="ISO-DATETIME",
        help="Only process ANSM/EMA documents updated since this time (default: 24 hours ago)",
    )
    semantic_db_cutoff.add_argument("--full", action="store_true", help="Process the full specialty catalog")
    semantic_db_parser.add_argument("--cis", help="Process only this CIS code")
    semantic_db_parser.add_argument("--limit", type=int, help="Limit the number of specialties processed")
    semantic_db_parser.add_argument(
        "--batch-size", type=int, default=500, help="Documents per database import batch (default: 500)"
    )
    semantic_db_source = semantic_db_parser.add_mutually_exclusive_group()
    semantic_db_source.add_argument(
        "--centralised-only",
        action="store_true",
        help="Process only centrally authorised EMA documents",
    )
    semantic_db_source.add_argument(
        "--non-centralised-only",
        action="store_true",
        help="Process only non-centralised ANSM documents",
    )

    # Download HTML files from S3 for local testing
    download_parser = subparsers.add_parser("download-html", help="Download raw HTML files from S3 locally")
    download_parser.add_argument("output_dir", help="Local output directory")
    download_parser.add_argument("--limite", type=int, help="Limit number of files to download")
    download_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")
    download_parser.add_argument(
        "--staging",
        action="store_true",
        help="Download files from the staging subdirectory",
    )

    # SQL to CSV mode
    sql_parser = subparsers.add_parser("sql-to-csv", help="Convert SQL INSERT statements to CSV")
    sql_parser.add_argument("sql_file", help="SQL file to convert")
    sql_parser.add_argument("--output", "-o", help="Output CSV file (default: same name with .csv)")
    sql_parser.add_argument("--encoding", "-e", default="iso-8859-1", help="Source file encoding")
    sql_parser.add_argument("--dialect", "-d", default="tsql", help="SQL dialect (tsql, mysql, postgres)")

    # Import from data.gouv.fr mode
    datagouv_parser = subparsers.add_parser("import-datagouv", help="Import datasets from data.gouv.fr into PostgreSQL")
    datagouv_parser.add_argument(
        "--config", required=True, type=Path, help="Path to YAML config file (e.g. data_sources/has.yml)"
    )
    datagouv_parser.add_argument("--dataset", help="Name of a specific dataset to import (default: all)")

    # Import ANSM datapackage
    datapackage_parser = subparsers.add_parser(
        "import-datapackage", help="Import the ANSM frictionless datapackage into PostgreSQL"
    )
    datapackage_parser.add_argument(
        "--package",
        required=True,
        help="Path or URL to a datapackage.json or a zip containing one",
    )
    datapackage_parser.add_argument(
        "--resource",
        help="Name of a single resource to load (default: all, in dependency order)",
    )

    subparsers.add_parser("sync-grist", help="Synchronize Grist reference data into PostgreSQL")

    pediatric_parser = subparsers.add_parser(
        "classify-pediatric",
        help="Classify semantic RCP content read from PostgreSQL",
    )
    pediatric_parser.add_argument("--cis", help="Classify only this CIS code")
    pediatric_parser.add_argument("--limit", type=int, help="Cap the number of RCPs processed")
    pediatric_parser.add_argument("--truth", help="Optional ground-truth CSV used for evaluation")
    pediatric_parser.add_argument("--output", "-o", default="data/predictions.csv", help="Output predictions CSV")
    pediatric_parser.add_argument(
        "--batch-size", type=int, default=500, help="PostgreSQL streaming batch size (default: 500)"
    )
    pediatric_parser.add_argument("--debug", action="store_true", help="Write extracted section text to JSONL")

    # Centralised EMA PDF pipeline (subcommand group)
    centralise_parser = subparsers.add_parser("centralise", help="Centrally-authorised EMA PDF pipeline")
    centralise_subparsers = centralise_parser.add_subparsers(dest="target", help="Centralise step")

    # centralise fetch — download + cache PDFs on S3
    centralise_fetch_parser = centralise_subparsers.add_parser(
        "fetch", help="Download and cache EMA product-information PDFs on S3"
    )
    centralise_fetch_parser.add_argument("--cis", help="Fetch only the PDF for this CIS code (for prototyping)")
    centralise_fetch_parser.add_argument(
        "--refresh", action="store_true", help="Force re-download from EMA even if already cached on S3"
    )
    centralise_fetch_parser.add_argument("--limite", type=int, help="Limit number of distinct PDFs to fetch")

    # centralise parse — parse PDFs and import semantic RCP + Notice HTML
    centralise_parse_parser = centralise_subparsers.add_parser(
        "parse", help="Parse EMA PDFs and import semantic RCP + Notice HTML directly into PostgreSQL"
    )
    centralise_parse_parser.add_argument("--cis", help="Parse only the PDF for this CIS code")
    centralise_parse_parser.add_argument("--pdf", help="Import a single local PDF file (requires --cis)")
    centralise_parse_parser.add_argument("--limite", type=int, help="Limit number of distinct PDFs to parse")
    centralise_parse_parser.add_argument(
        "--batch-size", type=int, default=500, help="Matched documents per database import batch (default: 500)"
    )
    centralise_parse_parser.add_argument(
        "--processed-file",
        help="Text file of imported PDF slugs; successful PDFs are appended and skipped on re-run",
    )

    # Global options
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    config = get_config()
    log_level = logging.DEBUG if args.verbose else getattr(logging, config.log_level, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # --verbose is for our own code; these libraries log a wall of DEBUG per request.
    if args.verbose:
        for noisy in ("boto3", "botocore", "s3transfer", "urllib3"):
            logging.getLogger(noisy).setLevel(logging.INFO)

    if args.command == "semantic-local":
        try:
            traiter_dossier_semantic_local(
                args.dossier_html,
                fichier_sortie=args.output,
                limite=args.limit,
                pattern=args.pattern,
                image_base_url=args.image_base_url,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "semantic-db-import":
        try:
            import_semantic_documents_from_db(
                since=args.since,
                full=args.full,
                cis=args.cis,
                limite=args.limit,
                batch_size=args.batch_size,
                centralised_only=args.centralised_only,
                non_centralised_only=args.non_centralised_only,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "sql-to-csv":
        try:
            output_path = Path(args.output) if args.output else None
            sql_to_csv(Path(args.sql_file), output_path, args.encoding, args.dialect)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "download-html":
        try:
            telecharger_html_depuis_s3(
                args.output_dir,
                limite=args.limite,
                pattern=args.pattern,
                staging=args.staging,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-datagouv":
        try:
            run_import_datagouv(args.config, dataset_name=args.dataset)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-datapackage":
        try:
            import_datapackage(args.package, resource_name=args.resource)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "sync-grist":
        try:
            sync_grist(config.grist.doc_id, config.grist.api_key, config.postgres)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "classify-pediatric":
        try:
            records = iter_pediatric_rcps(
                config.postgres,
                cis=args.cis,
                limit=args.limit,
                batch_size=args.batch_size,
            )
            run_pediatric_classification(records, args.truth, args.output, debug=args.debug)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "centralise":
        if not getattr(args, "target", None):
            centralise_parser.print_help()
            raise SystemExit(1)
        if args.target == "fetch":
            try:
                run_centralise_fetch(cis=args.cis, refresh=args.refresh, limite=args.limite)
            except Exception as e:
                logger.exception(f"Error: {e}")
                raise SystemExit(1)
        elif args.target == "parse":
            try:
                run_centralise_parse(
                    cis=args.cis,
                    pdf_path=args.pdf,
                    limite=args.limite,
                    batch_size=args.batch_size,
                    processed_file=args.processed_file,
                )
            except Exception as e:
                logger.exception(f"Error: {e}")
                raise SystemExit(1)

    else:
        parser.print_help()
        raise SystemExit(1)


if __name__ == "__main__":
    main()
