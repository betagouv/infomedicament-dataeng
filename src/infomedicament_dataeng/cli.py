"""Command-line interface for the HTML parser."""

import argparse
import csv
import glob
import json
import logging
import multiprocessing as mp
import os
import time
from datetime import date, datetime
from pathlib import Path

import chardet
from tqdm import tqdm

from .bdpm import import_ceps_prices
from .config import get_config
from .convert import sql_to_csv
from .datagouv import import_dataset, load_datasets
from .datapackage_importer import import_datapackage
from .db import (
    check_sequences,
    get_authorized_cis,
    get_filename_to_cis_mapping,
    get_glossary_terms,
    import_semantic_documents,
    import_to_postgres,
)
from .io import charger_liste_cis
from .opensearch.notice_chunks import DEFAULT_INDEX as NOTICE_CHUNKS_DEFAULT_INDEX
from .opensearch.notice_chunks import index_from_local as index_notice_chunks_from_local
from .opensearch.notice_chunks import index_from_s3 as index_notice_chunks_from_s3
from .opensearch.sections import DEFAULT_INDEX as SECTIONS_DEFAULT_INDEX
from .opensearch.sections import index_from_local, index_from_s3
from .opensearch.specialites import DEFAULT_INDEX as SPECIALITES_DEFAULT_INDEX
from .opensearch.specialites import index_specialites
from .parsing import DEFAULT_IMAGE_BASE_URL, html_vers_json, parse_semantic_document
from .s3 import make_s3_client

logger = logging.getLogger(__name__)


def charger_html_bytes(content: bytes) -> str:
    """Decode HTML bytes with automatic encoding detection."""
    detected = chardet.detect(content)
    encoding = detected.get("encoding", "utf-8") or "utf-8"
    return content.decode(encoding)


def traiter_fichier_local(fichier_data: tuple) -> dict | None:
    """
    Process a local HTML file (function for multiprocessing).

    Args:
        fichier_data: Tuple containing (file_path, mapping, authorized_cis)

    Returns:
        Dict with JSON data or None if error/skipped
    """
    from .io import charger_html

    fichier, mapping, cis_autorises = fichier_data

    try:
        base = os.path.basename(fichier)
        cis = mapping.get(base)

        if not cis or cis not in cis_autorises:
            return None

        html = charger_html(fichier)
        data = html_vers_json(html)

        return {"source": {"filename": base, "cis": cis}, "content": data}

    except Exception as e:
        logger.error(f"Error processing {fichier}: {e}")
        return None


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


def traiter_fichier_s3(fichier_data: tuple) -> dict | None:
    """
    Process an HTML file from S3 (function for multiprocessing).

    Args:
        fichier_data: Tuple containing (s3_key, html_content_bytes, mapping, cis_autorises)

    Returns:
        Dict with JSON data or None if error/skipped
    """
    s3_key, html_bytes, mapping, cis_autorises = fichier_data

    try:
        filename = s3_key.split("/")[-1]
        cis = mapping.get(filename)

        if not cis or cis not in cis_autorises:
            return None

        html = charger_html_bytes(html_bytes)
        data = html_vers_json(html)

        return {"source": {"filename": filename, "cis": cis}, "content": data}

    except Exception as e:
        logger.error(f"Error processing {s3_key}: {e}")
        return None


def traiter_dossier_local(
    dossier_html: str,
    fichier_cis: str | None = None,
    fichier_sortie: str = "output.jsonl",
    limite: int | None = None,
    num_processes: int | None = None,
    pattern: str = "N",
) -> None:
    """
    Process a local folder of HTML files using multiprocessing.

    Args:
        dossier_html: Path to the folder containing HTML files
        fichier_cis: Text file containing authorized CIS codes (if None, uses database)
        fichier_sortie: Output JSONL file
        limite: Limit number of files to process (for testing)
        num_processes: Number of processes to use (default: CPU count)
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
    """
    if num_processes is None:
        num_processes = mp.cpu_count()

    logger.info(f"Local mode - {num_processes} processes")

    # Build glob pattern
    fichiers = glob.glob(os.path.join(dossier_html, f"{pattern}*.htm"))

    if limite is not None:
        fichiers = fichiers[:limite]

    logger.info(f"{len(fichiers)} HTML files found")

    # Load CIS list from file or database
    if fichier_cis:
        cis_autorises = charger_liste_cis(fichier_cis)
        logger.info(f"CIS list loaded from file: {fichier_cis}")
    else:
        logger.info("Loading authorized CIS from database (Specialite.isBdm)...")
        cis_autorises = get_authorized_cis()
    if not cis_autorises:
        logger.error("No CIS codes loaded, stopping processing")
        return
    logger.info(f"{len(cis_autorises)} CIS codes loaded")

    logger.info("Loading filename -> CIS mapping...")
    mapping = get_filename_to_cis_mapping()
    logger.info(f"{len(mapping)} mappings loaded")

    fichiers_data = [(fichier, mapping, cis_autorises) for fichier in fichiers]

    with open(fichier_sortie, "w", encoding="utf-8") as f_out:
        pass

    files_processed = 0
    files_skipped = 0

    logger.info("Starting processing...")

    with mp.Pool(processes=num_processes) as pool:
        chunk_size = max(1, len(fichiers_data) // (num_processes * 4))

        with tqdm(total=len(fichiers_data), desc="Processing", unit="file") as pbar:
            for result in pool.imap(traiter_fichier_local, fichiers_data, chunksize=chunk_size):
                if result is not None:
                    with open(fichier_sortie, "a", encoding="utf-8") as f_out:
                        f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                    files_processed += 1
                else:
                    files_skipped += 1
                pbar.set_postfix(processed=files_processed, skipped=files_skipped)
                pbar.update(1)

    logger.info(f"Processing complete: {files_processed} processed, {files_skipped} skipped")
    logger.info(f"Output: {fichier_sortie}")


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


def import_semantic_documents_from_s3(
    pattern: str = "N",
    limite: int | None = None,
    staging: bool = False,
    image_base_url: str = DEFAULT_IMAGE_BASE_URL,
    cis: str | None = None,
) -> None:
    """Parse documents from S3 and upsert their semantic HTML into PostgreSQL.

    This experimental pipeline is deliberately independent from the legacy
    JSONL pipeline. In particular, it never moves source files out of staging.
    """
    if pattern not in {"N", "R"}:
        raise ValueError('pattern must be "N" or "R"')

    s3_client = make_s3_client()
    config = get_config()
    table = "notices" if pattern == "N" else "rcp"

    logger.info("Loading authorized CIS codes and filename mapping...")
    cis_autorises = get_authorized_cis()
    mapping = get_filename_to_cis_mapping()
    glossary_terms = get_glossary_terms(config.postgres)
    logger.info("Loaded %d glossary terms to annotate", len(glossary_terms))

    if cis is not None:
        cis = str(cis)
        logger.info("Restricting semantic import to CIS %s", cis)

    if staging:
        keys = sorted(s3_client.list_staging_html_files(pattern))
    else:
        keys = sorted(s3_client.list_html_files(pattern))

    candidates = []
    skipped = 0
    for key in keys:
        filename = key.split("/")[-1]
        mapped_cis = mapping.get(filename)
        document_cis = str(mapped_cis) if mapped_cis is not None else ""
        if not document_cis or document_cis not in cis_autorises or (cis is not None and document_cis != cis):
            skipped += 1
            continue
        candidates.append((key, filename, document_cis))

    if limite is not None:
        candidates = candidates[:limite]

    logger.info(f"{len(candidates)} semantic documents to process, {skipped} skipped before parsing")
    parse_errors = 0

    def parsed_records():
        nonlocal parse_errors
        for key, filename, cis in tqdm(candidates, desc="Semantic documents", unit="file"):
            try:
                source = s3_client.download_file_content(key)
                document = parse_semantic_document(
                    source,
                    image_base_url=image_base_url,
                    glossary_terms=glossary_terms,
                )
                yield {
                    "cis": cis,
                    "filename": filename,
                    "date_notif": document.date_notif.isoformat() if document.date_notif else None,
                    "indication": document.indication,
                    "content_html": document.content_html,
                }
            except Exception as e:
                logger.error(f"Error parsing {key}: {e}")
                parse_errors += 1

    imported, db_errors = import_semantic_documents(parsed_records(), table, config.postgres)
    logger.info(
        "Semantic import complete: %d imported, %d parse errors, %d database errors, %d skipped",
        imported,
        parse_errors,
        db_errors,
        skipped,
    )


def traiter_depuis_s3(
    fichier_cis: str | None = None,
    fichier_sortie: str | None = None,
    limite: int | None = None,
    pattern: str = "N",
    batch_size: int = 500,
    staging: bool = False,
) -> None:
    """
    Process HTML files from S3 and write results to S3 or locally.

    Args:
        fichier_cis: Local file containing authorized CIS codes (if None, uses database)
        fichier_sortie: Local output JSONL file (if None, writes to S3)
        limite: Limit number of files to process (for testing)
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
        batch_size: Number of files to process per batch (to limit memory usage)
        staging: If True, process only files in the staging subdirectory and move them
                 to the main prefix after each batch is written.
    """
    s3_client = make_s3_client()

    config = get_config()
    logger.info("S3 mode - Clever Cloud Cellar")
    logger.info(f"Bucket: {config.s3.bucket_name}")
    html_prefix = config.s3.notice_prefix if pattern == "N" else config.s3.rcp_prefix
    logger.info(f"HTML prefix: {html_prefix}")

    # Load CIS list from file or database
    if fichier_cis:
        cis_autorises = charger_liste_cis(fichier_cis)
        logger.info(f"CIS list loaded from file: {fichier_cis}")
    else:
        logger.info("Loading authorized CIS from database (Specialite.isBdm)...")
        cis_autorises = get_authorized_cis()

    if not cis_autorises:
        logger.error("No CIS codes loaded, stopping processing")
        return
    logger.info(f"{len(cis_autorises)} CIS codes loaded")

    # Get filename -> CIS mapping from database
    logger.info("Loading filename -> CIS mapping...")
    mapping = get_filename_to_cis_mapping()
    logger.info(f"{len(mapping)} mappings loaded")

    # Pre-filter: only keep filenames that map to authorized CIS codes
    # This avoids downloading files we'll skip anyway
    files_to_fetch = {
        filename: cis for filename, cis in mapping.items() if cis in cis_autorises and filename.startswith(pattern)
    }
    logger.info(f"{len(files_to_fetch)} files match authorized CIS codes with pattern '{pattern}'")

    if not files_to_fetch:
        logger.warning("No files to process after filtering")
        return

    # List existing files in S3 to avoid NoSuchKey errors
    staging_prefix = f"{html_prefix}staging/"
    if staging:
        logger.info(f"Staging mode — listing files in {staging_prefix}...")
        existing_keys = set(s3_client.list_staging_html_files(pattern))
    else:
        logger.info("Listing existing files in S3...")
        existing_keys = set(s3_client.list_html_files(pattern))
    existing_filenames = {key.split("/")[-1] for key in existing_keys}
    logger.info(f"{len(existing_filenames)} files exist in S3")

    # Filter to only files that exist in S3
    files_to_fetch = {f: cis for f, cis in files_to_fetch.items() if f in existing_filenames}
    logger.info(f"{len(files_to_fetch)} files to download after S3 existence check")

    if not files_to_fetch:
        logger.warning("No files to process after S3 existence check")
        return

    # Build full S3 keys from filenames
    key_prefix = staging_prefix if staging else html_prefix
    html_keys = [f"{key_prefix}{filename}" for filename in files_to_fetch.keys()]
    if limite is not None:
        html_keys = html_keys[:limite]

    total_files = len(html_keys)
    num_batches = (total_files + batch_size - 1) // batch_size
    logger.info(f"{total_files} files to process in {num_batches} batches of {batch_size}")

    # If writing locally, initialize the output file
    if fichier_sortie:
        with open(fichier_sortie, "w", encoding="utf-8") as f_out:
            pass
        logger.info(f"Local output: {fichier_sortie}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_processed = 0
    total_skipped = 0

    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, total_files)
        batch_keys = html_keys[batch_start:batch_end]

        logger.info(f"Batch {batch_num + 1}/{num_batches}: processing files {batch_start + 1}-{batch_end}")

        # Download batch
        batch_results = []
        for key in tqdm(batch_keys, desc=f"Batch {batch_num + 1}", unit="file"):
            try:
                content = s3_client.download_file_content(key)
                result = traiter_fichier_s3((key, content, mapping, cis_autorises))
                if result is not None:
                    batch_results.append(result)
                    total_processed += 1
                else:
                    total_skipped += 1
            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                total_skipped += 1

        # Write batch results
        if batch_results:
            if fichier_sortie:
                with open(fichier_sortie, "a", encoding="utf-8") as f_out:
                    for r in batch_results:
                        f_out.write(json.dumps(r, ensure_ascii=False) + "\n")
                logger.info(f"Batch {batch_num + 1} appended to {fichier_sortie} ({len(batch_results)} results)")
            else:
                output_key = f"{config.s3.output_prefix}parsed_{pattern}_{timestamp}_batch{batch_num + 1:03d}.jsonl"
                output_content = "\n".join(json.dumps(r, ensure_ascii=False) for r in batch_results)
                s3_client.upload_file_content(output_key, output_content, content_type="application/x-ndjson")
                logger.info(f"Batch {batch_num + 1} written to S3: {output_key} ({len(batch_results)} results)")

        # Move processed files from staging to main prefix
        if staging:
            logger.info(f"Moving {len(batch_keys)} files from staging to main prefix...")
            for staging_key in batch_keys:
                filename = staging_key.split("/")[-1]
                main_key = f"{html_prefix}{filename}"
                s3_client.move_file(staging_key, main_key)
            logger.info(f"Batch {batch_num + 1}: {len(batch_keys)} files moved to main prefix")

    logger.info(f"Processing complete: {total_processed} processed, {total_skipped} skipped")


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
    rcp_lines,  # Iterable[str] — can be a list, file object, or generator
    truth_path: str | None,
    output_path: str,
    debug: bool = False,
    batch_size: int = 500,
) -> None:
    """Run pediatric classification on parsed RCPs and optionally evaluate."""
    from .db import get_cis_atc_mapping
    from .pediatric import (
        PediatricClassification,
        classify,
        compute_metrics,
        extract_section_texts,
        format_metrics,
        load_ground_truth,
    )

    # Load ground truth and ATC mapping upfront (both small)
    ground_truth = {}
    if truth_path:
        ground_truth = load_ground_truth(truth_path)
        logger.info(f"Ground truth loaded: {len(ground_truth)} entries")

    atc_mapping = get_cis_atc_mapping()
    logger.info(f"ATC mapping loaded: {len(atc_mapping)} entries")

    # Build CSV header and write it once (truncates the file)
    header = ["cis", "pred_A", "pred_B", "pred_C"]
    if ground_truth:
        header += ["truth_A", "truth_B", "truth_C", "match_A", "match_B", "match_C"]
    header += ["a_reasons", "b_reasons", "c_reasons", "keywords_41_42", "keywords_43", "evidence_41_42", "evidence_43"]
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(header)

    # Truncate debug file once if needed
    debug_path = None
    if debug:
        debug_path = os.path.join(os.path.dirname(output_path) or ".", "debug_sections.jsonl")
        with open(debug_path, "w", encoding="utf-8"):
            pass

    seen_cis: set[str] = set()
    all_predictions: list[PediatricClassification] = []
    it = iter(rcp_lines)
    batch_num = 0

    while True:
        batch = []
        for _ in range(batch_size):
            try:
                batch.append(next(it))
            except StopIteration:
                break
        if not batch:
            break
        batch_num += 1
        logger.info(f"Processing batch {batch_num}…")

        # Parse only this batch into memory
        rcp_by_cis: dict[str, dict] = {}
        for line in batch:
            rcp_json = json.loads(line)
            source = rcp_json.get("source", {})
            cis = source.get("cis", "") if isinstance(source, dict) else ""
            if cis:
                rcp_by_cis[cis] = rcp_json

        # Append debug entries for this batch
        if debug_path:
            with open(debug_path, "a", encoding="utf-8") as f_debug:
                for cis, rcp_json in rcp_by_cis.items():
                    entry = {
                        "cis": cis,
                        "atc_code": atc_mapping.get(cis, ""),
                        "raw_41": "\n".join(extract_section_texts(rcp_json, "4.1")),
                        "raw_42": "\n".join(extract_section_texts(rcp_json, "4.2")),
                        "raw_43": "\n".join(extract_section_texts(rcp_json, "4.3")),
                    }
                    f_debug.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # Classify and append CSV rows
        with open(output_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for cis, rcp_json in rcp_by_cis.items():
                pred = classify(rcp_json, atc_code=atc_mapping.get(cis, ""))
                seen_cis.add(cis)
                if ground_truth:
                    all_predictions.append(pred)

                gt = ground_truth.get(cis, {})
                row = [pred.cis, int(pred.condition_a), int(pred.condition_b), int(pred.condition_c)]
                if ground_truth:
                    truth_a, truth_b, truth_c = gt.get("A", ""), gt.get("B", ""), gt.get("C", "")
                    row += [
                        int(truth_a) if isinstance(truth_a, bool) else "",
                        int(truth_b) if isinstance(truth_b, bool) else "",
                        int(truth_c) if isinstance(truth_c, bool) else "",
                        int(pred.condition_a == truth_a) if isinstance(truth_a, bool) else "",
                        int(pred.condition_b == truth_b) if isinstance(truth_b, bool) else "",
                        int(pred.condition_c == truth_c) if isinstance(truth_c, bool) else "",
                    ]
                kw_41_42 = [kw for m in pred.matches_41_42 for kw in m.keywords]
                kw_43 = [kw for m in pred.matches_43 for kw in m.keywords]
                row += [
                    " | ".join(pred.a_reasons),
                    " | ".join(pred.b_reasons),
                    " | ".join(pred.c_reasons),
                    " | ".join(dict.fromkeys(kw_41_42)),
                    " | ".join(dict.fromkeys(kw_43)),
                    " ||| ".join(m.text[:200] for m in pred.matches_41_42),
                    " ||| ".join(m.text[:200] for m in pred.matches_43),
                ]
                writer.writerow(row)

    # Write "missing RCP" rows for ground truth CIS codes not found in any batch
    if ground_truth:
        missing_cis = [cis for cis in ground_truth if cis not in seen_cis]
        if missing_cis:
            with open(output_path, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                for cis in missing_cis:
                    gt = ground_truth[cis]
                    truth_a, truth_b, truth_c = gt.get("A", ""), gt.get("B", ""), gt.get("C", "")
                    row = [cis, "", "", ""]
                    row += [
                        int(truth_a) if isinstance(truth_a, bool) else "",
                        int(truth_b) if isinstance(truth_b, bool) else "",
                        int(truth_c) if isinstance(truth_c, bool) else "",
                        "",
                        "",
                        "",
                    ]
                    row += ["", "", "RCP manquant", "", "", "", ""]
                    writer.writerow(row)

    missing_count = len(ground_truth) - len(seen_cis) if ground_truth else 0
    logger.info(f"Classified {len(seen_cis)} drugs, {missing_count} missing RCP")
    logger.info(f"Predictions written to {output_path}")
    if debug_path:
        logger.info(f"Debug sections written to {debug_path}")

    if ground_truth:
        metrics = compute_metrics(all_predictions, ground_truth)
        print(format_metrics(metrics))


def db_import(pattern: str, limite: int | None = None, since: date | None = None, fail_fast: bool = False) -> None:
    """
    Import legacy-tree or semantic-HTML JSONL files from S3 into PostgreSQL.

    Args:
        pattern: "N" for Notices, "R" for RCP.
        limite: Limit total number of records imported (for testing).
        since: If provided, only import JSONL files dated on or after this date.
        fail_fast: Abort on the first failing record, with its full traceback.
    """
    s3_client = make_s3_client()
    config = get_config()
    main_table = "notices" if pattern == "N" else "rcp"
    content_table = "notices_content" if pattern == "N" else "rcp_content"

    logger.info(f"Listing parsed JSONL files for pattern '{pattern}' from S3...")
    jsonl_keys = list(s3_client.list_parsed_files(pattern, since=since))
    logger.info(f"Found {len(jsonl_keys)} files to import into '{main_table}'")

    total_imported = 0
    total_errors = 0
    legacy_sequence_checked = False

    for key in tqdm(jsonl_keys, desc="Files", unit="file"):
        content = s3_client.download_file_content(key)
        lines = [line for line in content.decode("utf-8").split("\n") if line.strip()]

        if limite is not None:
            remaining = limite - total_imported
            if remaining <= 0:
                break
            lines = lines[:remaining]

        records = []
        parse_errors = 0
        for line_num, line in enumerate(lines, start=1):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse line {line_num} in {key}: {e}")
                if fail_fast:
                    raise
                parse_errors += 1

        semantic_records = [record for record in records if "content_html" in record]
        legacy_records = [record for record in records if "content_html" not in record]
        imported = db_errors = 0
        if semantic_records:
            semantic_imported, semantic_errors = import_semantic_documents(
                tqdm(semantic_records, desc="semantic records", unit="rec", leave=False),
                main_table,
                config.postgres,
                fail_fast=fail_fast,
            )
            imported += semantic_imported
            db_errors += semantic_errors
        if legacy_records:
            if not legacy_sequence_checked:
                for table, next_id, max_id, drifted in check_sequences([content_table], config=config.postgres):
                    if drifted:
                        logger.warning(
                            f"{table}: next id {next_id} but MAX(id)={max_id} — legacy inserts will fail with"
                            f" duplicate key errors. Run: infomedicament-dataeng db-check --fix"
                        )
                legacy_sequence_checked = True
            legacy_imported, legacy_errors = import_to_postgres(
                tqdm(legacy_records, desc="records", unit="rec", leave=False),
                main_table,
                content_table,
                config.postgres,
                fail_fast=fail_fast,
            )
            imported += legacy_imported
            db_errors += legacy_errors
        total_imported += imported
        total_errors += parse_errors + db_errors
        level = logging.WARNING if (parse_errors + db_errors) else logging.INFO
        logger.log(level, f"{key}: {imported} imported, {parse_errors + db_errors} errors")

    logger.info(f"Import complete: {total_imported} records imported, {total_errors} errors")


def db_check(fix: bool = False) -> None:
    """Report id-sequence drift on the import target tables, optionally repairing it."""
    tables = ["notices_content", "rcp_content"]  # main tables key on codeCIS, no id sequence
    rows = check_sequences(tables, fix=fix, config=get_config().postgres)

    print(f"{'table':<20} {'next id':>12} {'max(id)':>12}  status")
    for table, next_id, max_id, drifted in rows:
        status = ("FIXED" if fix else "DRIFTED — inserts will fail") if drifted else "ok"
        print(f"{table:<20} {next_id:>12} {max_id:>12}  {status}")

    if any(d for *_, d in rows) and not fix:
        print("\nRun with --fix to reset the sequences.")


def run_import_datagouv(config_path: Path, dataset_name: str | None = None) -> None:
    """Import one or all datasets defined in a data.gouv.fr YAML config file."""
    datasets = load_datasets(config_path)

    to_import = {dataset_name: datasets[dataset_name]} if dataset_name else datasets

    for name, dataset in to_import.items():
        logger.info(f"Importing dataset '{name}' into '{dataset.postgresql_table}'...")
        count = import_dataset(dataset)
        logger.info(f"Done: {count} rows imported into '{dataset.postgresql_table}'")


def run_centralise_fetch(cis: str | None = None, refresh: bool = False, limite: int | None = None) -> None:
    """Download and cache EMA product-information PDFs on S3 (acquisition step).

    With ``cis`` set, fetches only that CIS's PDF, so the full pipeline can be
    prototyped on one PDF without an expensive initial parse run.
    """
    from .centralise.acquire import get_ema_pdf, pdf_cache_key
    from .db import get_centralised_worklist

    s3_client = make_s3_client()
    worklist = get_centralised_worklist(cis=cis)
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
    from .db import get_centralised_worklist

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
        worklist = get_centralised_worklist(cis=cis)
        cis_row = next((row for rows in worklist.values() for row in rows if str(row[0]) == str(cis)), None)
        if cis_row is None:
            raise ValueError(f"No centralised medicine found for CIS {cis}")
        denomination = cis_row[1]
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

    worklist = get_centralised_worklist(cis=cis)
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
                for cis_code, denom in worklist[url]:
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


def run_index_sections(
    doc_type: str,
    index_name: str,
    input_path: str | None = None,
    use_s3: bool = False,
    since: date | None = None,
    limite: int | None = None,
) -> None:
    """Index parsed Notice/RCP sections into OpenSearch."""
    if use_s3:
        pattern = "N" if doc_type == "notice" else "R"
        total = index_from_s3(pattern=pattern, index_name=index_name, doc_type=doc_type, since=since, limite=limite)
    else:
        if not input_path:
            raise ValueError("--input is required when not using --s3")
        total = index_from_local(path=input_path, index_name=index_name, doc_type=doc_type, limite=limite)
    logger.info(f"Done: {total} section documents indexed into '{index_name}'")


def main():
    parser = argparse.ArgumentParser(
        description="Parse ANSM medication HTML documents (Notices and RCPs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local mode (uses database for CIS list by default)
  infomedicament-dataeng local ./html_files -o output.jsonl

  # Local mode with CIS file override
  infomedicament-dataeng local ./html_files --cis-file cis_list.txt -o output.jsonl

  # Test the semantic document parser locally (no database required)
  infomedicament-dataeng semantic-local ./html_files -o semantic_output.jsonl --limit 10

  # Experimental semantic parser: S3 documents directly to PostgreSQL
  infomedicament-dataeng semantic-s3-import --pattern N --staging --limit 10

  # S3 mode (production on Scalingo)
  infomedicament-dataeng s3 --pattern N

Environment variables for S3 mode:
  S3_HOST       S3 endpoint URL
  S3_KEY_ID     S3 access key
  S3_KEY_SECRET S3 secret key
  S3_BUCKET_NAME          Bucket name (default: info-medicaments)
  S3_HTML_PREFIX          Prefix for HTML files (default: exports/html/)
  S3_OUTPUT_PREFIX        Prefix for output files (default: exports/parsed/)

Environment variables for database:
  MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Processing mode")

    # Local mode
    local_parser = subparsers.add_parser("local", help="Process local files")
    local_parser.add_argument("dossier_html", help="Folder containing HTML files")
    local_parser.add_argument("--cis-file", help="CIS file (default: uses database)")
    local_parser.add_argument("--output", "-o", default="output.jsonl", help="Output JSONL file")
    local_parser.add_argument("--limite", type=int, help="Limit number of files to process")
    local_parser.add_argument("--processes", type=int, default=None, help="Number of processes")
    local_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")

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

    # Experimental semantic parser: S3 directly to PostgreSQL
    semantic_s3_parser = subparsers.add_parser(
        "semantic-s3-import",
        help="Parse S3 Notices/RCPs as semantic HTML and upsert content_html",
    )
    semantic_s3_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")
    semantic_s3_parser.add_argument("--cis", help="Process only the document for this CIS code")
    semantic_s3_parser.add_argument("--limit", type=int, help="Limit number of documents processed")
    semantic_s3_parser.add_argument(
        "--staging",
        action="store_true",
        help="Read documents from staging without moving them",
    )
    semantic_s3_parser.add_argument(
        "--image-base-url",
        default=DEFAULT_IMAGE_BASE_URL,
        help="Base URL used to rewrite relative document images",
    )

    # S3 mode
    s3_parser = subparsers.add_parser("s3", help="Process from S3 (Clever Cloud Cellar)")
    s3_parser.add_argument("--cis-file", help="CIS file (default: uses database)")
    s3_parser.add_argument("--output", "-o", help="Local output JSONL file (default: writes to S3)")
    s3_parser.add_argument("--limite", type=int, help="Limit number of files to process")
    s3_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")
    s3_parser.add_argument("--batch-size", type=int, default=500, help="Files per batch (default: 500)")
    s3_parser.add_argument(
        "--staging",
        action="store_true",
        help="Process only files in the staging subdirectory and move them to the main prefix after parsing",
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

    # DB import mode
    db_import_parser = subparsers.add_parser("db-import", help="Import parsed JSONL files from S3 into PostgreSQL")
    db_import_parser.add_argument("--pattern", required=True, choices=["N", "R"], help="N=Notice, R=RCP")
    db_import_parser.add_argument("--limite", type=int, help="Limit number of records to import (for testing)")
    db_import_parser.add_argument(
        "--since",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        metavar="YYYY-MM-DD",
        help="Only import JSONL files whose filename timestamp is on or after this date",
    )
    db_import_parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Abort on the first invalid JSON or failed database record and show its full traceback",
    )

    db_check_parser = subparsers.add_parser("db-check", help="Diagnose the import target tables (id sequence drift)")
    db_check_parser.add_argument("--fix", action="store_true", help="Reset any drifted sequence to MAX(id)+1")

    # Import from data.gouv.fr mode
    datagouv_parser = subparsers.add_parser("import-datagouv", help="Import datasets from data.gouv.fr into PostgreSQL")
    datagouv_parser.add_argument(
        "--config", required=True, type=Path, help="Path to YAML config file (e.g. data_sources/has.yml)"
    )
    datagouv_parser.add_argument("--dataset", help="Name of a specific dataset to import (default: all)")

    subparsers.add_parser(
        "import-ceps-prices",
        help="Replace ceps_price with presentation prices from the public medicines database",
    )

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

    # Pediatric classification mode
    ped_parser = subparsers.add_parser("classify-pediatric", help="Classify drugs for pediatric use")
    ped_source = ped_parser.add_mutually_exclusive_group(required=True)
    ped_source.add_argument("--local-rcp", metavar="PATH", help="Parsed RCP JSONL file (local)")
    ped_source.add_argument("--s3", action="store_true", help="Fetch parsed RCP JSONL files from S3")
    ped_parser.add_argument(
        "--since",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        metavar="YYYY-MM-DD",
        help="S3 mode only: only use JSONL files dated on or after this date",
    )
    ped_parser.add_argument("--truth", help="Ground truth CSV (for evaluation)")
    ped_parser.add_argument("--output", "-o", default="data/predictions.csv", help="Output predictions CSV")
    ped_parser.add_argument("--batch-size", type=int, default=500, help="RCPs per batch (default: 500)")
    ped_parser.add_argument("--debug", action="store_true", help="Write debug_sections.jsonl with raw section texts")

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

    # Index into OpenSearch (subcommand group)
    os_parser = subparsers.add_parser("index-opensearch", help="Index data into OpenSearch")
    os_subparsers = os_parser.add_subparsers(dest="target", help="Index target")

    # index-opensearch sections
    sections_parser = os_subparsers.add_parser("sections", help="Index Notice/RCP sections")
    sections_parser.add_argument("--doc-type", required=True, choices=["notice", "rcp"], help="Type of documents")
    sections_parser.add_argument(
        "--index", default=SECTIONS_DEFAULT_INDEX, help=f"Index name (default: {SECTIONS_DEFAULT_INDEX})"
    )
    sections_source = sections_parser.add_mutually_exclusive_group(required=True)
    sections_source.add_argument("--input", help="Local JSONL file to index")
    sections_source.add_argument("--s3", action="store_true", help="Read from S3 parsed files")
    sections_parser.add_argument(
        "--since",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        metavar="YYYY-MM-DD",
        help="S3 mode only: only index files dated on or after this date",
    )
    sections_parser.add_argument("--limite", type=int, help="Cap on records indexed (for testing)")

    # index-opensearch specialites
    specialites_parser = os_subparsers.add_parser("specialites", help="Index specialités from PostgreSQL")
    specialites_parser.add_argument(
        "--index", default=SPECIALITES_DEFAULT_INDEX, help=f"Index name (default: {SPECIALITES_DEFAULT_INDEX})"
    )
    specialites_parser.add_argument("--limite", type=int, help="Cap on documents indexed (for testing)")

    # index-opensearch notice-chunks
    notice_chunks_parser = os_subparsers.add_parser(
        "notice-chunks", help="Index notices as fine-grained vector-embedded chunks"
    )
    nc_source = notice_chunks_parser.add_mutually_exclusive_group(required=True)
    nc_source.add_argument("--input", help="Local parsed notice JSONL file")
    nc_source.add_argument("--s3", action="store_true", help="Read from S3 parsed notice files")
    notice_chunks_parser.add_argument("--since", help="S3 mode only: only process files dated on or after YYYY-MM-DD")
    notice_chunks_parser.add_argument(
        "--save-embeddings", action="store_true", help="Write per-CIS embedding cache to S3"
    )
    notice_chunks_parser.add_argument(
        "--load-embeddings", action="store_true", help="Load embeddings from S3 cache (skip Albert API on cache hit)"
    )
    notice_chunks_parser.add_argument(
        "--chunk-batch-size", type=int, default=64, help="Chunks per embedding API call (default: 64)"
    )
    notice_chunks_parser.add_argument(
        "--index", default=NOTICE_CHUNKS_DEFAULT_INDEX, help=f"Index name (default: {NOTICE_CHUNKS_DEFAULT_INDEX})"
    )
    notice_chunks_parser.add_argument("--limite", type=int, help="Cap on records indexed (for testing)")

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
        for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "opensearchpy"):
            logging.getLogger(noisy).setLevel(logging.INFO)

    if args.command == "local":
        try:
            traiter_dossier_local(
                args.dossier_html,
                fichier_cis=args.cis_file,
                fichier_sortie=args.output,
                limite=args.limite,
                num_processes=args.processes,
                pattern=args.pattern,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "s3":
        try:
            traiter_depuis_s3(
                fichier_cis=args.cis_file,
                fichier_sortie=args.output,
                limite=args.limite,
                pattern=args.pattern,
                batch_size=args.batch_size,
                staging=args.staging,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "semantic-local":
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

    elif args.command == "semantic-s3-import":
        try:
            import_semantic_documents_from_s3(
                pattern=args.pattern,
                limite=args.limit,
                staging=args.staging,
                image_base_url=args.image_base_url,
                cis=args.cis,
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

    elif args.command == "db-import":
        try:
            db_import(args.pattern, limite=args.limite, since=args.since, fail_fast=args.fail_fast)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "db-check":
        try:
            db_check(fix=args.fix)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-datagouv":
        try:
            run_import_datagouv(args.config, dataset_name=args.dataset)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-ceps-prices":
        try:
            import_ceps_prices()
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-datapackage":
        try:
            import_datapackage(args.package, resource_name=args.resource)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "classify-pediatric":
        try:
            if args.local_rcp:
                with open(args.local_rcp, encoding="utf-8") as f:
                    run_pediatric_classification(
                        (line for line in f if line.strip()),
                        args.truth,
                        args.output,
                        debug=args.debug,
                        batch_size=args.batch_size,
                    )
            else:
                s3_client = make_s3_client()
                keys = list(s3_client.list_parsed_files("R", since=args.since))
                logger.info(f"Found {len(keys)} RCP JSONL file(s) in S3")

                def s3_lines():
                    for key in tqdm(keys, desc="Files", unit="file"):
                        content = s3_client.download_file_content(key)
                        yield from (line for line in content.decode("utf-8").split("\n") if line.strip())

                run_pediatric_classification(
                    s3_lines(),
                    args.truth,
                    args.output,
                    debug=args.debug,
                    batch_size=args.batch_size,
                )
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

    elif args.command == "index-opensearch":
        if not getattr(args, "target", None):
            os_parser.print_help()
            raise SystemExit(1)
        if args.target == "sections":
            try:
                run_index_sections(
                    doc_type=args.doc_type,
                    index_name=args.index,
                    input_path=args.input,
                    use_s3=args.s3,
                    since=args.since,
                    limite=args.limite,
                )
            except Exception as e:
                logger.exception(f"Error: {e}")
                raise SystemExit(1)
        elif args.target == "specialites":
            try:
                index_specialites(index_name=args.index, limite=args.limite)
            except Exception as e:
                logger.exception(f"Error: {e}")
                raise SystemExit(1)
        elif args.target == "notice-chunks":
            try:
                if args.input:
                    index_notice_chunks_from_local(
                        path=args.input,
                        index_name=args.index,
                        limite=args.limite,
                        chunk_batch_size=args.chunk_batch_size,
                    )
                else:
                    index_notice_chunks_from_s3(
                        index_name=args.index,
                        limite=args.limite,
                        since=args.since,
                        chunk_batch_size=args.chunk_batch_size,
                        save_embeddings=args.save_embeddings,
                        load_embeddings=args.load_embeddings,
                    )
            except Exception as e:
                logger.exception(f"Error: {e}")
                raise SystemExit(1)

    else:
        parser.print_help()
        raise SystemExit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()
