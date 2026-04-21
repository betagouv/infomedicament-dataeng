"""Command-line interface for the HTML parser."""

import argparse
import csv
import glob
import json
import logging
import multiprocessing as mp
import os
from datetime import date, datetime
from pathlib import Path

import chardet
from tqdm import tqdm

from .config import get_config
from .convert import sql_to_csv
from .datagouv import import_dataset, load_datasets
from .db import get_authorized_cis, get_filename_to_cis_mapping, import_to_postgres
from .io import charger_liste_cis
from .opensearch.notice_chunks import DEFAULT_INDEX as NOTICE_CHUNKS_DEFAULT_INDEX
from .opensearch.notice_chunks import index_from_local as index_notice_chunks_from_local
from .opensearch.notice_chunks import index_from_s3 as index_notice_chunks_from_s3
from .opensearch.sections import DEFAULT_INDEX as SECTIONS_DEFAULT_INDEX
from .opensearch.sections import index_from_local, index_from_s3
from .opensearch.specialites import DEFAULT_INDEX as SPECIALITES_DEFAULT_INDEX
from .opensearch.specialites import index_specialites
from .parsing import html_vers_json
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


def db_import(pattern: str, limite: int | None = None, since: date | None = None) -> None:
    """
    Import parsed JSONL files from S3 into PostgreSQL.

    Args:
        pattern: "N" for Notices, "R" for RCP.
        limite: Limit total number of records imported (for testing).
        since: If provided, only import JSONL files dated on or after this date.
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
            except Exception as e:
                logger.error(f"Failed to parse line {line_num} in {key}: {e}")
                parse_errors += 1

        imported, db_errors = import_to_postgres(
            tqdm(records, desc="records", unit="rec", leave=False),
            main_table,
            content_table,
            config.postgres,
        )
        total_imported += imported
        total_errors += parse_errors + db_errors
        logger.info(f"{key}: {imported} imported, {parse_errors + db_errors} errors")

    logger.info(f"Import complete: {total_imported} records imported, {total_errors} errors")


def run_import_datagouv(config_path: Path, dataset_name: str | None = None) -> None:
    """Import one or all datasets defined in a data.gouv.fr YAML config file."""
    datasets = load_datasets(config_path)

    to_import = {dataset_name: datasets[dataset_name]} if dataset_name else datasets

    for name, dataset in to_import.items():
        logger.info(f"Importing dataset '{name}' into '{dataset.postgresql_table}'...")
        count = import_dataset(dataset)
        logger.info(f"Done: {count} rows imported into '{dataset.postgresql_table}'")


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

    # Import from data.gouv.fr mode
    datagouv_parser = subparsers.add_parser("import-datagouv", help="Import datasets from data.gouv.fr into PostgreSQL")
    datagouv_parser.add_argument(
        "--config", required=True, type=Path, help="Path to YAML config file (e.g. data_sources/has.yml)"
    )
    datagouv_parser.add_argument("--dataset", help="Name of a specific dataset to import (default: all)")

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
        "--chunk-batch-size", type=int, default=512, help="Chunks per embedding API call (default: 512)"
    )
    notice_chunks_parser.add_argument(
        "--requests-per-minute", type=int, default=500, help="Albert API rate limit (default: 500)"
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

    elif args.command == "sql-to-csv":
        try:
            output_path = Path(args.output) if args.output else None
            sql_to_csv(Path(args.sql_file), output_path, args.encoding, args.dialect)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "db-import":
        try:
            db_import(args.pattern, limite=args.limite, since=args.since)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "import-datagouv":
        try:
            run_import_datagouv(args.config, dataset_name=args.dataset)
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
                        requests_per_minute=args.requests_per_minute,
                    )
                else:
                    index_notice_chunks_from_s3(
                        index_name=args.index,
                        limite=args.limite,
                        since=args.since,
                        chunk_batch_size=args.chunk_batch_size,
                        requests_per_minute=args.requests_per_minute,
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
