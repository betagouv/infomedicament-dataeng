# infomedicament-dataeng

Data engineering tools for ANSM's [infomedicament](https://infomedicament.beta.gouv.fr) website.

## Features

- [HTML Parsing](#html-parsing) — parse ANSM Notice/RCP files into semantic HTML from local disk or S3
- [Centralised EMA PDFs](#centralised-ema-pdfs) — parse centrally-authorised medicines' EMA PDFs into the same Notice/RCP shape
- [DB Import](#db-import) — import parsed JSONL files into PostgreSQL
- [OpenSearch Indexing](#opensearch-indexing) — index parsed sections into OpenSearch for full-text search
- [SQL to CSV](#sql-to-csv-conversion) — convert T-SQL/MySQL dump files to CSV
- [Pediatric Classification](#pediatric-classification) — classify RCPs for pediatric use
- [Import from data.gouv.fr](#import-from-datagouvfr) — fetch open datasets and load them into PostgreSQL

## Installation

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then create the project environment:

```bash
uv sync
```

## Commands

### HTML Parsing

The semantic HTML parser is the supported parser for new imports. Use
`semantic-local` for local files and `semantic-s3-import` for S3 documents.

The legacy tree parser remains available through the `local` and `s3` commands
for compatibility, but it is deprecated and should not be used for new imports.

#### Legacy Local Mode (deprecated)

Process HTML files from a local directory:

```bash
uv run infomedicament-dataeng local <html_folder> [options]
```

Arguments:
- `html_folder`: Directory containing HTML files (N*.htm for Notices, R*.htm for RCPs)

Options:
- `--cis-file`: Text file with allowed CIS codes (default: uses database)
- `--output, -o`: Output JSONL file (default: output.jsonl)
- `--limite`: Limit number of files to process (for testing)
- `--processes`: Number of parallel processes (default: CPU count)
- `--pattern`: File pattern - N=Notice, R=RCP (default: N)

Example:
```bash
# Uses database for CIS list (Specialite.isBdm)
uv run infomedicament-dataeng local ./html_files -o output.jsonl --pattern N

# With CIS file override
uv run infomedicament-dataeng local ./html_files --cis-file cis_list.txt -o output.jsonl
```

#### Local Semantic Document Mode (recommended)

Convert local `N*.htm` notices and `R*.htm` RCPs to sanitized, render-ready semantic HTML without database access:

```bash
uv run infomedicament-dataeng semantic-local ./html_files \
  --output semantic_output.jsonl \
  --limit 10
```

Options:
- `--output, -o`: Output JSONL file (default: `semantic_output.jsonl`)
- `--limit`: Limit the number of documents processed
- `--pattern`: Select `N`, `R`, or `all` documents (default: `all`)
- `--image-base-url`: HTTPS base URL used to rewrite relative document images

Each JSONL record contains `source.filename`, ISO `date_notif`, plain-text
`indication`, and sanitized `content_html`. The indication block is also marked
with `data-document-role="indication"` in the semantic HTML.

#### Semantic S3 Import (recommended)

Parse Notice or RCP HTML from S3 with the semantic parser and write the result
directly to PostgreSQL:

```bash
uv run infomedicament-dataeng semantic-s3-import --pattern N --staging --limit 10
uv run infomedicament-dataeng semantic-s3-import --pattern R --staging --limit 10
uv run infomedicament-dataeng semantic-s3-import --pattern N --cis 61234567
uv run infomedicament-dataeng semantic-s3-import --pattern R --cis 61234567
```

`N` writes `notices.content_html`; `R` writes `rcp.content_html`. Omit
`--staging` to process the corresponding main prefix. This command does not
create JSONL files and never moves staged source files. `--cis` restricts the
update to one CIS; without it, all authorized CIS codes are processed as before.
The command updates `content_html` and replaces `dateNotif` with the date extracted from the HTML;
the legacy parser's `title` and `children` fields remain unchanged. For notices,
it also writes the extracted indication to `specialites_metadata.description`
where the CIS matches. RCP imports do not update that metadata table. The
`content_html` column must exist on the selected document table before running
the command. Terms whose `ref_glossaire.a_souligner` value is true are wrapped
as `<span data-definition="Canonical glossary name">…</span>` so the frontend
can attach an interactive definition UI.

#### Legacy S3 Mode (deprecated)

Process HTML files from S3 (Clever Cloud Cellar) and write results back to S3:

```bash
uv run infomedicament-dataeng s3 [options]
```

Options:
- `--cis-file`: Text file with allowed CIS codes (default: uses database)
- `--limite`: Limit number of files to process (for testing)
- `--pattern`: File pattern - N=Notice, R=RCP (default: N)
- `--batch-size`: Files per batch (default: 500). Results are written after each batch to limit memory usage.
- `--staging`: Process only files in the staging subdirectory (`imports/notice/staging/` or `imports/rcp/staging/`). After each batch is parsed, files are moved to the main prefix.

Example:
```bash
# Full reprocessing of all files
uv run infomedicament-dataeng s3 --pattern R --limite 100

# Delta: parse only newly uploaded files from staging
uv run infomedicament-dataeng s3 --pattern N --staging
```

#### Global Options

- `--verbose, -v`: Enable debug logging

### Centralised EMA PDFs

Centrally-authorised medicines (approved via the EMA, not the ANSM) have no ANSM Notice/RCP HTML.
Instead their product information is published as a single EMA PDF (the EU QRD template).
This pipeline parses those PDFs into the same **sanitized semantic HTML** used for ANSM HTML files.
Each imported document contains `content_html`, an ISO `date_notif` when the PDF supplies a complete
date, and the plain-text `indication`. The semantic HTML includes deterministic `data-block-id`
attributes, section IDs, safe tables and images, and glossary annotations.

Two things make these PDFs different:

- **One PDF bundles several presentations.** A PDF often contains one SmPC + Notice per device
  (cartouche, pen…) or per dosage (5/10/15/20 mg), and the several CIS that share one `UrlEpar` are
  one-per-presentation — so content genuinely differs per CIS. The parser extracts *all*
  presentations and matches each CIS to its own by Jaccard token-overlap of the PDF denomination
  against the CIS's `SpecDenom01`. A CIS with an empty `SpecDenom01` that can't be disambiguated is
  skipped (no record).
- **They must be acquired, not just read.** PDFs are scraped from EMA once and cached forever on S3
  (under `S3_EMA_PDF_PREFIX`, with a `.sha256` sidecar); re-runs serve from cache unless `--refresh`.

The worklist comes from the PDBM database (`VUEmaEpar` LEFT JOIN `Specialite`).

#### 1. Fetch — cache PDFs on S3

```bash
uv run infomedicament-dataeng centralise fetch [options]
```

Options:
- `--cis`: Fetch only the PDF for this CIS code (for prototyping the pipeline on one drug)
- `--refresh`: Force re-download from EMA even if already cached on S3
- `--limite`: Limit number of distinct PDFs to fetch

The first full run is the expensive one (one HTTP round-trip per distinct PDF, ~2 MB each);
subsequent runs only do a cheap existence check and skip anything already cached.

#### 2. Parse and import semantic Notice/RCP HTML

```bash
uv run infomedicament-dataeng centralise parse [options]
```

Options:
- `--cis`: Parse only the PDF for this CIS code
- `--pdf PATH`: Parse and import a single **local** PDF file (requires `--cis`); the matching
  presentation is selected using that CIS's denomination
- `--limite`: Limit number of distinct PDFs to parse
- `--batch-size`: Number of matched documents per database import batch (default: 500)
- `--processed-file`: Optional text file used to resume long runs; a PDF slug is recorded only after
  its database batch succeeds

Worklist mode (no `--pdf`) fetches each distinct PDF via the S3 cache, parses all its presentations
once, matches each CIS to its own presentation, loads the glossary terms marked for annotation, and
upserts each matched document directly into PostgreSQL. RCPs update `rcp.content_html` and
`rcp.dateNotif`; notices update `notices.content_html`, `notices.dateNotif`, and
`specialites_metadata.description` with the extracted indication. Referenced images are uploaded
before the database rows that use them.

#### Full pipeline (all centralised medicines)

Drop `--cis` to process every centrally-authorised CIS. The parse command performs the database
imports itself; no JSONL generation or separate `db-import` step is required.

```bash
# 1. Acquire every EMA PDF into the S3 cache
uv run infomedicament-dataeng centralise fetch

# 2. Parse all cached PDFs and import semantic HTML directly into PostgreSQL
uv run infomedicament-dataeng centralise parse
```

To prototype the whole flow end-to-end on a single drug, thread `--cis <code>` through steps 1–2
and use `--limite` on the import/index steps.

### DB Import

Import parsed JSONL files from S3 into PostgreSQL. This replaces the legacy TypeScript `importNoticeRCP.ts` script.

```bash
uv run infomedicament-dataeng db-import --pattern <N|R> [options]
```

Options:
- `--pattern`: N=Notices, R=RCPs (required)
- `--limite`: Limit number of records to import (for testing)
- `--since YYYY-MM-DD`: Only import JSONL files whose filename timestamp is on or after this date.
- `--fail-fast`: Stop on the first malformed JSON or failed database record and print its traceback.

Example:
```bash
# Import all RCP records
uv run infomedicament-dataeng db-import --pattern R

# Import only JSONL files produced on or after a given date
uv run infomedicament-dataeng db-import --pattern N --since 2026-03-18

# Test with 10 records
uv run infomedicament-dataeng db-import --pattern N --limite 10

# Diagnose one failing record without continuing the import
uv run infomedicament-dataeng db-import --pattern N --fail-fast
```

The command lists `parsed_<pattern>_*.jsonl` files under `S3_OUTPUT_PREFIX`, downloads each one, and
upserts the records into PostgreSQL (by `codeCIS`). Semantic records update `content_html` directly;
historical records still replace their existing content trees.

#### Legacy content sequence check

Legacy tree records insert rows into `notices_content` and `rcp_content`. If either table was restored
with explicit IDs, its PostgreSQL sequence may lag behind `MAX(id)` and cause duplicate-key errors.
Inspect both sequences without changing them:

```bash
uv run infomedicament-dataeng db-check
```

Repair any drifted sequence by resetting its next value to `MAX(id) + 1`:

```bash
uv run infomedicament-dataeng db-check --fix
```

This check does not apply to semantic HTML records, which update the main `notices` or `rcp` table
without inserting legacy content-tree rows. `--fix` changes the two legacy content-table sequences.

### OpenSearch Indexing

Two separate indices power search:

- **`specialites`** — one document per CIS code, used for the main medication search. Matches on specialité name, active substances, pathologies, and ATC classes.
- **`specialite_sections`** — one document per notice/RCP section, used for deep search within documents.

Both use a French analyzer (elision, stopwords, stemming).

#### Specialités index

```bash
uv run infomedicament-dataeng index-opensearch specialites [options]
```

Options:
- `--index`: OpenSearch index name (default: `specialites`)
- `--limite`: Cap on documents indexed (for testing)

Examples:
```bash
# Full index from PostgreSQL
uv run infomedicament-dataeng index-opensearch specialites

# Test with 100 documents
uv run infomedicament-dataeng index-opensearch specialites --limite 100
```

Re-indexing is idempotent — `_id` is the CIS code, so re-running overwrites existing documents.

#### Sections index

Index parsed Notice/RCP sections into OpenSearch. Each section of a notice or RCP becomes one document (~40 sections × ~15k medications ≈ 600k documents).

```bash
uv run infomedicament-dataeng index-opensearch sections --doc-type <notice|rcp> [options]
```

Options:
- `--doc-type`: `notice` or `rcp` (required)
- `--index`: OpenSearch index name (default: `specialite_sections`)
- `--input`: Local JSONL file to index (mutually exclusive with `--s3`)
- `--s3`: Read from S3 parsed files instead of a local file (mutually exclusive with `--input`)
- `--since YYYY-MM-DD`: S3 mode only — only index JSONL files dated on or after this date
- `--limite`: Cap on number of records indexed (for testing)

Examples:
```bash
# Index a local JSONL file (development)
uv run infomedicament-dataeng index-opensearch sections --doc-type notice --input output.jsonl

# Index with a record limit for testing
uv run infomedicament-dataeng index-opensearch sections --doc-type notice --input output.jsonl --limite 100

# Index from S3 (production)
uv run infomedicament-dataeng index-opensearch sections --doc-type notice --s3
uv run infomedicament-dataeng index-opensearch sections --doc-type rcp --s3

# Delta: only index JSONL files produced since a given date
uv run infomedicament-dataeng index-opensearch sections --doc-type notice --s3 --since 2026-03-01
```

Re-indexing is idempotent — each document has a deterministic ID (`{cis}_{anchor}_{doc_type}`), so re-running overwrites existing documents without creating duplicates.

#### Notice chunks index (semantic search)

Index notice content as fine-grained chunks with vector embeddings for semantic (kNN) search. Each logical block of a notice (a sub-section, a bold-headed paragraph, a list of side effects…) becomes one document with a 1024-dimension embedding produced by the Albert API (bge-m3 model).

Requires `ALBERT_API_KEY` — see [Configuration](#configuration).

```bash
uv run infomedicament-dataeng index-opensearch notice-chunks (--input PATH | --s3) [options]
```

Options:
- `--input PATH`: Local parsed notice JSONL file (mutually exclusive with `--s3`)
- `--s3`: Read from S3 parsed notice files (mutually exclusive with `--input`)
- `--save-embeddings`: Write per-notice embedding cache to S3 (avoids re-calling Albert on re-runs)
- `--load-embeddings`: Load embeddings from S3 cache when available (skip Albert API on cache hit)
- `--since YYYY-MM-DD`: S3 mode only — only index JSONL files dated on or after this date
- `--chunk-batch-size N`: Chunks per Albert API call (default: 512, AlbertAPI hard limit: 64)
- `--index`: OpenSearch index name (default: `notice_chunks`)
- `--limite N`: Cap on records indexed (for testing)

Examples:
```bash
# Development: index a local file
uv run infomedicament-dataeng index-opensearch notice-chunks \
  --input parsed_notices.jsonl --chunk-batch-size 64

# Production: embed from S3, cache results, then load from cache on subsequent runs
uv run infomedicament-dataeng index-opensearch notice-chunks \
  --s3 --save-embeddings --load-embeddings --chunk-batch-size 64

# Delta: only newly parsed notices
uv run infomedicament-dataeng index-opensearch notice-chunks \
  --s3 --save-embeddings --load-embeddings --since 2026-04-01 --chunk-batch-size 64
```

The embedding cache is stored on S3 at `exports/parsed/embeddings/notices/{cis}.jsonl.gz` and is invalidated automatically when the notice source content changes (SHA1 hash check).

### SQL to CSV Conversion

Convert SQL INSERT statements (T-SQL, MySQL, PostgreSQL) to CSV files.

```bash
uv run infomedicament-dataeng sql-to-csv <sql_file> [options]
```

Options:
- `--output, -o`: Output CSV file (default: same name with .csv extension)
- `--encoding, -e`: Source file encoding (default: iso-8859-1)
- `--dialect, -d`: SQL dialect - tsql, mysql, postgres (default: tsql)

Example with Codex Triam ATC files:
```bash
# Convert ClasseATC
uv run infomedicament-dataeng sql-to-csv ClasseATC_data.sql -o classe_atc.csv

# Convert VUClassesATC (CIS <-> ATC links)
uv run infomedicament-dataeng sql-to-csv VUClassesATC_data.sql -o cis_atc.csv
```

#### Importing ATC data into PostgreSQL

After generating the CSV files, use the provided SQL script to load them:

```bash
# Run the migrations in infomedicament first
cd ../infomedicament && npm run db:migrate:latest

# Then import the data (paths are configurable via environment variables)
export ATC_CSV_PATH=/path/to/classe_atc.csv
export CIS_ATC_CSV_PATH=/path/to/cis_atc.csv
psql -v atc_csv="$ATC_CSV_PATH" -v cis_atc_csv="$CIS_ATC_CSV_PATH" $APP_DB_URL -f sql/import_atc.sql
```

### Pediatric Classification

Classify medications for pediatric use based on their parsed RCP content (sections 4.1, 4.2, 4.3). Produces three independent boolean labels:

- **A**: Indication pédiatrique (pediatric indication exists)
- **B**: Contre-indication pédiatrique (pediatric contraindication exists)
- **C**: Sur avis d'un professionnel de santé (requires professional advice)

```bash
uv run infomedicament-dataeng classify-pediatric (--local-rcp <path> | --s3) [options]
```

Options:
- `--local-rcp PATH`: Local parsed RCP JSONL file (mutually exclusive with `--s3`)
- `--s3`: Fetch parsed RCP JSONL files directly from S3 (mutually exclusive with `--local-rcp`)
- `--since YYYY-MM-DD`: S3 mode only — only use JSONL files dated on or after this date
- `--batch-size`: Number of RCPs classified per batch (default: 500). Files are streamed and processed incrementally to keep memory usage bounded.
- `--truth`: Ground truth CSV for evaluation (columns: `cis,code_atc,A:...,B:...,C:...` with `oui/non` values)
- `--output, -o`: Output predictions CSV (default: `data/predictions.csv`)

Examples:
```bash
# From a local file (development / evaluation)
uv run infomedicament-dataeng classify-pediatric \
  --local-rcp data/rcp_pediatrie.jsonl \
  --truth data/ground_truth.csv \
  -o data/predictions.csv

# From S3 (no prior download needed)
uv run infomedicament-dataeng classify-pediatric --s3 -o data/predictions.csv

# From S3, only files produced since a given date
uv run infomedicament-dataeng classify-pediatric --s3 --since 2026-01-01 -o data/predictions.csv
```

The predictions CSV includes explainability columns (matched keywords, evidence text, C-reasons) for manual review.

### Import from data.gouv.fr

Fetch datasets from the French open-data platform and load them into PostgreSQL. Each run truncates the target table and re-inserts all rows.

```bash
uv run infomedicament-dataeng import-datagouv --config <yaml_file> [--dataset <name>]
```

Options:
- `--config`: Path to a YAML dataset config file (required)
- `--dataset`: Name of a specific dataset to import (default: all datasets in the file)

Example:
```bash
# Import all datasets defined in data_sources/has.yml (asmr and smr)
uv run infomedicament-dataeng import-datagouv --config data_sources/has.yml

# Import only the smr table
uv run infomedicament-dataeng import-datagouv --config data_sources/has.yml --dataset smr

# Import all ANSM tables from the data.gouv.fr demo environment
uv run infomedicament-dataeng import-datagouv --config data_sources/ansm.yml
```

#### Adding a new dataset

Dataset configuration lives in YAML files under `data_sources/`. Each entry maps a data.gouv.fr resource to a PostgreSQL table:

```yaml
# Optional file-wide override; defaults to https://www.data.gouv.fr/api/1/datasets/r/
base_url: https://demo.data.gouv.fr/api/1/datasets/r/

datasets:
  my_dataset:
    datagouv_dataset_id: "<resource UUID from data.gouv.fr>"
    postgresql_table: my_table
    source:
      type: csv
      delimiter: ";"
      quotechar: "$"   # optional, defaults to standard "
      encoding: utf-8  # or cp1252 for Windows-encoded files
    columns:
      - name: col_one
        type: str
      - name: col_two
        type: str
```

`base_url` applies to every dataset in the file. Remove it when the resource is
moved to the production data.gouv.fr site; update `datagouv_dataset_id` as well
if the resource UUID changes.

The table must be created first via a Kysely migration in the [`infomedicament`](https://github.com/betagouv/infomed) NextJS project.

## Delta workflow (monthly updates)

When only a small number of new or updated HTML files arrive, avoid reprocessing everything:

1. **Upload new HTML files to the staging subdirectory** (instead of the main prefix):
   - Notices: `imports/notice/staging/`
   - RCPs: `imports/rcp/staging/`

2. **Parse only the staged files:**
   ```bash
   uv run infomedicament-dataeng s3 --pattern N --staging
   uv run infomedicament-dataeng s3 --pattern R --staging
   ```
   Files are automatically moved from staging to the main prefix after each batch.

3. **Import only the new JSONL output:**
   ```bash
   uv run infomedicament-dataeng db-import --pattern N --since YYYY-MM-DD
   uv run infomedicament-dataeng db-import --pattern R --since YYYY-MM-DD
   ```

## Configuration

### S3/Cellar

- `S3_HOST`: S3 endpoint URL (default: https://cellar-c2.services.clever-cloud.com)
- `S3_KEY_ID`: S3 access key (required for S3 mode)
- `S3_KEY_SECRET`: S3 secret key (required for S3 mode)
- `S3_BUCKET_NAME`: Bucket name (default: info-medicaments)
- `S3_HTML_NOTICE_PREFIX`: Prefix for Notice HTML files (default: imports/notice/)
- `S3_HTML_RCP_PREFIX`: Prefix for RCP HTML files (default: imports/rcp/)
- `S3_EMA_PDF_PREFIX`: Prefix for cached centralised EMA PDFs (default: imports/ema_pdf/)
- `S3_OUTPUT_PREFIX`: Prefix for output files (default: exports/parsed/)

### Database

The database is used for two purposes:
1. **CIS list**: By default, authorized CIS codes are loaded from `SELECT SpecId FROM Specialite WHERE isBdm`
2. **Filename mapping**: Maps HTML filenames to CIS codes via the `Spec_Doc` and `Document` tables

Two configuration formats are supported:

**Option 1: Connection URL (recommended for Scalingo)**
- `DATABASE_URL` or `SCALINGO_MYSQL_URL`: Full connection string for mySQL
- `POSTGRES_URL` or `APP_DATABASE_URL`: Full connection string for PostgreSQL

**Option 2: Individual variables (for local development)**
- `MYSQL_HOST` (default: localhost)
- `MYSQL_USER` (default: root)
- `MYSQL_PASSWORD` (default: mysql)
- `MYSQL_DATABASE` (default: pdbm_bdd)
- `MYSQL_PORT` (default: 3306)
- `POSTGRES_HOST` (default: localhost)
- `POSTGRES_USER` (default: postgres)
- `POSTGRES_PASSWORD` (default: postgres)
- `POSTGRES_DATABASE` (default: postgres)
- `POSTGRES_PORT` (default: 5432)

### OpenSearch

- `SCALINGO_OPENSEARCH_URL` or `OPENSEARCH_URL`: Full connection URL including credentials (e.g. `http://user:pass@host:port`). Scalingo provides this automatically when an OpenSearch addon is attached.
- `OPENSEARCH_HOST`: Fallback for local development (default: `http://localhost:9200`)

### Albert API (embeddings)

- `ALBERT_API_KEY`: API key for the [Albert API](https://albert.api.etalab.gouv.fr) — required for `index-opensearch notice-chunks`

### Application

- `LOG_LEVEL`: Logging level (default: INFO)
- `CDN_BASE_URL`: Base URL for image CDN (default: https://cellar-c2.services.clever-cloud.com/info-medicaments/exports/images)

## Scalingo Deployment

This project is a [web-less application](https://doc.scalingo.com/platform/app/web-less-app) designed to run as scheduled tasks on Scalingo.

### Initial Setup

After the first deployment, scale the web process to 0:

```bash
scalingo --app your-app scale web:0
```

### Running Tasks

Run tasks as one-off containers:

```bash
# Delta parse: only staged files (recommended for monthly updates)
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli s3 --pattern N --staging"
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli s3 --pattern R --staging"

# Full reparse: all files (initial load or full reprocessing)
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli s3 --pattern N --batch-size 1000"
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli s3 --pattern R --batch-size 1000"

# Test with a limit
scalingo --app your-app run "python -m infomedicament_dataeng.cli s3 --pattern N --limite 10"

# Import Notices into PostgreSQL (delta: only today's JSONL files)
scalingo --app your-app run "python -m infomedicament_dataeng.cli db-import --pattern N --since $(date +%Y-%m-%d)"

# Import RCPs into PostgreSQL (delta)
scalingo --app your-app run "python -m infomedicament_dataeng.cli db-import --pattern R --since $(date +%Y-%m-%d)"

# Full import: all JSONL files
scalingo --app your-app run "python -m infomedicament_dataeng.cli db-import --pattern N"
scalingo --app your-app run "python -m infomedicament_dataeng.cli db-import --pattern R"

# Index specialités into OpenSearch (full reindex from PostgreSQL)
scalingo --app your-app run "python -m infomedicament_dataeng.cli index-opensearch specialites"

# Index notices and RCPs into OpenSearch (delta)
scalingo --app your-app run "python -m infomedicament_dataeng.cli index-opensearch sections --doc-type notice --s3 --since $(date +%Y-%m-%d)"
scalingo --app your-app run "python -m infomedicament_dataeng.cli index-opensearch sections --doc-type rcp --s3 --since $(date +%Y-%m-%d)"

# Full reindex
scalingo --app your-app run "python -m infomedicament_dataeng.cli index-opensearch sections --doc-type notice --s3"
scalingo --app your-app run "python -m infomedicament_dataeng.cli index-opensearch sections --doc-type rcp --s3"

# Centralised EMA PDFs: acquire + parse (feeds the same db-import / index steps above)
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli centralise fetch"
scalingo --app your-app run --size 2XL "python -m infomedicament_dataeng.cli centralise parse"
```

For automated execution, we will use [Scalingo Scheduler](https://doc.scalingo.com/platform/app/task-scheduling/scalingo-scheduler) with a `cron.json` file.

### Required Environment Variables

Set these in your Scalingo app settings:

- `S3_KEY_ID` and `S3_KEY_SECRET` (from Clever Cloud Cellar addon)
- `DATABASE_URL`: Copy the MySQL connection string from the app containing the database addon

## Development

```bash
# Install with dev dependencies
uv sync

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=infomedicament_dataeng

# Lint and format
uv run ruff check .
uv run ruff format .

# Auto-fix linting issues
uv run ruff check . --fix
```

### Pre-commit hooks

This repo uses [pre-commit](https://pre-commit.com/) to enforce code quality:

- **pre-commit**: ruff linting (with auto-fix) and formatting
- **pre-push**: full test suite via pytest

After installing dependencies, register the hooks once:

```bash
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```
