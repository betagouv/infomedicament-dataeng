# infomedicament-dataeng

Data-engineering tools for ANSM's [Info Médicament](https://infomedicament.beta.gouv.fr) website.

PostgreSQL `ansm_*` tables are the source of truth for specialties and document URLs. Notice and RCP content is parsed into sanitized semantic HTML and written directly to PostgreSQL. The project no longer uses MySQL, S3-hosted ANSM HTML as a production parsing source, or the legacy tree-shaped document format.

## Features

- [Semantic Notice/RCP import](#database-driven-semantic-import) — import changed ANSM and EMA documents from the PostgreSQL catalog.
- [ANSM catalog imports](#ansm-catalog-imports) — load Frictionless datapackages and configured data.gouv.fr datasets into PostgreSQL.
- [Grist reference data](#grist-reference-data) — synchronize hand-maintained reference tables into PostgreSQL.
- [Indications](#indications) — derive pathology and clinical-class indications from ANSM and Grist data.
- [Resume tables](#resume-tables) — materialize browse and search summaries.
- [Centralised EMA utilities](#centralised-ema-utilities) — cache, inspect, and selectively reprocess EMA product-information PDFs.
- [Pediatric classification](#pediatric-classification) — classify medicines from semantic RCP content stored in PostgreSQL.
- [Local semantic parser](#local-semantic-parser) — inspect semantic parser output without database access.
- [Maintenance utilities](#maintenance-utilities) — download debugging fixtures and convert SQL dumps to CSV.

## Installation

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then create the environment:

```bash
uv sync
```

## Primary document pipeline

### Database-driven semantic import

`semantic-db-import` reads specialties and document URLs from PostgreSQL. National-procedure documents are downloaded from the URLs in `ansm_document`; centralised specialties are resolved through the EMA report and parsed from EMA PDFs.

```bash
# Import documents changed during the last 24 hours
uv run infomedicament-dataeng semantic-db-import

# Import changes since an explicit cutoff
uv run infomedicament-dataeng semantic-db-import \
  --since 2026-09-20T08:00:00+00:00

# Re-import the full catalog
uv run infomedicament-dataeng semantic-db-import --full

# Re-import only non-centralised ANSM documents
uv run infomedicament-dataeng semantic-db-import \
  --full --non-centralised-only

# Import only centralised EMA documents changed during the last 24 hours
uv run infomedicament-dataeng semantic-db-import --centralised-only

# Target one specialty
uv run infomedicament-dataeng semantic-db-import \
  --cis 61234567 --full --limit 1
```

Options:

- `--since ISO-DATETIME`: import documents changed since the cutoff; defaults to 24 hours ago.
- `--full`: process the full catalog; mutually exclusive with `--since`.
- `--cis`: restrict processing to one CIS code.
- `--limit`: cap the number of selected specialties.
- `--batch-size`: number of documents written per PostgreSQL batch; default `500`.
- `--centralised-only`: process only centrally authorised documents sourced from EMA.
- `--non-centralised-only`: process only non-centralised documents sourced from ANSM.

`--centralised-only` and `--non-centralised-only` are mutually exclusive. Either flag can be combined with `--full`, `--since`, `--cis`, `--limit`, and `--batch-size`.

The importer writes semantic HTML to `notices.content_html` and `rcp.content_html`. Notice imports upsert `specialites_metadata.description`, while full semantic imports and ANSM specialty catalog imports reconcile metadata `CIS` and `title` values. Glossary terms marked with `ref_glossaire.a_souligner` are annotated in the generated HTML.

Some ANSM UTF-8 exports contain C1 controls inherited from an earlier Latin-1
conversion of Windows-1252 text (`U+0092` for `’`, `U+009C` for `œ`). The parser
repairs these defined Windows-1252 characters while preserving correct Unicode.
Use `semantic-db-import --full` to regenerate already imported documents with
the corrected parser.

HAS SMR/ASMR labels normalize `££` paragraph separators to blank lines during
import. Consumers should preserve blank lines when rendering
these plain-text labels (for example, split them into escaped paragraphs).

### Local semantic parser

Use `semantic-local` to inspect parser output without accessing a database:

```bash
uv run infomedicament-dataeng semantic-local ./html_files \
  --output semantic_output.jsonl \
  --limit 10
```

Options:

- `--output`, `-o`: output JSONL path; default `semantic_output.jsonl`.
- `--limit`: cap the number of files.
- `--pattern`: `N`, `R`, or `all`; default `all`.
- `--image-base-url`: HTTPS base URL used to rewrite relative image paths.

## ANSM catalog imports

### Frictionless datapackage

Load the ANSM datapackage into PostgreSQL `ansm_*` tables:

```bash
uv run infomedicament-dataeng import-datapackage \
  --package /path/to/datapackage.zip
```

Use `--resource NAME` to load one resource. Target tables must already exist.

### data.gouv.fr datasets

Load configured CSV datasets into PostgreSQL:

```bash
uv run infomedicament-dataeng import-datagouv \
  --config data_sources/has.yml
```

Each selected target table is truncated and fully reloaded. The CSV is downloaded, parsed, validated, and prepared before
opening the replacement transaction. Empty datasets and rows with an unexpected column count are rejected, leaving the
table unchanged. Set `allow_empty: true` on an individual dataset in its YAML configuration only when an empty replacement
is expected. A single surplus trailing empty CSV field remains supported. Truncation and insertion share one transaction,
so database insertion errors roll back the replacement. Use `--dataset NAME` to import one dataset from the YAML file.

## Daily orchestration

Run the complete sequential refresh (ANSM, HAS, Grist, semantic documents, indications, and resume tables):

```bash
uv run infomedicament-dataeng daily-sync
```

The dependency-oriented target is shown below. This is not the current execution order: the first implementation runs
each step sequentially, but the graph makes the safe parallelism explicit.

```text
[ANSM import] --+
                +--> [ANSM + Grist ready] --+--> [Semantic import] --------+
[Grist sync] ---+                            |                              |
                                             +--> [Build indications]       |
                                                       |                    |
                                                       v                    v
                                                 [Build resume] ------> [Run success]
                                                                            ^
[HAS import] ----------------------------------------------------------------+
```

`ANSM + Grist ready` is a dependency join, not an additional script. Semantic import and indication building can run in
parallel after that join. Resume building waits for indications; HAS has no downstream dependency in this repository.
The run succeeds only after the HAS, semantic, and resume branches have all completed.

Scheduled runs should use `daily-sync --trigger schedule`. The command prevents overlap with a PostgreSQL advisory lock,
stops on the first fatal step failure, and records its name and error on the pipeline run. Individual semantic document
failures are recorded for retry and do not stop the remaining documents, indications, or resume builders. After semantic
processing, the latest analyzed ANSM or EMA source modification date is persisted as its semantic watermark. The next run subtracts
24 hours from that watermark and reprocesses documents from the resulting cutoff; the first run performs a full semantic
import. ANSM and HAS resources are fully imported on every run, and Grist, indications, and resume tables are always
synchronized or rebuilt.

### Document failures and retries

Before deploying this version, deploy the migration managed in the Info Médicament application repository that adds a
nullable `semantic_report` JSONB column to the existing `pipeline_run` table; no new table is needed. The application
repository owns the database schema and migrations. This pipeline does not apply migrations automatically.

Each semantic import logs individual failures at `ERROR` with CIS, document type, URL (when resolved), stage, and error.
An end-of-step summary logs imported RCP/Notice counts, pending document count, and database error count. The summary uses
`WARNING` when documents remain pending and `INFO` otherwise; detailed exception tracebacks use `DEBUG`.

The daily pipeline saves that report and the source watermark together. Each subsequent run reads the latest non-null
report, including reports from runs whose later steps failed, and selects pending CIS codes regardless of their source
date. Retries use current catalog URLs/EMA metadata and refresh cached EMA PDFs for source/parse/match retries, even when
their URLs have not changed. Database-only retries reuse validated cached PDFs. A failed or missing document stays pending
until a successful database write clears it; successful documents continue to be published and the watermark can advance. A process interruption
before saving the report leaves the previous watermark and report intact, so the next run repeats that selection.
A document removed from the catalog remains pending with a resolution error for investigation.

Inspect the latest report:

```sql
SELECT id, started_at, status, semantic_report
FROM pipeline_run
WHERE semantic_report IS NOT NULL
ORDER BY started_at DESC
LIMIT 1;
```

`semantic_report.imported` contains per-type counts, `failed_count` contains the pending count, and `failed_documents`
contains the CIS, `document_type`, `url`, `stage`, and `error` for every outstanding document. A run can have `status = 'success'`
with pending documents: it means the pipeline completed, including indications and resume tables. Scheduled
retry persistence belongs to `daily-sync`; standalone `semantic-db-import` logs its outcomes but does not update the ledger.

## Grist reference data

Synchronize the hand-maintained reference tables from the Info Médicament Grist document:

```bash
uv run infomedicament-dataeng sync-grist
```

The command requires `GRIST_DOC_ID` and `GRIST_API_KEY`. It reads from `https://grist.numerique.gouv.fr`, matching the previous application-side synchronization script. Each non-empty Grist table replaces its corresponding PostgreSQL table in a transaction; an unexpectedly empty Grist table leaves the existing PostgreSQL data unchanged.

## Indications

Build the application `indications` table after importing the ANSM catalog and synchronizing Grist:

```bash
uv run infomedicament-dataeng build-indications
```

The command combines `ansm_pathologie`, `ansm_classe_clinique`, their specialty relationships, visible specialties, and editorial definitions from `ref_pathologies`. Existing indication IDs are retained according to the previous aggregation rules. The rebuild is atomic and does not use the legacy MySQL database.

## Resume tables

After importing ANSM data, synchronizing Grist, and building indications, rebuild all denormalized application summaries:

```bash
uv run infomedicament-dataeng build-resume
```

Individual outputs can be rebuilt with `--target indications`, `substances`, `generiques`, `medicaments`, or `specialites`. Each output table and its associated alphabetic navigation row are replaced in one transaction. The builders use only PostgreSQL `ansm_*`, `ref_*`, and `indications` tables; they do not access the legacy MySQL database.

Rows are inserted in batches of up to 500 to reduce network round trips. Each table logs its start and, after committing, its row count and elapsed time. All batches for a table share the same transaction, so an interrupted or failed rebuild rolls back that table's replacement.

The `substances` builder retains names used by visible specialties, including combinations and kits. Names without a qualifying single-substance specialty are stored with `specialites = 0`; only names with positive counts contribute to alphabetic navigation.

The `substances` builder also fills `resume_substances.title`, and `specialites` fills `resume_specialites.subsTitle`, using the title rules from [infomedicament PR #287](https://github.com/betagouv/infomedicament/pull/287). Titles prefer a canonical name, excluding names starting with `(NE PAS UTILISER)` when alternatives exist, then fall back to the lowest numeric name ID. Substances without catalog names use the first nonempty composition label sorted by label. HTML entities and extra whitespace are cleaned. Specialty titles preserve substance order and duplicates; unresolved substances produce a null title. These fields are computed within each builder's transaction without a separate title-seeding step.

The destination schema must already include the application's `resume_substances.title` and `resume_specialites.subsTitle` columns. To refresh only these outputs:

```bash
uv run infomedicament-dataeng build-resume --target substances
uv run infomedicament-dataeng build-resume --target specialites
```

## Centralised EMA utilities

`semantic-db-import` already processes centralised medicines. These commands remain available for cache warming, targeted recovery, and parser development.

```bash
# Resolve centralised specialties from PostgreSQL and cache their PDFs on S3
uv run infomedicament-dataeng centralise fetch

# Parse cached/current PDFs and import semantic HTML directly into PostgreSQL
uv run infomedicament-dataeng centralise parse
```

Both commands accept `--cis` and `--limite`. `centralise fetch` also accepts `--refresh`. `centralise parse` accepts `--pdf`, `--batch-size`, and `--processed-file`.

## Pediatric classification

Classify medicines from semantic RCP HTML stored in PostgreSQL. The command reads `rcp.content_html` directly and joins `ansm_specialite_atc` to `ansm_atc` for the contraceptive ATC rule (the public code is `libelle_abr`, not the numeric catalog key); it does not read JSONL or S3 document exports.

```bash
uv run infomedicament-dataeng classify-pediatric \
  --output data/predictions.csv

# Target one medicine while validating the pipeline
uv run infomedicament-dataeng classify-pediatric \
  --cis 61234567 --limit 1 --debug
```

Options:

- `--cis`: classify one CIS code.
- `--limit`: cap the number of RCPs.
- `--truth`: optional ground-truth CSV used to compute evaluation metrics.
- `--output`, `-o`: predictions CSV; default `data/predictions.csv`.
- `--batch-size`: PostgreSQL streaming batch size; default `500`.
- `--debug`: write extracted RCP sections 4.1, 4.2, and 4.3 to `debug_sections.jsonl`.

## Maintenance utilities

### Download HTML for parser debugging

This command copies raw HTML from S3 to a local directory. It is a debugging helper and is not part of the production parsing pipeline.

```bash
uv run infomedicament-dataeng download-html ./html_files \
  --pattern N --limite 10
```

### Convert SQL dumps to CSV

```bash
uv run infomedicament-dataeng sql-to-csv input.sql \
  --output output.csv \
  --dialect tsql
```

The supported input dialects are `tsql`, `mysql`, and `postgres`. This is file conversion only and does not connect to a MySQL database.

## Configuration

### PostgreSQL

Use either:

- `POSTGRESQL_URL` or `SCALINGO_POSTGRESQL_URL`; or
- `PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`, and `PG_PORT`.

### S3/Cellar

- `S3_HOST`
- `S3_KEY_ID`
- `S3_KEY_SECRET`
- `S3_BUCKET_NAME`
- `S3_HTML_NOTICE_PREFIX`
- `S3_HTML_RCP_PREFIX`
- `S3_EMA_PDF_PREFIX`
- `S3_IMAGE_PREFIX`

### Application

- `CDN_BASE_URL`
- `LOG_LEVEL`
- `GRIST_DOC_ID`
- `GRIST_API_KEY`

## Scalingo tasks

This is a web-less application. A typical scheduled delta import is:

```bash
scalingo --app your-app run --size 2XL \
  "python -m infomedicament_dataeng.cli semantic-db-import"
```

Other useful one-off tasks:

```bash
# Full semantic re-import
scalingo --app your-app run --size 2XL \
  "python -m infomedicament_dataeng.cli semantic-db-import --full"
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
```
