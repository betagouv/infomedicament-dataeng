# infomedicament-dataeng

Data-engineering tools for ANSM's [Info Médicament](https://infomedicament.beta.gouv.fr) website.

PostgreSQL `ansm_*` tables are the source of truth for specialties and document URLs. Notice and RCP content is parsed into sanitized semantic HTML and written directly to PostgreSQL. The project no longer uses MySQL, S3-hosted ANSM HTML as a production parsing source, or the legacy tree-shaped document format.

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

The importer writes semantic HTML to `notices.content_html` and `rcp.content_html`. Notice indications also update `specialites_metadata.description`. Glossary terms marked with `ref_glossaire.a_souligner` are annotated in the generated HTML.

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

Each selected target table is truncated and fully reloaded. Use `--dataset NAME` to import one dataset from the YAML file.

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

Classify medicines from semantic RCP HTML stored in PostgreSQL. The command reads `rcp.content_html` directly and uses `cis_atc` for the contraceptive ATC rule; it does not read JSONL or S3 document exports.

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

## OpenSearch

The supported indexer builds the main medication search index from PostgreSQL:

```bash
uv run infomedicament-dataeng index-opensearch specialites
```

Options:

- `--index`: target index; default `specialites`.
- `--limite`: cap the number of documents for testing.

The removed `sections` and `notice-chunks` indexers consumed the retired tree-shaped JSONL format and could not index current semantic HTML documents.

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

### OpenSearch

- `SCALINGO_OPENSEARCH_URL` or `OPENSEARCH_URL`
- `OPENSEARCH_HOST` as a local-development fallback

### Application

- `CDN_BASE_URL`
- `LOG_LEVEL`

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

# Refresh the medication search index
scalingo --app your-app run \
  "python -m infomedicament_dataeng.cli index-opensearch specialites"
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
```
