# Point-in-Time Financials Lakehouse

![CI](https://github.com/Jahnavidoppalapudi/sec-financials-lakehouse/actions/workflows/ci.yml/badge.svg)

> A lakehouse for public-company financial statements from SEC EDGAR that preserves
> **what was known when**. Companies restate earlier numbers; this pipeline keeps every
> version so analysts and backtests never accidentally use information from the future.

**Status:** 🚧 Bronze and silver complete. Gold (bitemporal model) in progress.

## The Problem

A company reports revenue of $10B for a quarter. A year later it restates that quarter to $9.6B.
Most pipelines overwrite the old value, so a report or model run "as of" the original date
silently uses the corrected number it could not have known. This project tracks two timelines:

- **Valid time:** the period a number describes (e.g. fiscal Q2 2024)
- **Knowledge time:** when that number was filed with the SEC

## Architecture

```
SEC EDGAR APIs ──▶ Airflow (daily) ──▶ S3-compatible object storage
                                        ├── bronze/  immutable JSON snapshots      ✅
                                        ├── silver/  one row per reported fact    ✅
                                        └── gold/    bitemporal financial model   (next)

bronze run finishes ──(Airflow asset)──▶ silver build ──▶ quality gate
```

## Bronze Layer: How It Works

DAG `sec_bronze_ingestion`, daily:

1. Snapshots the SEC ticker → CIK map and resolves the [watchlist](config/watchlist.txt) (20 companies across 8 sectors).
2. Fetches each company's XBRL company facts **in parallel** using Airflow dynamic task mapping,
   capped at 4 concurrent requests to stay well within the SEC's 10 requests/second policy.
3. Validates each response against a minimal contract (right company, required fields, non-empty facts).
4. Writes a new gzip snapshot **only if the content hash changed** ([ADR-003](docs/adr/003-immutable-bronze-snapshots.md)).
5. Writes a run summary (new vs unchanged per company) for auditing.

**Reliability choices**

- **Idempotent and append-only:** re-runs cost one small manifest read per unchanged company.
- **Polite, resilient HTTP:** required SEC User-Agent, retries with backoff on 429/5xx, honors `Retry-After`.
- **Fail loudly:** unknown tickers, wrong company returned, or empty data stop the run with a clear message.
- **Testable:** logic lives in `src/` with no Airflow imports; unit tests use an in-memory S3 fake.

## Silver Layer: How It Works

DAG `sec_silver_facts` runs automatically whenever bronze updates (Airflow asset scheduling):

1. Reads the company registry written by bronze (which CIKs belong to which company).
2. Flattens each company's nested JSON into one row per reported fact: concept, unit, period,
   value, and the filing it came from (accession number, form, filed date).
3. Writes one Parquet file per CIK, named after the bronze snapshot it came from. Unchanged
   companies are skipped; changed ones are rebuilt and the old file removed.
4. Runs a company-level **quality gate** and writes a report to `silver/sec/_quality/`.

**Design choices**

- **Restatements are kept, not overwritten:** the same period reported in two filings gives two
  rows, each tied to its filing. Gold decides "what was known when."
- **Exact money:** values are `decimal(38, 6)`, never floating point.
- **Corporate actions:** [entity lineage](config/entity_lineage.csv) links a company's old and new
  legal entities (CIKs) so history stays continuous. Every row carries `company_key` and `source_cik`.
- **Quality gate:** flags any company with far fewer reported concepts than its peers or less than
  five years of history, the signature of a silent entity change. The run fails with a message
  pointing to the lineage file.

## Key Design Decisions

- [ADR-001](docs/adr/001-object-storage.md): S3-compatible storage; switched from MinIO to SeaweedFS when MinIO's images became unavailable
- [ADR-002](docs/adr/002-airflow-standalone.md): single-container Airflow to fit an 8 GB laptop
- [ADR-003](docs/adr/003-immutable-bronze-snapshots.md): immutable, content-addressed bronze snapshots
- [ADR-004](docs/adr/004-entity-lineage.md): entity lineage for companies that change legal entity
- [ADR-005](docs/adr/005-silver-format.md): silver as typed Parquet with exact decimals, and why not Spark yet

## How to Run

Requires Docker Desktop (6 GB memory) and Python 3.12.

```bash
git clone git@github.com:Jahnavidoppalapudi/sec-financials-lakehouse.git
cd sec-financials-lakehouse
make env          # creates .env; set SEC_USER_AGENT to your name and email
make setup        # local dev tools
make up           # start Airflow, Postgres, object storage
make ingest       # run the SEC ingestion now; silver follows automatically
make bronze       # list landed snapshots
make facts        # row counts and history per company in silver
make quality      # latest data quality report
```

- Airflow UI: http://localhost:8080
- Storage file browser: http://localhost:8888/buckets/lakehouse/

```bash
make test    # unit tests
make down    # stop (data kept)
make reset   # stop and delete all data
```

## Project Structure

```
├── config/      # watchlist and entity lineage
├── dags/        # Airflow DAGs (orchestration only)
├── src/         # ingestion, silver and quality logic, orchestrator-independent
├── tests/       # unit tests with in-memory S3
├── docs/adr/    # architecture decision records
├── Dockerfile
└── docker-compose.yml
```

## What I'd Do in Production

- Use the SEC nightly bulk archive for the full universe of companies instead of per-company API calls.
- Real S3 with versioning and lifecycle rules; IAM roles instead of static keys; secrets in a secrets manager.
- Alert when a run has zero new snapshots for several business days (likely a silent failure upstream).
- Track freshness SLAs and lineage (OpenLineage) per dataset.

## Data Source

U.S. Securities and Exchange Commission, EDGAR APIs (public domain). Used in line with the
SEC's fair access policy: identified User-Agent and a request rate well under 10/second.

## Problems I Hit

### 1. The storage image disappeared
- **What happened:** The first `make up` failed before anything started. Docker could not pull
  the MinIO image and returned `401 UNAUTHORIZED` from quay.io.
- **Cause:** MinIO archived its open-source project, and the image I had pinned is no longer
  publicly available. A dependency I relied on disappeared overnight.
- **Fix:** I replaced MinIO with SeaweedFS, an actively maintained open-source server with an
  S3-compatible API. Only `docker-compose.yml` and the `.env` settings changed.
- **Lesson:** No pipeline code changed, because everything talks to storage through the standard
  S3 API and reads the endpoint from configuration. In production I would mirror critical images
  into a private registry (for example Amazon ECR) so an upstream change can't break deployments.
  Details in [ADR-001](docs/adr/001-object-storage.md).

### 2. ExxonMobil's history went missing
- **What happened:** After the first run, one snapshot was 14 KB compressed while every other
  company was 180–510 KB. The run summary showed XOM with 105 reported concepts, compared with
  477–936 for the other 19 companies.
- **Cause:** In 2026 ExxonMobil created a new holding company (ExxonMobil Holdings Corp) as part
  of moving its legal home to Texas. The new entity got a new SEC ID (CIK 0002115436), and the SEC
  ticker map now points XOM to it. Decades of history remain under the old CIK (0000034088).
- **Impact:** Nothing failed, which is what makes it dangerous. Multi-year trends, growth rates and
  peer comparisons for XOM would have been built on a few months of data, and any backtest would
  have silently dropped years of history.
- **Fix:** An entity lineage file that links successor and predecessor CIKs, so the pipeline fetches
  both and stitches them into one continuous history. Plus a data quality check that flags any
  company whose history is far shorter than its peers, so the next reorganization is caught
  automatically instead of silently. Implemented in [ADR-004](docs/adr/004-entity-lineage.md).
