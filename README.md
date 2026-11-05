# Point-in-Time Financials Lakehouse

![CI](https://github.com/Jahnavidoppalapudi/sec-financials-lakehouse/actions/workflows/ci.yml/badge.svg)

> A lakehouse for public-company financial statements from SEC EDGAR that preserves
> **what was known when**. Companies restate earlier numbers; this pipeline keeps every
> version so analysts and backtests never accidentally use information from the future.

**Status:** 🚧 Bronze layer complete. Silver (normalized facts) and gold (bitemporal model) in progress.

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
                                        ├── silver/  one row per reported fact    (next)
                                        └── gold/    bitemporal financial model   (next)
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

## Key Design Decisions

- [ADR-001](docs/adr/001-object-storage.md): S3-compatible storage; switched from MinIO to SeaweedFS when MinIO's images became unavailable
- [ADR-002](docs/adr/002-airflow-standalone.md): single-container Airflow to fit an 8 GB laptop
- [ADR-003](docs/adr/003-immutable-bronze-snapshots.md): immutable, content-addressed bronze snapshots

## How to Run

Requires Docker Desktop (6 GB memory) and Python 3.12.

```bash
git clone git@github.com:Jahnavidoppalapudi/sec-financials-lakehouse.git
cd sec-financials-lakehouse
make env          # creates .env; set SEC_USER_AGENT to your name and email
make setup        # local dev tools
make up           # start Airflow, Postgres, object storage
make ingest       # run the SEC ingestion now
make bronze       # list landed snapshots
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
├── config/      # watchlist of companies
├── dags/        # Airflow DAGs (orchestration only)
├── src/         # ingestion logic, orchestrator-independent
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
