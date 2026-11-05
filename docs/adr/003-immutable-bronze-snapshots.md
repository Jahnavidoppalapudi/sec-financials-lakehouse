# ADR-003: Immutable, content-addressed bronze snapshots

**Status:** Accepted
**Date:** 2026-11-05

## Context
The SEC "company facts" API returns each company's full reporting history, and that
history changes: new filings arrive and earlier numbers get restated. The gold layer
must answer *"what did we know on date X?"*, so bronze cannot simply overwrite the
latest file.

## Options Considered
1. **Overwrite one file per company:** cheapest, but destroys history; point-in-time queries become impossible.
2. **Save a full copy every day:** keeps history, but stores identical files ~250 days a year.
3. **Save a new snapshot only when the content hash changes:** keeps full history, stores only real changes.

## Decision
Option 3. Each fetch is hashed (SHA-256). A small manifest per company points to its
latest snapshot; if the hash matches, nothing is written. New snapshots are gzip-compressed
and stored under `fetch_date=YYYY-MM-DD/` with the hash in the file name.

## Consequences
- Re-runs are idempotent and cheap: unchanged companies cost one small manifest read.
- Bronze is append-only, so any silver or gold table can be rebuilt from scratch.
- The manifest is a single point of truth for "latest"; if it is lost, it can be rebuilt by
  listing snapshots. At larger scale a table format (Delta/Iceberg) would replace manifests.
