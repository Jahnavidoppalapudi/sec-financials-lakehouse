# ADR-005: Silver as Parquet per company, exact decimals, no Spark

**Status:** Accepted
**Date:** 2026-11-06

## Context
Silver turns nested SEC JSON into one row per reported fact (about 50,000–100,000 rows per
company, roughly 1–2 million in total). It must be rebuildable from bronze, idempotent,
and precise enough for financial figures.

## Options Considered
1. **PySpark + Delta Lake:** industry standard at scale, but needs a JVM and several GB of
   memory on an 8 GB laptop for a dataset that fits in a few hundred MB.
2. **Pandas:** familiar, but its default float type would round financial values.
3. **PyArrow with an explicit schema, one Parquet file per CIK:** small, fast, typed, no JVM.

## Decision
Option 3. Each silver file is built from exactly one bronze snapshot and named after its hash,
so it is skipped when unchanged and replaced when bronze changes (new file written first, old
file deleted after). Values use `decimal(38, 6)`, never floats. Every row keeps its filing's
accession number and filed date; restated values are kept, not overwritten.

## Consequences
- Fast and cheap at this scale; the full table rebuilds in minutes.
- Per-company files avoid concurrent writers, so a table format is not needed yet.
- At the full SEC universe (~15,000 companies, ~1 billion facts) or with concurrent writers,
  I would move to Spark with Delta Lake or Iceberg for distributed processing, ACID commits
  and time travel. The schema and partitioning are designed so that move is mechanical.
