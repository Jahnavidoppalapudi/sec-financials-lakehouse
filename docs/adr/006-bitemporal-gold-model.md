# ADR-006: Bitemporal gold model built with dbt and DuckDB

**Status:** Accepted
**Date:** 2026-11-09

## Context
Analysts need two kinds of answers from the same data:
- *What is the number?* The latest value of a metric, after any restatements.
- *What did we know then?* The value as it stood on a past date, for backtests and audits.
  Using today's restated figures in a historical analysis is look-ahead bias.

Silver keeps every reported value with its filing date, but it is too granular to query
directly: hundreds of XBRL concepts, companies tagging the same idea differently, and the
same value repeated in every later filing as a comparative figure.

## Options Considered
1. **Latest-value table only (overwrite on restatement):** simplest, but cannot answer "what did we know then".
2. **Daily snapshots of the latest table:** answers point-in-time questions, but stores
   every number thousands of times and only at daily resolution.
3. **Bitemporal versions:** one row per *change* to a number, with `known_from` and `known_to`.

## Decision
Option 3, modelled in dbt on DuckDB:
- `int_metric_observations` maps XBRL concepts to 10 standard metrics through a reviewed
  seed (`concept_map.csv`), choosing one concept per company, metric and period so all
  versions of a number are comparable.
- `fct_metric_versions` keeps a row only when a filing reports a *different* value.
  Repeated comparatives collapse into the existing version. This also stitches entity
  changes: ExxonMobil's new holding company repeating old figures does not create versions.
- `fct_metrics_latest`, `rpt_restatements` and `dim_company` are built on top.
- Point-in-time query: `known_from <= date and (known_to is null or known_to > date)`,
  packaged as the `metrics_as_of` macro and the `make asof` command.

dbt runs in its own virtual environment inside the Airflow image so its dependencies cannot
conflict with Airflow's. DuckDB reads a local mirror of silver; the gold tables are exported
back to the lake as Parquet, which remains the source of truth.

## Tests
`dbt build` runs 16 data tests with the models. The key ones: knowledge intervals for each
fact are contiguous with exactly one current version (error), the balance sheet balances
(warn), and no value is known before its period ends (warn). The model is also tested in CI
against a synthetic fixture covering a restatement, a concept switch, an 8-K row, and a
company reorganization.

## Consequences
- Point-in-time questions are one indexed filter, and storage grows only with real changes.
- Restatements become a first-class, reportable dataset.
- A single DuckDB file allows one writer at a time; queries during a gold build may wait.
  For many concurrent users I would serve gold from a warehouse (Snowflake, BigQuery,
  Redshift) or query the exported Parquet with an engine such as Trino or Athena.
- The concept map covers 10 metrics; extending it is a reviewed change to a CSV file.
