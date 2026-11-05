# ADR-002: Single-container Airflow (standalone mode) with LocalExecutor

**Status:** Accepted
**Date:** 2026-11-05

## Context
The official Airflow Docker Compose runs separate containers for the API server,
scheduler, DAG processor, triggerer, workers, Redis and Postgres. That needs roughly
4 GB of memory before any data processing starts, and the target laptop has 8 GB total.

## Options Considered
1. **Official multi-container setup with CeleryExecutor:** closest to a scaled production layout; too heavy here.
2. **Standalone mode with SQLite:** lightest, but SQLite cannot run tasks in parallel.
3. **Standalone mode with Postgres and LocalExecutor:** one Airflow container, real database, parallel tasks.

## Decision
Option 3. Airflow runs all its components in one container; metadata lives in Postgres.

## Consequences
- Fits comfortably in memory and starts with one command.
- Production would split components and use Celery or Kubernetes executors for scale and isolation.
  Because DAG code is executor-agnostic, that move requires no DAG changes.
