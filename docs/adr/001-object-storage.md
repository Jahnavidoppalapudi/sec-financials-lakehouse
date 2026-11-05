# ADR-001: S3-compatible object storage, SeaweedFS for local development

**Status:** Accepted (revised)
**Date:** 2026-11-05

## Context
The lakehouse needs object storage that behaves like Amazon S3, runs on a laptop for free,
and fits in an 8 GB machine alongside Airflow.

The first version used MinIO, pinned to a 2025 release. On first start-up the image pull failed
with `401 UNAUTHORIZED`: MinIO's open-source project was archived and its published images are
no longer publicly available. A pinned dependency had disappeared from under the project.

## Options Considered
1. **Local filesystem folders:** simplest, but hides real S3 behavior (keys, metadata, listing).
2. **SeaweedFS:** actively maintained, open source, single container, S3 API built in.
3. **Garage / RustFS:** maintained, but Garage needs extra cluster set-up steps and RustFS is still alpha.

## Decision
Use SeaweedFS for local development. All code talks to storage only through the standard
S3 API (boto3, path-style addressing) and reads the endpoint from `S3_ENDPOINT_URL`.

## Consequences
- Swapping storage (SeaweedFS, AWS S3, any S3-compatible store) is a configuration change, not a code change.
  The MinIO → SeaweedFS switch itself required zero pipeline code changes, which validated this design.
- Lesson: third-party images can vanish. Production would mirror critical images into a private
  registry (e.g. Amazon ECR) rather than depend on public availability.
