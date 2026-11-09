# ADR-004: Entity lineage for companies that change legal entity

**Status:** Accepted
**Date:** 2026-11-06

## Context
The SEC ticker map links each ticker to the company's *current* legal entity (CIK).
In 2026 ExxonMobil created a new holding company with a new CIK. XOM now resolves to
an entity with a few months of filings, while decades of history remain under the old
CIK. Nothing failed: the pipeline silently loaded about a sixth of the expected data.

## Options Considered
1. **Hardcode the old CIK for XOM:** fixes one case, hides the problem, breaks when the mapping changes again.
2. **Detect successors automatically from SEC filings (Form 8-K12B):** most complete, but parsing filings is a project of its own.
3. **Explicit lineage file plus a quality gate:** a small reviewed config links predecessor and successor CIKs; a data quality check catches new cases.

## Decision
Option 3. `config/entity_lineage.csv` maps successor → predecessor CIKs per ticker.
Bronze fetches both; silver tags every row with `company_key` (the ticker) and `source_cik`,
so gold can build one continuous history. If the SEC ticker map no longer matches a lineage
entry, ingestion stops and asks for review. A quality gate flags companies whose history is
far shorter than their peers.

## Consequences
- Corporate actions become visible, reviewed changes in version control instead of silent data loss.
- The quality gate turns the next reorganization into a failed run with a clear message.
- Requires a human to add new links; option 2 is the natural next step at larger scale.
