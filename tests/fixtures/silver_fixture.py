"""Small, synthetic silver dataset with the edge cases the gold model must handle.

Companies are fictional; numbers are made up. Scenarios:
- ACME restates 2023 revenue (1,000 -> 960) a year later, then repeats 960 (no new version)
- ACME switched revenue tags: 2021 only has "SalesRevenueNet", later years use "Revenues"
- ACME has an 8-K row that must be ignored, and a balanced balance sheet
- OILCO reorganized: history under an old CIK, recent filings under a new CIK, and the new
  entity repeats a prior-year value already reported by the old one (must not be a restatement)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from src.silver import flatten_company_facts, silver_key


def obs(end, val, accn, filed, form="10-K", start=None):
    row = {"end": end, "val": val, "accn": accn, "filed": filed, "form": form}
    if start:
        row["start"] = start
    return row


def concept(observations, unit="USD", label=None):
    return {"label": label, "units": {unit: observations}}


ACME = {
    "cik": 1,
    "entityName": "Acme Corp",
    "facts": {
        "us-gaap": {
            "Revenues": concept(
                [
                    obs("2023-12-31", 1000, "acme-23", "2024-02-15", start="2023-01-01"),
                    obs("2023-12-31", 960, "acme-24", "2025-02-14", start="2023-01-01"),
                    obs("2023-12-31", 960, "acme-25", "2026-02-13", start="2023-01-01"),
                    obs("2024-12-31", 1100, "acme-24", "2025-02-14", start="2024-01-01"),
                    obs("2024-12-31", 1100, "acme-25", "2026-02-13", start="2024-01-01"),
                    obs("2024-03-31", 250, "acme-q1", "2024-05-01", "10-Q", "2024-01-01"),
                    obs("2024-12-31", 1100, "acme-8k", "2025-01-20", "8-K", "2024-01-01"),
                ]
            ),
            "SalesRevenueNet": concept(
                [obs("2021-12-31", 800, "acme-21", "2022-02-15", start="2021-01-01")]
            ),
            "Assets": concept([obs("2024-12-31", 5000, "acme-24", "2025-02-14")]),
            "Liabilities": concept([obs("2024-12-31", 3000, "acme-24", "2025-02-14")]),
            "StockholdersEquity": concept([obs("2024-12-31", 2000, "acme-24", "2025-02-14")]),
            "LiabilitiesAndStockholdersEquity": concept(
                [obs("2024-12-31", 5000, "acme-24", "2025-02-14")]
            ),
        }
    },
}

OILCO_OLD = {
    "cik": 10,
    "entityName": "OilCo Corp",
    "facts": {
        "us-gaap": {
            "Revenues": concept(
                [
                    obs("2023-12-31", 3000, "oil-23", "2024-02-20", start="2023-01-01"),
                    obs("2024-12-31", 3200, "oil-24", "2025-02-20", start="2024-01-01"),
                    obs("2025-06-30", 800, "oil-q2", "2025-08-01", "10-Q", "2025-04-01"),
                ]
            )
        }
    },
}

OILCO_NEW = {
    "cik": 11,
    "entityName": "OilCo Holdings Corp",
    "facts": {
        "us-gaap": {
            "Revenues": concept(
                [
                    obs("2026-06-30", 900, "hold-q2", "2026-08-01", "10-Q", "2026-04-01"),
                    # Prior-year comparative, same value the old entity reported
                    obs("2025-06-30", 800, "hold-q2", "2026-08-01", "10-Q", "2025-04-01"),
                ]
            )
        }
    },
}

DATASET = [("ACME", ACME), ("OILCO", OILCO_OLD), ("OILCO", OILCO_NEW)]


def build(target_dir: str | Path) -> Path:
    """Write the fixture as silver Parquet files, laid out exactly like the real lake."""
    target_dir = Path(target_dir)
    for company_key, payload in DATASET:
        sha = f"{payload['cik']:064d}"
        table = flatten_company_facts(payload, company_key, sha)["table"]
        path = target_dir / silver_key(company_key, str(payload["cik"]).zfill(10), sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
    return target_dir
