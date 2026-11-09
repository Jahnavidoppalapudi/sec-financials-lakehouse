import gzip
import io
import json
from datetime import date
from decimal import Decimal

import pyarrow.parquet as pq
from fakes import FakeS3

from src.silver import build_silver_for_cik, flatten_company_facts, silver_key

# Shaped like the real SEC response: taxonomy -> concept -> units -> observations
PAYLOAD = {
    "cik": 320193,
    "entityName": "Apple Inc.",
    "facts": {
        "us-gaap": {
            "Revenues": {
                "label": "Revenues",
                "units": {
                    "USD": [
                        # Original 10-K
                        {
                            "start": "2023-01-01",
                            "end": "2023-12-31",
                            "val": 1000,
                            "accn": "A-1",
                            "fy": 2023,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2024-02-01",
                            "frame": "CY2023",
                        },
                        # Same period restated in next year's 10-K
                        {
                            "start": "2023-01-01",
                            "end": "2023-12-31",
                            "val": 960,
                            "accn": "A-2",
                            "fy": 2024,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2025-02-01",
                        },
                        # Exact duplicate inside the same filing
                        {
                            "start": "2023-01-01",
                            "end": "2023-12-31",
                            "val": 960,
                            "accn": "A-2",
                            "fy": 2024,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2025-02-01",
                        },
                    ]
                },
            },
            "Assets": {
                "label": "Assets",
                # Balance sheet items are "instant" facts: no start date
                "units": {
                    "USD": [
                        {
                            "end": "2023-12-31",
                            "val": 5000.25,
                            "accn": "A-1",
                            "fy": 2023,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2024-02-01",
                        }
                    ]
                },
            },
        },
        "dei": {
            "EntityCommonStockSharesOutstanding": {
                "label": "Shares outstanding",
                "units": {
                    "shares": [
                        {
                            "end": "2024-01-15",
                            "val": 15000000000,
                            "accn": "A-1",
                            "fy": 2023,
                            "fp": "FY",
                            "form": "10-K",
                            "filed": "2024-02-01",
                        }
                    ]
                },
            }
        },
    },
}
SHA = "abc123def456" + "0" * 52


def test_flatten_keeps_every_filing_and_removes_exact_duplicates():
    result = flatten_company_facts(PAYLOAD, "AAPL", SHA)
    t, stats = result["table"], result["stats"]

    assert t.num_rows == 4
    assert stats["duplicates_removed"] == 1
    assert stats["concepts"] == 3
    revenue = [r for r in t.to_pylist() if r["concept"] == "Revenues"]
    # Both the original and the restated value survive, each tied to its filing
    assert {(r["accession_number"], r["value"]) for r in revenue} == {
        ("A-1", Decimal("1000")),
        ("A-2", Decimal("960")),
    }


def test_flatten_types_are_exact_and_instant_facts_have_no_start():
    rows = flatten_company_facts(PAYLOAD, "AAPL", SHA)["table"].to_pylist()
    assets = next(r for r in rows if r["concept"] == "Assets")
    assert assets["value"] == Decimal("5000.25")  # exact decimal, not 5000.2499999
    assert assets["period_start"] is None
    assert assets["period_end"] == date(2023, 12, 31)
    assert all(r["company_key"] == "AAPL" and r["source_cik"] == "0000320193" for r in rows)


def _bronze(s3, payload, sha, key):
    s3.put_object("lake", key, gzip.compress(json.dumps(payload).encode()))
    return {"key": key, "sha256": sha}


def test_build_is_idempotent_and_replaces_old_versions():
    s3 = FakeS3()
    company = {"company_key": "AAPL", "cik": "0000320193"}

    v1 = _bronze(s3, PAYLOAD, "a" * 64, "bronze/v1.json.gz")
    first = build_silver_for_cik(s3, "lake", company, v1)
    again = build_silver_for_cik(s3, "lake", company, v1)
    assert first["status"] == "rebuilt" and again["status"] == "unchanged"
    assert again["rows"] == first["rows"]  # stats recovered from file metadata

    v2 = _bronze(s3, {**PAYLOAD, "entityName": "Apple Inc. v2"}, "b" * 64, "bronze/v2.json.gz")
    build_silver_for_cik(s3, "lake", company, v2)
    silver_files = [k for k in s3.objects if k.startswith("silver/")]
    assert silver_files == [silver_key("AAPL", "0000320193", "b" * 64)]

    table = pq.read_table(io.BytesIO(s3.objects[silver_files[0]]))
    assert table.column("entity_name")[0].as_py() == "Apple Inc. v2"
