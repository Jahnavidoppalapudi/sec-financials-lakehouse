"""Silver layer: flatten SEC company facts into one row per reported fact.

Input:  the latest bronze snapshot per CIK (gzip JSON, nested by taxonomy/concept/unit).
Output: one Parquet file per CIK, rebuilt only when its bronze snapshot changes.

Every fact keeps the filing it came from (accession number and filed date). The same
number for the same period often appears in several filings, sometimes with a different
value after a restatement; silver keeps all of them. Picking "what was known when"
happens in the gold layer.
"""

from __future__ import annotations

import gzip
import io
import json
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

SILVER_PREFIX = "silver/sec/facts"

# Money is stored as an exact decimal, never as a float (no rounding drift in sums)
VALUE_TYPE = pa.decimal128(38, 6)

FACTS_SCHEMA = pa.schema(
    [
        ("company_key", pa.string()),
        ("source_cik", pa.string()),
        ("entity_name", pa.string()),
        ("taxonomy", pa.string()),
        ("concept", pa.string()),
        ("label", pa.string()),
        ("unit", pa.string()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
        ("value", VALUE_TYPE),
        ("accession_number", pa.string()),
        ("fiscal_year", pa.int32()),
        ("fiscal_period", pa.string()),
        ("form", pa.string()),
        ("filed_date", pa.date32()),
        ("frame", pa.string()),
        ("snapshot_sha256", pa.string()),
    ]
)


def _date(text: str | None) -> date | None:
    return date.fromisoformat(text) if text else None


def flatten_company_facts(payload: dict, company_key: str, snapshot_sha256: str) -> dict:
    """Turn the nested SEC JSON into columns. Returns {"table": pa.Table, "stats": {...}}."""
    cik = str(int(payload["cik"])).zfill(10)
    rows: dict[str, list] = {name: [] for name in FACTS_SCHEMA.names}
    seen: set[tuple] = set()
    duplicates = 0

    for taxonomy, concepts in payload.get("facts", {}).items():
        for concept, detail in concepts.items():
            for unit, observations in detail.get("units", {}).items():
                for obs in observations:
                    # Identical row repeated inside one filing: keep one copy
                    identity = (
                        taxonomy,
                        concept,
                        unit,
                        obs.get("start"),
                        obs["end"],
                        obs.get("accn"),
                        str(obs["val"]),
                    )
                    if identity in seen:
                        duplicates += 1
                        continue
                    seen.add(identity)

                    rows["company_key"].append(company_key)
                    rows["source_cik"].append(cik)
                    rows["entity_name"].append(payload.get("entityName"))
                    rows["taxonomy"].append(taxonomy)
                    rows["concept"].append(concept)
                    rows["label"].append(detail.get("label"))
                    rows["unit"].append(unit)
                    rows["period_start"].append(_date(obs.get("start")))
                    rows["period_end"].append(_date(obs["end"]))
                    rows["value"].append(Decimal(str(obs["val"])))
                    rows["accession_number"].append(obs.get("accn"))
                    rows["fiscal_year"].append(obs.get("fy"))
                    rows["fiscal_period"].append(obs.get("fp"))
                    rows["form"].append(obs.get("form"))
                    rows["filed_date"].append(_date(obs.get("filed")))
                    rows["frame"].append(obs.get("frame"))
                    rows["snapshot_sha256"].append(snapshot_sha256)

    table = pa.table(rows, schema=FACTS_SCHEMA)
    ends = [d for d in rows["period_end"] if d]
    stats = {
        "company_key": company_key,
        "source_cik": cik,
        "rows": table.num_rows,
        "duplicates_removed": duplicates,
        "concepts": len({(r[0], r[1]) for r in seen}),
        "earliest_period_end": min(ends).isoformat() if ends else None,
        "latest_period_end": max(ends).isoformat() if ends else None,
    }
    return {"table": table, "stats": stats}


def silver_prefix(company_key: str, cik: str) -> str:
    return f"{SILVER_PREFIX}/company_key={company_key}/cik={cik}/"


def silver_key(company_key: str, cik: str, snapshot_sha256: str) -> str:
    """Silver file is named after the bronze snapshot it was built from (full lineage)."""
    return f"{silver_prefix(company_key, cik)}facts_{snapshot_sha256[:12]}.parquet"


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        keys += [o["Key"] for o in page.get("Contents", [])]
        if not page.get("IsTruncated"):
            return keys
        token = page["NextContinuationToken"]


def build_silver_for_cik(s3, bucket: str, company: dict, manifest: dict) -> dict:
    """Rebuild the silver file for one CIK if its bronze snapshot changed. Idempotent."""
    company_key, cik, sha = company["company_key"], company["cik"], manifest["sha256"]
    target = silver_key(company_key, cik, sha)
    existing = _list_keys(s3, bucket, silver_prefix(company_key, cik))

    if target in existing:
        meta = s3.head_object(Bucket=bucket, Key=target).get("Metadata", {})
        stats = json.loads(meta["stats"]) if "stats" in meta else {}
        return {**stats, "company_key": company_key, "source_cik": cik, "status": "unchanged"}

    raw = gzip.decompress(s3.get_object(Bucket=bucket, Key=manifest["key"])["Body"].read())
    result = flatten_company_facts(json.loads(raw), company_key, sha)

    buffer = io.BytesIO()
    pq.write_table(result["table"], buffer, compression="zstd")
    s3.put_object(
        Bucket=bucket,
        Key=target,
        Body=buffer.getvalue(),
        Metadata={"source-snapshot": manifest["key"], "stats": json.dumps(result["stats"])},
    )
    # Remove files built from older snapshots only after the new one is safely written
    for old in existing:
        s3.delete_object(Bucket=bucket, Key=old)
    return {**result["stats"], "status": "rebuilt"}
