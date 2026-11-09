"""Daily SEC EDGAR ingestion into the bronze layer.

1. Snapshot the SEC ticker -> CIK map
2. Resolve the watchlist to CIKs, adding predecessor CIKs from config/entity_lineage.csv
3. Fetch company facts for each company in parallel (dynamic task mapping),
   writing a new immutable snapshot only when the data changed
4. Validate the run and write a run summary
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime, timedelta

from airflow.sdk import Asset, dag, task

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/opt/airflow/config")
WATCHLIST = os.path.join(CONFIG_DIR, "watchlist.txt")
LINEAGE = os.path.join(CONFIG_DIR, "entity_lineage.csv")
COMPANY_REGISTRY_KEY = "bronze/sec/_reference/companies.json"

# Downstream DAGs (silver) are scheduled on this asset: they run when bronze updates
BRONZE_COMPANYFACTS = Asset("s3://lakehouse/bronze/sec/companyfacts")


def _s3():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def _session():
    from src.edgar import build_session

    return build_session(os.environ.get("SEC_USER_AGENT"))


@dag(
    dag_id="sec_bronze_ingestion",
    description="Land SEC EDGAR company facts as immutable bronze snapshots",
    schedule="30 6 * * *",  # daily, early morning UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        # Fail fast instead of hanging (e.g. when the laptop sleeps mid-run)
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["bronze", "sec", "ingestion"],
)
def sec_bronze_ingestion():
    @task
    def create_bucket() -> None:
        from src.edgar import ensure_bucket

        ensure_bucket(_s3(), BUCKET)

    @task
    def snapshot_ticker_map() -> list[dict]:
        from src.edgar import (
            TICKER_MAP_URL,
            add_predecessors,
            fetch,
            read_lineage,
            read_watchlist,
            resolve_ciks,
            store_snapshot,
        )

        raw = fetch(_session(), TICKER_MAP_URL)
        s3 = _s3()
        store_snapshot(s3, BUCKET, "company_tickers", "all", raw, TICKER_MAP_URL)
        companies = add_predecessors(
            resolve_ciks(json.loads(raw), read_watchlist(WATCHLIST)), read_lineage(LINEAGE)
        )
        # Registry of which CIKs belong to which company, used by the silver layer
        s3.put_object(
            Bucket=BUCKET,
            Key=COMPANY_REGISTRY_KEY,
            Body=json.dumps(companies, indent=2).encode(),
            ContentType="application/json",
        )
        predecessors = [c for c in companies if c["role"] == "predecessor"]
        print(f"Resolved {len(companies)} CIKs ({len(predecessors)} predecessor entities)")
        return companies

    # Max 4 companies at once keeps us well under the SEC limit of 10 requests/second
    @task(max_active_tis_per_dagrun=4)
    def fetch_company_facts(company: dict) -> dict:
        from src.edgar import COMPANY_FACTS_URL, fetch, store_snapshot, validate_company_facts

        url = COMPANY_FACTS_URL.format(cik=company["cik"])
        raw = fetch(_session(), url)
        time.sleep(0.2)  # be polite to the SEC
        concepts = validate_company_facts(json.loads(raw), company["cik"])
        result = store_snapshot(_s3(), BUCKET, "companyfacts", company["cik"], raw, url)
        print(f"{company['ticker']}: {result.status}, {concepts} concepts, {result.bytes:,} bytes")
        return {
            **result.as_dict(),
            "ticker": company["ticker"],
            "role": company["role"],
            "concepts": concepts,
        }

    @task(outlets=[BRONZE_COMPANYFACTS])
    def write_run_summary(results: list[dict]) -> None:
        results = [results[i] for i in range(len(results))]  # avoids reading past the end
        summary = {
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "ciks": len(results),
            "new_snapshots": sum(r["status"] == "new" for r in results),
            "unchanged": sum(r["status"] == "unchanged" for r in results),
            "results": results,
        }
        key = f"bronze/sec/_runs/{summary['finished_at'][:10]}/summary.json"
        _s3().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(summary, indent=2).encode())
        print(
            f"{summary['new_snapshots']} new, {summary['unchanged']} unchanged "
            f"of {summary['ciks']} CIKs"
        )

    companies = snapshot_ticker_map()
    create_bucket() >> companies
    write_run_summary(fetch_company_facts.expand(company=companies))


sec_bronze_ingestion()
