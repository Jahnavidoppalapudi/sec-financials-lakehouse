"""Daily SEC EDGAR ingestion into the bronze layer.

1. Snapshot the SEC ticker -> CIK map
2. Resolve the watchlist to CIKs
3. Fetch company facts for each company in parallel (dynamic task mapping),
   writing a new immutable snapshot only when the data changed
4. Validate the run and write a run summary
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime

from airflow.sdk import dag, task

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
WATCHLIST = os.environ.get("WATCHLIST_PATH", "/opt/airflow/config/watchlist.txt")


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
    default_args={"retries": 2},
    tags=["bronze", "sec", "ingestion"],
)
def sec_bronze_ingestion():
    @task
    def create_bucket() -> None:
        from src.edgar import ensure_bucket

        ensure_bucket(_s3(), BUCKET)

    @task
    def snapshot_ticker_map() -> list[dict]:
        from src.edgar import TICKER_MAP_URL, fetch, read_watchlist, resolve_ciks, store_snapshot

        raw = fetch(_session(), TICKER_MAP_URL)
        store_snapshot(_s3(), BUCKET, "company_tickers", "all", raw, TICKER_MAP_URL)
        companies = resolve_ciks(json.loads(raw), read_watchlist(WATCHLIST))
        print(f"Resolved {len(companies)} companies from the watchlist")
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
        return {**result.as_dict(), "ticker": company["ticker"], "concepts": concepts}

    @task
    def write_run_summary(results: list[dict]) -> None:
        results = [results[i] for i in range(len(results))]  # avoids reading past the end
        summary = {
            "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "companies": len(results),
            "new_snapshots": sum(r["status"] == "new" for r in results),
            "unchanged": sum(r["status"] == "unchanged" for r in results),
            "results": results,
        }
        key = f"bronze/sec/_runs/{summary['finished_at'][:10]}/summary.json"
        _s3().put_object(Bucket=BUCKET, Key=key, Body=json.dumps(summary, indent=2).encode())
        print(
            f"{summary['new_snapshots']} new, {summary['unchanged']} unchanged "
            f"of {summary['companies']} companies"
        )

    companies = snapshot_ticker_map()
    create_bucket() >> companies
    write_run_summary(fetch_company_facts.expand(company=companies))


sec_bronze_ingestion()
