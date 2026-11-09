"""Build the silver facts table from the latest bronze snapshots.

Runs automatically after every bronze ingestion (Airflow asset scheduling):
1. Load the company registry (which CIKs belong to which company)
2. Rebuild silver Parquet per CIK, only where the bronze snapshot changed
3. Run company-level quality checks and write a report; fail on problems
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Asset, dag, task

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
COMPANY_REGISTRY_KEY = "bronze/sec/_reference/companies.json"

BRONZE_COMPANYFACTS = Asset("s3://lakehouse/bronze/sec/companyfacts")
SILVER_FACTS = Asset("s3://lakehouse/silver/sec/facts")


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


@dag(
    dag_id="sec_silver_facts",
    description="Flatten bronze company facts into one row per reported fact",
    schedule=[BRONZE_COMPANYFACTS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        # Fail fast instead of hanging (e.g. when the laptop sleeps mid-run)
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["silver", "sec"],
)
def sec_silver_facts():
    @task
    def load_companies() -> list[dict]:
        s3 = _s3()
        companies = json.loads(
            s3.get_object(Bucket=BUCKET, Key=COMPANY_REGISTRY_KEY)["Body"].read()
        )
        print(f"{len(companies)} CIKs to process")
        return companies

    # Each CIK holds up to ~1M values in memory while flattening; 2 at a time fits 8 GB
    @task(max_active_tis_per_dagrun=2)
    def build_cik(company: dict) -> dict:
        from src.edgar import manifest_key
        from src.silver import build_silver_for_cik

        s3 = _s3()
        manifest = json.loads(
            s3.get_object(Bucket=BUCKET, Key=manifest_key("companyfacts", company["cik"]))[
                "Body"
            ].read()
        )
        stats = build_silver_for_cik(s3, BUCKET, company, manifest)
        print(
            f"{company['company_key']} / {company['cik']}: {stats['status']}, "
            f"{stats.get('rows', 0):,} rows, {stats.get('duplicates_removed', 0)} dupes removed"
        )
        return stats

    @task(outlets=[SILVER_FACTS])
    def quality_gate(results) -> None:
        from src.quality import check_history, combine_by_company, failure_message

        stats = [results[i] for i in range(len(results))]
        companies = combine_by_company(stats)
        issues = check_history(companies, today=date.today())
        report = {
            "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "companies": companies,
            "issues": issues,
            "passed": not issues,
        }
        _s3().put_object(
            Bucket=BUCKET,
            Key=f"silver/sec/_quality/{report['checked_at'][:10]}/report.json",
            Body=json.dumps(report, indent=2).encode(),
        )
        total = sum(c["rows"] for c in companies.values())
        print(f"{len(companies)} companies, {total:,} fact rows, {len(issues)} issues")
        if issues:
            raise ValueError(failure_message(issues))

    quality_gate(build_cik.expand(company=load_companies()))


sec_silver_facts()
