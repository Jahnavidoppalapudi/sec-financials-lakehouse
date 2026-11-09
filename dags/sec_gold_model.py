"""Build the gold point-in-time model with dbt, after every silver update.

1. Mirror silver Parquet files to local disk
2. dbt build: models plus data tests (fails the run if a test errors)
3. Export the gold tables back to the lake as Parquet
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
WAREHOUSE = os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse")
SILVER_CACHE = os.path.join(WAREHOUSE, "silver_cache")

SILVER_FACTS = Asset("s3://lakehouse/silver/sec/facts")
GOLD_MODEL = Asset("s3://lakehouse/gold/sec")


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
    dag_id="sec_gold_model",
    description="Point-in-time financial model built with dbt and DuckDB",
    schedule=[SILVER_FACTS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "execution_timeout": timedelta(minutes=20)},
    tags=["gold", "sec", "dbt"],
)
def sec_gold_model():
    @task
    def sync_silver() -> str:
        from src.gold import sync_silver

        result = sync_silver(_s3(), BUCKET, SILVER_CACHE)
        print(f"Synced {result['files']} silver files ({result['removed']} stale removed)")
        return result["silver_dir"]

    @task
    def dbt_build(silver_dir: str) -> None:
        from src.gold import run_dbt

        output = run_dbt(
            os.environ["DBT_BIN"], os.environ["DBT_PROJECT_DIR"], silver_dir, WAREHOUSE
        )
        print(output)

    @task(outlets=[GOLD_MODEL])
    def export_gold() -> None:
        from src.gold import export_gold

        counts = export_gold(_s3(), BUCKET, os.environ["GOLD_DUCKDB_PATH"])
        for table, n in counts.items():
            print(f"{table}: {n:,} rows")

    dbt_build(sync_silver()) >> export_gold()


sec_gold_model()
