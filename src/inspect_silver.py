"""Summarize the silver facts table per company: python -m src.inspect_silver"""

import io
import os

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.config import Config

from src.silver import SILVER_PREFIX, _list_keys


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = os.environ.get("LAKE_BUCKET", "lakehouse")
    print(
        f"{'company':8} {'cik':11} {'rows':>9} {'concepts':>9}  {'first period':12} {'last period'}"
    )
    total = 0
    for key in sorted(_list_keys(s3, bucket, SILVER_PREFIX + "/")):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        t = pq.read_table(
            io.BytesIO(body), columns=["company_key", "source_cik", "concept", "period_end"]
        )
        company, cik = t["company_key"][0].as_py(), t["source_cik"][0].as_py()
        span = pc.min_max(t["period_end"])  # PyArrow's own min/max, skips nulls
        first, last = span["min"].as_py(), span["max"].as_py()
        concepts = pc.count_distinct(t["concept"]).as_py()
        print(f"{company:8} {cik:11} {t.num_rows:>9,} {concepts:>9}  {first!s:12} {last}")
        total += t.num_rows
    print(f"\nTotal: {total:,} fact rows")


if __name__ == "__main__":
    main()
