"""List objects in the lake: python -m src.inspect_lake bronze/sec/"""

import os
import sys

import boto3
from botocore.config import Config


def main(prefix: str = "bronze/", limit: int = 40) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = os.environ.get("LAKE_BUCKET", "lakehouse")
    objects = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    objects.sort(key=lambda o: o["LastModified"], reverse=True)
    total = sum(o["Size"] for o in objects)
    print(f"{len(objects)} objects, {total / 1_048_576:,.1f} MB under s3://{bucket}/{prefix}\n")
    for o in objects[:limit]:
        print(f"{o['LastModified']:%Y-%m-%d %H:%M}  {o['Size']:>12,}  {o['Key']}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "bronze/")
