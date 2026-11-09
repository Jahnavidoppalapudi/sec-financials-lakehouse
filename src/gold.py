"""Gold layer helpers: sync silver to local disk for dbt, export gold tables to the lake.

dbt + DuckDB build the gold model from a local mirror of silver. The lake stays the
source of truth: the finished gold tables are exported back to it as Parquet.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from src.silver import SILVER_PREFIX, _list_keys

GOLD_PREFIX = "gold/sec"
GOLD_TABLES = ("dim_company", "fct_metric_versions", "fct_metrics_latest", "rpt_restatements")


def sync_silver(s3, bucket: str, local_dir: str | Path) -> dict:
    """Mirror silver Parquet files to `local_dir`, removing local files no longer in the lake."""
    local_dir = Path(local_dir)
    keys = [k for k in _list_keys(s3, bucket, SILVER_PREFIX + "/") if k.endswith(".parquet")]
    if not keys:
        raise FileNotFoundError(f"No silver files under s3://{bucket}/{SILVER_PREFIX}/")
    wanted = set()
    for key in keys:
        path = local_dir / key
        wanted.add(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    removed = 0
    for path in local_dir.rglob("*.parquet"):
        if path not in wanted:
            path.unlink()
            removed += 1
    return {"files": len(keys), "removed": removed, "silver_dir": str(local_dir / SILVER_PREFIX)}


def run_dbt(dbt_bin: str, project_dir: str, silver_dir: str, work_dir: str) -> str:
    """Run `dbt build` (models + data tests). Raises with dbt's output if anything fails."""
    env = {
        **os.environ,
        "SILVER_DIR": silver_dir,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    cmd = [
        dbt_bin, "build",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
        "--target-path", os.path.join(work_dir, "dbt_target"),
        "--log-path", os.path.join(work_dir, "dbt_logs"),
    ]  # fmt: skip
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed:\n{result.stdout[-6000:]}{result.stderr[-2000:]}")
    return result.stdout


def export_gold(s3, bucket: str, db_path: str, tables=GOLD_TABLES) -> dict[str, int]:
    """Copy each gold table out of DuckDB into the lake as a Parquet file."""
    import duckdb

    counts = {}
    con = duckdb.connect(db_path, read_only=True)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            for table in tables:
                path = os.path.join(tmp, f"{table}.parquet")
                con.execute(f"copy {table} to '{path}' (format parquet, compression zstd)")
                counts[table] = con.execute(f"select count(*) from {table}").fetchone()[0]
                with open(path, "rb") as f:
                    s3.put_object(
                        Bucket=bucket, Key=f"{GOLD_PREFIX}/{table}/{table}.parquet", Body=f.read()
                    )
    finally:
        con.close()
    return counts
