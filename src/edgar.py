"""SEC EDGAR bronze ingestion.

Lands immutable, content-addressed snapshots of each company's XBRL "company facts".
A new snapshot is written only when the SEC data actually changed, so bronze keeps a
complete history of *what was known when*, which the bitemporal gold model relies on.

No Airflow imports here: the logic is unit-testable and orchestrator-independent.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from botocore.exceptions import ClientError

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"


# ---------------------------------------------------------------- config
def read_watchlist(path: str | Path) -> list[str]:
    """Tickers from a text file, one per line; blank lines and # comments ignored."""
    tickers: list[str] = []
    for line in Path(path).read_text().splitlines():
        ticker = line.split("#", 1)[0].strip().upper()
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    if not tickers:
        raise ValueError(f"Watchlist {path} is empty")
    return tickers


def check_user_agent(user_agent: str | None) -> str:
    """SEC blocks requests without a descriptive User-Agent that includes contact details."""
    if not user_agent or "@" not in user_agent or "example.com" in user_agent:
        raise ValueError(
            "Set SEC_USER_AGENT in .env to 'Your Name your-email@domain.com'. "
            "The SEC requires it for automated access."
        )
    return user_agent


# ---------------------------------------------------------------- SEC helpers
def pad_cik(cik: int | str) -> str:
    return str(int(cik)).zfill(10)


def resolve_ciks(ticker_map: dict, tickers: list[str]) -> list[dict]:
    """Map tickers to CIKs using the SEC ticker file. Fails loudly on unknown tickers."""
    by_ticker = {row["ticker"].upper(): row for row in ticker_map.values()}
    missing = [t for t in tickers if t not in by_ticker]
    if missing:
        raise KeyError(f"Tickers not found in SEC ticker map: {missing}")
    return [
        {"ticker": t, "cik": pad_cik(by_ticker[t]["cik_str"]), "name": by_ticker[t]["title"]}
        for t in tickers
    ]


def read_lineage(path: str | Path) -> list[dict]:
    """Successor -> predecessor CIK links for companies that changed legal entity."""
    path = Path(path)
    if not path.exists():
        return []
    rows = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    "ticker": row["ticker"].strip().upper(),
                    "predecessor_cik": pad_cik(row["predecessor_cik"]),
                    "successor_cik": pad_cik(row["successor_cik"]),
                }
            )
    return rows


def add_predecessors(companies: list[dict], lineage: list[dict]) -> list[dict]:
    """Tag every company with a stable `company_key` and add predecessor CIKs to fetch.

    The SEC ticker map only knows a ticker's *current* legal entity. When a company
    reorganizes, its history stays under the old CIK, so we fetch both and later
    stitch them together under one company_key (the ticker).
    """
    result = [{**c, "company_key": c["ticker"], "role": "current"} for c in companies]
    for link in lineage:
        current = next((c for c in result if c["ticker"] == link["ticker"]), None)
        if current is None:
            continue  # not on the watchlist
        if current["cik"] != link["successor_cik"]:
            raise ValueError(
                f"Lineage for {link['ticker']} expects current CIK {link['successor_cik']}, "
                f"but the SEC ticker map says {current['cik']}. Review config/entity_lineage.csv."
            )
        result.append(
            {
                "ticker": link["ticker"],
                "cik": link["predecessor_cik"],
                "name": f"{current['name']} (predecessor entity)",
                "company_key": link["ticker"],
                "role": "predecessor",
            }
        )
    return result


def validate_company_facts(payload: dict, expected_cik: str) -> int:
    """Basic contract check on the SEC response. Returns the number of reported concepts."""
    for field in ("cik", "entityName", "facts"):
        if field not in payload:
            raise ValueError(f"Company facts for CIK {expected_cik} missing field '{field}'")
    if pad_cik(payload["cik"]) != expected_cik:
        raise ValueError(f"Asked for CIK {expected_cik}, got {payload['cik']}")
    concepts = sum(len(v) for v in payload["facts"].values())
    if concepts == 0:
        raise ValueError(f"CIK {expected_cik} returned no facts")
    return concepts


# ---------------------------------------------------------------- storage layout
def snapshot_key(source: str, entity: str, fetched_at: datetime, digest: str) -> str:
    """Immutable key: partitioned by fetch date, named by content hash."""
    return (
        f"bronze/sec/{source}/fetch_date={fetched_at:%Y-%m-%d}/"
        f"{entity}_{fetched_at:%H%M%S}_{digest[:12]}.json.gz"
    )


def manifest_key(source: str, entity: str) -> str:
    """Small pointer file to the latest snapshot, used to detect changes cheaply."""
    return f"bronze/sec/_manifests/{source}/{entity}.json"


@dataclass
class SnapshotResult:
    entity: str
    status: str  # "new" or "unchanged"
    key: str
    sha256: str
    bytes: int

    def as_dict(self) -> dict:
        return self.__dict__.copy()


# ---------------------------------------------------------------- S3 helpers
def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def _read_json(s3, bucket: str, key: str) -> dict | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return json.loads(body)


def store_snapshot(
    s3,
    bucket: str,
    source: str,
    entity: str,
    raw: bytes,
    source_url: str,
    fetched_at: datetime | None = None,
) -> SnapshotResult:
    """Write `raw` as a new immutable snapshot only if its content changed since the last one."""
    fetched_at = fetched_at or datetime.now(UTC)
    digest = hashlib.sha256(raw).hexdigest()
    previous = _read_json(s3, bucket, manifest_key(source, entity))
    if previous and previous["sha256"] == digest:
        return SnapshotResult(entity, "unchanged", previous["key"], digest, len(raw))

    key = snapshot_key(source, entity, fetched_at, digest)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=gzip.compress(raw),
        ContentType="application/json",
        ContentEncoding="gzip",
        Metadata={"source-url": source_url, "sha256": digest},
    )
    manifest = {
        "entity": entity,
        "key": key,
        "sha256": digest,
        "bytes": len(raw),
        "source_url": source_url,
        "fetched_at": fetched_at.isoformat(timespec="seconds"),
    }
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key(source, entity),
        Body=json.dumps(manifest).encode(),
        ContentType="application/json",
    )
    return SnapshotResult(entity, "new", key, digest, len(raw))


# ---------------------------------------------------------------- HTTP
def build_session(user_agent: str):
    """HTTP session that retries politely on SEC throttling (429) and server errors."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.headers.update({"User-Agent": check_user_agent(user_agent), "Accept-Encoding": "gzip"})
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch(session, url: str) -> bytes:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.content
