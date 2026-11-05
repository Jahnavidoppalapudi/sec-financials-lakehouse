import gzip
import io
import json
from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError

from src.edgar import (
    check_user_agent,
    manifest_key,
    pad_cik,
    read_watchlist,
    resolve_ciks,
    snapshot_key,
    store_snapshot,
    validate_company_facts,
)


class FakeS3:
    """In-memory stand-in for an S3 client: no network, no Docker."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.writes = 0

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body
        self.writes += 1

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}


TICKER_MAP = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
}
FACTS = {
    "cik": 320193,
    "entityName": "Apple Inc.",
    "facts": {
        "us-gaap": {"Revenues": {}, "Assets": {}},
        "dei": {"EntityCommonStockSharesOutstanding": {}},
    },
}
T1 = datetime(2026, 11, 5, 6, 30, tzinfo=UTC)
T2 = datetime(2026, 11, 6, 6, 30, tzinfo=UTC)


def test_read_watchlist_ignores_comments_blanks_and_duplicates(tmp_path):
    f = tmp_path / "w.txt"
    f.write_text("# header\naapl\n\nMSFT  # inline\nAAPL\n")
    assert read_watchlist(f) == ["AAPL", "MSFT"]


def test_empty_watchlist_fails(tmp_path):
    f = tmp_path / "w.txt"
    f.write_text("# nothing\n")
    with pytest.raises(ValueError):
        read_watchlist(f)


@pytest.mark.parametrize("bad", [None, "", "my-bot", "Name you@example.com"])
def test_user_agent_must_have_real_contact(bad):
    with pytest.raises(ValueError):
        check_user_agent(bad)


def test_resolve_ciks_pads_and_names():
    assert resolve_ciks(TICKER_MAP, ["MSFT"]) == [
        {"ticker": "MSFT", "cik": "0000789019", "name": "MICROSOFT CORP"}
    ]


def test_resolve_ciks_reports_unknown_tickers():
    with pytest.raises(KeyError, match="NOPE"):
        resolve_ciks(TICKER_MAP, ["AAPL", "NOPE"])


def test_validate_company_facts_counts_concepts():
    assert validate_company_facts(FACTS, pad_cik(320193)) == 3


def test_validate_company_facts_rejects_wrong_company():
    with pytest.raises(ValueError, match="Asked for CIK"):
        validate_company_facts(FACTS, pad_cik(789019))


def test_snapshot_key_is_partitioned_and_content_addressed():
    key = snapshot_key("companyfacts", "0000320193", T1, "abcdef1234567890")
    assert (
        key
        == "bronze/sec/companyfacts/fetch_date=2026-11-05/0000320193_063000_abcdef123456.json.gz"
    )


def test_unchanged_data_is_not_rewritten():
    s3, raw = FakeS3(), json.dumps(FACTS).encode()
    first = store_snapshot(s3, "lake", "companyfacts", "0000320193", raw, "u", T1)
    second = store_snapshot(s3, "lake", "companyfacts", "0000320193", raw, "u", T2)

    assert first.status == "new"
    assert second.status == "unchanged"
    assert second.key == first.key
    assert s3.writes == 2  # one snapshot + one manifest, nothing on the rerun


def test_changed_data_creates_new_snapshot_and_keeps_old_one():
    s3 = FakeS3()
    v1 = json.dumps(FACTS).encode()
    v2 = json.dumps({**FACTS, "entityName": "Apple Inc. (restated)"}).encode()
    first = store_snapshot(s3, "lake", "companyfacts", "0000320193", v1, "u", T1)
    second = store_snapshot(s3, "lake", "companyfacts", "0000320193", v2, "u", T2)

    assert second.status == "new" and second.key != first.key
    assert gzip.decompress(s3.objects[first.key]) == v1  # history preserved
    manifest = json.loads(s3.objects[manifest_key("companyfacts", "0000320193")])
    assert manifest["key"] == second.key
