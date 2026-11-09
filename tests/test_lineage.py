import pytest

from src.edgar import add_predecessors, read_lineage

COMPANIES = [
    {"ticker": "AAPL", "cik": "0000320193", "name": "Apple Inc."},
    {"ticker": "XOM", "cik": "0002115436", "name": "ExxonMobil Holdings Corp"},
]
LINEAGE = [{"ticker": "XOM", "predecessor_cik": "0000034088", "successor_cik": "0002115436"}]


def test_read_lineage_pads_ciks(tmp_path):
    f = tmp_path / "lineage.csv"
    f.write_text(
        "ticker,predecessor_cik,successor_cik,reason,added_on\nxom,34088,2115436,reorg,2026-11-05\n"
    )
    assert read_lineage(f) == LINEAGE


def test_missing_lineage_file_means_no_links(tmp_path):
    assert read_lineage(tmp_path / "nope.csv") == []


def test_predecessor_is_added_under_same_company_key():
    result = add_predecessors(COMPANIES, LINEAGE)
    xom = [c for c in result if c["company_key"] == "XOM"]
    assert {c["cik"]: c["role"] for c in xom} == {
        "0002115436": "current",
        "0000034088": "predecessor",
    }
    assert len(result) == 3


def test_lineage_for_company_not_on_watchlist_is_ignored():
    assert len(add_predecessors(COMPANIES[:1], LINEAGE)) == 1


def test_stale_lineage_fails_loudly():
    moved_again = [{**COMPANIES[1], "cik": "0009999999"}]
    with pytest.raises(ValueError, match="Review config/entity_lineage.csv"):
        add_predecessors(moved_again, LINEAGE)
