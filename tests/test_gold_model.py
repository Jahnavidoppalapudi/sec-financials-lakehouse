"""Builds the dbt gold model on the synthetic fixture and checks the results.

Skipped automatically if dbt is not installed in this environment.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
pytestmark = pytest.mark.skipif(not Path(DBT).exists(), reason="dbt not installed")

PROJECT = Path(__file__).resolve().parents[1] / "dbt"


@pytest.fixture(scope="module")
def gold_db(tmp_path_factory):
    """Run dbt build on the fixture once; returns the path of the gold DuckDB file."""
    from fixtures.silver_fixture import build

    work = tmp_path_factory.mktemp("gold")
    silver_dir = build(work / "silver")
    db = work / "gold.duckdb"
    env = {
        **os.environ,
        "SILVER_DIR": str(silver_dir),
        "GOLD_DUCKDB_PATH": str(db),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    cmd = [
        DBT,
        "build",
        "--project-dir",
        str(PROJECT),
        "--profiles-dir",
        str(PROJECT),
        "--target-path",
        str(work / "target"),
        "--log-path",
        str(work / "logs"),
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-4000:]
    return str(db)


@pytest.fixture
def gold(gold_db):
    con = duckdb.connect(gold_db, read_only=True)
    yield con
    con.close()


def rows(con, sql):
    return con.execute(sql).fetchall()


def test_restatement_creates_exactly_one_new_version(gold):
    versions = rows(
        gold,
        """
        select value, known_from::varchar, known_to::varchar, is_current
        from fct_metric_versions
        where company_key = 'ACME' and metric = 'revenue' and period_end = '2023-12-31'
        order by version_number""",
    )
    # 1,000 then 960; the repeated 960 in 2026 does not create a third version
    assert versions == [
        (1000, "2024-02-15", "2025-02-14", False),
        (960, "2025-02-14", None, True),
    ]


def test_as_of_query_returns_what_was_known_then(gold):
    def revenue_2023_as_of(day):
        return rows(
            gold,
            f"""
            select value from fct_metric_versions
            where company_key = 'ACME' and metric = 'revenue' and period_end = '2023-12-31'
              and known_from <= date '{day}'
              and (known_to is null or known_to > date '{day}')""",
        )

    assert revenue_2023_as_of("2024-01-31") == []  # not yet reported
    assert revenue_2023_as_of("2024-06-30") == [(1000,)]
    assert revenue_2023_as_of("2025-02-14") == [(960,)]  # restated that day


def test_restatement_report(gold):
    assert rows(
        gold,
        """
        select company_key, first_reported_value, restated_value, change_vs_first_pct
        from rpt_restatements""",
    ) == [("ACME", 1000, 960, -4.0)]


def test_concept_switch_and_8k_are_handled(gold):
    annual = rows(
        gold,
        """
        select period_end::varchar, concept, value from fct_metrics_latest
        where company_key = 'ACME' and metric = 'revenue' and period_length = 'annual'
        order by period_end""",
    )
    assert annual == [
        ("2021-12-31", "SalesRevenueNet", 800),
        ("2023-12-31", "Revenues", 960),
        ("2024-12-31", "Revenues", 1100),
    ]
    assert rows(gold, "select count(*) from fct_metric_versions where form = '8-K'") == [(0,)]


def test_reorganized_company_has_one_continuous_history(gold):
    assert rows(
        gold,
        """
        select current_entity_name, current_cik, all_ciks, history_starts::varchar
        from dim_company where company_key = 'OILCO'""",
    ) == [("OilCo Holdings Corp", "0000000011", "0000000010, 0000000011", "2023-12-31")]
    # Q2 2025 was reported by the old entity and repeated by the new one: still one version
    assert rows(
        gold,
        """
        select count(*), any_value(source_cik) from fct_metric_versions
        where company_key = 'OILCO' and period_end = '2025-06-30'""",
    ) == [(1, "0000000010")]


def test_as_of_report_flags_restated_figures(gold):
    from src.asof import report

    before = report(gold, "acme", "2024-06-30")
    assert [(m, str(p), then, today, restated) for m, p, then, _, today, restated in before] == [
        ("revenue", "2023-12-31", 1000, 960, True)
    ]
    after = report(gold, "ACME", "2025-06-30")
    revenue = next(r for r in after if r[0] == "revenue")
    assert (str(revenue[1]), revenue[2], revenue[5]) == ("2024-12-31", 1100, False)


def test_sync_and_export_round_trip(gold_db, tmp_path):
    from fakes import FakeS3
    from fixtures.silver_fixture import build

    from src.gold import GOLD_TABLES, export_gold, sync_silver

    s3 = FakeS3()
    source = build(tmp_path / "src")
    for f in source.rglob("*.parquet"):
        s3.put_object("lake", str(f.relative_to(source)), f.read_bytes())

    local = tmp_path / "cache"
    (local / "silver/sec/facts/company_key=OLD").mkdir(parents=True)
    (local / "silver/sec/facts/company_key=OLD/stale.parquet").write_bytes(b"x")
    result = sync_silver(s3, "lake", local)
    assert result == {"files": 3, "removed": 1, "silver_dir": str(local / "silver/sec/facts")}

    counts = export_gold(s3, "lake", gold_db)
    assert set(counts) == set(GOLD_TABLES)
    assert counts["rpt_restatements"] == 1
    assert all(f"gold/sec/{t}/{t}.parquet" in s3.objects for t in GOLD_TABLES)


def test_figures_format_exact_decimals():
    from decimal import Decimal

    from src.asof import change, fmt

    assert fmt(Decimal("391035000000"), "revenue") == "$391.0B"
    assert fmt(Decimal("-2500000"), "net_income") == "$-2.5M"
    assert fmt(Decimal("6.08"), "eps_diluted") == "$6.08/sh"
    assert fmt(Decimal("15115823000"), "shares_outstanding") == "15.1B sh"
    assert fmt(None, "revenue") == "-"
    assert change(Decimal("1000"), Decimal("960")) == "-4.000%"
    assert change(Decimal("1000000007"), Decimal("1000000000")) == "-0.000%"
    assert change(Decimal("5"), Decimal("5")) == "-"
