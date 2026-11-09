"""Summarize the gold model: python -m src.inspect_gold"""

import os

import duckdb

from src.asof import fmt


def main() -> None:
    db = os.environ.get("GOLD_DUCKDB_PATH", "/opt/airflow/warehouse/gold.duckdb")
    con = duckdb.connect(db, read_only=True)

    print("\nCompanies\n")
    print(f"{'company':8} {'current entity':40} {'CIKs':26} {'history from'}")
    for key, name, ciks, start in con.execute(
        "select company_key, current_entity_name, all_ciks, history_starts "
        "from dim_company order by company_key"
    ).fetchall():
        print(f"{key:8} {name[:40]:40} {ciks:26} {start}")

    versions, facts, restated = con.execute(
        "select count(*), count(distinct (company_key, metric, period_start, period_end)), "
        "count(*) filter (where is_restatement) from fct_metric_versions"
    ).fetchone()
    print(f"\n{facts:,} facts, {versions:,} versions, {restated:,} restatements\n")

    print("Largest restatements (by % change from first report)\n")
    print(f"{'company':8} {'metric':22} {'period end':11} {'first':>10} {'restated':>10} "
          f"{'change':>9} {'restated on'}")  # fmt: skip
    for key, metric, end, first, new, pct, on in con.execute(
        "select company_key, metric, period_end, first_reported_value, restated_value, "
        "change_vs_first_pct, restated_on from rpt_restatements "
        "order by abs(change_vs_first_pct) desc nulls last limit 15"
    ).fetchall():
        pct_text = f"{pct:+.2f}%" if pct is not None else "-"
        print(f"{key:8} {metric:22} {end!s:11} {fmt(first, metric):>10} {fmt(new, metric):>10} "
              f"{pct_text:>9} {on}")  # fmt: skip


if __name__ == "__main__":
    main()
