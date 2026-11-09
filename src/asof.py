"""What did we know about a company on a given date?

    python -m src.asof AAPL 2024-03-01

For each metric, shows the latest annual figure as it was known on that date, next to the
value for the same period as known today, and flags anything restated since.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal

AS_OF_SQL = """
with then_known as (
    select *
    from fct_metric_versions
    where company_key = $ticker
      and period_length = $period_length
      and known_from <= $as_of
      and (known_to is null or known_to > $as_of)
),
latest_period as (
    select *
    from then_known
    qualify row_number() over (partition by metric order by period_end desc) = 1
)
select
    p.metric,
    p.period_end,
    p.value as known_then,
    p.known_from as reported_on,
    n.value as known_today,
    p.value <> n.value as restated_since
from latest_period as p
join fct_metrics_latest as n
    using (company_key, metric, period_start, period_end)
order by p.metric
"""


def report(con, ticker: str, as_of: str, period_length: str = "annual") -> list[tuple]:
    params = {"ticker": ticker.upper(), "as_of": date.fromisoformat(as_of),
              "period_length": period_length}  # fmt: skip
    return con.execute(AS_OF_SQL, params).fetchall()


def fmt(value, metric: str) -> str:
    """Human-readable figure. Values are exact Decimals, so scale with Decimals too."""
    if value is None:
        return "-"
    value = Decimal(value)
    if metric == "eps_diluted":
        return f"${value:,.2f}/sh"
    prefix, suffix = ("", " sh") if metric == "shares_outstanding" else ("$", "")
    for size, label in ((Decimal(10) ** 9, "B"), (Decimal(10) ** 6, "M")):
        if abs(value) >= size:
            return f"{prefix}{value / size:,.1f}{label}{suffix}"
    return f"{prefix}{value:,.0f}{suffix}"


def change(then, today) -> str:
    """Exact difference between what was known then and today, e.g. '-4.000%'."""
    if then is None or today is None or then == today:
        return "-"
    if then == 0:
        return "new"
    return f"{(Decimal(today) - Decimal(then)) / abs(Decimal(then)) * 100:+.3f}%"


def main(argv: list[str]) -> None:
    import duckdb

    if len(argv) < 2:
        sys.exit("usage: python -m src.asof TICKER YYYY-MM-DD")
    ticker, as_of = argv[0], argv[1]
    db = os.environ.get("GOLD_DUCKDB_PATH", "/opt/airflow/warehouse/gold.duckdb")
    con = duckdb.connect(db, read_only=True)
    rows = report(con, ticker, as_of)
    if not rows:
        sys.exit(f"Nothing known about {ticker.upper()} on {as_of}")

    print(f"\n{ticker.upper()}: latest annual figures as known on {as_of}\n")
    header = (
        f"{'metric':22} {'period end':11} {'known then':>12} {'reported':11} "
        f"{'known today':>12} {'change':>12}"
    )
    print(header)
    print("-" * len(header))
    for metric, period_end, then, reported, today, _restated in rows:
        print(
            f"{metric:22} {period_end!s:11} {fmt(then, metric):>12} {reported!s:11} "
            f"{fmt(today, metric):>12} {change(then, today):>12}"
        )
    restated = sum(1 for r in rows if r[5])
    print(f"\n{restated} of {len(rows)} figures have been restated since {as_of}.")


if __name__ == "__main__":
    main(sys.argv[1:])
