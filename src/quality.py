"""Company-level data quality checks run after silver is built.

Catches problems that do not raise errors on their own, like a company whose history
silently shrank because its ticker now points to a new legal entity.
"""

from __future__ import annotations

from datetime import date
from statistics import median


def combine_by_company(cik_stats: list[dict]) -> dict[str, dict]:
    """Merge per-CIK stats into one view per company (current + predecessor CIKs)."""
    companies: dict[str, dict] = {}
    for s in cik_stats:
        c = companies.setdefault(
            s["company_key"],
            {"ciks": [], "rows": 0, "concepts": 0, "earliest_period_end": None},
        )
        c["ciks"].append(s["source_cik"])
        c["rows"] += s.get("rows", 0)
        # Concepts overlap between entities, so use the larger count as a lower bound
        c["concepts"] = max(c["concepts"], s.get("concepts", 0))
        earliest = s.get("earliest_period_end")
        if earliest and (c["earliest_period_end"] is None or earliest < c["earliest_period_end"]):
            c["earliest_period_end"] = earliest
    return companies


def check_history(
    companies: dict[str, dict],
    today: date,
    min_concept_ratio: float = 0.3,
    min_history_years: int = 5,
) -> list[dict]:
    """Flag companies with far fewer concepts than peers, or too little history."""
    typical = median(c["concepts"] for c in companies.values())
    issues = []
    for key, c in sorted(companies.items()):
        problems = []
        if c["concepts"] < min_concept_ratio * typical:
            problems.append(
                f"only {c['concepts']} concepts vs median {typical:.0f} "
                f"(threshold {min_concept_ratio:.0%})"
            )
        earliest = c["earliest_period_end"]
        if earliest is None or int(earliest[:4]) > today.year - min_history_years:
            problems.append(
                f"history starts {earliest}, expected at least {min_history_years} years"
            )
        if problems:
            issues.append({"company_key": key, "ciks": c["ciks"], "problems": problems})
    return issues


def failure_message(issues: list[dict]) -> str:
    lines = ["Data quality gate failed:"]
    for i in issues:
        ciks = ", ".join(i["ciks"])
        lines.append(f"  {i['company_key']} (CIKs {ciks}): {'; '.join(i['problems'])}")
    lines.append(
        "If a company reorganized or changed legal entity, link its old and new CIKs "
        "in config/entity_lineage.csv."
    )
    return "\n".join(lines)
