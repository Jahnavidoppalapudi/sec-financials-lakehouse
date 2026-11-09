from datetime import date

from src.quality import check_history, combine_by_company, failure_message

TODAY = date(2026, 11, 5)


def stats(company, cik, concepts, earliest):
    return {
        "company_key": company,
        "source_cik": cik,
        "rows": concepts * 50,
        "concepts": concepts,
        "earliest_period_end": earliest,
    }


PEERS = [stats(t, f"000000000{i}", 550, "2009-06-30") for i, t in enumerate(["A", "B", "C", "D"])]


def test_reorganized_company_without_lineage_is_flagged():
    xom_new_entity_only = stats("XOM", "0002115436", 105, "2024-12-31")
    issues = check_history(combine_by_company([*PEERS, xom_new_entity_only]), TODAY)

    assert [i["company_key"] for i in issues] == ["XOM"]
    assert len(issues[0]["problems"]) == 2  # too few concepts AND too little history
    assert "entity_lineage.csv" in failure_message(issues)


def test_company_passes_once_predecessor_is_stitched_in():
    xom = [
        stats("XOM", "0002115436", 105, "2024-12-31"),
        stats("XOM", "0000034088", 610, "2009-03-31"),
    ]
    combined = combine_by_company([*PEERS, *xom])

    assert combined["XOM"]["ciks"] == ["0002115436", "0000034088"]
    assert combined["XOM"]["earliest_period_end"] == "2009-03-31"
    assert check_history(combined, TODAY) == []
