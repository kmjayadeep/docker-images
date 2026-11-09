import os
import shutil
from datetime import date
from pathlib import Path

import pytest

from finance_mcp.ledger import Ledger

FIXTURE = Path(__file__).parent / "ledger"


@pytest.fixture()
def ledger():
    return Ledger(str(FIXTURE / "journal.beancount"))


def test_fixture_loads_cleanly(ledger):
    assert ledger.snapshot().errors == []


def test_net_worth_values_every_holding_and_skips_projected_entries(ledger):
    # Bank 8888 + India 9000 INR (90) + ACME 10 x 150 USD x 0.80 (1200) - card 140
    result = ledger.net_worth("2026-03-31")
    assert result["net_worth"] == 10038
    assert result["groups"] == {"Assets:Cash": 8978, "Assets:Investments": 1200, "Liabilities:CC": -140}


def test_net_worth_reports_unpriced_holdings(ledger):
    assert ledger.net_worth("2026-03-31")["unpriced_holdings"] == {"XYZ": 5}


def test_net_worth_as_of_ignores_later_entries(ledger):
    assert ledger.net_worth("2026-01-05")["net_worth"] == 5000


def test_net_worth_flags_prices_older_than_the_limit(ledger):
    stale = {row["commodity"]: row["price_date"] for row in ledger.net_worth("2026-03-31")["stale_prices"]}
    assert stale["INR"] == "2026-01-25"
    assert stale["ACME"] == "2026-01-10"  # its USD leg is older than its own price


def test_spend_summary_uses_price_paid_and_day_rate(ledger):
    # Food 100 + 12 + 200 + 1000 INR (10); Travel 50 EUR paid as 40 CHF
    result = ledger.spend_summary("2026-01-01", "2026-02-28")
    assert result["expenses"] == 362
    assert result["income"] == 10000
    assert result["net_saved"] == 9638
    assert result["savings_rate_pct"] == 96.4
    assert result["expenses_by_group"] == {"Expenses:Food": 322, "Expenses:Travel": 40}
    assert result["by_month"] == [
        {"month": "2026-01", "expenses": 152, "income": 5000},
        {"month": "2026-02", "expenses": 210, "income": 5000},
    ]


def test_spend_summary_excludes_projected_entries(ledger):
    assert ledger.spend_summary("2026-03-01", "2026-03-31")["expenses"] == 0


def test_spend_summary_rejects_reversed_range(ledger):
    with pytest.raises(ValueError):
        ledger.spend_summary("2026-02-01", "2026-01-01")


def test_tag_total(ledger):
    result = ledger.tag_total("#trip-x")
    assert result["total"] == 52
    assert result["transactions"] == 2
    assert (result["first_date"], result["last_date"]) == ("2026-01-20", "2026-01-21")


def test_unknown_tag_suggests_close_matches(ledger):
    result = ledger.tag_total("trip-y")
    assert result["found"] is False
    assert result["did_you_mean"] == ["trip-x"]


def test_cash_outlook(ledger):
    result = ledger.cash_outlook(10000, today=date(2026, 3, 1))
    assert result["cash_in_base_currency"] == 8888
    assert result["card_debt"] == -140
    assert result["available_after_cards"] == 8748
    assert result["cash_in_other_currencies"] == {"INR": 9000}
    assert result["other_currencies_value"] == 90
    assert result["average_monthly_saving"] == 4819
    assert result["average_over_months"] == 2
    assert result["months_to_target"] == 0.3


def test_cash_outlook_when_target_is_already_covered(ledger):
    result = ledger.cash_outlook(100, today=date(2026, 3, 1))
    assert result["reachable_now"] is True


def test_freshness_lists_each_source_oldest_first(ledger):
    result = ledger.ledger_freshness(today=date(2026, 3, 1))
    assert result["load_errors"] == 0
    assert [row["source"] for row in result["sources"]] == ["card", "bank"]
    bank = result["sources"][1]
    assert bank["last_transaction"] == "2026-02-15"
    assert bank["last_balance_check"] == "2026-02-20"


def test_reloads_when_a_new_file_appears_and_journal_is_touched(tmp_path):
    root = tmp_path / "ledger"
    shutil.copytree(FIXTURE, root)
    ledger = Ledger(str(root / "journal.beancount"))
    assert ledger.spend_summary("2026-04-01", "2026-04-30")["expenses"] == 0
    (root / "2026" / "new.beancount").write_text('2026-04-02 * "Shop" "new"\n  Expenses:Food  7 CHF\n  Assets:Cash:Bank\n')
    journal = root / "journal.beancount"
    os.utime(journal, ns=(journal.stat().st_atime_ns, journal.stat().st_mtime_ns + 1_000_000))
    assert ledger.spend_summary("2026-04-01", "2026-04-30")["expenses"] == 7
