"""Read-only questions over a Beancount ledger. Every figure is computed here, never by the model."""

from __future__ import annotations

import difflib
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from beancount import loader
from beancount.core import convert, data, inventory, prices

ZERO = Decimal(0)


def money(value: Decimal) -> float:
    return round(float(value), 2)


def parse_day(value: str | None, default: date | None = None) -> date | None:
    """An ISO date, or the default when the value is empty. Raises ValueError for anything else."""
    return date.fromisoformat(value) if value else default


def group_of(account: str, depth: int) -> str:
    return ":".join(account.split(":")[:max(depth, 1)])


def first_of_month(day: date, months_back: int = 0) -> date:
    index = day.year * 12 + day.month - 1 - months_back
    return date(index // 12, index % 12 + 1, 1)


@dataclass
class Snapshot:
    transactions: list[data.Transaction]
    balances: list[data.Balance]
    price_map: dict
    errors: list
    files: list[str]
    stamp: tuple


class Ledger:
    def __init__(
        self,
        path: str,
        base: str = "CHF",
        exclude_tag: str = "UPCOMING",
        cash_root: str = "Assets:Cash",
        card_root: str = "Liabilities:CC",
        stale_days: int = 30,
    ) -> None:
        self.path = path
        self.base = base
        self.exclude_tag = exclude_tag
        self.cash_root = cash_root
        self.card_root = card_root
        self.stale_days = stale_days
        self._snapshot: Snapshot | None = None

    # ---------- Loading ----------
    @staticmethod
    def _stamp(files: list[str]) -> tuple:
        return tuple(os.stat(f).st_mtime_ns if os.path.exists(f) else 0 for f in files)

    def snapshot(self) -> Snapshot:
        """The parsed ledger, reloaded only when the journal or an included file changes."""
        cached = self._snapshot
        if cached and self._stamp(cached.files) == cached.stamp:
            return cached
        entries, errors, options = loader.load_file(self.path)
        files = sorted(set(options.get("include") or []) | {os.path.abspath(self.path)})
        self._snapshot = Snapshot(
            transactions=[
                e for e in entries if isinstance(e, data.Transaction) and self.exclude_tag not in (e.tags or ())
            ],
            balances=[e for e in entries if isinstance(e, data.Balance)],
            price_map=prices.build_price_map(entries),
            errors=errors,
            files=files,
            stamp=self._stamp(files),
        )
        return self._snapshot

    # ---------- Valuation ----------
    def _balances(self, snap: Snapshot, on: date, roots: tuple[str, ...]) -> dict[str, inventory.Inventory]:
        held: dict[str, inventory.Inventory] = defaultdict(inventory.Inventory)
        for txn in snap.transactions:
            if txn.date > on:
                break
            for posting in txn.postings:
                if posting.account.startswith(roots):
                    held[posting.account].add_position(posting)
        return held

    def _value(self, snap: Snapshot, held: inventory.Inventory, on: date, unpriced: dict) -> Decimal:
        total = ZERO
        for position in held:
            converted = convert.convert_position(position, self.base, snap.price_map, on)
            if converted.currency == self.base:
                total += converted.number
            else:
                unpriced[converted.currency] += converted.number
        return total

    def _price_date(self, snap: Snapshot, position, on: date) -> date | None:
        """Date of the oldest price needed to value this position in the base currency."""
        currency = position.units.currency
        direct, _ = prices.get_price(snap.price_map, (currency, self.base), on)
        if direct:
            return direct
        via = position.cost.currency if position.cost else None
        if not via:
            return None
        first, _ = prices.get_price(snap.price_map, (currency, via), on)
        second, _ = prices.get_price(snap.price_map, (via, self.base), on)
        return min(first, second) if first and second else None

    def _spend_amount(self, snap: Snapshot, posting, on: date, unpriced: dict) -> Decimal:
        """What a posting cost in the base currency: its own price or cost when it has one, else that day's rate."""
        weight = convert.get_weight(posting)
        if weight.currency == self.base:
            return weight.number
        converted = convert.convert_amount(weight, self.base, snap.price_map, on)
        if converted.currency == self.base:
            return converted.number
        unpriced[converted.currency] += converted.number
        return ZERO

    # ---------- Questions ----------
    def net_worth(self, as_of: str | None = None, depth: int = 2) -> dict:
        snap = self.snapshot()
        on = parse_day(as_of, date.today())
        held = self._balances(snap, on, ("Assets", "Liabilities"))
        groups: dict[str, Decimal] = defaultdict(Decimal)
        unpriced: dict[str, Decimal] = defaultdict(Decimal)
        total = ZERO
        price_ages: dict[str, tuple[date, Decimal]] = {}
        for account, inv in held.items():
            for position in inv:
                single = inventory.Inventory([position])
                value = self._value(snap, single, on, unpriced)
                total += value
                groups[group_of(account, depth)] += value
                currency = position.units.currency
                if currency == self.base or currency in unpriced:
                    continue
                seen = self._price_date(snap, position, on)
                if seen:
                    old = price_ages.get(currency, (seen, ZERO))
                    price_ages[currency] = (min(old[0], seen), old[1] + abs(value))
        limit = on - timedelta(days=self.stale_days)
        stale = sorted(
            ((c, d, v) for c, (d, v) in price_ages.items() if d < limit), key=lambda row: row[2], reverse=True
        )
        gross = sum((abs(v) for _, v in price_ages.values()), ZERO)
        return {
            "as_of": on.isoformat(),
            "currency": self.base,
            "net_worth": money(total),
            "groups": {k: money(v) for k, v in sorted(groups.items(), key=lambda kv: kv[1], reverse=True) if v},
            "unpriced_holdings": {k: float(v) for k, v in unpriced.items() if v},
            "stale_prices": [
                {"commodity": c, "price_date": d.isoformat(), "value": money(v)} for c, d, v in stale[:10]
            ],
            "stale_value_pct_of_non_base": round(float(sum((v for _, _, v in stale), ZERO) / gross * 100), 1)
            if gross
            else 0,
        }

    def spend_summary(self, start: str | None = None, end: str | None = None, depth: int = 2, top: int = 15) -> dict:
        snap = self.snapshot()
        today = date.today()
        last = parse_day(end, today)
        first = parse_day(start, first_of_month(last))
        if first > last:
            raise ValueError("start is after end")
        expenses: dict[str, Decimal] = defaultdict(Decimal)
        income: dict[str, Decimal] = defaultdict(Decimal)
        months: dict[str, list[Decimal]] = defaultdict(lambda: [ZERO, ZERO])
        unpriced: dict[str, Decimal] = defaultdict(Decimal)
        for txn in snap.transactions:
            if not first <= txn.date <= last:
                continue
            month = txn.date.strftime("%Y-%m")
            for posting in txn.postings:
                if posting.account.startswith("Expenses"):
                    amount = self._spend_amount(snap, posting, txn.date, unpriced)
                    expenses[group_of(posting.account, depth)] += amount
                    months[month][0] += amount
                elif posting.account.startswith("Income"):
                    amount = -self._spend_amount(snap, posting, txn.date, unpriced)
                    income[group_of(posting.account, depth)] += amount
                    months[month][1] += amount
        spent = sum(expenses.values(), ZERO)
        earned = sum(income.values(), ZERO)
        ranked = sorted(expenses.items(), key=lambda kv: kv[1], reverse=True)
        return {
            "start": first.isoformat(),
            "end": last.isoformat(),
            "currency": self.base,
            "expenses": money(spent),
            "income": money(earned),
            "net_saved": money(earned - spent),
            "savings_rate_pct": round(float((earned - spent) / earned * 100), 1) if earned > 0 else None,
            "expenses_by_group": {k: money(v) for k, v in ranked[: max(top, 1)]},
            "other_groups_total": money(sum((v for _, v in ranked[max(top, 1) :]), ZERO)),
            "income_by_group": {k: money(v) for k, v in sorted(income.items(), key=lambda kv: kv[1], reverse=True)},
            "by_month": [
                {"month": m, "expenses": money(v[0]), "income": money(v[1])} for m, v in sorted(months.items())
            ],
            "unpriced": {k: float(v) for k, v in unpriced.items() if v},
        }

    def tag_total(self, tag: str, depth: int = 2) -> dict:
        snap = self.snapshot()
        wanted = tag.strip().lstrip("#")
        groups: dict[str, Decimal] = defaultdict(Decimal)
        unpriced: dict[str, Decimal] = defaultdict(Decimal)
        dates: list[date] = []
        known: set[str] = set()
        for txn in snap.transactions:
            known |= set(txn.tags or ())
            if wanted not in (txn.tags or ()):
                continue
            dates.append(txn.date)
            for posting in txn.postings:
                if posting.account.startswith("Expenses"):
                    groups[group_of(posting.account, depth)] += self._spend_amount(snap, posting, txn.date, unpriced)
        if not dates:
            return {"tag": wanted, "found": False, "did_you_mean": difflib.get_close_matches(wanted, known, 5, 0.5)}
        return {
            "tag": wanted,
            "found": True,
            "currency": self.base,
            "total": money(sum(groups.values(), ZERO)),
            "transactions": len(dates),
            "first_date": min(dates).isoformat(),
            "last_date": max(dates).isoformat(),
            "by_group": {k: money(v) for k, v in sorted(groups.items(), key=lambda kv: kv[1], reverse=True)},
            "unpriced": {k: float(v) for k, v in unpriced.items() if v},
        }

    def cash_outlook(self, target: float | None = None, months: int = 6, today: date | None = None) -> dict:
        """Cash and card debt now, recent monthly saving, and how long a target amount would take."""
        snap = self.snapshot()
        today = today or date.today()
        held = self._balances(snap, today, (self.cash_root, self.card_root))
        unpriced: dict[str, Decimal] = defaultdict(Decimal)
        base_cash = base_cards = other_value = ZERO
        other: dict[str, Decimal] = defaultdict(Decimal)
        for account, inv in held.items():
            for position in inv:
                currency = position.units.currency
                if currency == self.base:
                    if account.startswith(self.cash_root):
                        base_cash += position.units.number
                    else:
                        base_cards += position.units.number
                else:
                    other[currency] += position.units.number
                    other_value += self._value(snap, inventory.Inventory([position]), today, unpriced)

        window_end = first_of_month(today) - timedelta(days=1)
        first_txn = snap.transactions[0].date if snap.transactions else today
        window_start = max(first_of_month(window_end, max(months, 1) - 1), first_of_month(first_txn))
        span = (window_end.year - window_start.year) * 12 + window_end.month - window_start.month + 1
        summary = self.spend_summary(window_start.isoformat(), window_end.isoformat()) if span > 0 else None
        monthly = (summary["net_saved"] / span) if summary else None

        available = base_cash + base_cards
        result = {
            "as_of": today.isoformat(),
            "currency": self.base,
            "cash_in_base_currency": money(base_cash),
            "card_debt": money(base_cards),
            "available_after_cards": money(available),
            "cash_in_other_currencies": {k: money(v) for k, v in other.items() if v},
            "other_currencies_value": money(other_value),
            "average_monthly_saving": round(monthly, 2) if monthly is not None else None,
            "average_over_months": span if summary else 0,
        }
        if target is not None:
            shortfall = Decimal(str(target)) - available
            result["target"] = target
            if shortfall <= 0:
                result.update(reachable_now=True, months_to_target=0)
            elif monthly and monthly > 0:
                needed = float(shortfall) / monthly
                result.update(reachable_now=False, months_to_target=round(needed, 1))
            else:
                result.update(reachable_now=False, months_to_target=None)
        return result

    def ledger_freshness(self, today: date | None = None, recent_days: int = 400) -> dict:
        snap = self.snapshot()
        today = today or date.today()
        horizon = today - timedelta(days=recent_days)
        last_txn: dict[str, date] = {}
        for txn in snap.transactions:
            stem = Path(txn.meta.get("filename", "")).stem
            last_txn[stem] = max(txn.date, last_txn.get(stem, txn.date))
        last_balance: dict[str, date] = {}
        for entry in snap.balances:
            stem = Path(entry.meta.get("filename", "")).stem
            last_balance[stem] = max(entry.date, last_balance.get(stem, entry.date))
        sources = [
            {
                "source": stem,
                "last_transaction": day.isoformat(),
                "days_ago": (today - day).days,
                "last_balance_check": last_balance[stem].isoformat() if stem in last_balance else None,
            }
            for stem, day in last_txn.items()
            if day >= horizon
        ]
        sources.sort(key=lambda row: row["days_ago"], reverse=True)
        return {"as_of": today.isoformat(), "load_errors": len(snap.errors), "sources": sources}
