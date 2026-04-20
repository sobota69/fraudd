from datetime import datetime, timedelta

import pytest

from domain.rules.base_rule import Severity
from domain.rules.group.frequency_group import FrequencyGroup
from domain.transaction import Transaction

GROUP = FrequencyGroup()


def _tx(
    id: str,
    ts: datetime,
    customer_id: int = 1,
    is_new_beneficiary: bool = False,
    amount: float = 100.0,
) -> Transaction:
    return Transaction(
        transaction_id=id,
        transaction_timestamp=ts,
        customer_id=customer_id,
        customer_account="NL01BANK0001",
        channel="web",
        device_id="d1",
        amount=amount,
        currency="EUR",
        is_new_beneficiary=is_new_beneficiary,
        beneficiary_account="NL02BANK0002",
        entered_beneficiary_name="Bob",
        official_beneficiary_account_name="Bob",
        customer_account_balance=5000.0,
    )


BASE_TS = datetime(2026, 4, 20, 12, 0, 0)


def _find(results, rule_id):
    return next(r for r in results if r.rule_id == rule_id)


# ── R7 – High Frequency Transfers ───────────────────────────


class TestR7:
    def test_not_triggered_below_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=i + 1)) for i in range(4)]
        r = _find(GROUP.evaluate(current, history), "R7")
        assert not r.triggered

    def test_triggered_at_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=i + 1)) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R7")
        assert r.triggered
        assert r.severity == Severity.STRONG

    def test_ignores_outside_window(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=11 + i)) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R7")
        assert not r.triggered

    def test_ignores_different_customer(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=1), customer_id=99) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R7")
        assert not r.triggered

    def test_no_history(self):
        r = _find(GROUP.evaluate(_tx("cur", BASE_TS), None), "R7")
        assert not r.triggered


# ── R8 – New Payees Burst ────────────────────────────────────


class TestR8:
    def test_not_triggered_below_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(hours=i + 1), is_new_beneficiary=True) for i in range(2)]
        r = _find(GROUP.evaluate(current, history), "R8")
        assert not r.triggered

    def test_triggered_at_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(hours=i + 1), is_new_beneficiary=True) for i in range(3)]
        r = _find(GROUP.evaluate(current, history), "R8")
        assert r.triggered
        assert r.severity == Severity.MILD

    def test_ignores_non_new_beneficiary(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(hours=i + 1), is_new_beneficiary=False) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R8")
        assert not r.triggered

    def test_ignores_outside_24h_window(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(hours=25 + i), is_new_beneficiary=True) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R8")
        assert not r.triggered

    def test_no_history(self):
        r = _find(GROUP.evaluate(_tx("cur", BASE_TS), None), "R8")
        assert not r.triggered


# ── R17 – Smurfing / Structuring ─────────────────────────────


class TestR17:
    def test_not_triggered_below_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=10 * (i + 1)), amount=14000.0) for i in range(4)]
        r = _find(GROUP.evaluate(current, history), "R17")
        assert not r.triggered

    def test_triggered_at_threshold(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=10 * (i + 1)), amount=14000.0) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R17")
        assert r.triggered
        assert r.severity == Severity.STRONG

    def test_ignores_amount_outside_range(self):
        current = _tx("cur", BASE_TS)
        # Below min
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=10 * (i + 1)), amount=10000.0) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R17")
        assert not r.triggered

    def test_ignores_amount_above_range(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(minutes=10 * (i + 1)), amount=15000.0) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R17")
        assert not r.triggered

    def test_ignores_outside_2h_window(self):
        current = _tx("cur", BASE_TS)
        history = [_tx(f"h{i}", BASE_TS - timedelta(hours=3 + i), amount=14000.0) for i in range(5)]
        r = _find(GROUP.evaluate(current, history), "R17")
        assert not r.triggered

    def test_no_history(self):
        r = _find(GROUP.evaluate(_tx("cur", BASE_TS), None), "R17")
        assert not r.triggered


# ── General ──────────────────────────────────────────────────


class TestGeneral:
    def test_returns_three_results(self):
        results = GROUP.evaluate(_tx("cur", BASE_TS), [])
        assert len(results) == 3
        assert {r.rule_id for r in results} == {"R7", "R8", "R17"}
