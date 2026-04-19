"""Unit tests for AmountStatsGroup – R6 (High Amount Spike) and R12 (Z-Score Amount).

The group filters the 30-day history once, computes mean/std once,
then evaluates both rules from those shared statistics.
"""

import pytest
import math
from datetime import datetime, timedelta, timezone

from domain.transaction import Transaction
from domain.rules.group.amount_stats_group import AmountStatsGroup
from domain.rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 100.0,
    timestamp: datetime = NOW,
    **kwargs,
) -> Transaction:
    defaults = dict(
        transaction_id=tx_id,
        transaction_timestamp=timestamp,
        customer_id=customer_id,
        customer_account="PL00000000000000000000000",
        channel="Mobile",
        device_id="MOB-IOS-AAAA",
        amount=amount,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account="DE00000000000000000000000",
        entered_beneficiary_name="John Doe",
        official_beneficiary_account_name="John Doe",
        customer_account_balance=50000.0,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _make_history(count: int, amount: float, days_back: int = 15) -> list[Transaction]:
    """Generate `count` transactions spread evenly within the last `days_back` days."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i}",
            amount=amount,
            timestamp=NOW - timedelta(days=days_back - i * (days_back / max(count, 1))),
        )
        for i in range(count)
    ]


def _result_by_id(results, rule_id):
    return next(r for r in results if r.rule_id == rule_id)


# ═══════════════════════════════════════════════════════════════════════════════
# Group structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestAmountStatsGroupStructure:
    group = AmountStatsGroup()

    def test_returns_two_results(self):
        tx = _make_tx()
        results = self.group.evaluate(tx, [])
        assert len(results) == 2

    def test_result_ids_match(self):
        tx = _make_tx()
        ids = {r.rule_id for r in self.group.evaluate(tx, [])}
        assert ids == {"R6", "R12"}


# ═══════════════════════════════════════════════════════════════════════════════
# R6 – High Amount Spike
# ═══════════════════════════════════════════════════════════════════════════════

class TestR6HighAmountSpike:
    """R6: amount > 3× 30-day avg → MILD, > 10× → STRONG."""

    group = AmountStatsGroup()

    def test_no_history_does_not_trigger(self):
        """Without history there is no average to compare against."""
        tx = _make_tx(amount=10_000)
        r6 = _result_by_id(self.group.evaluate(tx, []), "R6")
        assert r6.triggered is False

    def test_normal_amount_does_not_trigger(self):
        """Amount equal to the average should not trigger."""
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=100.0)
        r6 = _result_by_id(self.group.evaluate(tx, history), "R6")
        assert r6.triggered is False

    def test_amount_just_below_3x_does_not_trigger(self):
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=299.0)  # 2.99×
        r6 = _result_by_id(self.group.evaluate(tx, history), "R6")
        assert r6.triggered is False

    def test_amount_above_3x_triggers_mild(self):
        """3.5× the average → MILD severity."""
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=350.0)
        r6 = _result_by_id(self.group.evaluate(tx, history), "R6")
        assert r6.triggered is True
        assert r6.severity == Severity.MILD
        assert r6.weight == 8

    def test_amount_above_10x_triggers_strong(self):
        """11× the average → STRONG severity."""
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=1100.0)
        r6 = _result_by_id(self.group.evaluate(tx, history), "R6")
        assert r6.triggered is True
        assert r6.severity == Severity.STRONG

    def test_details_include_multiplier(self):
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=500.0)
        r6 = _result_by_id(self.group.evaluate(tx, history), "R6")
        assert "multiplier" in r6.details
        assert r6.details["multiplier"] == 5.0

    def test_old_history_beyond_30_days_is_ignored(self):
        """Transactions older than 30 days should not count."""
        old = [
            _make_tx(tx_id=f"TX-OLD-{i}", amount=100.0,
                     timestamp=NOW - timedelta(days=60 + i))
            for i in range(10)
        ]
        tx = _make_tx(amount=500.0)
        r6 = _result_by_id(self.group.evaluate(tx, old), "R6")
        assert r6.triggered is False


# ═══════════════════════════════════════════════════════════════════════════════
# R12 – Z-Score Amount
# ═══════════════════════════════════════════════════════════════════════════════

class TestR12ZscoreAmount:
    """R12: z-score > 3 (based on 30-day stats) → STRONG."""

    group = AmountStatsGroup()

    def test_no_history_does_not_trigger(self):
        tx = _make_tx(amount=10_000)
        r12 = _result_by_id(self.group.evaluate(tx, []), "R12")
        assert r12.triggered is False

    def test_insufficient_history_does_not_trigger(self):
        """Need at least 2 historical transactions."""
        history = _make_history(1, amount=100.0)
        tx = _make_tx(amount=10_000)
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert r12.triggered is False

    def test_normal_amount_does_not_trigger(self):
        history = _make_history(20, amount=100.0)
        tx = _make_tx(amount=100.0)
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert r12.triggered is False

    def test_extreme_outlier_triggers(self):
        """History averages ~100, current is 10000 → z-score >> 3."""
        history = [
            _make_tx(tx_id=f"TX-H-{i}", amount=90.0 + i,
                     timestamp=NOW - timedelta(days=i + 1))
            for i in range(20)
        ]
        tx = _make_tx(amount=10_000.0)
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert r12.triggered is True
        assert r12.severity == Severity.STRONG
        assert r12.weight == 12

    def test_details_include_zscore(self):
        history = [
            _make_tx(tx_id=f"TX-H-{i}", amount=90.0 + i,
                     timestamp=NOW - timedelta(days=i + 1))
            for i in range(20)
        ]
        tx = _make_tx(amount=10_000.0)
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert "zscore" in r12.details
        assert r12.details["zscore"] > 3.0

    def test_zero_std_does_not_trigger(self):
        """If all amounts are identical, std=0 → cannot compute z-score."""
        history = _make_history(10, amount=100.0)
        tx = _make_tx(amount=100.0)
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert r12.triggered is False

    def test_moderate_deviation_does_not_trigger(self):
        """Amount slightly above average but within 3 std should not trigger."""
        # Mean=100, std≈14.14 → 3*std≈42.4 → threshold ~142.4
        amounts = [80, 90, 100, 110, 120, 80, 90, 100, 110, 120]
        history = [
            _make_tx(tx_id=f"TX-H-{i}", amount=float(a),
                     timestamp=NOW - timedelta(days=i + 1))
            for i, a in enumerate(amounts)
        ]
        tx = _make_tx(amount=140.0)  # above avg but within 3 std
        r12 = _result_by_id(self.group.evaluate(tx, history), "R12")
        assert r12.triggered is False


# ═══════════════════════════════════════════════════════════════════════════════
# Both rules share the same history filter
# ═══════════════════════════════════════════════════════════════════════════════

class TestSharedComputation:
    """Verify both rules are evaluated from the same 30-day history scan."""

    group = AmountStatsGroup()

    def test_both_can_trigger_simultaneously(self):
        """An extreme outlier should trigger both R6 (spike) and R12 (z-score)."""
        history = [
            _make_tx(tx_id=f"TX-H-{i}", amount=90.0 + i,
                     timestamp=NOW - timedelta(days=i + 1))
            for i in range(20)
        ]
        tx = _make_tx(amount=50_000.0)  # ~500× avg, z-score >> 3
        results = self.group.evaluate(tx, history)
        r6 = _result_by_id(results, "R6")
        r12 = _result_by_id(results, "R12")
        assert r6.triggered is True
        assert r12.triggered is True

    def test_neither_triggers_on_normal_tx(self):
        history = _make_history(20, amount=100.0)
        tx = _make_tx(amount=100.0)
        results = self.group.evaluate(tx, history)
        assert all(not r.triggered for r in results)
