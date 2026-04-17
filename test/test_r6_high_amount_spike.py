"""Unit tests for R6 – High Amount Spike rule."""

import pytest
from datetime import datetime, timedelta, timezone

from transaction.transaction import Transaction
from rules.r6_high_amount_spike import R6HighAmountSpike
from rules.base_rule import Severity


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


def _make_history(
    count: int = 10,
    avg_amount: float = 100.0,
    customer_id: int = 100,
    days_back_start: int = 1,
) -> list[Transaction]:
    """Create *count* history transactions spread over recent days."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            amount=avg_amount,
            timestamp=NOW - timedelta(days=days_back_start + i),
        )
        for i in range(count)
    ]


class TestR6HighAmountSpike:
    """Tests for R6HighAmountSpike rule."""

    def setup_method(self):
        self.rule = R6HighAmountSpike()

    # ── no-trigger cases ──────────────────────────────────────────────

    def test_no_history_does_not_trigger(self):
        tx = _make_tx(amount=999999.0)
        result = self.rule.evaluate(tx, history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        tx = _make_tx(amount=999999.0)
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    def test_no_matching_customer_does_not_trigger(self):
        tx = _make_tx(amount=999999.0, customer_id=100)
        history = _make_history(customer_id=999)  # different customer
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_history_older_than_30_days_does_not_trigger(self):
        tx = _make_tx(amount=999999.0)
        history = _make_history(days_back_start=31)  # all beyond 30-day window
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_amount_below_3x_does_not_trigger(self):
        """Amount = 2.5× average → should NOT trigger."""
        tx = _make_tx(amount=250.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_amount_exactly_3x_does_not_trigger(self):
        """Amount = exactly 3× average → should NOT trigger (must exceed)."""
        tx = _make_tx(amount=300.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    # ── MILD trigger cases ────────────────────────────────────────────

    def test_mild_trigger_above_3x(self):
        """Amount = 5× average → MILD."""
        tx = _make_tx(amount=500.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD
        assert result.details["multiplier"] == 5.0

    def test_mild_trigger_just_above_3x(self):
        """Amount = 3.01× average → MILD."""
        tx = _make_tx(amount=301.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD

    def test_mild_trigger_at_10x_boundary(self):
        """Amount = exactly 10× average → MILD (must exceed 10× for STRONG)."""
        tx = _make_tx(amount=1000.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD

    # ── STRONG trigger cases ──────────────────────────────────────────

    def test_strong_trigger_above_10x(self):
        """Amount = 15× average → STRONG."""
        tx = _make_tx(amount=1500.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG
        assert result.details["multiplier"] == 15.0

    def test_strong_trigger_just_above_10x(self):
        """Amount = 10.01× average → STRONG."""
        tx = _make_tx(amount=1001.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG

    # ── details / metadata ────────────────────────────────────────────

    def test_result_contains_correct_rule_id(self):
        tx = _make_tx(amount=500.0)
        history = _make_history(avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.rule_id == "R6"
        assert result.rule_name == "High Amount Spike"
        assert result.weight == 8

    def test_details_include_avg_and_history_count(self):
        tx = _make_tx(amount=500.0)
        history = _make_history(count=5, avg_amount=100.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.details["avg_amount_30d"] == 100.0
        assert result.details["history_count"] == 5

    # ── edge cases ────────────────────────────────────────────────────

    def test_zero_avg_does_not_trigger(self):
        """All history amounts are 0 → avg is 0 → should not trigger."""
        tx = _make_tx(amount=500.0)
        history = _make_history(avg_amount=0.0)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_current_tx_excluded_from_history(self):
        """The current transaction should not count towards the average."""
        tx = _make_tx(tx_id="TX-CURRENT", amount=500.0)
        history = [
            tx,  # same tx in history – should be excluded
            *_make_history(count=5, avg_amount=100.0),
        ]
        result = self.rule.evaluate(tx, history=history)
        assert result.details["history_count"] == 5

    def test_mixed_customers_only_uses_matching(self):
        """Only transactions from the same customer_id are considered."""
        tx = _make_tx(amount=500.0, customer_id=100)
        history = [
            *_make_history(count=5, avg_amount=100.0, customer_id=100),
            *_make_history(count=5, avg_amount=10.0, customer_id=999),
        ]
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.details["history_count"] == 5
        assert result.details["avg_amount_30d"] == 100.0
