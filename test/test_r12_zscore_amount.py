"""Unit tests for R12 – Z-Score Amount rule."""

import math
import pytest
from datetime import datetime, timedelta, timezone

from transaction.transaction import Transaction
from rules.r12_zscore_amount import R12ZscoreAmount
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
    amounts: list[float],
    customer_id: int = 100,
    days_back_start: int = 1,
) -> list[Transaction]:
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            amount=amt,
            timestamp=NOW - timedelta(days=days_back_start + i),
        )
        for i, amt in enumerate(amounts)
    ]


def _uniform_history(value: float = 100.0, count: int = 10, customer_id: int = 100) -> list[Transaction]:
    return _make_history([value] * count, customer_id=customer_id)


class TestR12ZscoreAmount:

    def setup_method(self):
        self.rule = R12ZscoreAmount()

    # ── no-trigger cases ──────────────────────────────────────────────

    def test_no_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(amount=999999.0), history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(amount=999999.0), history=[])
        assert result.triggered is False

    def test_single_history_tx_does_not_trigger(self):
        """Need ≥2 history points for std; 1 is insufficient."""
        history = _make_history([100.0])
        result = self.rule.evaluate(_make_tx(amount=999999.0), history=history)
        assert result.triggered is False

    def test_different_customer_does_not_trigger(self):
        history = _uniform_history(customer_id=999)
        result = self.rule.evaluate(_make_tx(amount=999999.0, customer_id=100), history=history)
        assert result.triggered is False

    def test_history_older_than_30_days_does_not_trigger(self):
        history = _make_history([100.0] * 10, days_back_start=31)
        result = self.rule.evaluate(_make_tx(amount=999999.0), history=history)
        assert result.triggered is False

    def test_zero_std_does_not_trigger(self):
        """All history amounts identical → std=0 → no trigger (avoid division by zero)."""
        history = _uniform_history(100.0, count=10)
        result = self.rule.evaluate(_make_tx(amount=200.0), history=history)
        assert result.triggered is False

    def test_zscore_below_threshold_does_not_trigger(self):
        """Construct history where current amount yields z ≈ 2."""
        # mean=100, std=10 → z = (120-100)/10 = 2.0
        amounts = [90.0, 110.0] * 5  # mean=100, std=10
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=120.0), history=history)
        assert result.triggered is False

    def test_zscore_exactly_3_does_not_trigger(self):
        """z = exactly 3.0 → must exceed, not equal."""
        # mean=100, std=10 → amount=130 gives z=3.0
        amounts = [90.0, 110.0] * 5
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=130.0), history=history)
        assert result.triggered is False

    # ── trigger cases ─────────────────────────────────────────────────

    def test_zscore_above_3_triggers(self):
        """mean=100, std=10 → amount=140 gives z=4.0 → triggers."""
        amounts = [90.0, 110.0] * 5
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=140.0), history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG
        assert result.details["zscore"] == 4.0

    def test_just_above_3_triggers(self):
        """mean=100, std=10 → amount=130.1 gives z=3.01 → triggers."""
        amounts = [90.0, 110.0] * 5
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=130.1), history=history)
        assert result.triggered is True

    def test_large_spike_triggers(self):
        history = _make_history([100.0, 105.0, 95.0, 100.0, 110.0, 90.0])
        result = self.rule.evaluate(_make_tx(amount=5000.0), history=history)
        assert result.triggered is True
        assert result.details["zscore"] > 3.0

    # ── edge cases ────────────────────────────────────────────────────

    def test_current_tx_excluded_from_stats(self):
        """Current tx must not be in the mean/std calculation."""
        current = _make_tx(tx_id="TX-CURRENT", amount=140.0)
        amounts = [90.0, 110.0] * 5
        history = [current, *_make_history(amounts)]
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is True
        assert result.details["history_count"] == 10  # current excluded

    def test_negative_zscore_does_not_trigger(self):
        """Amount far below mean → negative z → no trigger."""
        amounts = [90.0, 110.0] * 5
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=1.0), history=history)
        assert result.triggered is False

    def test_exactly_2_history_points_works(self):
        """Minimum viable history (2 points)."""
        history = _make_history([100.0, 200.0])  # mean=150, std=50
        # z = (500-150)/50 = 7.0
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is True
        assert result.details["history_count"] == 2

    def test_mixed_customers_only_uses_matching(self):
        matching = _make_history([90.0, 110.0] * 5, customer_id=100)
        other = _make_history([1.0, 2.0] * 5, customer_id=999)
        for i, tx in enumerate(other):
            tx.transaction_id = f"TX-OTHER-{i}"
        result = self.rule.evaluate(
            _make_tx(amount=140.0, customer_id=100),
            history=matching + other,
        )
        assert result.triggered is True
        assert result.details["history_count"] == 10

    # ── metadata ──────────────────────────────────────────────────────

    def test_result_metadata(self):
        amounts = [90.0, 110.0] * 5
        history = _make_history(amounts)
        result = self.rule.evaluate(_make_tx(amount=140.0), history=history)
        assert result.rule_id == "R12"
        assert result.rule_name == "Z-Score Amount"
        assert result.weight == 12
        assert result.details["mean_30d"] == 100.0
        assert result.details["std_30d"] == 10.0
        assert result.details["threshold"] == 3.0

    def test_no_trigger_severity_is_none(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.severity is None
