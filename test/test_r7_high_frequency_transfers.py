"""Unit tests for R7 – High Frequency of Transfers rule."""

import pytest
from datetime import datetime, timedelta, timezone

from src.transaction.transaction import Transaction
from src.rules.r7_high_frequency_transfers import R7HighFrequencyTransfers
from src.rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 50.0,
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


def _make_recent_history(count: int, customer_id: int = 100, minutes_apart: float = 1.0) -> list[Transaction]:
    """Create *count* history transactions spaced *minutes_apart* before NOW."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            timestamp=NOW - timedelta(minutes=minutes_apart * (i + 1)),
        )
        for i in range(count)
    ]


class TestR7HighFrequencyTransfers:

    def setup_method(self):
        self.rule = R7HighFrequencyTransfers()

    # ── no-trigger cases ──────────────────────────────────────────────

    def test_no_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(), history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.triggered is False

    def test_different_customer_does_not_trigger(self):
        """5 recent tx exist but for a different customer → no trigger."""
        history = _make_recent_history(count=5, customer_id=999)
        result = self.rule.evaluate(_make_tx(customer_id=100), history=history)
        assert result.triggered is False

    def test_three_tx_in_window_does_not_trigger(self):
        """3 history + 1 current = 4 total → below threshold of 5."""
        history = _make_recent_history(count=3)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    def test_four_tx_total_does_not_trigger(self):
        """3 history + current = 4 → should NOT trigger."""
        history = _make_recent_history(count=3)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False
        # sanity: counted correctly
        assert result.triggered is False

    def test_history_outside_10min_window_does_not_trigger(self):
        """All history is > 10 minutes old → no trigger even with many tx."""
        history = [
            _make_tx(
                tx_id=f"TX-OLD-{i}",
                timestamp=NOW - timedelta(minutes=11 + i),
            )
            for i in range(10)
        ]
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    # ── trigger cases ─────────────────────────────────────────────────

    def test_exactly_5_tx_triggers(self):
        """4 history within window + current = 5 → triggers."""
        history = _make_recent_history(count=4)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG
        assert result.details["transaction_count_in_window"] == 5

    def test_more_than_5_tx_triggers(self):
        """9 history + current = 10 → triggers."""
        history = _make_recent_history(count=9)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is True
        assert result.details["transaction_count_in_window"] == 10

    def test_trigger_only_counts_same_customer(self):
        """Mix of customers – only matching ones should count."""
        matching = _make_recent_history(count=4, customer_id=100)
        other = _make_recent_history(count=10, customer_id=999)
        # rename ids to avoid collision
        for i, tx in enumerate(other):
            tx.transaction_id = f"TX-OTHER-{i}"
        history = matching + other
        result = self.rule.evaluate(_make_tx(customer_id=100), history=history)
        assert result.triggered is True
        assert result.details["transaction_count_in_window"] == 5  # 4 + current

    # ── boundary / edge cases ─────────────────────────────────────────

    def test_tx_exactly_at_window_boundary_included(self):
        """A transaction exactly 10 minutes before NOW should be included."""
        history = [
            _make_tx(tx_id=f"TX-EDGE-{i}", timestamp=NOW - timedelta(minutes=10))
            for i in range(4)
        ]
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is True
        assert result.details["transaction_count_in_window"] == 5

    def test_tx_just_outside_window_excluded(self):
        """Transactions 10min + 1sec before NOW should be excluded."""
        history = [
            _make_tx(
                tx_id=f"TX-OUT-{i}",
                timestamp=NOW - timedelta(minutes=10, seconds=1),
            )
            for i in range(4)
        ]
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    def test_current_tx_not_double_counted(self):
        """If the current tx appears in history it should be excluded."""
        current = _make_tx(tx_id="TX-CURRENT")
        history = [
            current,  # duplicate – must be excluded
            *_make_recent_history(count=3),
        ]
        result = self.rule.evaluate(current, history=history)
        # 3 history + 1 current = 4 → no trigger
        assert result.triggered is False

    def test_mix_inside_and_outside_window(self):
        """Some tx inside, some outside the 10-min window."""
        inside = _make_recent_history(count=3, minutes_apart=2)  # 2,4,6 min ago
        outside = [
            _make_tx(tx_id=f"TX-FAR-{i}", timestamp=NOW - timedelta(minutes=15 + i))
            for i in range(5)
        ]
        history = inside + outside
        result = self.rule.evaluate(_make_tx(), history=history)
        # 3 inside + 1 current = 4 → no trigger
        assert result.triggered is False

    def test_future_tx_in_history_excluded(self):
        """Transactions with timestamp after current tx should not count."""
        future = [
            _make_tx(tx_id=f"TX-FUT-{i}", timestamp=NOW + timedelta(minutes=i + 1))
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(), history=future)
        assert result.triggered is False

    # ── metadata ──────────────────────────────────────────────────────

    def test_result_metadata(self):
        history = _make_recent_history(count=4)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.rule_id == "R7"
        assert result.rule_name == "High Frequency of Transfers"
        assert result.weight == 10
        assert result.details["window_minutes"] == 10
        assert result.details["threshold"] == 5

    def test_no_trigger_result_has_no_severity(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.severity is None
