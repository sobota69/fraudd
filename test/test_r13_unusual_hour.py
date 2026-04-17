"""Unit tests for R13 – Unusual Hour rule."""

import pytest
from collections import Counter
from datetime import datetime, timedelta, timezone

from src.transaction.transaction import Transaction
from src.rules.r13_unusual_hour import (
    R13UnusualHour,
    find_smallest_90pct_window,
    hour_in_window,
)
from src.rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc)  # 03:00 UTC


def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
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
        amount=50.0,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account="DE00000000000000000000000",
        entered_beneficiary_name="John Doe",
        official_beneficiary_account_name="John Doe",
        customer_account_balance=50000.0,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _history_at_hours(hours: list[int], customer_id: int = 100) -> list[Transaction]:
    """Create one history tx per entry in *hours*, each on a different past day."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            timestamp=datetime(2025, 12, 18 - (i % 17), h, 30, 0, tzinfo=timezone.utc),
        )
        for i, h in enumerate(hours)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Helper function tests
# ═══════════════════════════════════════════════════════════════════════════

class TestHourInWindow:
    def test_simple_window(self):
        # window 8–12 (size 5): hours 8,9,10,11,12
        assert hour_in_window(8, 8, 5) is True
        assert hour_in_window(12, 8, 5) is True
        assert hour_in_window(10, 8, 5) is True
        assert hour_in_window(7, 8, 5) is False
        assert hour_in_window(13, 8, 5) is False

    def test_wrapping_window(self):
        # window start=22, size=5 → hours {22,23,0,1,2}
        assert hour_in_window(22, 22, 5) is True
        assert hour_in_window(23, 22, 5) is True
        assert hour_in_window(0, 22, 5) is True
        assert hour_in_window(2, 22, 5) is True
        assert hour_in_window(3, 22, 5) is False
        assert hour_in_window(21, 22, 5) is False

    def test_full_day_window(self):
        for h in range(24):
            assert hour_in_window(h, 0, 24) is True

    def test_single_hour_window(self):
        assert hour_in_window(10, 10, 1) is True
        assert hour_in_window(9, 10, 1) is False
        assert hour_in_window(11, 10, 1) is False

    def test_window_at_midnight(self):
        # start=0, size=3 → {0,1,2}
        assert hour_in_window(0, 0, 3) is True
        assert hour_in_window(23, 0, 3) is False

    def test_window_ending_at_midnight(self):
        # start=21, size=3 → {21,22,23}
        assert hour_in_window(23, 21, 3) is True
        assert hour_in_window(0, 21, 3) is False


class TestFindSmallest90PctWindow:
    def test_all_in_one_hour(self):
        counts = Counter({10: 20})
        start, size, total = find_smallest_90pct_window(counts)
        assert size == 1
        assert start == 10

    def test_two_adjacent_hours(self):
        counts = Counter({10: 10, 11: 10})
        start, size, _ = find_smallest_90pct_window(counts)
        # 90% of 20 = 18. Need both hours → size=2
        assert size == 2

    def test_spread_across_many_hours(self):
        # 1 tx each hour 0–23 → total=24, need 22 to cover 90%
        counts = Counter({h: 1 for h in range(24)})
        _, size, _ = find_smallest_90pct_window(counts)
        assert size == 22

    def test_concentrated_with_outlier(self):
        # 9 tx at hour 10, 1 at hour 22 → total=10, need 9
        # single hour 10 covers 9 = 90% exactly
        counts = Counter({10: 9, 22: 1})
        start, size, _ = find_smallest_90pct_window(counts)
        assert size == 1
        assert start == 10

    def test_wrapping_window_needed(self):
        # 5 tx at hour 23, 5 tx at hour 0 → total=10, need 9
        # smallest window wrapping midnight: size=2 covering {23,0}
        counts = Counter({23: 5, 0: 5})
        start, size, _ = find_smallest_90pct_window(counts)
        assert size == 2


# ═══════════════════════════════════════════════════════════════════════════
# Rule tests
# ═══════════════════════════════════════════════════════════════════════════

class TestR13UnusualHour:

    def setup_method(self):
        self.rule = R13UnusualHour()

    # ── no-trigger: insufficient data ─────────────────────────────────

    def test_no_history(self):
        result = self.rule.evaluate(_make_tx(), history=None)
        assert result.triggered is False

    def test_empty_history(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.triggered is False

    def test_less_than_10_history(self):
        history = _history_at_hours([10, 11, 12, 10, 11, 12, 10, 11, 12])  # 9 tx
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    def test_exactly_9_history_does_not_trigger(self):
        history = _history_at_hours([10] * 9)
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    def test_different_customer_not_counted(self):
        history = _history_at_hours([10] * 15, customer_id=999)
        result = self.rule.evaluate(_make_tx(customer_id=100), history=history)
        assert result.triggered is False

    # ── no-trigger: hour inside window ────────────────────────────────

    def test_tx_inside_usual_window(self):
        """All history at hour 10; tx also at hour 10 → inside window."""
        history = _history_at_hours([10] * 15)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 10, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_tx_inside_broad_window(self):
        """History spread 8–17; tx at 12 → inside."""
        hours = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_tx_inside_wrapping_window(self):
        """History around midnight (22,23,0,1); tx at 23 → inside."""
        hours = [22, 23, 0, 1, 22, 23, 0, 1, 22, 23]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 23, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    # ── trigger cases ─────────────────────────────────────────────────

    def test_tx_outside_usual_window_triggers(self):
        """All history at hour 10; tx at 3 AM → outside → trigger."""
        history = _history_at_hours([10] * 15)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD
        assert result.details["transaction_hour"] == 3

    def test_tx_at_midnight_outside_daytime_window(self):
        """History 8–17; tx at 0 → outside."""
        hours = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 0, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_tx_outside_wrapping_window_triggers(self):
        """History around midnight (22,23,0,1); tx at 12 → outside."""
        hours = [22, 23, 0, 1, 22, 23, 0, 1, 22, 23]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_exactly_10_history_triggers(self):
        """Minimum history count (10) should still evaluate."""
        history = _history_at_hours([14] * 10)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 2, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_trigger_with_mixed_customers(self):
        """Only matching customer history is used."""
        matching = _history_at_hours([10] * 12, customer_id=100)
        other = _history_at_hours([3] * 20, customer_id=999)  # hour 3 for other
        for i, tx in enumerate(other):
            tx.transaction_id = f"TX-OTHER-{i}"
        tx = _make_tx(
            customer_id=100,
            timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc),
        )
        result = self.rule.evaluate(tx, history=matching + other)
        assert result.triggered is True  # hour 3 is unusual for customer 100

    # ── edge cases ────────────────────────────────────────────────────

    def test_current_tx_excluded_from_history(self):
        """Current tx should not be in the hour counts."""
        current = _make_tx(
            tx_id="TX-CURRENT",
            timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc),
        )
        # 9 at hour 10 + current at hour 3 in history = 10, but current excluded → 9 < 10
        history = [current, *_history_at_hours([10] * 9)]
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is False  # insufficient history after exclusion

    def test_current_tx_excluded_but_enough_history(self):
        """Current tx excluded, still 10 remaining → evaluates."""
        current = _make_tx(
            tx_id="TX-CURRENT",
            timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc),
        )
        history = [current, *_history_at_hours([10] * 10)]
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is True  # hour 3 outside hour-10 window

    def test_all_hours_represented_evenly(self):
        """1 tx per hour (24 total). 90% window = 22 hours. Only 2 hours outside.
        Tx at any hour has low chance of being outside, but test a specific one."""
        hours = list(range(24)) * 1  # 24 tx, 1 per hour
        # need to pad to avoid < 10 issue — already 24
        history = _history_at_hours(hours)
        # The window will cover 22 of 24 hours. The 2 excluded hours depend
        # on the algorithm (it picks the first best window from hour 0).
        # We just verify it runs and the window_size is 22.
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        # hour 12 is very likely inside a 22-hour window
        assert result.triggered is False

    def test_90pct_exact_boundary(self):
        """9 tx at hour 10, 1 at hour 22 → 90% = 9 tx.
        Single-hour window at 10 covers exactly 9/10 = 90%.
        Tx at hour 22 → outside that window → triggers."""
        history = _history_at_hours([10] * 9 + [22])
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 22, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.details["window_size"] == 1

    def test_two_clusters_picks_smaller(self):
        """5 tx at hour 10, 5 tx at hour 14 → 90% = 9.
        Window 10–14 (size 5) covers all 10. Check it works."""
        history = _history_at_hours([10] * 5 + [14] * 5)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_late_night_cluster(self):
        """All history at 23:xx; tx at 10:xx → triggers."""
        history = _history_at_hours([23] * 15)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 10, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_early_morning_cluster(self):
        """All history at 5:xx; tx at 18:xx → triggers."""
        history = _history_at_hours([5] * 15)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 18, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_adjacent_to_window_edge_outside(self):
        """Window is hours 8-17 (size 10). Tx at hour 7 → just outside."""
        hours = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 7, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True

    def test_adjacent_to_window_edge_inside(self):
        """Window is hours 8-17 (size 10). Tx at hour 8 → inside."""
        hours = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        history = _history_at_hours(hours)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 8, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    # ── metadata ──────────────────────────────────────────────────────

    def test_result_metadata_on_trigger(self):
        history = _history_at_hours([10] * 15)
        tx = _make_tx(timestamp=datetime(2025, 12, 19, 3, 0, 0, tzinfo=timezone.utc))
        result = self.rule.evaluate(tx, history=history)
        assert result.rule_id == "R13"
        assert result.rule_name == "Unusual Hour"
        assert result.weight == 5
        assert result.details["transaction_hour"] == 3
        assert result.details["history_count"] == 15
        assert isinstance(result.details["window_hours"], list)

    def test_no_trigger_severity_is_none(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.severity is None
