"""Unit tests for R17 – Smurfing / Structuring rule.

The rule detects "structuring" (also known as "smurfing"): a money-laundering
technique where a criminal deliberately breaks a large transaction into
several smaller ones that stay just below the €15,000 reporting threshold.

Rule specification (from HACKATHON_FRAML_RULES.xlsx)
----------------------------------------------------
* Suspicious amount range : €13,500 – €14,999 (inclusive on both ends).
* Time window             : 2 hours, ending at the current transaction.
* Minimum transaction count: 5 (including the current tx if its amount is
  inside the range).
* Filters by              : customer_id.
"""

import pytest
from datetime import datetime, timedelta, timezone

from src.transaction.transaction import Transaction
from src.rules.r17_smurfing_structuring import R17SmurfingStructuring, _is_structuring_amount
from src.rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 14_000.0,
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
        customer_account_balance=100000.0,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _structuring_history(
    count: int,
    customer_id: int = 100,
    amount: float = 14_000.0,
    minutes_apart: float = 10.0,
) -> list[Transaction]:
    """Create *count* history transactions in the structuring range,
    spaced *minutes_apart* minutes before NOW."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            amount=amount,
            timestamp=NOW - timedelta(minutes=minutes_apart * (i + 1)),
        )
        for i in range(count)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Tests for the helper _is_structuring_amount
# ═══════════════════════════════════════════════════════════════════════════

class TestIsStructuringAmount:
    """Validate the €13,500 – €14,999 range check used by the rule."""

    def test_exactly_lower_bound(self):
        """€13,500.00 is the lower boundary and should be IN range."""
        assert _is_structuring_amount(13_500.0) is True

    def test_exactly_upper_bound(self):
        """€14,999.00 is the upper boundary and should be IN range."""
        assert _is_structuring_amount(14_999.0) is True

    def test_midpoint_in_range(self):
        """€14,000 is clearly inside the range."""
        assert _is_structuring_amount(14_000.0) is True

    def test_just_below_lower_bound(self):
        """€13,499.99 is one cent below the lower boundary → OUT of range."""
        assert _is_structuring_amount(13_499.99) is False

    def test_just_above_upper_bound(self):
        """€14,999.01 is one cent above the upper boundary → OUT of range.
        (This is effectively €15,000 territory – above the reporting threshold.)"""
        assert _is_structuring_amount(14_999.01) is False

    def test_exactly_15000(self):
        """€15,000 is the AML reporting threshold itself → OUT of range."""
        assert _is_structuring_amount(15_000.0) is False

    def test_small_amount(self):
        """A normal everyday amount should be out of range."""
        assert _is_structuring_amount(50.0) is False

    def test_zero(self):
        assert _is_structuring_amount(0.0) is False

    def test_negative(self):
        assert _is_structuring_amount(-14_000.0) is False


# ═══════════════════════════════════════════════════════════════════════════
# Rule tests
# ═══════════════════════════════════════════════════════════════════════════

class TestR17SmurfingStructuring:

    def setup_method(self):
        self.rule = R17SmurfingStructuring()

    # ── Section 1: No-trigger – insufficient data ─────────────────────

    def test_no_history_single_tx_does_not_trigger(self):
        """A single transaction (the current one) can never reach the
        threshold of 5, even if its amount is in the suspicious range."""
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        """Same as above but with an explicit empty list."""
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=[])
        assert result.triggered is False

    # ── Section 2: No-trigger – wrong customer ────────────────────────

    def test_different_customer_history_ignored(self):
        """No same-customer history (pre-filtered upstream) → no trigger."""
        result = self.rule.evaluate(
            _make_tx(customer_id=100, amount=14_000.0), history=[]
        )
        assert result.triggered is False

    # ── Section 3: No-trigger – amounts outside range ─────────────────

    def test_amounts_below_range_do_not_count(self):
        """5 transactions at €13,000 (below €13,500) within 2 hours
        should NOT trigger because the amounts are below the structuring
        range, even though there are enough transactions."""
        history = _structuring_history(count=5, amount=13_000.0)
        result = self.rule.evaluate(_make_tx(amount=13_000.0), history=history)
        assert result.triggered is False

    def test_amounts_above_range_do_not_count(self):
        """5 transactions at €15,000 (above €14,999) within 2 hours
        should NOT trigger.  These would likely trip a different rule
        (R22 – high value threshold) but not the structuring rule."""
        history = _structuring_history(count=5, amount=15_000.0)
        result = self.rule.evaluate(_make_tx(amount=15_000.0), history=history)
        assert result.triggered is False

    # ── Section 4: No-trigger – too few transactions ──────────────────

    def test_four_total_does_not_trigger(self):
        """3 history in range + 1 current in range = 4 total.
        Threshold is 5 → no trigger."""
        history = _structuring_history(count=3)
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is False

    def test_four_history_but_current_out_of_range(self):
        """4 history in range + current at €100 (out of range) = 4.
        Still below threshold."""
        history = _structuring_history(count=4)
        result = self.rule.evaluate(_make_tx(amount=100.0), history=history)
        assert result.triggered is False

    # ── Section 5: No-trigger – outside time window ───────────────────

    def test_history_outside_2h_window_does_not_trigger(self):
        """5 structuring transactions all happened > 2 hours ago.
        They fall outside the window and must not count."""
        history = [
            _make_tx(
                tx_id=f"TX-OLD-{i}",
                amount=14_000.0,
                timestamp=NOW - timedelta(hours=2, minutes=1 + i),
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is False

    # ── Section 6: Trigger – basic cases ──────────────────────────────

    def test_exactly_5_in_range_triggers(self):
        """4 history in range + 1 current in range = 5 → triggers.
        This is the minimum trigger scenario."""
        history = _structuring_history(count=4)
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG
        assert result.details["structuring_tx_count"] == 5

    def test_more_than_5_triggers(self):
        """9 history + 1 current = 10 in range → clearly triggers."""
        history = _structuring_history(count=9)
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is True
        assert result.details["structuring_tx_count"] == 10

    def test_five_history_current_out_of_range_triggers(self):
        """5 history in range + current at €100 (out of range) = 5.
        The current transaction does not need to be in range itself;
        it's the one that 'closes' the detection window."""
        history = _structuring_history(count=5)
        result = self.rule.evaluate(_make_tx(amount=100.0), history=history)
        assert result.triggered is True
        assert result.details["current_in_range"] is False

    # ── Section 7: Trigger – boundary amounts ─────────────────────────

    def test_trigger_at_lower_bound_amount(self):
        """All transactions at exactly €13,500 (lower bound) → triggers."""
        history = _structuring_history(count=4, amount=13_500.0)
        result = self.rule.evaluate(_make_tx(amount=13_500.0), history=history)
        assert result.triggered is True

    def test_trigger_at_upper_bound_amount(self):
        """All transactions at exactly €14,999 (upper bound) → triggers."""
        history = _structuring_history(count=4, amount=14_999.0)
        result = self.rule.evaluate(_make_tx(amount=14_999.0), history=history)
        assert result.triggered is True

    def test_mixed_amounts_within_range(self):
        """Different amounts all within the range → still counts."""
        amounts = [13_500.0, 13_750.0, 14_000.0, 14_500.0]
        history = [
            _make_tx(
                tx_id=f"TX-MIX-{i}",
                amount=amt,
                timestamp=NOW - timedelta(minutes=10 * (i + 1)),
            )
            for i, amt in enumerate(amounts)
        ]
        result = self.rule.evaluate(_make_tx(amount=14_999.0), history=history)
        assert result.triggered is True

    # ── Section 8: Edge cases – time window boundaries ────────────────

    def test_tx_exactly_at_2h_boundary_included(self):
        """A transaction exactly 2 hours before NOW should be included
        in the window (boundary is inclusive)."""
        history = [
            _make_tx(
                tx_id=f"TX-EDGE-{i}",
                amount=14_000.0,
                timestamp=NOW - timedelta(hours=2),
            )
            for i in range(4)
        ]
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is True

    def test_tx_one_second_outside_2h_excluded(self):
        """A transaction 2h + 1s before NOW should be excluded."""
        history = [
            _make_tx(
                tx_id=f"TX-OUT-{i}",
                amount=14_000.0,
                timestamp=NOW - timedelta(hours=2, seconds=1),
            )
            for i in range(4)
        ]
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.triggered is False

    # ── Section 9: Edge cases – deduplication ─────────────────────────

    def test_current_tx_not_double_counted(self):
        """If the current transaction appears in history, it should be
        excluded from the history count (it already counts as +1 if in
        range).  Without this guard we'd count it twice."""
        current = _make_tx(tx_id="TX-CURRENT", amount=14_000.0)
        history = [current, *_structuring_history(count=3)]
        # 3 history + 1 current = 4 → no trigger
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is False

    # ── Section 10: Edge cases – future transactions ──────────────────

    def test_future_tx_in_history_excluded(self):
        """Transactions timestamped AFTER the current one should not
        count, even if they are in the structuring range."""
        future = [
            _make_tx(
                tx_id=f"TX-FUT-{i}",
                amount=14_000.0,
                timestamp=NOW + timedelta(minutes=10 * (i + 1)),
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=future)
        assert result.triggered is False

    # ── Section 11: Edge cases – mixed in-range and out-of-range ──────

    def test_mixed_range_only_qualifying_counted(self):
        """History contains a mix of amounts inside and outside the
        structuring range.  Only in-range ones should be counted."""
        in_range = _structuring_history(count=4, amount=14_000.0)
        out_of_range = [
            _make_tx(
                tx_id=f"TX-NORM-{i}",
                amount=100.0,
                timestamp=NOW - timedelta(minutes=5 * (i + 1)),
            )
            for i in range(10)
        ]
        history = in_range + out_of_range
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        # 4 in-range history + 1 current in range = 5 → triggers
        assert result.triggered is True
        assert result.details["structuring_tx_count"] == 5

    # ── Section 12: Edge cases – mixed customers ──────────────────────

    def test_only_same_customer_counted(self):
        """Only same-customer history is passed (pre-filtered upstream)."""
        matching = _structuring_history(count=3, customer_id=100)
        result = self.rule.evaluate(
            _make_tx(customer_id=100, amount=14_000.0),
            history=matching,
        )
        # 3 matching + 1 current = 4 → no trigger
        assert result.triggered is False

    # ── Section 13: Edge cases – time window partially covered ────────

    def test_some_inside_some_outside_window(self):
        """3 transactions within the 2h window and 3 outside it.
        Only the 3 inside + current = 4 → no trigger."""
        inside = _structuring_history(count=3, minutes_apart=30)
        outside = [
            _make_tx(
                tx_id=f"TX-FAR-{i}",
                amount=14_000.0,
                timestamp=NOW - timedelta(hours=3 + i),
            )
            for i in range(3)
        ]
        result = self.rule.evaluate(
            _make_tx(amount=14_000.0), history=inside + outside
        )
        assert result.triggered is False

    def test_enough_inside_window_triggers_despite_outside(self):
        """4 inside + 3 outside + current in range = 5 inside → triggers."""
        inside = _structuring_history(count=4, minutes_apart=20)
        outside = [
            _make_tx(
                tx_id=f"TX-FAR-{i}",
                amount=14_000.0,
                timestamp=NOW - timedelta(hours=5 + i),
            )
            for i in range(3)
        ]
        result = self.rule.evaluate(
            _make_tx(amount=14_000.0), history=inside + outside
        )
        assert result.triggered is True
        assert result.details["structuring_tx_count"] == 5

    # ── Section 14: Metadata ──────────────────────────────────────────

    def test_result_metadata_on_trigger(self):
        """Verify all expected detail fields are present and correct."""
        history = _structuring_history(count=4, amount=14_500.0)
        result = self.rule.evaluate(_make_tx(amount=14_500.0), history=history)
        assert result.rule_id == "R17"
        assert result.rule_name == "Smurfing / Structuring"
        assert result.weight == 15
        assert result.details["window_hours"] == 2
        assert result.details["threshold"] == 5
        assert result.details["current_in_range"] is True
        assert result.details["current_amount"] == 14_500.0
        assert "€13,500" in result.details["amount_range"]
        assert "€14,999" in result.details["amount_range"]

    def test_no_trigger_severity_is_none(self):
        """When the rule does not trigger, severity must be None."""
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.severity is None

    def test_trigger_severity_is_strong(self):
        """Structuring is a serious AML red flag → STRONG severity."""
        history = _structuring_history(count=4)
        result = self.rule.evaluate(_make_tx(amount=14_000.0), history=history)
        assert result.severity == Severity.STRONG
