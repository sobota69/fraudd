"""Unit tests for R18 – Round Amounts Anomaly rule.

The rule detects patterns of round-number payments (multiples of €10) that
are typical of scam-instructed transfers.  Victims are often told to send
exact round sums like €500, €1,000, or €2,000.

Rule specification (from HACKATHON_FRAML_RULES.xlsx)
----------------------------------------------------
* Round amount definition : amount is a positive multiple of 10.
* Time window             : 48 hours, ending at the current transaction.
* Minimum round-tx count  : 3 (including the current tx if round).
* Filters by              : customer_id.
* Severity                : STRONG (2).
* Weight                  : 3.
* Mandatory               : Yes.
"""

import pytest
from datetime import datetime, timedelta, timezone

from transaction.transaction import Transaction
from rules.r18_round_amounts_anomaly import R18RoundAmountsAnomaly, _is_round_amount
from rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 500.0,
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


def _round_history(
    count: int,
    customer_id: int = 100,
    amount: float = 500.0,
    hours_apart: float = 2.0,
) -> list[Transaction]:
    """Create *count* history transactions with a round amount."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            amount=amount,
            timestamp=NOW - timedelta(hours=hours_apart * (i + 1)),
        )
        for i in range(count)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Tests for _is_round_amount helper
# ═══════════════════════════════════════════════════════════════════════════

class TestIsRoundAmount:
    """Validate what counts as a 'round' amount (positive multiple of 10)."""

    def test_ten_is_round(self):
        """€10 is the smallest round amount."""
        assert _is_round_amount(10.0) is True

    def test_hundred_is_round(self):
        assert _is_round_amount(100.0) is True

    def test_thousand_is_round(self):
        assert _is_round_amount(1000.0) is True

    def test_large_round(self):
        """€14,990 is a multiple of 10."""
        assert _is_round_amount(14_990.0) is True

    def test_non_round_with_cents(self):
        """€99.99 has cents → not a multiple of 10."""
        assert _is_round_amount(99.99) is False

    def test_five_is_not_round(self):
        """€5 is not a multiple of 10."""
        assert _is_round_amount(5.0) is False

    def test_one_is_not_round(self):
        assert _is_round_amount(1.0) is False

    def test_zero_is_not_round(self):
        """€0 is technically a multiple of 10 but not a positive amount."""
        assert _is_round_amount(0.0) is False

    def test_negative_round_not_counted(self):
        """Negative amounts (refunds?) should not count as round."""
        assert _is_round_amount(-100.0) is False

    def test_point_one_not_round(self):
        """€0.10 — a very small non-round amount."""
        assert _is_round_amount(0.10) is False


# ═══════════════════════════════════════════════════════════════════════════
# Rule tests
# ═══════════════════════════════════════════════════════════════════════════

class TestR18RoundAmountsAnomaly:

    def setup_method(self):
        self.rule = R18RoundAmountsAnomaly()

    # ── Section 1: No-trigger – insufficient data ─────────────────────

    def test_no_history_does_not_trigger(self):
        """A single round-amount transaction cannot reach the threshold
        of 3 without history."""
        result = self.rule.evaluate(_make_tx(amount=500.0), history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(amount=500.0), history=[])
        assert result.triggered is False

    # ── Section 2: No-trigger – wrong customer ────────────────────────

    def test_different_customer_ignored(self):
        """Round-amount transactions from another customer must not count
        towards the target customer's total."""
        history = _round_history(count=5, customer_id=999)
        result = self.rule.evaluate(
            _make_tx(customer_id=100, amount=500.0), history=history
        )
        assert result.triggered is False

    # ── Section 3: No-trigger – non-round amounts ─────────────────────

    def test_non_round_amounts_do_not_count(self):
        """5 transactions with non-round amounts (€99.99) within 48h
        should NOT trigger, no matter how many there are."""
        history = [
            _make_tx(
                tx_id=f"TX-NR-{i}",
                amount=99.99,
                timestamp=NOW - timedelta(hours=i + 1),
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=99.99), history=history)
        assert result.triggered is False

    # ── Section 4: No-trigger – below threshold ───────────────────────

    def test_two_total_does_not_trigger(self):
        """1 round history + 1 round current = 2 → below threshold of 3."""
        history = _round_history(count=1)
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is False

    def test_two_round_history_current_not_round(self):
        """2 round history + current non-round = 2 → no trigger."""
        history = _round_history(count=2)
        result = self.rule.evaluate(_make_tx(amount=99.99), history=history)
        assert result.triggered is False

    # ── Section 5: No-trigger – outside 48h window ────────────────────

    def test_history_outside_48h_not_counted(self):
        """Round transactions older than 48h must be excluded."""
        history = [
            _make_tx(
                tx_id=f"TX-OLD-{i}",
                amount=500.0,
                timestamp=NOW - timedelta(hours=49 + i),
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is False

    # ── Section 6: Trigger – basic cases ──────────────────────────────

    def test_exactly_3_round_triggers(self):
        """2 round history + 1 round current = 3 → minimum trigger.
        This is the boundary case for the rule."""
        history = _round_history(count=2)
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is True
        assert result.severity == Severity.STRONG
        assert result.details["round_tx_count"] == 3

    def test_more_than_3_triggers(self):
        """5 round history + 1 round current = 6 → clearly triggers."""
        history = _round_history(count=5)
        result = self.rule.evaluate(_make_tx(amount=1000.0), history=history)
        assert result.triggered is True
        assert result.details["round_tx_count"] == 6

    def test_three_round_history_current_not_round(self):
        """3 round history + non-round current = 3 → still triggers.
        The current transaction doesn't have to be round itself."""
        history = _round_history(count=3)
        result = self.rule.evaluate(_make_tx(amount=99.99), history=history)
        assert result.triggered is True
        assert result.details["current_is_round"] is False

    # ── Section 7: Trigger – various round amounts ────────────────────

    def test_different_round_amounts(self):
        """Each transaction has a different round amount — the rule doesn't
        require the same amount, just that each is a multiple of 10."""
        history = [
            _make_tx(tx_id="TX-A", amount=100.0, timestamp=NOW - timedelta(hours=1)),
            _make_tx(tx_id="TX-B", amount=2000.0, timestamp=NOW - timedelta(hours=2)),
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is True

    def test_ten_euro_amounts(self):
        """€10 is the smallest qualifying round amount. Three €10
        transactions should trigger."""
        history = _round_history(count=2, amount=10.0)
        result = self.rule.evaluate(_make_tx(amount=10.0), history=history)
        assert result.triggered is True

    # ── Section 8: Edge cases – time window boundaries ────────────────

    def test_tx_exactly_at_48h_boundary_included(self):
        """A transaction exactly 48 hours before NOW should be included
        (boundary is inclusive)."""
        history = [
            _make_tx(tx_id=f"TX-EDGE-{i}", amount=500.0, timestamp=NOW - timedelta(hours=48))
            for i in range(2)
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is True

    def test_tx_one_second_outside_48h_excluded(self):
        """A transaction 48h + 1s before NOW should be excluded."""
        history = [
            _make_tx(
                tx_id=f"TX-OUT-{i}",
                amount=500.0,
                timestamp=NOW - timedelta(hours=48, seconds=1),
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.triggered is False

    # ── Section 9: Edge cases – deduplication ─────────────────────────

    def test_current_tx_not_double_counted(self):
        """If the current tx appears in history it must only be counted
        once (as the 'current' contribution)."""
        current = _make_tx(tx_id="TX-CURRENT", amount=500.0)
        history = [current, *_round_history(count=1)]
        # 1 history + 1 current = 2 → no trigger
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is False

    # ── Section 10: Edge cases – future transactions ──────────────────

    def test_future_tx_excluded(self):
        """Transactions timestamped after the current one must not count."""
        future = [
            _make_tx(tx_id=f"TX-FUT-{i}", amount=500.0, timestamp=NOW + timedelta(hours=i + 1))
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=future)
        assert result.triggered is False

    # ── Section 11: Edge cases – mixed round and non-round ────────────

    def test_mixed_round_and_non_round_only_round_counted(self):
        """History with a mix of round and non-round amounts.  Only the
        round ones should contribute to the count."""
        round_txs = _round_history(count=2, amount=500.0)
        non_round = [
            _make_tx(
                tx_id=f"TX-NR-{i}",
                amount=123.45,
                timestamp=NOW - timedelta(hours=i + 1),
            )
            for i in range(10)
        ]
        history = round_txs + non_round
        # 2 round history + 1 round current = 3 → triggers
        result = self.rule.evaluate(_make_tx(amount=1000.0), history=history)
        assert result.triggered is True
        assert result.details["round_tx_count"] == 3

    # ── Section 12: Edge cases – mixed customers ──────────────────────

    def test_only_same_customer_counted(self):
        """Round transactions from another customer must not inflate
        the target customer's count."""
        matching = _round_history(count=1, customer_id=100)
        other = _round_history(count=10, customer_id=999)
        for i, tx in enumerate(other):
            tx.transaction_id = f"TX-OTHER-{i}"
        # 1 matching history + 1 current = 2 → no trigger
        result = self.rule.evaluate(
            _make_tx(customer_id=100, amount=500.0),
            history=matching + other,
        )
        assert result.triggered is False

    # ── Section 13: Edge cases – partial window coverage ──────────────

    def test_some_inside_some_outside_window(self):
        """2 round tx inside 48h + 3 round tx outside 48h.
        Only 2 inside + current = 3 → triggers."""
        inside = _round_history(count=2, hours_apart=10)
        outside = [
            _make_tx(tx_id=f"TX-FAR-{i}", amount=500.0, timestamp=NOW - timedelta(hours=50 + i))
            for i in range(3)
        ]
        result = self.rule.evaluate(_make_tx(amount=500.0), history=inside + outside)
        assert result.triggered is True
        assert result.details["round_tx_count"] == 3

    # ── Section 14: Metadata ──────────────────────────────────────────

    def test_result_metadata_on_trigger(self):
        """Verify all expected detail fields on a triggered result."""
        history = _round_history(count=2, amount=1000.0)
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.rule_id == "R18"
        assert result.rule_name == "Round Amounts Anomaly"
        assert result.weight == 3
        assert result.details["window_hours"] == 48
        assert result.details["threshold"] == 3
        assert result.details["current_is_round"] is True
        assert result.details["current_amount"] == 500.0

    def test_no_trigger_severity_is_none(self):
        """When the rule does not trigger, severity must be None."""
        result = self.rule.evaluate(_make_tx(amount=500.0), history=[])
        assert result.severity is None

    def test_trigger_severity_is_strong(self):
        """Round-amount patterns are scam indicators → STRONG severity."""
        history = _round_history(count=2)
        result = self.rule.evaluate(_make_tx(amount=500.0), history=history)
        assert result.severity == Severity.STRONG
