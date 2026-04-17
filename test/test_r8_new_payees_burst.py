"""Unit tests for R8 – New Payees Burst rule."""

import pytest
from datetime import datetime, timedelta, timezone

from transaction.transaction import Transaction
from rules.r8_new_payees_burst import R8NewPayeesBurst
from rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 50.0,
    timestamp: datetime = NOW,
    is_new_beneficiary: bool = True,
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
        is_new_beneficiary=is_new_beneficiary,
        beneficiary_account="DE00000000000000000000000",
        entered_beneficiary_name="John Doe",
        official_beneficiary_account_name="John Doe",
        customer_account_balance=50000.0,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _make_new_payee_history(
    count: int,
    customer_id: int = 100,
    hours_apart: float = 1.0,
    is_new: bool = True,
) -> list[Transaction]:
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i:04d}",
            customer_id=customer_id,
            timestamp=NOW - timedelta(hours=hours_apart * (i + 1)),
            is_new_beneficiary=is_new,
        )
        for i in range(count)
    ]


class TestR8NewPayeesBurst:

    def setup_method(self):
        self.rule = R8NewPayeesBurst()

    # ── no-trigger cases ──────────────────────────────────────────────

    def test_no_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(), history=None)
        assert result.triggered is False

    def test_empty_history_does_not_trigger(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.triggered is False

    def test_different_customer_does_not_trigger(self):
        history = _make_new_payee_history(count=5, customer_id=999)
        result = self.rule.evaluate(_make_tx(customer_id=100), history=history)
        assert result.triggered is False

    def test_existing_beneficiaries_do_not_trigger(self):
        """All history has is_new_beneficiary=False → no trigger."""
        history = _make_new_payee_history(count=5, is_new=False)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=False), history=history)
        assert result.triggered is False

    def test_two_new_payees_total_does_not_trigger(self):
        """1 history new + 1 current new = 2 → below threshold."""
        history = _make_new_payee_history(count=1)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.triggered is False

    def test_history_outside_24h_does_not_trigger(self):
        """All history > 24h old → no trigger."""
        history = [
            _make_tx(
                tx_id=f"TX-OLD-{i}",
                timestamp=NOW - timedelta(hours=25 + i),
                is_new_beneficiary=True,
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(), history=history)
        assert result.triggered is False

    def test_current_not_new_and_only_2_history_new(self):
        """Current is existing + 2 new in history = 2 → no trigger."""
        history = _make_new_payee_history(count=2)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=False), history=history)
        assert result.triggered is False

    # ── trigger cases ─────────────────────────────────────────────────

    def test_exactly_3_new_payees_triggers(self):
        """2 history new + 1 current new = 3 → triggers."""
        history = _make_new_payee_history(count=2)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD
        assert result.details["new_payee_count_in_window"] == 3

    def test_more_than_3_triggers(self):
        """5 history new + 1 current new = 6 → triggers."""
        history = _make_new_payee_history(count=5)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.triggered is True
        assert result.details["new_payee_count_in_window"] == 6

    def test_current_not_new_but_3_history_new_triggers(self):
        """Current is existing + 3 new in history = 3 → triggers."""
        history = _make_new_payee_history(count=3)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=False), history=history)
        assert result.triggered is True
        assert result.details["new_payee_count_in_window"] == 3
        assert result.details["current_is_new_beneficiary"] is False

    def test_mixed_new_and_existing_in_history(self):
        """3 new + 3 existing in history, current new = 4 new total → triggers."""
        new = _make_new_payee_history(count=3, is_new=True)
        existing = _make_new_payee_history(count=3, is_new=False)
        # fix duplicate ids
        for i, tx in enumerate(existing):
            tx.transaction_id = f"TX-EXIST-{i}"
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=new + existing)
        assert result.triggered is True
        assert result.details["new_payee_count_in_window"] == 4

    def test_only_same_customer_counted(self):
        """2 new for customer 100, 5 new for customer 999 → only 3 total with current."""
        matching = _make_new_payee_history(count=2, customer_id=100)
        other = _make_new_payee_history(count=5, customer_id=999)
        for i, tx in enumerate(other):
            tx.transaction_id = f"TX-OTHER-{i}"
        result = self.rule.evaluate(
            _make_tx(customer_id=100, is_new_beneficiary=True),
            history=matching + other,
        )
        assert result.triggered is True
        assert result.details["new_payee_count_in_window"] == 3

    # ── boundary / edge cases ─────────────────────────────────────────

    def test_tx_exactly_at_24h_boundary_included(self):
        """Transaction exactly 24h before NOW should be included."""
        history = [
            _make_tx(tx_id=f"TX-EDGE-{i}", timestamp=NOW - timedelta(hours=24), is_new_beneficiary=True)
            for i in range(2)
        ]
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.triggered is True
        assert result.details["new_payee_count_in_window"] == 3

    def test_tx_just_outside_24h_excluded(self):
        """Transactions 24h + 1sec before NOW should be excluded."""
        history = [
            _make_tx(
                tx_id=f"TX-OUT-{i}",
                timestamp=NOW - timedelta(hours=24, seconds=1),
                is_new_beneficiary=True,
            )
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.triggered is False

    def test_current_tx_not_double_counted(self):
        """Current tx in history should be excluded from history count."""
        current = _make_tx(tx_id="TX-CURRENT", is_new_beneficiary=True)
        history = [
            current,  # duplicate
            *_make_new_payee_history(count=1),
        ]
        # 1 history + 1 current = 2 → no trigger
        result = self.rule.evaluate(current, history=history)
        assert result.triggered is False

    def test_future_tx_in_history_excluded(self):
        """Transactions after current tx timestamp should not count."""
        future = [
            _make_tx(tx_id=f"TX-FUT-{i}", timestamp=NOW + timedelta(hours=i + 1), is_new_beneficiary=True)
            for i in range(5)
        ]
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=future)
        assert result.triggered is False

    # ── metadata ──────────────────────────────────────────────────────

    def test_result_metadata_on_trigger(self):
        history = _make_new_payee_history(count=2)
        result = self.rule.evaluate(_make_tx(is_new_beneficiary=True), history=history)
        assert result.rule_id == "R8"
        assert result.rule_name == "New Payees Burst"
        assert result.weight == 8
        assert result.details["window_hours"] == 24
        assert result.details["threshold"] == 3

    def test_no_trigger_severity_is_none(self):
        result = self.rule.evaluate(_make_tx(), history=[])
        assert result.severity is None
