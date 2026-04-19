"""Unit tests for ThresholdGroup – R22 (Absolute High-Value) and R24 (Channel-Specific).

Both rules only inspect the current transaction (no history). The group
evaluates them in a single pass.
"""

import pytest
from datetime import datetime, timezone

from domain.transaction import Transaction
from domain.rules.group.threshold_group import ThresholdGroup
from domain.rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


def _make_tx(
    amount: float = 100.0,
    channel: str = "Mobile",
    **kwargs,
) -> Transaction:
    defaults = dict(
        transaction_id="TX-THR",
        transaction_timestamp=NOW,
        customer_id=100,
        customer_account="PL00000000000000000000000",
        channel=channel,
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


def _result_by_id(results, rule_id):
    return next(r for r in results if r.rule_id == rule_id)


# ═══════════════════════════════════════════════════════════════════════════════
# Group structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestThresholdGroupStructure:
    group = ThresholdGroup()

    def test_returns_two_results(self):
        results = self.group.evaluate(_make_tx())
        assert len(results) == 2

    def test_result_ids_match(self):
        ids = {r.rule_id for r in self.group.evaluate(_make_tx())}
        assert ids == {"R22", "R24"}


# ═══════════════════════════════════════════════════════════════════════════════
# R22 – Absolute High-Value Threshold (amount > €15,000)
# ═══════════════════════════════════════════════════════════════════════════════

class TestR22AbsoluteHighValue:
    """R22 triggers when amount exceeds €15,000."""

    group = ThresholdGroup()

    def test_below_threshold_does_not_trigger(self):
        r22 = _result_by_id(self.group.evaluate(_make_tx(amount=14_999.0)), "R22")
        assert r22.triggered is False

    def test_at_threshold_does_not_trigger(self):
        """Exactly €15,000 should NOT trigger (condition is strictly greater)."""
        r22 = _result_by_id(self.group.evaluate(_make_tx(amount=15_000.0)), "R22")
        assert r22.triggered is False

    def test_above_threshold_triggers(self):
        r22 = _result_by_id(self.group.evaluate(_make_tx(amount=15_001.0)), "R22")
        assert r22.triggered is True
        assert r22.severity == Severity.STRONG
        assert r22.weight == 10

    def test_large_amount_triggers(self):
        r22 = _result_by_id(self.group.evaluate(_make_tx(amount=100_000.0)), "R22")
        assert r22.triggered is True

    def test_details_contain_amount_and_threshold(self):
        r22 = _result_by_id(self.group.evaluate(_make_tx(amount=20_000.0)), "R22")
        assert r22.details["amount"] == 20_000.0
        assert r22.details["threshold"] == 15_000.0


# ═══════════════════════════════════════════════════════════════════════════════
# R24 – Channel-Specific Threshold
# ═══════════════════════════════════════════════════════════════════════════════

class TestR24ChannelSpecific:
    """R24 triggers when amount exceeds the channel's safe maximum.

    Thresholds: Mobile €2k, Web €5k, Phone €1k, ATM €1k, Branch €10k, Corp API €25k.
    """

    group = ThresholdGroup()

    # ── Mobile (€2,000) ──────────────────────────────────────────────────────
    def test_mobile_below_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=1_999, channel="Mobile")), "R24")
        assert r24.triggered is False

    def test_mobile_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=2_001, channel="Mobile")), "R24")
        assert r24.triggered is True
        assert r24.severity == Severity.MILD
        assert r24.weight == 5

    # ── Web (€5,000) ─────────────────────────────────────────────────────────
    def test_web_below_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=4_999, channel="Web")), "R24")
        assert r24.triggered is False

    def test_web_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=5_001, channel="Web")), "R24")
        assert r24.triggered is True

    # ── Phone (€1,000) ───────────────────────────────────────────────────────
    def test_phone_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=1_500, channel="Phone")), "R24")
        assert r24.triggered is True

    # ── ATM (€1,000) ─────────────────────────────────────────────────────────
    def test_atm_below_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=999, channel="ATM")), "R24")
        assert r24.triggered is False

    def test_atm_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=1_001, channel="ATM")), "R24")
        assert r24.triggered is True

    # ── Branch (€10,000) ─────────────────────────────────────────────────────
    def test_branch_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=10_500, channel="Branch")), "R24")
        assert r24.triggered is True

    # ── Corporate API (€25,000) ──────────────────────────────────────────────
    def test_corporate_api_below_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=24_999, channel="Corporate API")), "R24")
        assert r24.triggered is False

    def test_corporate_api_above_threshold(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=25_001, channel="Corporate API")), "R24")
        assert r24.triggered is True

    # ── Unknown channel ──────────────────────────────────────────────────────
    def test_unknown_channel_does_not_trigger(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=999_999, channel="Carrier Pigeon")), "R24")
        assert r24.triggered is False

    # ── Details ──────────────────────────────────────────────────────────────
    def test_details_contain_channel_info(self):
        r24 = _result_by_id(self.group.evaluate(_make_tx(amount=3_000, channel="Mobile")), "R24")
        assert r24.details["channel"] == "Mobile"
        assert r24.details["channel_threshold"] == 2_000


# ═══════════════════════════════════════════════════════════════════════════════
# Both rules evaluated together
# ═══════════════════════════════════════════════════════════════════════════════

class TestBothThresholds:
    group = ThresholdGroup()

    def test_both_trigger_on_large_mobile_tx(self):
        """€20k on Mobile exceeds both €15k absolute and €2k mobile limit."""
        results = self.group.evaluate(_make_tx(amount=20_000, channel="Mobile"))
        r22 = _result_by_id(results, "R22")
        r24 = _result_by_id(results, "R24")
        assert r22.triggered is True
        assert r24.triggered is True

    def test_neither_triggers_on_small_tx(self):
        results = self.group.evaluate(_make_tx(amount=50, channel="Web"))
        assert all(not r.triggered for r in results)

    def test_only_r24_triggers_for_moderate_mobile_tx(self):
        """€3k on Mobile exceeds channel limit but not €15k absolute."""
        results = self.group.evaluate(_make_tx(amount=3_000, channel="Mobile"))
        r22 = _result_by_id(results, "R22")
        r24 = _result_by_id(results, "R24")
        assert r22.triggered is False
        assert r24.triggered is True
