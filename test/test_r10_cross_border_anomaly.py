"""Unit tests for R10 – Cross-Border Anomaly rule."""

import pytest
from datetime import datetime, timezone

from transaction.transaction import Transaction
from rules.r10_cross_border_anomaly import R10CrossBorderAnomaly, _country_from_iban
from rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)


def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    amount: float = 20000.0,
    beneficiary_account: str = "FR7630006000011234567890189",
    **kwargs,
) -> Transaction:
    defaults = dict(
        transaction_id=tx_id,
        transaction_timestamp=NOW,
        customer_id=customer_id,
        customer_account="PL00000000000000000000000",
        channel="Mobile",
        device_id="MOB-IOS-AAAA",
        amount=amount,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account=beneficiary_account,
        entered_beneficiary_name="John Doe",
        official_beneficiary_account_name="John Doe",
        customer_account_balance=50000.0,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _history_with_countries(countries: list[str], customer_id: int = 100) -> list[Transaction]:
    """Create history where each tx sends to a given country IBAN."""
    return [
        _make_tx(
            tx_id=f"TX-HIST-{i}",
            customer_id=customer_id,
            amount=100.0,
            beneficiary_account=f"{cc}0000000000000000000000",
        )
        for i, cc in enumerate(countries)
    ]


# ── helper function tests ────────────────────────────────────────────────

class TestCountryFromIban:
    def test_valid_iban(self):
        assert _country_from_iban("DE358075381662562554536117") == "DE"

    def test_lowercase_normalised(self):
        assert _country_from_iban("de358075381662562554536117") == "DE"

    def test_empty_string(self):
        assert _country_from_iban("") is None

    def test_none_value(self):
        assert _country_from_iban(None) is None

    def test_too_short(self):
        assert _country_from_iban("D") is None

    def test_numeric_prefix(self):
        assert _country_from_iban("12345678901234") is None


# ── rule tests ────────────────────────────────────────────────────────────

class TestR10CrossBorderAnomaly:

    def setup_method(self):
        self.rule = R10CrossBorderAnomaly()

    # ── no-trigger cases ──────────────────────────────────────────────

    def test_amount_below_threshold_does_not_trigger(self):
        tx = _make_tx(amount=14999.99, beneficiary_account="FR7630006000011234567890189")
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    def test_amount_exactly_at_threshold_does_not_trigger(self):
        tx = _make_tx(amount=15000.0, beneficiary_account="FR7630006000011234567890189")
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    def test_country_already_seen_does_not_trigger(self):
        """FR already in history → no trigger even if amount > 15k."""
        tx = _make_tx(amount=20000.0, beneficiary_account="FR7630006000011234567890189")
        history = _history_with_countries(["FR", "DE"])
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False

    def test_invalid_iban_does_not_trigger(self):
        tx = _make_tx(amount=20000.0, beneficiary_account="1234567890")
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    def test_different_customer_history_ignored(self):
        """History from another customer shouldn't count as 'seen'."""
        tx = _make_tx(customer_id=100, amount=20000.0, beneficiary_account="FR0000000000000000000000")
        history = _history_with_countries(["FR"], customer_id=999)
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True  # FR not seen for customer 100

    # ── trigger cases ─────────────────────────────────────────────────

    def test_new_country_high_amount_triggers(self):
        """New country + amount > 15k → MILD trigger."""
        tx = _make_tx(amount=20000.0, beneficiary_account="GB0000000000000000000000")
        history = _history_with_countries(["FR", "DE"])
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.severity == Severity.MILD
        assert result.details["destination_country"] == "GB"

    def test_no_history_and_high_amount_triggers(self):
        """No history at all → country is new by definition."""
        tx = _make_tx(amount=16000.0, beneficiary_account="FR0000000000000000000000")
        result = self.rule.evaluate(tx, history=None)
        assert result.triggered is True

    def test_empty_history_and_high_amount_triggers(self):
        tx = _make_tx(amount=16000.0, beneficiary_account="FR0000000000000000000000")
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is True

    def test_just_above_threshold_triggers(self):
        tx = _make_tx(amount=15000.01, beneficiary_account="IT0000000000000000000000")
        result = self.rule.evaluate(tx, history=_history_with_countries(["DE"]))
        assert result.triggered is True

    # ── edge cases ────────────────────────────────────────────────────

    def test_current_tx_excluded_from_seen_countries(self):
        """Current tx should not count itself when building seen set."""
        tx = _make_tx(tx_id="TX-CURRENT", amount=20000.0, beneficiary_account="GB0000000000000000000000")
        history = [
            tx,  # same tx in history – should be excluded
            *_history_with_countries(["FR", "DE"]),
        ]
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert result.details["destination_country"] == "GB"

    def test_case_insensitive_iban_country(self):
        """Lowercase IBAN prefix in history should still match."""
        tx = _make_tx(amount=20000.0, beneficiary_account="FR0000000000000000000000")
        history = [
            _make_tx(tx_id="TX-HIST-0", amount=100.0, beneficiary_account="fr0000000000000000000000"),
        ]
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is False  # FR seen (case-insensitive)

    def test_many_countries_seen_new_one_triggers(self):
        tx = _make_tx(amount=50000.0, beneficiary_account="JP0000000000000000000000")
        history = _history_with_countries(["FR", "DE", "GB", "IT", "ES", "NL", "PL"])
        result = self.rule.evaluate(tx, history=history)
        assert result.triggered is True
        assert "JP" not in result.details["previously_seen_countries"]

    # ── metadata ──────────────────────────────────────────────────────

    def test_result_metadata(self):
        tx = _make_tx(amount=20000.0, beneficiary_account="GB0000000000000000000000")
        history = _history_with_countries(["FR", "DE"])
        result = self.rule.evaluate(tx, history=history)
        assert result.rule_id == "R10"
        assert result.rule_name == "Cross-Border Anomaly"
        assert result.weight == 8
        assert result.details["amount"] == 20000.0
        assert result.details["amount_threshold"] == 15000.0
        assert sorted(result.details["previously_seen_countries"]) == ["DE", "FR"]

    def test_no_trigger_severity_is_none(self):
        tx = _make_tx(amount=10000.0)
        result = self.rule.evaluate(tx, history=[])
        assert result.severity is None
