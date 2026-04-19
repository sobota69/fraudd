"""Unit tests for RiskCalculator."""

import pytest
from datetime import datetime

from domain.risk import RiskCalculator, RiskAssessment
from domain.rules.base_rule import RuleResult, Severity
from domain.transaction import Transaction


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tx(tx_id: str = "TX-001") -> Transaction:
    return Transaction(
        transaction_id=tx_id,
        transaction_timestamp=datetime(2026, 1, 1, 12, 0),
        customer_id=1,
        customer_account="NL00TEST0000000001",
        channel="ONLINE",
        device_id="D-1",
        amount=100.0,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account="NL00BENF0000000001",
        entered_beneficiary_name="Test",
        official_beneficiary_account_name="Test",
        customer_account_balance=5000.0,
    )


def _rr(rule_id: str, triggered: bool, severity: Severity = Severity.MILD, weight: int = 10) -> RuleResult:
    return RuleResult(
        rule_id=rule_id,
        rule_name=f"Rule {rule_id}",
        triggered=triggered,
        severity=severity,
        weight=weight,
    )


@pytest.fixture
def calc() -> RiskCalculator:
    return RiskCalculator()


# ── No rules triggered ───────────────────────────────────────────────────────

class TestNoTriggers:
    def test_no_rules_at_all(self, calc):
        result = calc.calculate_risk([], _tx())
        assert result.risk_score == 0.0
        assert result.triggered_rules == ""
        assert result.is_fraud_transaction == "False"
        assert result.risk_category == "LOW"

    def test_all_rules_not_triggered(self, calc):
        rules = [_rr("R1", False), _rr("R2", False), _rr("R3", False)]
        result = calc.calculate_risk(rules, _tx())
        assert result.risk_score == 0.0
        assert result.triggered_rules == ""
        assert result.is_fraud_transaction == "False"
        assert result.risk_category == "LOW"


# ── Triggered rules string ───────────────────────────────────────────────────

class TestTriggeredRules:
    def test_single_triggered_rule(self, calc):
        result = calc.calculate_risk([_rr("R6", True, Severity.MILD, 8)], _tx())
        assert result.triggered_rules == "R6"

    def test_multiple_triggered_semicolon_separated(self, calc):
        rules = [_rr("R1", True, Severity.MILD, 5), _rr("R2", False), _rr("R7", True, Severity.STRONG, 10)]
        result = calc.calculate_risk(rules, _tx())
        assert result.triggered_rules == "R1;R7"

    def test_empty_when_none_triggered(self, calc):
        result = calc.calculate_risk([_rr("R1", False)], _tx())
        assert result.triggered_rules == ""


# ── Score formula: min(100, Σ severity.value × weight) ───────────────────────

class TestScoreFormula:
    def test_mild_severity(self, calc):
        # MILD=1, weight=20 → score=20
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 20)], _tx())
        assert result.risk_score == 20.0

    def test_strong_severity(self, calc):
        # STRONG=2, weight=15 → score=30
        result = calc.calculate_risk([_rr("R1", True, Severity.STRONG, 15)], _tx())
        assert result.risk_score == 30.0

    def test_sum_of_multiple_rules(self, calc):
        # MILD×10 + STRONG×20 = 10 + 40 = 50
        rules = [_rr("R1", True, Severity.MILD, 10), _rr("R2", True, Severity.STRONG, 20)]
        result = calc.calculate_risk(rules, _tx())
        assert result.risk_score == 50.0

    def test_non_triggered_not_counted(self, calc):
        rules = [_rr("R1", True, Severity.MILD, 10), _rr("R2", False, Severity.STRONG, 50)]
        result = calc.calculate_risk(rules, _tx())
        assert result.risk_score == 10.0

    def test_capped_at_100(self, calc):
        rules = [_rr("R1", True, Severity.STRONG, 60), _rr("R2", True, Severity.STRONG, 50)]
        # 2×60 + 2×50 = 220 → capped to 100
        result = calc.calculate_risk(rules, _tx())
        assert result.risk_score == 100.0

    def test_exactly_100(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.STRONG, 50)], _tx())
        assert result.risk_score == 100.0

    def test_severity_none_defaults_to_1(self, calc):
        rr = RuleResult(rule_id="R1", rule_name="Test", triggered=True, severity=None, weight=25)
        result = calc.calculate_risk([rr], _tx())
        assert result.risk_score == 25.0


# ── Category bands ────────────────────────────────────────────────────────────

class TestCategoryBands:
    def test_low_at_zero(self, calc):
        result = calc.calculate_risk([], _tx())
        assert result.risk_category == "LOW"

    def test_low_at_29(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 29)], _tx())
        assert result.risk_category == "LOW"

    def test_medium_at_30(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 30)], _tx())
        assert result.risk_category == "MEDIUM"

    def test_medium_at_69(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 69)], _tx())
        assert result.risk_category == "MEDIUM"

    def test_high_at_70(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.STRONG, 35)], _tx())
        # 2×35 = 70
        assert result.risk_category == "HIGH"

    def test_high_at_100(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.STRONG, 60)], _tx())
        assert result.risk_category == "HIGH"


# ── Fraud threshold ───────────────────────────────────────────────────────────

class TestFraudThreshold:
    def test_below_threshold_is_false(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 29)], _tx())
        assert result.is_fraud_transaction == "False"

    def test_at_threshold_is_true(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 30)], _tx())
        assert result.is_fraud_transaction == "True"

    def test_above_threshold_is_true(self, calc):
        result = calc.calculate_risk([_rr("R1", True, Severity.STRONG, 50)], _tx())
        assert result.is_fraud_transaction == "True"

    def test_custom_threshold(self):
        calc = RiskCalculator()
        calc.FRAUD_THRESHOLD = 50.0
        result = calc.calculate_risk([_rr("R1", True, Severity.MILD, 45)], _tx())
        assert result.is_fraud_transaction == "False"
        result2 = calc.calculate_risk([_rr("R1", True, Severity.MILD, 50)], _tx())
        assert result2.is_fraud_transaction == "True"


# ── Transaction ID passthrough ────────────────────────────────────────────────

class TestTransactionId:
    def test_id_matches_input(self, calc):
        result = calc.calculate_risk([], _tx("TX-999"))
        assert result.transaction_id == "TX-999"


# ── Return type ───────────────────────────────────────────────────────────────

class TestReturnType:
    def test_returns_risk_assessment(self, calc):
        result = calc.calculate_risk([], _tx())
        assert isinstance(result, RiskAssessment)

    def test_is_fraud_is_string(self, calc):
        result = calc.calculate_risk([], _tx())
        assert isinstance(result.is_fraud_transaction, str)
        assert result.is_fraud_transaction in ("True", "False")

    def test_triggered_rules_is_string(self, calc):
        result = calc.calculate_risk([_rr("R1", True)], _tx())
        assert isinstance(result.triggered_rules, str)
