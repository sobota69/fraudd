"""Risk calculator – turns a list of RuleResults into a single risk verdict."""

from __future__ import annotations

from typing import List

from domain.rules.base_rule import RuleResult
from domain.transaction import Transaction
from domain.risk.risk_assessment import RiskAssessment


class RiskCalculator:
    """Compute an aggregate risk score from individual rule results."""

    FRAUD_THRESHOLD: float = 30.0

    def calculate_risk(
        self,
        rule_results: List[RuleResult],
        transaction: Transaction,
    ) -> RiskAssessment:
        triggered: List[str] = []
        raw_score: float = 0.0

        for rr in rule_results:
            if rr.triggered:
                triggered.append(rr.rule_id)
                severity_val = rr.severity.value if rr.severity else 1
                raw_score += severity_val * rr.weight

        risk_score = min(100.0, round(raw_score, 2))

        if risk_score >= 70:
            risk_category = "HIGH"
        elif risk_score >= 30:
            risk_category = "MEDIUM"
        else:
            risk_category = "LOW"

        return RiskAssessment(
            transaction_id=transaction.transaction_id,
            triggered_rules=";".join(triggered),
            is_fraud_transaction=str(risk_score >= self.FRAUD_THRESHOLD),
            risk_score=risk_score,
            risk_category=risk_category,
        )
