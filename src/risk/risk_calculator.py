"""Risk calculator – turns a list of RuleResults into a single risk verdict."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from src.rules.base_rule import RuleResult, Severity
from src.transaction.transaction import Transaction


@dataclass
class RiskAssessment:
    """Final risk verdict for a single transaction.

    Fields match the required output specification:
      - transaction_id : str
      - triggered_rules: semicolon-separated rule IDs (empty string if none)
      - is_fraud_transaction: "True" / "False"
      - risk_score: float 0.0 – 100.0
      - risk_category: LOW / MEDIUM / HIGH
    """
    transaction_id: str
    triggered_rules: str          # semicolon-separated
    is_fraud_transaction: str     # "True" or "False"
    risk_score: float
    risk_category: str            # LOW | MEDIUM | HIGH


class RiskCalculator:
    """Compute an aggregate risk score from individual rule results.

    Scoring formula (from spec):
        risk_score = min(100, Σ severity_i × weight_i)

    Category bands:
        LOW    =  0 – 29
        MEDIUM = 30 – 69
        HIGH   = 70 – 100

    Fraud threshold is configurable (default ≥ 30).
    """

    FRAUD_THRESHOLD: float = 30.0

    # ── public API ────────────────────────────────────────────────────────
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

        # Category bands per spec
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