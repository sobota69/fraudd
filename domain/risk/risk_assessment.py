"""Data class for the final risk verdict of a single transaction."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RiskAssessment:
    """Final risk verdict for a single transaction."""
    transaction_id: str
    triggered_rules: str          # semicolon-separated
    is_fraud_transaction: str     # "True" or "False"
    risk_score: float
    risk_category: str            # LOW | MEDIUM | HIGH
