"""R10 – Cross-Border Anomaly.

Trigger if destination country is new for customer AND amount > €15,000.
Severity: MILD (1) | Weight: 8 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R10CrossBorderAnomaly(BaseRule):
    rule_id = "R10"
    rule_name = "Cross-Border Anomaly"
    category = "Anomaly"
    weight = 8
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: extract country from IBAN, check if new for customer, and amount > 15000
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
