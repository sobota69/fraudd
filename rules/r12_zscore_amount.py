"""R12 – Z-Score Amount.

Trigger if z-score > 3 (amount deviates strongly from customer's 30-day pattern).
Severity: STRONG (2) | Weight: 12 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R12ZscoreAmount(BaseRule):
    rule_id = "R12"
    rule_name = "Z-Score Amount"
    category = "Anomaly"
    weight = 12
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: z = (amount - mean) / std over 30-day window
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
