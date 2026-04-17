"""R6 – High Amount Spike.

Trigger if amount > 3× the customer's 30-day average. STRONG if > 10×.
Severity: MILD/STRONG | Weight: 8 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R6HighAmountSpike(BaseRule):
    rule_id = "R6"
    rule_name = "High Amount Spike"
    category = "Velocity"
    weight = 8
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: compare amount to 30-day average for customer_id
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
