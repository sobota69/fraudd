"""R22 – Absolute High-Value Threshold.

Trigger if amount > €15,000.
Severity: STRONG (2) | Weight: 10 | Mandatory
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R22AbsoluteHighValue(BaseRule):
    rule_id = "R22"
    rule_name = "Absolute High-Value Threshold"
    category = "Threshold"
    weight = 10
    mandatory = True

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: check if amount > 15000
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
