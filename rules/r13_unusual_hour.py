"""R13 – Unusual Hour.

Trigger if transaction hour is outside customer's usual 90% activity window.
Minimum 10 transactions history required.
Severity: MILD (1) | Weight: 5 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R13UnusualHour(BaseRule):
    rule_id = "R13"
    rule_name = "Unusual Hour"
    category = "Anomaly"
    weight = 5
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: determine 90% activity window and flag if outside
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
