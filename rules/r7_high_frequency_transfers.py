"""R7 – High Frequency of Transfers.

Trigger if ≥5 transactions in 10 minutes.
Severity: STRONG (2) | Weight: 10 | Mandatory
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R7HighFrequencyTransfers(BaseRule):
    rule_id = "R7"
    rule_name = "High Frequency of Transfers"
    category = "Velocity"
    weight = 10
    mandatory = True

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: count transactions in 10-minute window for customer_id
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
