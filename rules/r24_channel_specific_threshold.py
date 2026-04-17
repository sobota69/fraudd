"""R24 – Channel-Specific Threshold.

Mobile: €2,000; Web: €5,000; Phone: €1,000; ATM: €1,000; Branch: €10,000; Corporate API: €25,000.
Severity: MILD (1) | Weight: 5 | Mandatory
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


CHANNEL_THRESHOLDS = {
    "Mobile": 2_000,
    "Web": 5_000,
    "Phone": 1_000,
    "ATM": 1_000,
    "Branch": 10_000,
    "Corporate API": 25_000,
}


class R24ChannelSpecificThreshold(BaseRule):
    rule_id = "R24"
    rule_name = "Channel-Specific Threshold"
    category = "Threshold"
    weight = 5
    mandatory = True

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: check amount against channel-specific threshold
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
