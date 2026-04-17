"""R18 – Round Amounts Anomaly.

Trigger if ≥3 round-number (multiple of 10) transactions in 48h.
Severity: STRONG (2) | Weight: 3 | Mandatory
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R18RoundAmountsAnomaly(BaseRule):
    rule_id = "R18"
    rule_name = "Round Amounts Anomaly"
    category = "FRAML"
    weight = 3
    mandatory = True

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: check if amount is multiple of 10 and count in 48h window
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
