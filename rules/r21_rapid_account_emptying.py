"""R21 – Rapid Account Emptying.

Trigger if balance drop > 70% in 1h (filtered by customer_account / IBAN).
Severity: STRONG (2) | Weight: 20 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R21RapidAccountEmptying(BaseRule):
    rule_id = "R21"
    rule_name = "Rapid Account Emptying"
    category = "FRAML"
    weight = 20
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: calculate balance drop ratio over 1h window by customer_account
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
