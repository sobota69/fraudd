"""R8 – New Payees Burst.

Trigger if ≥3 new payees in 24h.
Severity: MILD (1) | Weight: 8 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R8NewPayeesBurst(BaseRule):
    rule_id = "R8"
    rule_name = "New Payees Burst"
    category = "Velocity"
    weight = 8
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: count new beneficiaries in 24h window for customer_id
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
