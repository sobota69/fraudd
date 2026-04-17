"""R2 – CoP Name Mismatch – Soft Warning.

Trigger if similarity between 80–89%; allow but warn.
Severity: MILD (1) | Weight: 5 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R2CopNameMismatchSoft(BaseRule):
    rule_id = "R2"
    rule_name = "CoP Name Mismatch – Soft Warning"
    category = "Confirmation of Payee (CoP)"
    weight = 5
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: implement soft-warning similarity logic (80-89%)
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
