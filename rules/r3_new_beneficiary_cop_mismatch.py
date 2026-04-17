"""R3 – New Beneficiary + CoP Mismatch.

Trigger if is_new_beneficiary = True AND similarity < 90%.
Severity: MILD (1) | Weight: 10 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R3NewBeneficiaryCopMismatch(BaseRule):
    rule_id = "R3"
    rule_name = "New Beneficiary + CoP Mismatch"
    category = "Confirmation of Payee (CoP)"
    weight = 10
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: check is_new_beneficiary flag AND name similarity < 90%
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.MILD,
            weight=self.weight,
        )
