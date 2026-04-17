"""R1 – CoP Name Mismatch – Hard Fail.

Trigger if similarity score between entered_beneficiary_name and
official_beneficiary_account_name is < 80%.
Severity: STRONG (2) | Weight: 25 | Mandatory
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R1CopNameMismatchHard(BaseRule):
    rule_id = "R1"
    rule_name = "CoP Name Mismatch – Hard Fail"
    category = "Confirmation of Payee (CoP)"
    weight = 25
    mandatory = True

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: implement name similarity logic (normalise, tokenise, filter, compare, score)
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=Severity.STRONG,
            weight=self.weight,
        )
