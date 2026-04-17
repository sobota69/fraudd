"""R17 – Smurfing / Structuring.

Trigger if ≥5 transactions between €13,500–€14,999 in 2h.
Weight: 15 | Optional
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

from .base_rule import BaseRule, RuleResult, Severity


class R17SmurfingStructuring(BaseRule):
    rule_id = "R17"
    rule_name = "Smurfing / Structuring"
    category = "FRAML"
    weight = 15
    mandatory = False

    def evaluate(self, transaction: pd.Series, history: Optional[pd.DataFrame] = None) -> RuleResult:
        # TODO: count tx in €13,500–€14,999 range within 2h window
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            weight=self.weight,
        )
