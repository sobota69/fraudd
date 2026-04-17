"""R12 – Z-Score Amount.

Trigger if the transaction amount's z-score exceeds 3 relative to the
customer's 30-day transaction history.
z-score = (amount − mean_amount) / std_amount
Severity: STRONG (2) | Weight: 12 | Optional
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import List, Optional

from transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

_LOOKBACK_DAYS = 30
_ZSCORE_THRESHOLD = 3.0
_MIN_HISTORY = 2  # need at least 2 data points to compute std


class R12ZscoreAmount(BaseRule):
    rule_id = "R12"
    rule_name = "Z-Score Amount"
    category = "Anomaly"
    weight = 12
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the Z-Score Amount rule.

        Steps
        -----
        1. Filter history to same customer_id within 30-day window.
        2. Need ≥ 2 historical transactions to compute a meaningful std.
        3. Compute mean and population std of historical amounts.
        4. z = (current amount − mean) / std
        5. If z > 3 → STRONG trigger.
        """

        if not history:
            return self._no_trigger()

        cutoff = transaction.transaction_timestamp - timedelta(days=_LOOKBACK_DAYS)
        amounts = [
            tx.amount for tx in history
            if tx.customer_id == transaction.customer_id
            and tx.transaction_id != transaction.transaction_id
            and tx.transaction_timestamp >= cutoff
        ]

        if len(amounts) < _MIN_HISTORY:
            return self._no_trigger()

        mean = sum(amounts) / len(amounts)
        variance = sum((a - mean) ** 2 for a in amounts) / len(amounts)
        std = math.sqrt(variance)

        if std == 0:
            return self._no_trigger()

        zscore = (transaction.amount - mean) / std

        if zscore > _ZSCORE_THRESHOLD:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.STRONG,
                weight=self.weight,
                details={
                    "amount": transaction.amount,
                    "mean_30d": round(mean, 2),
                    "std_30d": round(std, 2),
                    "zscore": round(zscore, 2),
                    "threshold": _ZSCORE_THRESHOLD,
                    "history_count": len(amounts),
                },
            )

        return self._no_trigger()

    def _no_trigger(self) -> RuleResult:
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=None,
            weight=self.weight,
        )
