"""R18 – Round Amounts Anomaly.

Multiple round-number payments are indicative of scam instructions where
victims are told to send exact round sums (e.g. €500, €1,000).

Trigger condition
-----------------
≥ 3 transactions whose amount is a multiple of 10 (€) from the same customer
within a 48-hour window ending at the current transaction.

Severity: STRONG (2) | Weight: 3 | Mandatory
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from domain.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity
from .bisect_helpers import window_slice

_WINDOW_HOURS = 48
_MIN_ROUND_COUNT = 3


def _is_round_amount(amount: float) -> bool:
    """Return True if *amount* is a multiple of 10."""
    return amount > 0 and amount % 10 == 0


class R18RoundAmountsAnomaly(BaseRule):
    rule_id = "R18"
    rule_name = "Round Amounts Anomaly"
    category = "FRAML"
    weight = 3
    mandatory = True

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        current_is_round = _is_round_amount(transaction.amount)

        if not history:
            return self._no_trigger()

        window_start = transaction.transaction_timestamp - timedelta(hours=_WINDOW_HOURS)

        round_txs = [
            tx for tx in window_slice(
                history, window_start, transaction.transaction_timestamp,
                exclude_id=transaction.transaction_id,
            )
            if _is_round_amount(tx.amount)
        ]

        total = len(round_txs) + (1 if current_is_round else 0)

        if total >= _MIN_ROUND_COUNT:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.STRONG,
                weight=self.weight,
                details={
                    "round_tx_count": total,
                    "window_hours": _WINDOW_HOURS,
                    "threshold": _MIN_ROUND_COUNT,
                    "current_is_round": current_is_round,
                    "current_amount": transaction.amount,
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
