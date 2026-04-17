"""R17 – Smurfing / Structuring.

Repeated small payments just below the €15,000 reporting threshold to avoid
detection.  This is a classic anti-money-laundering (AML) pattern where
criminals split a large sum into multiple smaller transactions.

Trigger condition
-----------------
≥ 5 transactions with amounts in the range €13,500 – €14,999 (inclusive)
from the same customer within a 2-hour window ending at (and including)
the current transaction.

The current transaction itself counts towards the total if its amount also
falls inside the suspicious range.

Weight: 15 | Optional
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from src.transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

_WINDOW_HOURS = 2
_MIN_TX_COUNT = 5
_AMOUNT_LOW = 13_500.0
_AMOUNT_HIGH = 14_999.0


def _is_structuring_amount(amount: float) -> bool:
    """Return True if the amount falls in the suspicious structuring range."""
    return _AMOUNT_LOW <= amount <= _AMOUNT_HIGH


class R17SmurfingStructuring(BaseRule):
    rule_id = "R17"
    rule_name = "Smurfing / Structuring"
    category = "FRAML"
    weight = 15
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the Smurfing / Structuring rule.

        Steps
        -----
        1. Define a 2-hour window ending at the current transaction's
           timestamp.
        2. Filter *history* to the same ``customer_id``, within that window,
           excluding the current transaction, **and** with an amount in
           €13,500 – €14,999.
        3. If the current transaction's amount is also in range, add 1.
        4. If total count ≥ 5 → trigger.
        """

        current_in_range = _is_structuring_amount(transaction.amount)

        if not history:
            return self._no_trigger()

        window_start = transaction.transaction_timestamp - timedelta(hours=_WINDOW_HOURS)

        matching_txs = [
            tx for tx in history
            if tx.transaction_id != transaction.transaction_id
            and window_start <= tx.transaction_timestamp <= transaction.transaction_timestamp
            and _is_structuring_amount(tx.amount)
        ]

        total = len(matching_txs) + (1 if current_in_range else 0)

        if total >= _MIN_TX_COUNT:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.STRONG,
                weight=self.weight,
                details={
                    "structuring_tx_count": total,
                    "window_hours": _WINDOW_HOURS,
                    "threshold": _MIN_TX_COUNT,
                    "amount_range": f"€{_AMOUNT_LOW:,.0f} – €{_AMOUNT_HIGH:,.0f}",
                    "current_in_range": current_in_range,
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
