"""R8 – New Payees Burst.

Trigger if ≥3 transactions with is_new_beneficiary=True from the same customer
within a 24-hour window ending at the current transaction.
Severity: MILD (1) | Weight: 8 | Optional
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from src.transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity
from .bisect_helpers import window_slice

_WINDOW_HOURS = 24
_MIN_NEW_PAYEES = 3


class R8NewPayeesBurst(BaseRule):
    rule_id = "R8"
    rule_name = "New Payees Burst"
    category = "Velocity"
    weight = 8
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the New Payees Burst rule.

        Steps
        -----
        1. Check if the current transaction itself is to a new beneficiary.
        2. Define a 24-hour window ending at the current transaction.
        3. Filter history to same customer_id, within window, with
           is_new_beneficiary=True (excluding current tx).
        4. Total new-payee count = filtered history + (1 if current is new).
        5. If count ≥ 3 → MILD trigger.
        """

        current_is_new = bool(transaction.is_new_beneficiary)

        if not history:
            # At most 1 new payee (the current tx) – can never reach 3
            return self._no_trigger()

        window_start = transaction.transaction_timestamp - timedelta(hours=_WINDOW_HOURS)

        new_payee_txs = [
            tx for tx in window_slice(
                history, window_start, transaction.transaction_timestamp,
                exclude_id=transaction.transaction_id,
            )
            if tx.is_new_beneficiary
        ]

        total_new = len(new_payee_txs) + (1 if current_is_new else 0)

        if total_new >= _MIN_NEW_PAYEES:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.MILD,
                weight=self.weight,
                details={
                    "new_payee_count_in_window": total_new,
                    "window_hours": _WINDOW_HOURS,
                    "threshold": _MIN_NEW_PAYEES,
                    "current_is_new_beneficiary": current_is_new,
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
