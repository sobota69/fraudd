"""R21 – Rapid Account Emptying.

Large proportion of an account's balance withdrawn quickly suggests coercion
or account takeover.

**Important**: this rule filters history by ``customer_account`` (IBAN), NOT
by ``customer_id``, because the balance belongs to a specific bank account.

Trigger condition
-----------------
Balance drops by more than 70 % within a 1-hour window ending at the
current transaction.

Algorithm
---------
1. Consider a 1-hour window *prior* to the current transaction.
2. Find the last transaction on the **same account** that occurred *before*
   the window start.  Its ``customer_account_balance`` is ``balance_before``.
3. If no such transaction exists, infer:
       balance_before = current_balance + current_amount
   (i.e. what the balance was just before this single transaction).
4. ``balance_after`` = current transaction's ``customer_account_balance``.
5. ``drop_ratio`` = (balance_before − balance_after) / balance_before.
6. If drop_ratio > 0.70 → STRONG trigger.

Severity: STRONG (2) | Weight: 20 | Optional
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from src.transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

_WINDOW_HOURS = 1
_DROP_THRESHOLD = 0.70


class R21RapidAccountEmptying(BaseRule):
    rule_id = "R21"
    rule_name = "Rapid Account Emptying"
    category = "FRAML"
    weight = 20
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        balance_after = transaction.customer_account_balance
        window_start = transaction.transaction_timestamp - timedelta(hours=_WINDOW_HOURS)

        # ── determine balance_before ─────────────────────────────────
        balance_before: Optional[float] = None

        if history:
            # Transactions on the SAME account that happened BEFORE the window
            pre_window_txs = [
                tx for tx in history
                if tx.customer_account == transaction.customer_account
                and tx.transaction_id != transaction.transaction_id
                and tx.transaction_timestamp < window_start
            ]
            if pre_window_txs:
                latest = max(pre_window_txs, key=lambda tx: tx.transaction_timestamp)
                balance_before = latest.customer_account_balance

        # Fallback: infer from current transaction
        if balance_before is None:
            balance_before = balance_after + transaction.amount

        # ── guard: cannot compute meaningful ratio ────────────────────
        if balance_before <= 0:
            return self._no_trigger()

        drop_ratio = (balance_before - balance_after) / balance_before

        if drop_ratio > _DROP_THRESHOLD:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.STRONG,
                weight=self.weight,
                details={
                    "balance_before": round(balance_before, 2),
                    "balance_after": round(balance_after, 2),
                    "drop_ratio": round(drop_ratio, 4),
                    "drop_threshold": _DROP_THRESHOLD,
                    "window_hours": _WINDOW_HOURS,
                    "customer_account": transaction.customer_account,
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
