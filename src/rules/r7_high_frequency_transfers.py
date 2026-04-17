"""R7 – High Frequency of Transfers.

Trigger if ≥5 transactions from the same customer within a 10-minute window
ending at (and including) the current transaction.
Severity: STRONG (2) | Weight: 10 | Mandatory
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from src.transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

# Window size and minimum transaction count (including the current one)
_WINDOW_MINUTES = 10
_MIN_TX_COUNT = 5


class R7HighFrequencyTransfers(BaseRule):
    rule_id = "R7"
    rule_name = "High Frequency of Transfers"
    category = "Velocity"
    weight = 10
    mandatory = True

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the High Frequency of Transfers rule.

        Steps
        -----
        1. Define a 10-minute window ending at the current transaction's
           timestamp.
        2. Filter *history* to transactions from the same ``customer_id``
           that fall within that window (excluding the current tx itself).
        3. Count those transactions **+ 1** (for the current tx).
        4. If count ≥ 5 → STRONG trigger.
        """

        if not history:
            return self._no_trigger()

        window_start = transaction.transaction_timestamp - timedelta(minutes=_WINDOW_MINUTES)

        recent_txs = [
            tx for tx in history
            if tx.transaction_id != transaction.transaction_id
            and window_start <= tx.transaction_timestamp <= transaction.transaction_timestamp
        ]

        # total count = historical matches + the current transaction itself
        total_count = len(recent_txs) + 1

        if total_count >= _MIN_TX_COUNT:
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.STRONG,
                weight=self.weight,
                details={
                    "transaction_count_in_window": total_count,
                    "window_minutes": _WINDOW_MINUTES,
                    "threshold": _MIN_TX_COUNT,
                },
            )

        return self._no_trigger()

    # ── helper ────────────────────────────────────────────────────────────
    def _no_trigger(self) -> RuleResult:
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=None,
            weight=self.weight,
        )
