"""R13 – Unusual Hour.

Trigger if the transaction hour falls outside the customer's usual activity
window that covers ≥ 90% of their historical transactions.
Minimum 10 historical transactions required; otherwise the rule does not trigger.

Algorithm
---------
1. Collect all hours (0–23) from the customer's past transactions.
2. Count transactions per hour.
3. Find the *smallest contiguous window* (wrapping around midnight) that
   covers ≥ 90 % of all historical transactions.
4. If the current transaction's hour is NOT inside that window → trigger.

Severity: MILD (1) | Weight: 5 | Optional
"""

from __future__ import annotations

from collections import Counter
from typing import List, Optional, Tuple

from transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

_MIN_HISTORY = 10
_COVERAGE_PCT = 0.90


def find_smallest_90pct_window(hour_counts: Counter) -> Tuple[int, int, int]:
    """Return (start_hour, window_size, covered_tx_count) of the smallest
    contiguous window of hours (wrapping at 24) that covers ≥ 90 % of total
    transactions.

    Hours are 0–23.  A window_size of e.g. 5 starting at hour 22 means
    hours {22, 23, 0, 1, 2}.
    """
    total = sum(hour_counts.values())
    target = total * _COVERAGE_PCT

    best_start = 0
    best_size = 24  # worst case: full day

    for start in range(24):
        covered = 0
        for size in range(1, 25):
            h = (start + size - 1) % 24
            covered += hour_counts.get(h, 0)
            if covered >= target:
                if size < best_size:
                    best_size = size
                    best_start = start
                break

    return best_start, best_size, total


def hour_in_window(hour: int, start: int, size: int) -> bool:
    """Check if *hour* falls inside the contiguous window [start, start+size)
    on a 24-hour clock."""
    for i in range(size):
        if (start + i) % 24 == hour:
            return True
    return False


class R13UnusualHour(BaseRule):
    rule_id = "R13"
    rule_name = "Unusual Hour"
    category = "Anomaly"
    weight = 5
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        if not history:
            return self._no_trigger()

        # Filter to same customer, exclude current tx
        customer_txs = [
            tx for tx in history
            if tx.customer_id == transaction.customer_id
            and tx.transaction_id != transaction.transaction_id
        ]

        if len(customer_txs) < _MIN_HISTORY:
            return self._no_trigger()

        hour_counts = Counter(tx.transaction_timestamp.hour for tx in customer_txs)
        start, size, _ = find_smallest_90pct_window(hour_counts)
        tx_hour = transaction.transaction_timestamp.hour

        if not hour_in_window(tx_hour, start, size):
            return RuleResult(
                rule_id=self.rule_id,
                rule_name=self.rule_name,
                triggered=True,
                severity=Severity.MILD,
                weight=self.weight,
                details={
                    "transaction_hour": tx_hour,
                    "window_start": start,
                    "window_size": size,
                    "window_hours": [(start + i) % 24 for i in range(size)],
                    "history_count": len(customer_txs),
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
