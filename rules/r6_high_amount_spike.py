"""R6 – High Amount Spike.

Trigger if amount > 3× the customer's 30-day average transaction amount.
Severity MILD(1) when multiplier is between 3× and 10×.
Severity STRONG(2) when multiplier is > 10×.
Weight: 8 | Optional
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

# How far back to look for the customer's average spending
_LOOKBACK_DAYS = 30
# Multiplier thresholds
_MILD_THRESHOLD = 3.0
_STRONG_THRESHOLD = 10.0


class R6HighAmountSpike(BaseRule):
    rule_id = "R6"
    rule_name = "High Amount Spike"
    category = "Velocity"
    weight = 8
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the High Amount Spike rule.

        Steps
        -----
        1. Filter *history* to transactions from the same ``customer_id``
           within the last 30 days relative to the current transaction.
        2. Compute the average transaction amount over that window.
        3. If the current ``amount`` exceeds 3× the average → MILD trigger.
           If it exceeds 10× the average → STRONG trigger.
        4. If there is no qualifying history, the rule does not trigger
           (not enough data to determine a spike).
        """

        # ── guard: need history to compute an average ────────────────────
        if not history:
            return self._no_trigger()

        # ── 1. filter by customer and 30-day window ──────────────────────
        cutoff = transaction.transaction_timestamp - timedelta(days=_LOOKBACK_DAYS)
        customer_txs = [
            tx for tx in history
            if tx.customer_id == transaction.customer_id
            and tx.transaction_timestamp >= cutoff
            and tx.transaction_id != transaction.transaction_id
        ]

        if not customer_txs:
            return self._no_trigger()

        # ── 2. compute 30-day average amount ─────────────────────────────
        avg_amount = sum(tx.amount for tx in customer_txs) / len(customer_txs)

        if avg_amount == 0:
            return self._no_trigger()

        # ── 3. compare current amount against thresholds ─────────────────
        multiplier = transaction.amount / avg_amount

        if multiplier > _STRONG_THRESHOLD:
            severity = Severity.STRONG
            threshold = _STRONG_THRESHOLD
        elif multiplier > _MILD_THRESHOLD:
            severity = Severity.MILD
            threshold = _MILD_THRESHOLD
        else:
            return self._no_trigger()

        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=True,
            severity=severity,
            weight=self.weight,
            details={
            "amount": transaction.amount,
            "avg_amount_30d": round(avg_amount, 2),
            "multiplier": round(multiplier, 2),
            "threshold": f">{threshold}×",
            "history_count": len(customer_txs),
            },
        )

        # ── 4. no spike detected ─────────────────────────────────────────
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
