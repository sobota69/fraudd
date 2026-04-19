"""Threshold Group – R22, R24.

Both rules only inspect the current transaction (no history required).
Evaluates them in a single pass.

R22 – Absolute High-Value: amount > €15,000 → STRONG.
R24 – Channel-Specific:   amount > channel limit → MILD.
"""

from __future__ import annotations

from typing import List, Optional

from domain.rules.base_rule import RuleResult, Severity
from domain.transaction import Transaction

_ABSOLUTE_THRESHOLD = 15_000.0

_CHANNEL_THRESHOLDS: dict[str, float] = {
    "Mobile": 2_000,
    "Web": 5_000,
    "Phone": 1_000,
    "ATM": 1_000,
    "Branch": 10_000,
    "Corporate API": 25_000,
}


class ThresholdGroup:
    """Evaluates R22 and R24 from the current transaction only."""

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> list[RuleResult]:
        results: list[RuleResult] = []

        # ── R22 – Absolute High-Value ─────────────────────────────────────
        r22_triggered = transaction.amount > _ABSOLUTE_THRESHOLD
        results.append(RuleResult(
            rule_id="R22",
            rule_name="Absolute High-Value Threshold",
            triggered=r22_triggered,
            severity=Severity.STRONG if r22_triggered else None,
            weight=10,
            details={
                "amount": transaction.amount,
                "threshold": _ABSOLUTE_THRESHOLD,
            } if r22_triggered else {},
        ))

        # ── R24 – Channel-Specific Threshold ──────────────────────────────
        channel = transaction.channel
        limit = _CHANNEL_THRESHOLDS.get(channel)
        r24_triggered = limit is not None and transaction.amount > limit
        results.append(RuleResult(
            rule_id="R24",
            rule_name="Channel-Specific Threshold",
            triggered=r24_triggered,
            severity=Severity.MILD if r24_triggered else None,
            weight=5,
            details={
                "amount": transaction.amount,
                "channel": channel,
                "channel_threshold": limit,
            } if r24_triggered else {},
        ))

        return results
