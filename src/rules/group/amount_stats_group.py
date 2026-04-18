"""Amount Stats Group – R6, R12.

Filters the customer's 30-day transaction history **once**, computes
mean / std / count, then evaluates both rules from those shared stats.

R6 – High Amount Spike:  amount > 3× mean → MILD, > 10× mean → STRONG.
R12 – Z-Score Amount:    z-score > 3 → STRONG.
"""

from __future__ import annotations

import math
from datetime import timedelta
from typing import List, Optional

from src.rules.base_rule import RuleResult, Severity
from src.rules.bisect_helpers import history_before
from src.transaction.transaction import Transaction

_LOOKBACK_DAYS = 30
_MILD_MULTIPLIER = 3.0
_STRONG_MULTIPLIER = 10.0
_ZSCORE_THRESHOLD = 3.0
_MIN_HISTORY_ZSCORE = 2


class AmountStatsGroup:
    """Evaluates R6 and R12 from a single 30-day history scan."""

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> list[RuleResult]:
        # ── Shared: filter 30-day amounts ─────────────────────────────────
        amounts: list[float] = []
        if history:
            cutoff = transaction.transaction_timestamp - timedelta(days=_LOOKBACK_DAYS)
            amounts = [
                tx.amount for tx in
                history_before(history, cutoff, exclude_id=transaction.transaction_id)
            ]

        n = len(amounts)

        # Compute stats once
        if n > 0:
            s = sum(amounts)
            s2 = sum(a * a for a in amounts)
            mean = s / n
            variance = s2 / n - mean * mean
            std = math.sqrt(max(0.0, variance))
        else:
            mean = 0.0
            std = 0.0

        results: list[RuleResult] = []

        # ── R6 – High Amount Spike ────────────────────────────────────────
        r6_triggered = False
        r6_severity = None
        r6_details: dict = {}
        if n > 0 and mean > 0:
            multiplier = transaction.amount / mean
            if multiplier > _STRONG_MULTIPLIER:
                r6_triggered = True
                r6_severity = Severity.STRONG
            elif multiplier > _MILD_MULTIPLIER:
                r6_triggered = True
                r6_severity = Severity.MILD
            if r6_triggered:
                r6_details = {
                    "amount": transaction.amount,
                    "avg_amount_30d": round(mean, 2),
                    "multiplier": round(multiplier, 2),
                    "history_count": n,
                }

        results.append(RuleResult(
            rule_id="R6",
            rule_name="High Amount Spike",
            triggered=r6_triggered,
            severity=r6_severity,
            weight=8,
            details=r6_details,
        ))

        # ── R12 – Z-Score Amount ──────────────────────────────────────────
        r12_triggered = False
        r12_details: dict = {}
        if n >= _MIN_HISTORY_ZSCORE and std > 0:
            zscore = (transaction.amount - mean) / std
            if zscore > _ZSCORE_THRESHOLD:
                r12_triggered = True
                r12_details = {
                    "amount": transaction.amount,
                    "mean_30d": round(mean, 2),
                    "std_30d": round(std, 2),
                    "zscore": round(zscore, 2),
                    "threshold": _ZSCORE_THRESHOLD,
                    "history_count": n,
                }

        results.append(RuleResult(
            rule_id="R12",
            rule_name="Z-Score Amount",
            triggered=r12_triggered,
            severity=Severity.STRONG if r12_triggered else None,
            weight=12,
            details=r12_details,
        ))

        return results
