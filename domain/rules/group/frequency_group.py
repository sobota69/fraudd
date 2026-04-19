"""Frequency Group – rules that check transaction frequency.

R7  – High Frequency Transfers.
R8  – New Payees Burst.
R17 – Smurfing / Structuring.
"""

from __future__ import annotations

from datetime import timedelta
from typing import List, Optional

from domain.rules.base_rule import RuleResult, Severity
from domain.transaction import Transaction

_LOOKBACK_DAYS = 1


class FrequencyGroup:
    """Evaluates frequency-based rules from a single history scan."""

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> list[RuleResult]:
        results: list[RuleResult] = []

        # ── R7 – High Frequency Transfers ────────────────────────────────
        _R7_WINDOW_MINUTES = 10
        _R7_THRESHOLD = 5

        window_start = transaction.transaction_timestamp - timedelta(minutes=_R7_WINDOW_MINUTES)
        transactions_in_last_10_min = sum(
            1 for tx in (history or [])
            if tx.transaction_id != transaction.transaction_id
            and tx.customer_id == transaction.customer_id
            and tx.transaction_timestamp >= window_start
            and tx.transaction_timestamp < transaction.transaction_timestamp
        )

        r7_triggered = transactions_in_last_10_min >= _R7_THRESHOLD
        results.append(RuleResult(
            rule_id="R7",
            rule_name="High Frequency Transfers",
            triggered=r7_triggered,
            severity=Severity.STRONG if r7_triggered else None,
            weight=10,
            details={
                "tx_count_last_10_min": transactions_in_last_10_min,
                "threshold": _R7_THRESHOLD,
            } if r7_triggered else {},
        ))

        # ── R8 – New Payees Burst ─────────────────────────────────────────
        _R8_WINDOW_HOURS = 24
        _R8_THRESHOLD = 3

        r8_window_start = transaction.transaction_timestamp - timedelta(hours=_R8_WINDOW_HOURS)
        new_payees_in_24h = sum(
            1 for tx in (history or [])
            if tx.transaction_id != transaction.transaction_id
            and tx.customer_id == transaction.customer_id
            and tx.transaction_timestamp >= r8_window_start
            and tx.transaction_timestamp < transaction.transaction_timestamp
            and tx.is_new_beneficiary
        )

        r8_triggered = new_payees_in_24h >= _R8_THRESHOLD
        results.append(RuleResult(
            rule_id="R8",
            rule_name="New Payees Burst",
            triggered=r8_triggered,
            severity=Severity.MILD if r8_triggered else None,
            weight=8,
            details={
                "new_payees_last_24h": new_payees_in_24h,
                "threshold": _R8_THRESHOLD,
            } if r8_triggered else {},
        ))

        # ── R17 – Smurfing / Structuring ──────────────────────────────────
        _R17_WINDOW_HOURS = 2
        _R17_THRESHOLD = 5
        _R17_AMOUNT_MIN = 13500.00
        _R17_AMOUNT_MAX = 14999.00

        r17_window_start = transaction.transaction_timestamp - timedelta(hours=_R17_WINDOW_HOURS)
        structured_tx_count = sum(
            1 for tx in (history or [])
            if tx.transaction_id != transaction.transaction_id
            and tx.customer_id == transaction.customer_id
            and tx.transaction_timestamp >= r17_window_start
            and tx.transaction_timestamp < transaction.transaction_timestamp
            and _R17_AMOUNT_MIN <= tx.amount <= _R17_AMOUNT_MAX
        )

        r17_triggered = structured_tx_count >= _R17_THRESHOLD
        results.append(RuleResult(
            rule_id="R17",
            rule_name="Smurfing / Structuring",
            triggered=r17_triggered,
            severity=Severity.STRONG if r17_triggered else None,
            weight=15,
            details={
                "structured_tx_count_last_2h": structured_tx_count,
                "amount_range": f"{_R17_AMOUNT_MIN}–{_R17_AMOUNT_MAX}",
                "threshold": _R17_THRESHOLD,
            } if r17_triggered else {},
        ))

        return results
