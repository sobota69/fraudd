"""Bisect helpers for sorted transaction histories.

All functions assume `history` is sorted by `transaction_timestamp` ascending.
They use binary search (O(log n)) to find the start of a time window,
avoiding full-list scans that cost O(n) per call.
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import datetime
from typing import List

from domain.transaction import Transaction


def _ts_key(tx: Transaction) -> datetime:
    return tx.transaction_timestamp


def window_slice(
    history: List[Transaction],
    window_start: datetime,
    window_end: datetime,
    exclude_id: str | None = None,
) -> List[Transaction]:
    """Return transactions in [window_start, window_end] excluding `exclude_id`.

    Uses bisect on the sorted list to skip irrelevant items.
    """
    # bisect_left on timestamps: find first index >= window_start
    lo = bisect_left(history, window_start, key=_ts_key)

    result: list[Transaction] = []
    for i in range(lo, len(history)):
        tx = history[i]
        if tx.transaction_timestamp > window_end:
            break
        if tx.transaction_id != exclude_id:
            result.append(tx)
    return result


def history_before(
    history: List[Transaction],
    cutoff: datetime,
    exclude_id: str | None = None,
) -> List[Transaction]:
    """Return transactions with timestamp >= cutoff, excluding `exclude_id`."""
    lo = bisect_left(history, cutoff, key=_ts_key)
    if exclude_id is None:
        return history[lo:]
    return [tx for tx in history[lo:] if tx.transaction_id != exclude_id]
