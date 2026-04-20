from datetime import datetime

import pytest

from domain.rules.bisect_helpers import history_before, window_slice
from domain.transaction import Transaction


def _tx(id: str, ts: str, **kw) -> Transaction:
    defaults = dict(
        transaction_id=id,
        transaction_timestamp=datetime.fromisoformat(ts),
        customer_id=1,
        customer_account="NL01BANK0001",
        channel="web",
        device_id="d1",
        amount=100.0,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account="NL02BANK0002",
        entered_beneficiary_name="Bob",
        official_beneficiary_account_name="Bob",
        customer_account_balance=5000.0,
    )
    defaults.update(kw)
    return Transaction(**defaults)


# --- Sorted history used across tests ---
HISTORY = [
    _tx("t1", "2026-04-20T08:00:00"),
    _tx("t2", "2026-04-20T09:00:00"),
    _tx("t3", "2026-04-20T10:00:00"),
    _tx("t4", "2026-04-20T11:00:00"),
    _tx("t5", "2026-04-20T12:00:00"),
]


# ── window_slice ────────────────────────────────────────────


class TestWindowSlice:
    def test_full_range(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T08:00:00"),
            datetime.fromisoformat("2026-04-20T12:00:00"),
        )
        assert [tx.transaction_id for tx in result] == ["t1", "t2", "t3", "t4", "t5"]

    def test_partial_range(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T09:00:00"),
            datetime.fromisoformat("2026-04-20T11:00:00"),
        )
        assert [tx.transaction_id for tx in result] == ["t2", "t3", "t4"]

    def test_no_match(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T13:00:00"),
            datetime.fromisoformat("2026-04-20T14:00:00"),
        )
        assert result == []

    def test_exclude_id(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T09:00:00"),
            datetime.fromisoformat("2026-04-20T11:00:00"),
            exclude_id="t3",
        )
        assert [tx.transaction_id for tx in result] == ["t2", "t4"]

    def test_single_match(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T10:00:00"),
            datetime.fromisoformat("2026-04-20T10:00:00"),
        )
        assert [tx.transaction_id for tx in result] == ["t3"]

    def test_empty_history(self):
        assert window_slice([], datetime(2026, 1, 1), datetime(2026, 12, 31)) == []

    def test_exclude_id_none(self):
        result = window_slice(
            HISTORY,
            datetime.fromisoformat("2026-04-20T10:00:00"),
            datetime.fromisoformat("2026-04-20T10:00:00"),
            exclude_id=None,
        )
        assert [tx.transaction_id for tx in result] == ["t3"]


# ── history_before ──────────────────────────────────────────


class TestHistoryBefore:
    def test_returns_all_from_cutoff(self):
        result = history_before(HISTORY, datetime.fromisoformat("2026-04-20T10:00:00"))
        assert [tx.transaction_id for tx in result] == ["t3", "t4", "t5"]

    def test_cutoff_before_all(self):
        result = history_before(HISTORY, datetime.fromisoformat("2026-04-20T07:00:00"))
        assert [tx.transaction_id for tx in result] == ["t1", "t2", "t3", "t4", "t5"]

    def test_cutoff_after_all(self):
        result = history_before(HISTORY, datetime.fromisoformat("2026-04-20T13:00:00"))
        assert result == []

    def test_exclude_id(self):
        result = history_before(
            HISTORY,
            datetime.fromisoformat("2026-04-20T10:00:00"),
            exclude_id="t4",
        )
        assert [tx.transaction_id for tx in result] == ["t3", "t5"]

    def test_empty_history(self):
        assert history_before([], datetime(2026, 1, 1)) == []
