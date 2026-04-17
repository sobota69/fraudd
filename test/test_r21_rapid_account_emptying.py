"""Unit tests for R21 – Rapid Account Emptying rule.

The rule detects situations where a large proportion (> 70 %) of an account's
balance is withdrawn within a 1-hour window.  This pattern suggests coercion,
account takeover, or authorised-push-payment fraud.

Key design detail
-----------------
Unlike most other rules, R21 filters history by ``customer_account`` (IBAN),
**not** ``customer_id``, because the balance belongs to a specific bank
account.  A customer may hold several accounts.

Balance inference
-----------------
* If a transaction on the same account exists before the 1-hour window,
  its ``customer_account_balance`` is used as ``balance_before``.
* Otherwise: ``balance_before = balance_after + amount`` (i.e. what the
  balance was just before this single outgoing payment).

Rule specification (from HACKATHON_FRAML_RULES.xlsx)
----------------------------------------------------
* Window     : 1 hour
* Threshold  : balance drop > 70 %
* Severity   : STRONG (2)
* Weight     : 20
* Mandatory  : No (optional)
"""

import pytest
from datetime import datetime, timedelta, timezone

from transaction.transaction import Transaction
from rules.r21_rapid_account_emptying import R21RapidAccountEmptying
from rules.base_rule import Severity


NOW = datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc)
ACCOUNT_A = "PL270398487102963371148627"
ACCOUNT_B = "PL881208393661999813073093"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tx(
    tx_id: str = "TX-CURRENT",
    customer_id: int = 100,
    customer_account: str = ACCOUNT_A,
    amount: float = 4000.0,
    balance: float = 1000.0,
    timestamp: datetime = NOW,
    **kwargs,
) -> Transaction:
    defaults = dict(
        transaction_id=tx_id,
        transaction_timestamp=timestamp,
        customer_id=customer_id,
        customer_account=customer_account,
        channel="Mobile",
        device_id="MOB-IOS-AAAA",
        amount=amount,
        currency="EUR",
        is_new_beneficiary=False,
        beneficiary_account="DE00000000000000000000000",
        entered_beneficiary_name="John Doe",
        official_beneficiary_account_name="John Doe",
        customer_account_balance=balance,
    )
    defaults.update(kwargs)
    return Transaction(**defaults)


def _pre_window_tx(
    balance: float,
    hours_before: float = 2.0,
    account: str = ACCOUNT_A,
    tx_id: str = "TX-PRE",
) -> Transaction:
    """A transaction that occurred *before* the 1-hour window,
    serving as the reference for balance_before."""
    return _make_tx(
        tx_id=tx_id,
        customer_account=account,
        amount=50.0,
        balance=balance,
        timestamp=NOW - timedelta(hours=hours_before),
    )


# ═══════════════════════════════════════════════════════════════════════════
# Rule tests
# ═══════════════════════════════════════════════════════════════════════════

class TestR21RapidAccountEmptying:

    def setup_method(self):
        self.rule = R21RapidAccountEmptying()

    # ── Section 1: No-trigger – no history (fallback inference) ───────

    def test_no_history_small_drop_no_trigger(self):
        """No history → balance_before = balance_after + amount.
        balance_before = 4000 + 1000 = 5000, balance_after = 4000.
        Drop = 1000/5000 = 20 % → no trigger."""
        tx = _make_tx(amount=1000.0, balance=4000.0)
        result = self.rule.evaluate(tx, history=None)
        assert result.triggered is False

    def test_empty_history_small_drop_no_trigger(self):
        """Same as above but with an explicit empty list."""
        tx = _make_tx(amount=1000.0, balance=4000.0)
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    # ── Section 2: No-trigger – drop ≤ 70 % ──────────────────────────

    def test_exactly_70_pct_drop_does_not_trigger(self):
        """balance_before=5000, after=1500 → drop = 3500/5000 = 70 %.
        Must EXCEED 70 %, not equal → no trigger."""
        pre = _pre_window_tx(balance=5000.0)
        tx = _make_tx(amount=3500.0, balance=1500.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is False

    def test_50_pct_drop_does_not_trigger(self):
        """balance_before=10000, after=5000 → drop = 50 %."""
        pre = _pre_window_tx(balance=10000.0)
        tx = _make_tx(amount=5000.0, balance=5000.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is False

    def test_balance_increases_does_not_trigger(self):
        """If the balance actually went UP (incoming transfer), drop is
        negative → certainly no trigger."""
        pre = _pre_window_tx(balance=1000.0)
        tx = _make_tx(amount=0.0, balance=5000.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is False

    # ── Section 3: No-trigger – different account ─────────────────────

    def test_different_account_history_ignored(self):
        """History on ACCOUNT_B must not be used for balance_before
        calculation on ACCOUNT_A.  Falls back to inference."""
        pre = _pre_window_tx(balance=10000.0, account=ACCOUNT_B, tx_id="TX-B")
        # Inferred: balance_before = 1000 + 4000 = 5000
        # drop = 4000/5000 = 80 % → BUT only if wrong account used.
        # With correct fallback: balance_before = 1000 + 4000 = 5000 → 80 %
        # Actually this WILL trigger via fallback. Let's use a small amount.
        tx = _make_tx(amount=100.0, balance=4900.0, customer_account=ACCOUNT_A)
        result = self.rule.evaluate(tx, history=[pre])
        # Inferred: 4900 + 100 = 5000, drop = 100/5000 = 2% → no trigger
        assert result.triggered is False

    # ── Section 4: Trigger – basic cases ──────────────────────────────

    def test_71_pct_drop_triggers(self):
        """balance_before=5000, after=1450 → drop = 3550/5000 = 71 %.
        Just above threshold → triggers."""
        pre = _pre_window_tx(balance=5000.0)
        tx = _make_tx(amount=3550.0, balance=1450.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is True
        assert result.severity == Severity.STRONG

    def test_90_pct_drop_triggers(self):
        """balance_before=10000, after=1000 → drop = 90 %."""
        pre = _pre_window_tx(balance=10000.0)
        tx = _make_tx(amount=9000.0, balance=1000.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is True
        assert result.details["drop_ratio"] == 0.9

    def test_near_total_emptying(self):
        """balance_before=5000, after=100 → drop = 98 %.
        Classic coercion pattern: nearly everything withdrawn."""
        pre = _pre_window_tx(balance=5000.0)
        tx = _make_tx(amount=4900.0, balance=100.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is True

    def test_exact_example_from_spec(self):
        """From the FRAML rules spreadsheet example:
        At 11:00 balance was €5,000.  At 12:00 balance is €1,200.
        Drop = (5000 − 1200) / 5000 = 76 % → triggers."""
        pre = _make_tx(
            tx_id="TX-1100",
            customer_account=ACCOUNT_A,
            amount=200.0,
            balance=5000.0,
            timestamp=datetime(2025, 12, 19, 10, 30, 0, tzinfo=timezone.utc),
        )
        tx = _make_tx(
            amount=800.0,
            balance=1200.0,
            timestamp=datetime(2025, 12, 19, 12, 0, 0, tzinfo=timezone.utc),
        )
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is True
        assert result.details["drop_ratio"] == 0.76

    # ── Section 5: Trigger – fallback inference (no pre-window tx) ────

    def test_no_pre_window_tx_large_single_withdrawal(self):
        """No history before the window → balance_before = after + amount.
        amount=9000, after=1000 → before=10000, drop = 90 % → triggers."""
        tx = _make_tx(amount=9000.0, balance=1000.0)
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is True
        assert result.details["balance_before"] == 10000.0

    def test_inferred_balance_with_only_in_window_history(self):
        """History exists but all within the 1-hour window (none before).
        Falls back to inference."""
        in_window = _make_tx(
            tx_id="TX-INWIN",
            customer_account=ACCOUNT_A,
            amount=500.0,
            balance=3000.0,
            timestamp=NOW - timedelta(minutes=30),
        )
        tx = _make_tx(amount=8000.0, balance=1000.0)
        # Inferred: 1000 + 8000 = 9000, drop = 8000/9000 ≈ 88.9 %
        result = self.rule.evaluate(tx, history=[in_window])
        assert result.triggered is True

    # ── Section 6: Edge cases – multiple pre-window transactions ──────

    def test_most_recent_pre_window_tx_used(self):
        """When multiple transactions exist before the window, the most
        recent one should be used for balance_before."""
        older = _pre_window_tx(balance=20000.0, hours_before=5.0, tx_id="TX-OLD")
        newer = _pre_window_tx(balance=5000.0, hours_before=2.0, tx_id="TX-NEW")
        tx = _make_tx(amount=3600.0, balance=1400.0)
        # balance_before = 5000 (from newer), drop = 3600/5000 = 72 %
        result = self.rule.evaluate(tx, history=[older, newer])
        assert result.triggered is True
        assert result.details["balance_before"] == 5000.0

    def test_older_pre_window_tx_not_used(self):
        """If we incorrectly used the older tx (balance 20000), drop would
        be 93 %.  But with the correct (newer) one at 3000, drop = 
        (3000-2000)/3000 = 33 % → no trigger."""
        older = _pre_window_tx(balance=20000.0, hours_before=5.0, tx_id="TX-OLD")
        newer = _pre_window_tx(balance=3000.0, hours_before=2.0, tx_id="TX-NEW")
        tx = _make_tx(amount=1000.0, balance=2000.0)
        result = self.rule.evaluate(tx, history=[older, newer])
        assert result.triggered is False

    # ── Section 7: Edge cases – zero / negative balance_before ────────

    def test_zero_balance_before_does_not_trigger(self):
        """balance_before = 0 → cannot compute ratio → no trigger."""
        pre = _pre_window_tx(balance=0.0)
        tx = _make_tx(amount=0.0, balance=0.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.triggered is False

    def test_negative_inferred_balance_does_not_trigger(self):
        """Edge case: if somehow balance + amount ≤ 0 (overdraft?),
        rule should not trigger to avoid nonsensical ratios."""
        tx = _make_tx(amount=-100.0, balance=-500.0)
        # Inferred: -500 + (-100) = -600 → balance_before ≤ 0
        result = self.rule.evaluate(tx, history=[])
        assert result.triggered is False

    # ── Section 8: Edge cases – current tx excluded from history ──────

    def test_current_tx_not_used_as_pre_window(self):
        """If the current tx appears in history it should be excluded.
        Without this guard, the rule might use the current tx's own
        balance as balance_before, leading to 0 % drop."""
        current = _make_tx(tx_id="TX-CURRENT", amount=8000.0, balance=1000.0)
        # The only history entry is the current tx itself (duplicate).
        # After exclusion, no pre-window tx → fallback: 1000+8000=9000
        # drop = 8000/9000 ≈ 89 %
        result = self.rule.evaluate(current, history=[current])
        assert result.triggered is True

    # ── Section 9: Edge cases – account isolation ─────────────────────

    def test_multiple_accounts_isolated(self):
        """A pre-window tx on ACCOUNT_B (balance 50000) must NOT be used
        for a current tx on ACCOUNT_A.  The rule should fall back to
        inference for ACCOUNT_A."""
        pre_b = _pre_window_tx(
            balance=50000.0, account=ACCOUNT_B, tx_id="TX-B-PRE"
        )
        pre_a = _pre_window_tx(
            balance=5000.0, account=ACCOUNT_A, tx_id="TX-A-PRE"
        )
        tx = _make_tx(
            customer_account=ACCOUNT_A, amount=3600.0, balance=1400.0
        )
        # balance_before from ACCOUNT_A = 5000, drop = 72 % → triggers
        result = self.rule.evaluate(tx, history=[pre_b, pre_a])
        assert result.triggered is True
        assert result.details["balance_before"] == 5000.0
        assert result.details["customer_account"] == ACCOUNT_A

    # ── Section 10: Edge cases – pre-window boundary ──────────────────

    def test_tx_exactly_at_1h_is_in_window_not_pre_window(self):
        """A transaction exactly 1 hour before NOW is at the window_start.
        window_start = NOW - 1h.  The filter uses `< window_start`, so
        a tx AT window_start is inside the window, not before it.
        Falls back to inference."""
        at_boundary = _make_tx(
            tx_id="TX-BOUNDARY",
            customer_account=ACCOUNT_A,
            amount=50.0,
            balance=10000.0,
            timestamp=NOW - timedelta(hours=1),
        )
        tx = _make_tx(amount=8000.0, balance=1000.0)
        # No pre-window tx → inferred: 1000+8000=9000, drop ≈ 89 %
        result = self.rule.evaluate(tx, history=[at_boundary])
        assert result.triggered is True
        assert result.details["balance_before"] == 9000.0

    def test_tx_just_before_1h_is_pre_window(self):
        """A transaction 1h + 1s before NOW is strictly before window_start.
        It should be used as balance_before."""
        just_before = _make_tx(
            tx_id="TX-JUST-BEFORE",
            customer_account=ACCOUNT_A,
            amount=50.0,
            balance=5000.0,
            timestamp=NOW - timedelta(hours=1, seconds=1),
        )
        tx = _make_tx(amount=3600.0, balance=1400.0)
        result = self.rule.evaluate(tx, history=[just_before])
        assert result.triggered is True
        assert result.details["balance_before"] == 5000.0

    # ── Section 11: Metadata ──────────────────────────────────────────

    def test_result_metadata_on_trigger(self):
        """Verify all expected detail fields on a triggered result."""
        pre = _pre_window_tx(balance=5000.0)
        tx = _make_tx(amount=4000.0, balance=1000.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.rule_id == "R21"
        assert result.rule_name == "Rapid Account Emptying"
        assert result.weight == 20
        assert result.details["balance_before"] == 5000.0
        assert result.details["balance_after"] == 1000.0
        assert result.details["drop_threshold"] == 0.70
        assert result.details["window_hours"] == 1
        assert result.details["customer_account"] == ACCOUNT_A

    def test_no_trigger_severity_is_none(self):
        tx = _make_tx(amount=100.0, balance=4900.0)
        result = self.rule.evaluate(tx, history=[])
        assert result.severity is None

    def test_trigger_severity_is_strong(self):
        """Account emptying indicates coercion → STRONG severity."""
        pre = _pre_window_tx(balance=5000.0)
        tx = _make_tx(amount=4000.0, balance=1000.0)
        result = self.rule.evaluate(tx, history=[pre])
        assert result.severity == Severity.STRONG
