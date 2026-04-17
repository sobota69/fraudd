"""R10 – Cross-Border Anomaly.

Trigger if the destination country (first 2 letters of beneficiary IBAN) has
never been seen in the customer's past transactions AND amount > €15,000.
Severity: MILD (1) | Weight: 8 | Optional
"""

from __future__ import annotations

from typing import List, Optional

from src.transaction.transaction import Transaction
from .base_rule import BaseRule, RuleResult, Severity

_AMOUNT_THRESHOLD = 15_000.0


def _country_from_iban(iban: str) -> str | None:
    """Extract the 2-letter ISO country code from an IBAN."""
    if iban and len(iban) >= 2 and iban[:2].isalpha():
        return iban[:2].upper()
    return None


class R10CrossBorderAnomaly(BaseRule):
    rule_id = "R10"
    rule_name = "Cross-Border Anomaly"
    category = "Anomaly"
    weight = 8
    mandatory = False

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> RuleResult:
        """Evaluate the Cross-Border Anomaly rule.

        Steps
        -----
        1. Extract the destination country from the beneficiary_account IBAN.
        2. If amount ≤ €15,000 → no trigger.
        3. Collect all distinct destination countries from the customer's
           historical transactions.
        4. If the current destination country has never appeared → MILD trigger.
        """

        dest_country = _country_from_iban(transaction.beneficiary_account)
        if dest_country is None:
            return self._no_trigger()

        if transaction.amount <= _AMOUNT_THRESHOLD:
            return self._no_trigger()

        # Collect historically seen countries for this customer
        if not history:
            # No history → country is inherently "new"
            return self._trigger(transaction, dest_country, set())

        seen_countries: set[str] = set()
        for tx in history:
            if (
                tx.customer_id == transaction.customer_id
                and tx.transaction_id != transaction.transaction_id
            ):
                c = _country_from_iban(tx.beneficiary_account)
                if c:
                    seen_countries.add(c)

        if dest_country not in seen_countries:
            return self._trigger(transaction, dest_country, seen_countries)

        return self._no_trigger()

    # ── helpers ───────────────────────────────────────────────────────────
    def _trigger(
        self, transaction: Transaction, dest_country: str, seen: set[str]
    ) -> RuleResult:
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=True,
            severity=Severity.MILD,
            weight=self.weight,
            details={
                "destination_country": dest_country,
                "amount": transaction.amount,
                "amount_threshold": _AMOUNT_THRESHOLD,
                "previously_seen_countries": sorted(seen),
            },
        )

    def _no_trigger(self) -> RuleResult:
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            triggered=False,
            severity=None,
            weight=self.weight,
        )
