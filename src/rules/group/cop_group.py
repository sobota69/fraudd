"""CoP Group – R1, R2, R3.

Computes the name-similarity score **once** and evaluates all three
Confirmation-of-Payee rules from that single score.

Similarity algorithm (from the HACKATHON_FRAML_RULES spec):
1. Normalisation – NFKD, lowercase, strip non-alphanumeric.
2. Tokenisation – split on whitespace.
3. Filtering – remove titles and single-char tokens.
4. For each token in A, find best fuzzy match in B.
5. Score = sum(best_matches) / max(|A|, |B|).
"""

from __future__ import annotations

import unicodedata
import re
from difflib import SequenceMatcher
from typing import List, Optional

from src.rules.base_rule import RuleResult, Severity
from src.transaction.transaction import Transaction

_TITLES = frozenset({
    "mr", "mrs", "ms", "dr", "prof", "sir", "jr", "sr",
    "sa", "sp", "z", "o", "oo", "pan", "pani", "univ",
})

# ── Similarity helpers ───────────────────────────────────────────────────────

def _normalise(name: str) -> str:
    """NFKD normalisation → lowercase → strip non-alphanumeric (keep spaces)."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9\s]", "", ascii_only.lower()).strip()


def _tokenise(text: str) -> list[str]:
    tokens = text.split()
    return [t for t in tokens if len(t) > 1 and t not in _TITLES]


def _fuzzy_score(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def compute_name_similarity(entered: str, official: str) -> float:
    """Return a similarity score in [0.0, 1.0]."""
    tokens_a = _tokenise(_normalise(entered))
    tokens_b = _tokenise(_normalise(official))

    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0

    total_best = 0.0
    for ta in tokens_a:
        best = max(_fuzzy_score(ta, tb) for tb in tokens_b)
        total_best += best

    denom = max(len(tokens_a), len(tokens_b))
    return total_best / denom if denom else 0.0


# ── Group evaluator ──────────────────────────────────────────────────────────

class CopGroup:
    """Evaluates R1, R2, R3 using a single similarity computation."""

    def evaluate(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> list[RuleResult]:
        score = compute_name_similarity(
            transaction.entered_beneficiary_name,
            transaction.official_beneficiary_account_name,
        )
        pct = score * 100.0

        results: list[RuleResult] = []

        # R1 – Hard Fail: similarity < 80 %
        results.append(RuleResult(
            rule_id="R1",
            rule_name="CoP Name Mismatch – Hard Fail",
            triggered=pct < 80,
            severity=Severity.STRONG if pct < 80 else None,
            weight=25,
            details={"similarity_pct": round(pct, 2)} if pct < 80 else {},
        ))

        # R2 – Soft Warning: 80 % ≤ similarity < 90 %
        soft_triggered = 80 <= pct < 90
        results.append(RuleResult(
            rule_id="R2",
            rule_name="CoP Name Mismatch – Soft Warning",
            triggered=soft_triggered,
            severity=Severity.MILD if soft_triggered else None,
            weight=5,
            details={"similarity_pct": round(pct, 2)} if soft_triggered else {},
        ))

        # R3 – New Beneficiary + CoP Mismatch: new_beneficiary AND similarity < 90 %
        r3_triggered = bool(transaction.is_new_beneficiary) and pct < 90
        results.append(RuleResult(
            rule_id="R3",
            rule_name="New Beneficiary + CoP Mismatch",
            triggered=r3_triggered,
            severity=Severity.MILD if r3_triggered else None,
            weight=10,
            details={
                "similarity_pct": round(pct, 2),
                "is_new_beneficiary": True,
            } if r3_triggered else {},
        ))

        return results
