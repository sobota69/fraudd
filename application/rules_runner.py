
from typing import List, Optional
from domain.rules.base_rule import RuleResult
from domain.rules.base_rule import BaseRule
from domain.rules.group.cop_group import CopGroup
from domain.rules.group.amount_stats_group import AmountStatsGroup
from domain.rules.group.threshold_group import ThresholdGroup
from domain.rules.group.frequency_group import FrequencyGroup
from domain.transaction import Transaction

# Rule IDs handled by each group – skip individual rules for these
_COP_IDS = frozenset({"R1", "R2", "R3"})
_AMOUNT_STATS_IDS = frozenset({"R6", "R12"})
_THRESHOLD_IDS = frozenset({"R22", "R24"})
_FREQUENCY_IDS = frozenset({"R7", "R8", "R17"})
_GROUPED_IDS = _COP_IDS | _AMOUNT_STATS_IDS | _THRESHOLD_IDS | _FREQUENCY_IDS


class RulesRunner:
    def __init__(self, rules: List[BaseRule]):
        # Separate individual (ungrouped) rules from grouped ones
        self._individual_rules = [r for r in rules if r.rule_id not in _GROUPED_IDS]

        # Instantiate group validators once
        self._cop_group = CopGroup()
        self._amount_stats_group = AmountStatsGroup()
        self._threshold_group = ThresholdGroup()
        self._frequency_group = FrequencyGroup()

    def run_detection(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> List[RuleResult]:
        """Run all rules against a single transaction."""
        history = history or []
        all_results: list[RuleResult] = []

        # ── Group evaluations ─────────────────────────────────────────────
        all_results.extend(self._cop_group.evaluate(transaction, history))
        all_results.extend(self._amount_stats_group.evaluate(transaction, history))
        all_results.extend(self._threshold_group.evaluate(transaction, history))
        all_results.extend(self._frequency_group.evaluate(transaction, history))

        # ── Individual rules ──────────────────────────────────────────────
        for rule in self._individual_rules:
            all_results.append(rule.evaluate(transaction, history=history))

        return all_results
