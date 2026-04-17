
from typing import List
from rules.base_rule import RuleResult
from rules.base_rule import BaseRule
from transaction import Transaction

class RulesRunner:
    def __init__(self, rules: List[BaseRule]):
        self.rules = rules

    def run_detection(self, transactions: List[Transaction]) -> List[RuleResult]:
        """Run all rules against each transaction, using the full list as history."""
        results: List[RuleResult] = []

        for transaction in transactions:
            for rule in self.rules:
                result = rule.evaluate(transaction, history=transactions)
                if result.triggered:
                    results.append(result)

        return results

    def all_passed(self, transactions: List[Transaction]) -> bool:
        """Return True if all rules pass"""
        return all(result.passed for result in self.run(transactions))
