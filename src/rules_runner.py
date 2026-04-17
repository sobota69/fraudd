
from typing import List
from src.rules.base_rule import RuleResult
from src.rules.base_rule import BaseRule
from src.transaction import Transaction

class RulesRunner:
    def __init__(self, rules: List[BaseRule]):
        self.rules = rules

    def run_detection(self, transactions: List[Transaction]) -> List[List[RuleResult]]:
        """Run all rules against each transaction, using the full list as history.

        Returns a list of lists – one inner list of RuleResults per transaction.
        """
        all_results: List[List[RuleResult]] = []

        for transaction in transactions:
            tx_results: List[RuleResult] = []
            for rule in self.rules:
                result = rule.evaluate(transaction, history=transactions)
                tx_results.append(result)
            all_results.append(tx_results)

        return all_results

    def all_passed(self, transactions: List[Transaction]) -> bool:
        """Return True if all rules pass"""
        return all(result.passed for result in self.run(transactions))
