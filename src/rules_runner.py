
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from src.rules.base_rule import RuleResult
from src.rules.base_rule import BaseRule
from src.transaction import Transaction


class RulesRunner:
    def __init__(self, rules: List[BaseRule], max_workers: int = 8):
        self.rules = rules
        self._max_workers = max_workers

    def run_detection(
        self,
        transaction: Transaction,
        history: Optional[List[Transaction]] = None,
    ) -> List[RuleResult]:
        """Run all rules against a single transaction in parallel.

        Returns a list of RuleResults (one per rule, same order as self.rules).
        """
        history = history or []

        def _eval(idx: int, rule: BaseRule) -> tuple[int, RuleResult, float]:
            t0 = _time.perf_counter()
            result = rule.evaluate(transaction, history=history)
            elapsed = _time.perf_counter() - t0
            return (idx, result, elapsed)

        results: list[tuple[int, RuleResult, float]] = []

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(_eval, i, rule): rule for i, rule in enumerate(self.rules)}
            for future in as_completed(futures):
                idx, result, elapsed = future.result()
                # print(
                #     f"  ⏱  {result.rule_id:6s} ({result.rule_name}): {elapsed:.4f}s"
                #     f"  {'⚠ TRIGGERED' if result.triggered else ''}"
                # )
                results.append((idx, result, elapsed))

        # Return in original rule order
        results.sort(key=lambda x: x[0])
        return [r for _, r, _ in results]
