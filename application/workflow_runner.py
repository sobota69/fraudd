"""Workflow orchestrator – coordinates the fraud detection pipeline.

This is the main application service. It depends on domain logic and
on abstract ports for IO (graph database, result export). Concrete
implementations are injected via the constructor.
"""

from __future__ import annotations

import time as _time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from application.ports import GraphRepository, ResultExporter
from application.rules_runner import RulesRunner
from domain.risk import RiskAssessment, RiskCalculator
from domain.rules import ALL_RULES
from domain.transaction import Transaction


@dataclass
class WorkflowResult:
    """Results returned by WorkflowRunner.run_process."""
    transactions: list
    assessments: list
    rule_results: list          # list[list[RuleResult]], one inner list per tx
    elapsed: float              # seconds
    risk_csv: bytes = b""       # risk_assessments.csv content


class WorkflowRunner:
    def __init__(
        self,
        graph_repository: Optional[GraphRepository] = None,
        result_exporter: Optional[ResultExporter] = None,
    ):
        self._graph = graph_repository
        self._exporter = result_exporter
        self._risk_calculator = RiskCalculator()

    def run_process_list(self, df: pd.DataFrame) -> WorkflowResult:
        if self._graph:
            self._graph.reset_database()

        rules_runner = RulesRunner(rules=[RuleClass() for RuleClass in ALL_RULES])
        risk_assessments: list[RiskAssessment] = []
        all_rule_results: list[list] = []
        total_start = _time.perf_counter()

        print(f"Processing {len(df)} transactions started")

        # Pre-build all Transaction objects and index by customer_id
        transactions: list[Transaction] = []
        customer_history: dict[int, list[Transaction]] = defaultdict(list)

        t0 = _time.perf_counter()
        for record in df.to_dict(orient="records"):
            tx = Transaction(**record)
            transactions.append(tx)
            customer_history[tx.customer_id].append(tx)

        # Pre-sort each customer's history by timestamp
        for cid in customer_history:
            customer_history[cid].sort(key=lambda t: t.transaction_timestamp)

        print(f"Built {len(transactions)} transactions + index in {_time.perf_counter() - t0:.2f}s")

        # Save to graph if available
        if self._graph:
            t1 = _time.perf_counter()
            self._graph.save_transactions(transactions)
            print(f"Saved transactions to graph in {_time.perf_counter() - t1:.2f}s")

        # Run rules + risk calculation
        t2 = _time.perf_counter()
        for tx in transactions:
            history = [t for t in customer_history.get(tx.customer_id, []) if t.transaction_timestamp < tx.transaction_timestamp]
            rule_results = rules_runner.run_detection(tx, history=history)
            all_rule_results.append(rule_results)
            assessment = self._risk_calculator.calculate_risk(rule_results, tx)
            risk_assessments.append(assessment)

        print(f"Calculated risk assessments in {_time.perf_counter() - t2:.2f}s")

        # Update graph with risk assessments
        if self._graph:
            t3 = _time.perf_counter()
            self._graph.update_risk_assessments(risk_assessments)
            print(f"Updated risk assessments in graph in {_time.perf_counter() - t3:.2f}s")

        elapsed = _time.perf_counter() - total_start

        # Export results
        risk_csv_bytes = b""
        if self._exporter:
            risk_csv_bytes = self._exporter.export(risk_assessments)

        print(f"\n✅ All {len(df)} transactions processed in {elapsed:.2f}s")

        return WorkflowResult(
            transactions=transactions,
            assessments=risk_assessments,
            rule_results=all_rule_results,
            elapsed=elapsed,
            risk_csv=risk_csv_bytes,
        )
