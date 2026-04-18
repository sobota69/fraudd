import pandas as pd
from src.graph.provider import Neo4jGraphProvider
from src.risk.risk_calculator import RiskCalculator, RiskAssessment
from src.rules import ALL_RULES
from src.rules_runner import RulesRunner
from src.transaction.transaction import Transaction
import time as _time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List as _List


@dataclass
class WorkflowResult:
    """Results returned by WorkflowRunner.run_process."""
    transactions: list
    assessments: list
    rule_results: list          # list[list[RuleResult]], one inner list per tx
    elapsed: float              # seconds


class WorkflowRunner:
    def __init__(self):
        self.provider = Neo4jGraphProvider()
        self.risk_calculator = RiskCalculator()

    def run_process_list(self, df: pd.DataFrame) -> WorkflowResult:
        self.provider.reset_database() # Graph reset in case of rerun

        risk_calculator = self.risk_calculator

        rules_runner = RulesRunner(rules=[RuleClass() for RuleClass in ALL_RULES])
        risk_assessments: list[RiskAssessment] = []
        all_rule_results: list[list] = []  # per-transaction rule results
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

        # Pre-sort each customer's history by timestamp (enables bisect in rules)
        for cid in customer_history:
            customer_history[cid].sort(key=lambda t: t.transaction_timestamp)

        print(f"Built {len(transactions)} transactions + index in {_time.perf_counter() - t0:.2f}s")

        t1 = _time.perf_counter()
        self.provider.save_transactions(transactions) # Batch save all transactions to graph

        print(f"Saved transactions to graph in {_time.perf_counter() - t1:.2f}s")

        t2 = _time.perf_counter()
        for i, tx in enumerate(transactions):
            history = customer_history.get(tx.customer_id, [])
            rule_results = rules_runner.run_detection(tx, history=history)
            all_rule_results.append(rule_results)
            assessment = risk_calculator.calculate_risk(rule_results, tx)
            risk_assessments.append(assessment)

        print(f"Calculated risk assessments for all transactions in {_time.perf_counter() - t2:.2f}s")

        t3 = _time.perf_counter()
        self.provider.update_risk_assesment(risk_assessments)

        print(f"Updated risk assessments in graph in {_time.perf_counter() - t3:.2f}s")        

        # Save all results to CSV at once
        output_file = "risk_assessments.csv"
        rows = [
            {
                "transaction_id": a.transaction_id,
                "triggered_rules": a.triggered_rules,
                "is_fraud_transaction": a.is_fraud_transaction,
                "risk_score": a.risk_score,
                "risk_category": a.risk_category,
            }
            for a in risk_assessments
        ]

        elapsed = _time.perf_counter() - total_start
        pd.DataFrame(rows).to_csv(output_file, index=False)
        print(f"\n✅ All {len(df)} transactions processed in {elapsed:.2f}s")

        return WorkflowResult(
            transactions=transactions,
            assessments=risk_assessments,
            rule_results=all_rule_results,
            elapsed=elapsed,
        )