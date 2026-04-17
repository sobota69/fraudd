import pandas as pd
from src.graph.provider import Neo4jGraphProvider
from src.risk.risk_calculator import RiskCalculator, RiskAssessment
from src.rules import ALL_RULES
from src.rules_runner import RulesRunner
from src.transaction.transaction import Transaction


class WorkflowRunner:
    def __init__(self):
        self.provider = Neo4jGraphProvider()
        self.risk_calculator = RiskCalculator()

    def run_process(self, df: pd.DataFrame):
        import time as _time
        from collections import defaultdict

        risk_calculator = self.risk_calculator

        rules_runner = RulesRunner(rules=[RuleClass() for RuleClass in ALL_RULES])
        risk_assessments: list[RiskAssessment] = []
        total_start = _time.perf_counter()

        # Pre-build all Transaction objects and index by customer_id
        transactions: list[Transaction] = []
        customer_history: dict[int, list[Transaction]] = defaultdict(list)

        t0 = _time.perf_counter()
        for record in df.to_dict(orient="records"):
            tx = Transaction(**record)
            transactions.append(tx)
            customer_history[tx.customer_id].append(tx)
        print(f"Built {len(transactions)} transactions + index in {_time.perf_counter() - t0:.2f}s")

        for i, tx in enumerate(transactions):
            history = customer_history.get(tx.customer_id, [])
            rule_results = rules_runner.run_detection(tx, history=history)
            assessment = risk_calculator.calculate_risk(rule_results, tx)
            risk_assessments.append(assessment)

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

        print(f"\n✅ All {len(df)} transactions processed before saving in {_time.perf_counter() - total_start:.2f}s")

        pd.DataFrame(rows).to_csv(output_file, index=False)

        print(f"\n✅ All {len(df)} transactions processed in {_time.perf_counter() - total_start:.2f}s")
        
        

