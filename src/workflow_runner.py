import pandas as pd
from graph.provider import provider
from risk import risk_calculator
from risk.risk_calculator import RiskAssessment
from rules import ALL_RULES
from rules_runner import RulesRunner
from transaction.transaction import Transaction


class WorkflowRunner:
    def __init__(self):
        ...

    def run_process(self, df: pd.DataFrame):
        import time as _time
        
        rules_runner = RulesRunner(rules=[RuleClass() for RuleClass in ALL_RULES])
        risk_assessments: list[RiskAssessment] = []
        total_start = _time.perf_counter()

        for i, record in enumerate(df.to_dict(orient="records")):
            tx_start = _time.perf_counter()

            # Create transaction object
            t0 = _time.perf_counter()
            tx = Transaction(**record)
            print(f"[TX {i}] Transaction object created in {_time.perf_counter() - t0:.4f}s")

            # Add to provider
            t0 = _time.perf_counter()
            provider.save_transaction(tx)
            elapsed_db = _time.perf_counter() - t0
            if i == 0:
                print(f"[TX {i}] 🗄️  First save_transaction took {elapsed_db:.4f}s")

            # Check rules (per-rule timing is printed inside RulesRunner)
            t0 = _time.perf_counter()
            rule_results = rules_runner.run_detection([tx])
            print(f"[TX {i}] Rules engine total: {_time.perf_counter() - t0:.4f}s")

            # Calculate risk
            t0 = _time.perf_counter()
            assessment = risk_calculator.calculate_risk(rule_results[0], tx)
            print(f"[TX {i}] Risk calculation: {_time.perf_counter() - t0:.4f}s")
            risk_assessments.append(assessment)

            # Save it to output CSV (append mode)
            t0 = _time.perf_counter()
            output_file = "risk_assessments.csv"
            assessment_data = {
                "transaction_id": assessment.transaction_id,
                "triggered_rules": assessment.triggered_rules,
                "is_fraud_transaction": assessment.is_fraud_transaction,
                "risk_score": assessment.risk_score,
                "risk_category": assessment.risk_category,
            }
            pd.DataFrame([assessment_data]).to_csv(output_file, mode='a', header=not pd.io.common.file_exists(output_file), index=False)
            print(f"[TX {i}] CSV write: {_time.perf_counter() - t0:.4f}s")

            print(f"[TX {i}] ── Total: {_time.perf_counter() - tx_start:.4f}s")

        print(f"\n✅ All {len(df)} transactions processed in {_time.perf_counter() - total_start:.2f}s")
        
        

