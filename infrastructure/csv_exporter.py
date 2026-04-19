"""CSV result exporter – implements the ResultExporter port."""

from __future__ import annotations

from typing import Sequence

import pandas as pd

from application.ports import ResultExporter
from domain.risk import RiskAssessment


class CsvResultExporter(ResultExporter):
    """Export risk assessments to CSV bytes, optionally saving a local copy."""

    def __init__(self, output_path: str | None = "MegaFraudDetector9000Plus_risk_assessments.csv"):
        self._output_path = output_path

    def export(self, assessments: Sequence[RiskAssessment]) -> bytes:
        rows = [
            {
                "transaction_id": a.transaction_id,
                "triggered_rules": a.triggered_rules,
                "is_fraud_transaction": a.is_fraud_transaction,
                "risk_score": a.risk_score,
                "risk_category": a.risk_category,
            }
            for a in assessments
        ]
        risk_df = pd.DataFrame(rows)
        csv_bytes = risk_df.to_csv(index=False).encode("utf-8")

        if self._output_path:
            risk_df.to_csv(self._output_path, index=False)

        return csv_bytes
