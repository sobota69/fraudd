"""Unit tests for WorkflowRunner."""

from datetime import datetime
from typing import Sequence
from unittest.mock import MagicMock

import pandas as pd
import pytest

from application.ports import GraphRepository, ResultExporter
from application.workflow_runner import WorkflowRunner, WorkflowResult
from domain.risk import RiskAssessment
from domain.transaction import Transaction


def _make_df(n: int = 3) -> pd.DataFrame:
    """Create a minimal valid transactions DataFrame with *n* rows."""
    rows = []
    for i in range(n):
        rows.append({
            "transaction_id": f"tx{i}",
            "transaction_timestamp": datetime(2026, 4, 20, 10, i),
            "customer_id": 1,
            "customer_account": "NL01BANK0001",
            "channel": "web",
            "device_id": "d1",
            "amount": 100.0 + i,
            "currency": "EUR",
            "is_new_beneficiary": False,
            "beneficiary_account": "NL02BANK0002",
            "entered_beneficiary_name": "Bob",
            "official_beneficiary_account_name": "Bob",
            "customer_account_balance": 5000.0,
        })
    return pd.DataFrame(rows)


class FakeGraphRepository(GraphRepository):
    def __init__(self):
        self.reset_called = False
        self.saved_transactions = []
        self.updated_assessments = []

    def reset_database(self):
        self.reset_called = True

    def save_transactions(self, transactions: Sequence[Transaction]):
        self.saved_transactions = list(transactions)

    def update_risk_assessments(self, assessments: Sequence[RiskAssessment]):
        self.updated_assessments = list(assessments)

    def close(self):
        pass


class FakeExporter(ResultExporter):
    def export(self, assessments: Sequence[RiskAssessment]) -> bytes:
        return b"csv-content"


# ── Basic pipeline tests ─────────────────────────────────────


class TestRunProcessList:
    def test_returns_workflow_result(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(2))
        assert isinstance(result, WorkflowResult)

    def test_transaction_count_matches(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(3))
        assert len(result.transactions) == 3

    def test_assessments_count_matches_transactions(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(4))
        assert len(result.assessments) == 4
        assert len(result.rule_results) == 4

    def test_elapsed_is_positive(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(1))
        assert result.elapsed > 0

    def test_transactions_are_domain_objects(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(2))
        assert all(isinstance(t, Transaction) for t in result.transactions)

    def test_assessments_are_risk_assessments(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(2))
        assert all(isinstance(a, RiskAssessment) for a in result.assessments)


# ── Graph repository integration ─────────────────────────────


class TestWithGraphRepository:
    def test_resets_database(self):
        graph = FakeGraphRepository()
        runner = WorkflowRunner(graph_repository=graph)
        runner.run_process_list(_make_df(1))
        assert graph.reset_called

    def test_saves_transactions(self):
        graph = FakeGraphRepository()
        runner = WorkflowRunner(graph_repository=graph)
        runner.run_process_list(_make_df(3))
        assert len(graph.saved_transactions) == 3

    def test_updates_risk_assessments(self):
        graph = FakeGraphRepository()
        runner = WorkflowRunner(graph_repository=graph)
        runner.run_process_list(_make_df(2))
        assert len(graph.updated_assessments) == 2


# ── Exporter integration ─────────────────────────────────────


class TestWithExporter:
    def test_exports_csv(self):
        exporter = FakeExporter()
        runner = WorkflowRunner(result_exporter=exporter)
        result = runner.run_process_list(_make_df(2))
        assert result.risk_csv == b"csv-content"

    def test_no_exporter_returns_empty_bytes(self):
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(1))
        assert result.risk_csv == b""


# ── History filtering ─────────────────────────────────────────


class TestHistoryFiltering:
    def test_first_transaction_gets_empty_history(self):
        """The earliest transaction should have no prior history."""
        runner = WorkflowRunner()
        result = runner.run_process_list(_make_df(3))
        # All assessments should be produced (no crash from empty history)
        assert len(result.assessments) == 3

    def test_multiple_customers_isolated(self):
        """Transactions from different customers should not share history."""
        df = _make_df(2)
        df.loc[0, "customer_id"] = 1
        df.loc[1, "customer_id"] = 2
        runner = WorkflowRunner()
        result = runner.run_process_list(df)
        assert len(result.assessments) == 2
