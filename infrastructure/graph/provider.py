from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Sequence

from neo4j import GraphDatabase, RoutingControl

from application.ports import GraphRepository
from domain.risk import RiskAssessment
from domain.transaction import Transaction
from .cypher_commands import (
    COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER,
    DELETE_ALL_NODES,
    DROP_SCHEMA_STATEMENTS,
    SCHEMA_CONSTRAINTS,
    SCHEMA_INDEXES_OPTIONAL,
    SCHEMA_INDEXES_REQUIRED,
    UPDATE_TRANSACTIONS_ASSESSMENTS,
    UPSERT_TRANSACTIONS_CYPHER,
)
from infrastructure.config import get_neo4j_config


class Neo4jGraphProvider(GraphRepository):
    """
    Concrete Neo4j provider implementing the GraphRepository port.

    (:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(:Transaction)-[:TO]->(:Beneficiary)
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> None:
        config = get_neo4j_config()
        self._uri = uri or config["uri"]
        self._user = user or config["user"]
        self._password = password or config["password"]
        self._database = database or config["database"]
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def __enter__(self) -> "Neo4jGraphProvider":
        self.verify_connectivity()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ── Lifecycle / schema ───────────────────────────────────────────────

    def verify_connectivity(self) -> None:
        self._driver.verify_connectivity()

    def close(self) -> None:
        self._driver.close()

    def init_schema(self, include_optional_indexes: bool = False) -> None:
        for query in SCHEMA_CONSTRAINTS:
            self._run_write(query)
        for query in SCHEMA_INDEXES_REQUIRED:
            self._run_write(query)
        if include_optional_indexes:
            for query in SCHEMA_INDEXES_OPTIONAL:
                self._run_write(query)

    def clear_database(self, drop_schema: bool = False) -> None:
        self._run_write(DELETE_ALL_NODES)
        if drop_schema:
            for query in DROP_SCHEMA_STATEMENTS:
                self._run_write(query)

    def reset_database(self, include_optional_indexes: bool = False) -> None:
        self.clear_database(drop_schema=True)
        self.init_schema(include_optional_indexes=include_optional_indexes)

    # ── Internal DB helpers ──────────────────────────────────────────────

    def _run_write(self, query: str, **params: Any) -> None:
        self._driver.execute_query(
            query,  # type: ignore
            database_=self._database,
            **params,
        )  # pyright: ignore[reportCallIssue]

    def _run_read_one(self, query: str, **params: Any) -> Optional[dict[str, Any]]:
        records, _, _ = self._driver.execute_query(
            query,  # type: ignore
            database_=self._database,
            routing_=RoutingControl.READ,
            **params,
        )  # pyright: ignore[reportCallIssue]
        if not records:
            return None
        return records[0].data()

    def _run_read_many(self, query: str, **params: Any) -> list[dict[str, Any]]:
        records, _, _ = self._driver.execute_query(
            query,  # type: ignore
            database_=self._database,
            routing_=RoutingControl.READ,
            **params,
        )  # pyright: ignore[reportCallIssue]
        return [record.data() for record in records]

    # ── Writes (GraphRepository port) ────────────────────────────────────

    def save_transactions(self, transactions: Sequence[Transaction]) -> None:
        batch: list[dict[str, Any]] = []
        for tx in transactions:
            batch.append(self._to_graph_row_transaction(tx))
        self._run_write(UPSERT_TRANSACTIONS_CYPHER, transactions=batch)

    def update_risk_assessments(self, assessments: Sequence[RiskAssessment]) -> None:
        batch: list[dict[str, Any]] = []
        for assessment in assessments:
            batch.append(self._to_graph_row_assessment(assessment))
        self._run_write(UPDATE_TRANSACTIONS_ASSESSMENTS, assessments=batch)

    # ── Reads ────────────────────────────────────────────────────────────

    def get_client_transactions_no_in_time(
        self,
        customer_id: int,
        timestamp: datetime,
        minutes: int,
    ) -> int:
        row = self._run_read_one(
            COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER,
            customer_id=customer_id,
            from_timestamp=timestamp - timedelta(minutes=minutes),
            to_timestamp=timestamp,
        )
        return int(row["tx_count"]) if row else 0

    # ── Internal mapping ─────────────────────────────────────────────────

    def _to_graph_row_transaction(self, tx: Transaction) -> dict[str, Any]:
        return {
            "transaction_id": tx.transaction_id,
            "transaction_timestamp": tx.transaction_timestamp,
            "customer_id": tx.customer_id,
            "customer_account": tx.customer_account,
            "channel": tx.channel,
            "device_id": tx.device_id,
            "amount": tx.amount,
            "currency": tx.currency,
            "is_new_beneficiary": tx.is_new_beneficiary,
            "beneficiary_account": tx.beneficiary_account,
            "entered_beneficiary_name": tx.entered_beneficiary_name,
            "official_beneficiary_account_name": tx.official_beneficiary_account_name,
            "customer_account_balance": tx.customer_account_balance,
            "beneficiary_country": tx.beneficiary_country,
            "transaction_day_of_week": tx.transaction_day_of_week,
            "transaction_hour_of_day": tx.transaction_hour_of_day,
        }

    def _to_graph_row_assessment(self, ass: RiskAssessment) -> dict[str, Any]:
        return {
            "transaction_id": ass.transaction_id,
            "triggered_rules": ass.triggered_rules,
            "is_fraud_transaction": ass.is_fraud_transaction,
            "risk_score": ass.risk_score,
            "risk_category": ass.risk_category,
        }
