from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Sequence

from neo4j import GraphDatabase, RoutingControl
from .model import ClientActivityWindow, CountryStat, Transaction, TransactionSummary
from .cypher_commands import COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER, DELETE_ALL_NODES, DROP_SCHEMA_STATEMENTS, GET_CLIENT_ACTIVITY_WINDOW_CYPHER, GET_CLIENT_AVG_AMOUNT_CYPHER, GET_CLIENT_TRANSACTION_COUNTRIES_CYPHER, GET_CLIENT_TRANSACTIONS_BY_PROPERTY, GET_PREVIOUS_TRANSACTION_CYPHER, SCHEMA_CONSTRAINTS, SCHEMA_INDEXES_OPTIONAL, SCHEMA_INDEXES_REQUIRED, UPSERT_TRANSACTIONS_CYPHER


RuleProperty = Literal[
    "channel",
    "device_id",
    "currency",
    "is_new_beneficiary",
    "beneficiary_country",
]


class Neo4jGraphProvider:
    """
    Concrete Neo4j provider for the finalized schema:

    (:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(:Transaction)-[:TO]->(:Beneficiary)
    (:Transaction)-[:USING_DEVICE]->(:Device)
    (:Transaction)-[:ON_CHANNEL]->(:Channel)

    Notes:
    - Designed for Neo4j Community Edition using the default DB name "neo4j".
    - Multi-database CREATE DATABASE is not used here.
    """

    PROPERTY_FILTERS: dict[RuleProperty, str] = {
        "channel": "t.channel = $property_value",
        "device_id": "t.device_id = $property_value",
        "currency": "t.currency = $property_value",
        "is_new_beneficiary": "t.is_new_beneficiary = $property_value",
        "beneficiary_country": "t.beneficiary_country = $property_value",
    }

    USUAL_ACTIVITY_WINDOW_HOURS = 6

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "capgemini",
        database: str = "neo4j",
    ) -> None:
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def __enter__(self) -> "Neo4jGraphProvider":
        self.verify_connectivity()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -------------------------------------------------------------------------
    # Public lifecycle / schema
    # -------------------------------------------------------------------------

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
        """
        Delete all nodes and relationships in the active database.
        Optionally drop provider-created constraints and indexes too.
        """
        self._run_write(DELETE_ALL_NODES)

        if drop_schema:
            for query in DROP_SCHEMA_STATEMENTS:
                self._run_write(query)

    def reset_database(self, include_optional_indexes: bool = False) -> None:
        """
        Full reset:
        - delete all graph data
        - drop provider schema if present
        - recreate provider schema
        """
        self.clear_database(drop_schema=True)
        self.init_schema(include_optional_indexes=include_optional_indexes)

    # -------------------------------------------------------------------------
    # Writes
    # -------------------------------------------------------------------------

    def save_transaction(self, tx: Transaction) -> None:
        self._validate_transaction(tx)
        row = self._to_graph_row(tx)
        self._run_write(UPSERT_TRANSACTIONS_CYPHER, rows=[row])

    # def save_transactions(
    #     self,
    #     transactions: Sequence[Transaction],
    #     batch_size: int = 1000,
    # ) -> None:
    #     if batch_size <= 0:
    #         raise ValueError("batch_size must be > 0")

    #     batch: list[dict[str, Any]] = []

    #     for tx in transactions:
    #         self._validate_transaction(tx)
    #         batch.append(self._to_graph_row(tx))
    #         if len(batch) >= batch_size:
    #             self._run_write(UPSERT_TRANSACTIONS_CYPHER, rows=batch)
    #             batch.clear()

    #     if batch:
    #         self._run_write(UPSERT_TRANSACTIONS_CYPHER, rows=batch)

    # -------------------------------------------------------------------------
    # Reads / rule queries
    # -------------------------------------------------------------------------

    def get_client_30d_avg_amount(
        self,
        customer_id: int,
        as_of: Optional[datetime] = None,
        days: int = 30,
        customer_account: Optional[str] = None,
    ) -> Optional[float]:
        if days <= 0:
            raise ValueError("days must be > 0")

        as_of = self._ensure_aware_datetime(as_of or datetime.now(timezone.utc))
        since = as_of - timedelta(days=days)

        row = self._run_read_one(
            GET_CLIENT_AVG_AMOUNT_CYPHER,
            customer_id=customer_id,
            since=since,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
        )
        if not row or row.get("avg_amount") is None:
            return None
        return float(row["avg_amount"])

    def count_client_transactions_since(
        self,
        customer_id: int,
        since: datetime,
        customer_account: Optional[str] = None,
    ) -> int:
        since = self._ensure_aware_datetime(since)

        row = self._run_read_one(
            COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER,
            customer_id=customer_id,
            since=since,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
        )
        return int(row["tx_count"]) if row else 0

    def count_client_transactions_by_property_since(
        self,
        customer_id: int,
        property_name: RuleProperty,
        property_value: Any,
        since: datetime,
        customer_account: Optional[str] = None,
    ) -> int:
        since = self._ensure_aware_datetime(since)

        if property_name not in self.PROPERTY_FILTERS:
            raise ValueError(
                f"Unsupported property_name: {property_name!r}. "
                f"Allowed: {sorted(self.PROPERTY_FILTERS.keys())}"
            )

        if property_name == "channel" and isinstance(property_value, str):
            property_value = self._normalize_channel(property_value)
        elif property_name == "device_id" and isinstance(property_value, str):
            property_value = self._normalize_device_id(property_value)
        elif property_name == "currency" and isinstance(property_value, str):
            property_value = self._normalize_currency(property_value)
        elif property_name == "beneficiary_country" and isinstance(property_value, str):
            property_value = property_value.strip().upper()

        property_filter = self.PROPERTY_FILTERS[property_name]

        row = self._run_read_one(
            GET_CLIENT_TRANSACTIONS_BY_PROPERTY.replace("{property_filter}", property_filter),
            customer_id=customer_id,
            since=since,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
            property_value=property_value,
        )
        return int(row["tx_count"]) if row else 0

    def get_client_transaction_countries(
        self,
        customer_id: int,
        since: Optional[datetime] = None,
        customer_account: Optional[str] = None,
    ) -> list[CountryStat]:
        since = self._ensure_aware_datetime(since) if since else None

        rows = self._run_read_many(
            GET_CLIENT_TRANSACTION_COUNTRIES_CYPHER,
            customer_id=customer_id,
            since=since,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
        )

        return [
            CountryStat(
                country=str(row["country"]),
                transaction_count=int(row["transaction_count"]),
                total_amount=float(row["total_amount"]),
            )
            for row in rows
        ]

    def get_client_activity_window(
        self,
        customer_id: int,
        lookback_days: int = 90,
        customer_account: Optional[str] = None,
    ) -> ClientActivityWindow:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be > 0")

        since = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        rows = self._run_read_many(
            GET_CLIENT_ACTIVITY_WINDOW_CYPHER,
            customer_id=customer_id,
            since=since,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
        )

        histogram = {int(row["hour"]): int(row["tx_count"]) for row in rows}

        if not histogram:
            return ClientActivityWindow(
                start_hour=None,
                end_hour=None,
                hourly_histogram={},
            )

        start_hour, end_hour = self._compute_usual_activity_window(
            histogram=histogram,
            window_hours=self.USUAL_ACTIVITY_WINDOW_HOURS,
        )

        return ClientActivityWindow(
            start_hour=start_hour,
            end_hour=end_hour,
            hourly_histogram=histogram,
        )

    def get_previous_transaction(
        self,
        customer_id: int,
        before: datetime,
        customer_account: Optional[str] = None,
    ) -> Optional[TransactionSummary]:
        before = self._ensure_aware_datetime(before)

        row = self._run_read_one(
            GET_PREVIOUS_TRANSACTION_CYPHER,
            customer_id=customer_id,
            before=before,
            customer_account=self._normalize_account(customer_account) if customer_account else None,
        )
        if not row:
            return None

        return TransactionSummary(
            transaction_id=str(row["transaction_id"]),
            transaction_timestamp=self._to_py_datetime(row["transaction_timestamp"]),
            amount=float(row["amount"]),
            currency=str(row["currency"]),
            customer_id=int(row["customer_id"]),
            customer_account=str(row["customer_account"]),
            beneficiary_account=str(row["beneficiary_account"]),
            beneficiary_country=row.get("beneficiary_country"),
            channel=row.get("channel"),
            device_id=row.get("device_id"),
        )

    # -------------------------------------------------------------------------
    # Internal DB helpers
    # -------------------------------------------------------------------------

    def _run_write(self, query: str, **params: Any) -> None:
        self._driver.execute_query(
            query,
            database_=self._database,
            **params,
        )

    def _run_read_one(self, query: str, **params: Any) -> Optional[dict[str, Any]]:
        records, _, _ = self._driver.execute_query(
            query,
            database_=self._database,
            routing_=RoutingControl.READ,
            **params,
        )
        if not records:
            return None
        return records[0].data()

    def _run_read_many(self, query: str, **params: Any) -> list[dict[str, Any]]:
        records, _, _ = self._driver.execute_query(
            query,
            database_=self._database,
            routing_=RoutingControl.READ,
            **params,
        )
        return [record.data() for record in records]

    # -------------------------------------------------------------------------
    # Internal mapping / validation
    # -------------------------------------------------------------------------

    def _validate_transaction(self, tx: Transaction) -> None:
        if not tx.transaction_id or not tx.transaction_id.strip():
            raise ValueError("transaction_id is required and cannot be blank")
        if tx.transaction_timestamp is None:
            raise ValueError("transaction_timestamp is required")
        if tx.customer_id is None:
            raise ValueError("customer_id is required")

        self._require_non_blank(tx.customer_account, "customer_account")
        self._require_non_blank(tx.channel, "channel")
        self._require_non_blank(tx.device_id, "device_id")
        self._require_non_blank(tx.currency, "currency")
        self._require_non_blank(tx.beneficiary_account, "beneficiary_account")

        if tx.amount is None:
            raise ValueError("amount is required")
        if tx.customer_account_balance is None:
            raise ValueError("customer_account_balance is required")

    def _to_graph_row(self, tx: Transaction) -> dict[str, Any]:
        transaction_timestamp = self._ensure_aware_datetime(tx.transaction_timestamp)
        customer_account = self._normalize_account(tx.customer_account)
        beneficiary_account = self._normalize_account(tx.beneficiary_account)
        device_id = self._normalize_device_id(tx.device_id)
        channel = self._normalize_channel(tx.channel)
        currency = self._normalize_currency(tx.currency)

        beneficiary_country = (
            tx.beneficiary_country.strip().upper()
            if tx.beneficiary_country
            else self._derive_beneficiary_country(beneficiary_account)
        )

        return {
            "transaction_id": tx.transaction_id.strip(),
            "transaction_timestamp": transaction_timestamp,
            "customer_id": int(tx.customer_id),
            "customer_account": customer_account,
            "channel": channel,
            "device_id": device_id,
            "amount": float(tx.amount),
            "currency": currency,
            "is_new_beneficiary": bool(tx.is_new_beneficiary),
            "beneficiary_account": beneficiary_account,
            "entered_beneficiary_name": (tx.entered_beneficiary_name or "").strip(),
            "official_beneficiary_account_name": (
                tx.official_beneficiary_account_name.strip()
                if tx.official_beneficiary_account_name
                else None
            ),
            "customer_account_balance": float(tx.customer_account_balance),
            "beneficiary_country": beneficiary_country,
            "transaction_hour_of_day": int(transaction_timestamp.hour),
            "transaction_day_of_week": int(transaction_timestamp.isoweekday()),
        }

    @staticmethod
    def _require_non_blank(value: str, field_name: str) -> None:
        if value is None or not str(value).strip():
            raise ValueError(f"{field_name} is required and cannot be blank")

    @staticmethod
    def _ensure_aware_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @staticmethod
    def _to_py_datetime(value: Any) -> datetime:
        if value is None:
            raise ValueError("Expected datetime value, got None")
        if isinstance(value, datetime):
            return value
        if hasattr(value, "to_native"):
            native = value.to_native()
            if isinstance(native, datetime):
                return native
        # best-effort fallback for iso-like strings
        return datetime.fromisoformat(str(value))

    @staticmethod
    def _normalize_account(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.replace(" ", "").strip().upper()

    @staticmethod
    def _normalize_device_id(value: str) -> str:
        return value.strip()

    @staticmethod
    def _normalize_channel(value: str) -> str:
        return value.strip().lower()

    @staticmethod
    def _normalize_currency(value: str) -> str:
        return value.strip().upper()

    @staticmethod
    def _derive_beneficiary_country(beneficiary_account: str) -> Optional[str]:
        """
        Basic IBAN-style heuristic:
        if the beneficiary account starts with two letters, treat them as country code.
        """
        if len(beneficiary_account) >= 2 and beneficiary_account[:2].isalpha():
            return beneficiary_account[:2].upper()
        return None

    @staticmethod
    def _compute_usual_activity_window(
        histogram: dict[int, int],
        window_hours: int = 6,
    ) -> tuple[int, int]:
        """
        Compute the densest rolling window over 24 hours.
        Returns (start_hour, end_hour), inclusive.
        """
        if window_hours <= 0 or window_hours > 24:
            raise ValueError("window_hours must be between 1 and 24")

        counts = [int(histogram.get(h, 0)) for h in range(24)]

        best_start = 0
        best_total = -1

        for start in range(24):
            total = 0
            for offset in range(window_hours):
                total += counts[(start + offset) % 24]
            if total > best_total:
                best_total = total
                best_start = start

        best_end = (best_start + window_hours - 1) % 24
        return best_start, best_end