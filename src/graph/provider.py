from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Sequence

from neo4j import GraphDatabase, RoutingControl
from .model import ClientActivityWindow, CountryStat, Transaction, TransactionSummary


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

    SCHEMA_CONSTRAINTS: tuple[str, ...] = (
        """
        CREATE CONSTRAINT customer_customer_id_unique IF NOT EXISTS
        FOR (c:Customer)
        REQUIRE c.customer_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT customer_account_account_unique IF NOT EXISTS
        FOR (a:CustomerAccount)
        REQUIRE a.customer_account IS UNIQUE
        """,
        """
        CREATE CONSTRAINT transaction_transaction_id_unique IF NOT EXISTS
        FOR (t:Transaction)
        REQUIRE t.transaction_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT beneficiary_account_unique IF NOT EXISTS
        FOR (b:Beneficiary)
        REQUIRE b.beneficiary_account IS UNIQUE
        """,
        """
        CREATE CONSTRAINT device_device_id_unique IF NOT EXISTS
        FOR (d:Device)
        REQUIRE d.device_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT channel_channel_unique IF NOT EXISTS
        FOR (ch:Channel)
        REQUIRE ch.channel IS UNIQUE
        """,
    )

    SCHEMA_INDEXES_REQUIRED: tuple[str, ...] = (
        """
        CREATE INDEX tx_customer_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.customer_id, t.transaction_timestamp)
        """,
        """
        CREATE INDEX tx_account_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.customer_account, t.transaction_timestamp)
        """,
        """
        CREATE INDEX tx_timestamp_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.transaction_timestamp)
        """,
    )

    SCHEMA_INDEXES_OPTIONAL: tuple[str, ...] = (
        """
        CREATE INDEX tx_channel_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.channel, t.transaction_timestamp)
        """,
        """
        CREATE INDEX tx_device_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.device_id, t.transaction_timestamp)
        """,
        """
        CREATE INDEX tx_new_beneficiary_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.is_new_beneficiary, t.transaction_timestamp)
        """,
        """
        CREATE INDEX tx_beneficiary_country_time_idx IF NOT EXISTS
        FOR (t:Transaction)
        ON (t.beneficiary_country, t.transaction_timestamp)
        """,
    )

    DROP_SCHEMA_STATEMENTS: tuple[str, ...] = (
        "DROP INDEX tx_beneficiary_country_time_idx IF EXISTS",
        "DROP INDEX tx_new_beneficiary_time_idx IF EXISTS",
        "DROP INDEX tx_device_time_idx IF EXISTS",
        "DROP INDEX tx_channel_time_idx IF EXISTS",
        "DROP INDEX tx_timestamp_idx IF EXISTS",
        "DROP INDEX tx_account_time_idx IF EXISTS",
        "DROP INDEX tx_customer_time_idx IF EXISTS",
        "DROP CONSTRAINT channel_channel_unique IF EXISTS",
        "DROP CONSTRAINT device_device_id_unique IF EXISTS",
        "DROP CONSTRAINT beneficiary_account_unique IF EXISTS",
        "DROP CONSTRAINT transaction_transaction_id_unique IF EXISTS",
        "DROP CONSTRAINT customer_account_account_unique IF EXISTS",
        "DROP CONSTRAINT customer_customer_id_unique IF EXISTS",
    )

    UPSERT_TRANSACTIONS_CYPHER = """
    UNWIND $rows AS row

    MERGE (c:Customer {customer_id: row.customer_id})
    MERGE (ca:CustomerAccount {customer_account: row.customer_account})

    // keep ownership canonical: one account -> one owner in this model
    OPTIONAL MATCH (other_c:Customer)-[old_owns:OWNS]->(ca)
    WHERE other_c.customer_id <> row.customer_id
    DELETE old_owns

    MERGE (c)-[:OWNS]->(ca)

    MERGE (t:Transaction {transaction_id: row.transaction_id})
    SET
        t.transaction_timestamp = row.transaction_timestamp,
        t.amount = row.amount,
        t.currency = row.currency,
        t.is_new_beneficiary = row.is_new_beneficiary,
        t.entered_beneficiary_name = row.entered_beneficiary_name,
        t.customer_account_balance = row.customer_account_balance,

        // denormalized query-helper fields
        t.customer_id = row.customer_id,
        t.customer_account = row.customer_account,
        t.beneficiary_account = row.beneficiary_account,
        t.beneficiary_country = row.beneficiary_country,
        t.device_id = row.device_id,
        t.channel = row.channel,

        // recommended helper fields
        t.transaction_hour_of_day = row.transaction_hour_of_day,
        t.transaction_day_of_week = row.transaction_day_of_week

    // keep source account canonical for this transaction
    OPTIONAL MATCH (old_ca:CustomerAccount)-[old_transfer:TRANSFER]->(t)
    WHERE old_ca.customer_account <> row.customer_account
    DELETE old_transfer

    MERGE (ca)-[:TRANSFER]->(t)

    MERGE (b:Beneficiary {beneficiary_account: row.beneficiary_account})
    SET
        b.official_beneficiary_account_name =
            coalesce(row.official_beneficiary_account_name, b.official_beneficiary_account_name),
        b.beneficiary_country =
            coalesce(row.beneficiary_country, b.beneficiary_country)

    // keep beneficiary canonical for this transaction
    OPTIONAL MATCH (t)-[old_to:TO]->(old_b:Beneficiary)
    WHERE old_b.beneficiary_account <> row.beneficiary_account
    DELETE old_to

    MERGE (t)-[:TO]->(b)

    MERGE (d:Device {device_id: row.device_id})

    // keep device canonical for this transaction
    OPTIONAL MATCH (t)-[old_ud:USING_DEVICE]->(old_d:Device)
    WHERE old_d.device_id <> row.device_id
    DELETE old_ud

    MERGE (t)-[:USING_DEVICE]->(d)

    MERGE (ch:Channel {channel: row.channel})

    // keep channel canonical for this transaction
    OPTIONAL MATCH (t)-[old_oc:ON_CHANNEL]->(old_ch:Channel)
    WHERE old_ch.channel <> row.channel
    DELETE old_oc

    MERGE (t)-[:ON_CHANNEL]->(ch)
    """

    GET_CLIENT_AVG_AMOUNT_CYPHER = """
    MATCH (t:Transaction)
    WHERE t.customer_id = $customer_id
      AND t.transaction_timestamp >= $since
      AND ($customer_account IS NULL OR t.customer_account = $customer_account)
    RETURN avg(t.amount) AS avg_amount
    """

    COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER = """
    MATCH (t:Transaction)
    WHERE t.customer_id = $customer_id
      AND t.transaction_timestamp >= $since
      AND ($customer_account IS NULL OR t.customer_account = $customer_account)
    RETURN count(t) AS tx_count
    """

    GET_CLIENT_TRANSACTION_COUNTRIES_CYPHER = """
    MATCH (t:Transaction)
    WHERE t.customer_id = $customer_id
      AND ($since IS NULL OR t.transaction_timestamp >= $since)
      AND ($customer_account IS NULL OR t.customer_account = $customer_account)
      AND t.beneficiary_country IS NOT NULL
    RETURN
        t.beneficiary_country AS country,
        count(t) AS transaction_count,
        coalesce(sum(t.amount), 0.0) AS total_amount
    ORDER BY transaction_count DESC, country ASC
    """

    GET_CLIENT_ACTIVITY_WINDOW_CYPHER = """
    MATCH (t:Transaction)
    WHERE t.customer_id = $customer_id
      AND t.transaction_timestamp >= $since
      AND ($customer_account IS NULL OR t.customer_account = $customer_account)
      AND t.transaction_hour_of_day IS NOT NULL
    RETURN
        t.transaction_hour_of_day AS hour,
        count(t) AS tx_count
    ORDER BY hour ASC
    """

    GET_PREVIOUS_TRANSACTION_CYPHER = """
    MATCH (t:Transaction)
    WHERE t.customer_id = $customer_id
      AND t.transaction_timestamp < $before
      AND ($customer_account IS NULL OR t.customer_account = $customer_account)
    RETURN
        t.transaction_id AS transaction_id,
        t.transaction_timestamp AS transaction_timestamp,
        t.amount AS amount,
        t.currency AS currency,
        t.customer_id AS customer_id,
        t.customer_account AS customer_account,
        t.beneficiary_account AS beneficiary_account,
        t.beneficiary_country AS beneficiary_country,
        t.channel AS channel,
        t.device_id AS device_id
    ORDER BY t.transaction_timestamp DESC
    LIMIT 1
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
        for query in self.SCHEMA_CONSTRAINTS:
            self._run_write(query)

        for query in self.SCHEMA_INDEXES_REQUIRED:
            self._run_write(query)

        if include_optional_indexes:
            for query in self.SCHEMA_INDEXES_OPTIONAL:
                self._run_write(query)

    def clear_database(self, drop_schema: bool = False) -> None:
        """
        Delete all nodes and relationships in the active database.
        Optionally drop provider-created constraints and indexes too.
        """
        self._run_write("MATCH (n) DETACH DELETE n")

        if drop_schema:
            for query in self.DROP_SCHEMA_STATEMENTS:
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
        self._run_write(self.UPSERT_TRANSACTIONS_CYPHER, rows=[row])

    def save_transactions(
        self,
        transactions: Sequence[Transaction],
        batch_size: int = 1000,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        batch: list[dict[str, Any]] = []

        for tx in transactions:
            self._validate_transaction(tx)
            batch.append(self._to_graph_row(tx))
            if len(batch) >= batch_size:
                self._run_write(self.UPSERT_TRANSACTIONS_CYPHER, rows=batch)
                batch.clear()

        if batch:
            self._run_write(self.UPSERT_TRANSACTIONS_CYPHER, rows=batch)

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
            self.GET_CLIENT_AVG_AMOUNT_CYPHER,
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
            self.COUNT_CLIENT_TRANSACTIONS_SINCE_CYPHER,
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

        query = f"""
        MATCH (t:Transaction)
        WHERE t.customer_id = $customer_id
          AND t.transaction_timestamp >= $since
          AND ($customer_account IS NULL OR t.customer_account = $customer_account)
          AND {property_filter}
        RETURN count(t) AS tx_count
        """

        row = self._run_read_one(
            query,
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
            self.GET_CLIENT_TRANSACTION_COUNTRIES_CYPHER,
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
            self.GET_CLIENT_ACTIVITY_WINDOW_CYPHER,
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
            self.GET_PREVIOUS_TRANSACTION_CYPHER,
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


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    provider = Neo4jGraphProvider(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="capgemini",   # <- replace
        database="neo4j",
    )

    try:
        provider.verify_connectivity()

        # Wipe existing data/schema and recreate the provider schema.
        provider.reset_database(include_optional_indexes=False)

        tx = Transaction(
            transaction_id="tx-0001",
            transaction_timestamp=datetime.now(timezone.utc),
            customer_id=123,
            customer_account="PL12 3456 7890 1234 5678 9012 3456",
            channel="mobile_app",
            device_id="device-abc-001",
            amount=150.25,
            currency="pln",
            is_new_beneficiary=True,
            beneficiary_account="DE89 3704 0044 0532 0130 00",
            entered_beneficiary_name="John Doe GmbH",
            official_beneficiary_account_name="John Doe GmbH",
            customer_account_balance=5200.75,
        )

        provider.save_transaction(tx)

        avg_amount = provider.get_client_30d_avg_amount(customer_id=123)
        print("30d avg amount:", avg_amount)

        prev_tx = provider.get_previous_transaction(
            customer_id=123,
            before=datetime.now(timezone.utc),
        )
        print("Previous transaction:", prev_tx)

    finally:
        provider.close()