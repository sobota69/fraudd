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

DELETE_ALL_NODES: str = """MATCH (n) DETACH DELETE n"""

DROP_SCHEMA_STATEMENTS: tuple[str, ...] = (
        "DROP INDEX tx_beneficiary_country_time_idx IF EXISTS",
        "DROP INDEX tx_new_beneficiary_time_idx IF EXISTS",
        "DROP INDEX tx_timestamp_idx IF EXISTS",
        "DROP INDEX tx_account_time_idx IF EXISTS",
        "DROP INDEX tx_customer_time_idx IF EXISTS",
        "DROP CONSTRAINT beneficiary_account_unique IF EXISTS",
        "DROP CONSTRAINT transaction_transaction_id_unique IF EXISTS",
        "DROP CONSTRAINT customer_account_account_unique IF EXISTS",
        "DROP CONSTRAINT customer_customer_id_unique IF EXISTS",
    )

UPSERT_TRANSACTION_CYPHER = """
    MERGE (c:Customer {customer_id: $tx.customer_id})
    MERGE (ca:CustomerAccount {customer_account: $tx.customer_account})

    // keep ownership canonical: one account -> one owner in this model
    OPTIONAL MATCH (other_c:Customer)-[old_owns:OWNS]->(ca)
    WHERE other_c.customer_id <> $tx.customer_id
    DELETE old_owns

    MERGE (c)-[:OWNS]->(ca)

    MERGE (t:Transaction {transaction_id: $tx.transaction_id})
    SET
        t.transaction_timestamp = datetime($tx.transaction_timestamp),
        t.amount = $tx.amount,
        t.currency = $tx.currency,
        t.is_new_beneficiary = $tx.is_new_beneficiary,
        t.entered_beneficiary_name = $tx.entered_beneficiary_name,
        t.customer_account_balance = $tx.customer_account_balance,
        t.device_id = $tx.device_id,
        t.channel = $tx.channel,

        // denormalized query-helper fields
        t.customer_id = $tx.customer_id,
        t.customer_account = $tx.customer_account,
        t.beneficiary_account = $tx.beneficiary_account,
        t.beneficiary_country = $tx.beneficiary_country,        

        // recommended helper fields
        t.transaction_hour_of_day = $tx.transaction_hour_of_day,
        t.transaction_day_of_week = $tx.transaction_day_of_week

    // keep source account canonical for this transaction
    OPTIONAL MATCH (old_ca:CustomerAccount)-[old_transfer:TRANSFER]->(t)
    WHERE old_ca.customer_account <> $tx.customer_account
    DELETE old_transfer

    MERGE (ca)-[:TRANSFER]->(t)

    MERGE (b:Beneficiary {beneficiary_account: $tx.beneficiary_account})
    SET
        b.official_beneficiary_account_name = $tx.official_beneficiary_account_name
        b.beneficiary_country = $tx.beneficiary_country

    // keep beneficiary canonical for this transaction
    OPTIONAL MATCH (t)-[old_to:TO]->(old_b:Beneficiary)
    WHERE old_b.beneficiary_account <> $tx.beneficiary_account
    DELETE old_to

    MERGE (t)-[:TO]->(b)
    """

UPDATE_TRANSACTION_ASSESSMENT = """
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

GET_CLIENT_TRANSACTIONS_BY_PROPERTY = """
        MATCH (t:Transaction)
        WHERE t.customer_id = $customer_id
          AND t.transaction_timestamp >= $since
          AND ($customer_account IS NULL OR t.customer_account = $customer_account)
          AND {property_filter}
        RETURN count(t) AS tx_count
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