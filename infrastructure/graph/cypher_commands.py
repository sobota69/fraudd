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

UPSERT_TRANSACTIONS_CYPHER = """
UNWIND $transactions AS tx

// ---- Customer ----
MERGE (c:Customer {customer_id: tx.customer_id})

// ---- Customer Account ----
MERGE (ca:CustomerAccount {customer_account: tx.customer_account})
MERGE (c)-[:OWNS]->(ca)

// ---- Transaction (insert-only semantics) ----
MERGE (t:Transaction {transaction_id: tx.transaction_id})
ON CREATE SET
    t.transaction_timestamp      = datetime(tx.transaction_timestamp),
    t.amount                     = tx.amount,
    t.currency                   = tx.currency,
    t.is_new_beneficiary         = tx.is_new_beneficiary,
    t.entered_beneficiary_name   = tx.entered_beneficiary_name,
    t.customer_account_balance   = tx.customer_account_balance,
    t.device_id                  = tx.device_id,
    t.channel                    = tx.channel,

    // denormalized fields for fast querying
    t.customer_id                = tx.customer_id,
    t.customer_account           = tx.customer_account,
    t.beneficiary_account        = tx.beneficiary_account,
    t.beneficiary_country        = tx.beneficiary_country,

    // time helpers
    t.transaction_hour_of_day    = tx.transaction_hour_of_day,
    t.transaction_day_of_week    = tx.transaction_day_of_week

// ---- Relationship: account → transaction ----
MERGE (ca)-[:TRANSFER]->(t)

// ---- Beneficiary ----
MERGE (b:Beneficiary {beneficiary_account: tx.beneficiary_account})
ON CREATE SET
    b.official_beneficiary_account_name = tx.official_beneficiary_account_name,
    b.beneficiary_country               = tx.beneficiary_country

// ---- Relationship: transaction → beneficiary ----
MERGE (t)-[:TO]->(b);
    """

UPDATE_TRANSACTIONS_ASSESSMENTS = """

UNWIND $assessments AS u
MATCH (t:Transaction {transaction_id: u.transaction_id})
SET
    t.triggered_rules       = u.triggered_rules,
    t.is_fraud_transaction  = u.is_fraud_transaction,
    t.risk_score            = u.risk_score,
    t.risk_category         = u.risk_category;

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
      AND t.transaction_timestamp >= $from_timestamp
      AND t.transaction_timestamp < $to_timestamp
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

# ── Dashboard-specific queries (moved from presentation layer) ───────────────

CUSTOMER_BENEFICIARY_NETWORK = """
MATCH (c:Customer)-[:OWNS]->(ca:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
RETURN c.customer_id AS customer_id,
       ca.customer_account AS account,
       b.beneficiary_account AS beneficiary,
       b.beneficiary_country AS country,
       count(t) AS tx_count,
       sum(t.amount) AS total_amount,
       avg(t.amount) AS avg_amount
ORDER BY total_amount DESC
LIMIT 500
"""

HIGH_RISK_TRANSACTIONS = """
MATCH (c:Customer)-[:OWNS]->(ca:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
WHERE t.risk_category IS NOT NULL AND t.risk_score > 0
RETURN t.transaction_id AS transaction_id,
       c.customer_id AS customer_id,
       ca.customer_account AS account,
       b.beneficiary_account AS beneficiary,
       t.amount AS amount,
       t.currency AS currency,
       t.channel AS channel,
       t.risk_score AS risk_score,
       t.risk_category AS risk_category,
       t.triggered_rules AS triggered_rules,
       t.is_fraud_transaction AS is_fraud
ORDER BY t.risk_score DESC
LIMIT 200
"""

CUSTOMER_RISK_PROFILE = """
MATCH (c:Customer)-[:OWNS]->(ca:CustomerAccount)-[:TRANSFER]->(t:Transaction)
WHERE t.risk_score IS NOT NULL
RETURN c.customer_id AS customer_id,
       count(t) AS total_transactions,
       sum(CASE WHEN t.risk_score > 0 THEN 1 ELSE 0 END) AS flagged_transactions,
       avg(t.risk_score) AS avg_risk_score,
       max(t.risk_score) AS max_risk_score,
       sum(t.amount) AS total_volume
ORDER BY avg_risk_score DESC
LIMIT 50
"""

BENEFICIARY_HOTSPOTS = """
MATCH (c:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
WITH b, count(DISTINCT c) AS unique_senders, count(t) AS tx_count, sum(t.amount) AS total_received
WHERE unique_senders > 1
RETURN b.beneficiary_account AS beneficiary,
       b.beneficiary_country AS country,
       unique_senders,
       tx_count,
       total_received
ORDER BY unique_senders DESC
LIMIT 30
"""

CROSS_BORDER_FLOWS = """
MATCH (t:Transaction)-[:TO]->(b:Beneficiary)
WHERE b.beneficiary_country IS NOT NULL
RETURN b.beneficiary_country AS country,
       count(t) AS tx_count,
       sum(t.amount) AS total_amount,
       avg(t.amount) AS avg_amount
ORDER BY total_amount DESC
"""

CHANNEL_RISK = """
MATCH (t:Transaction)
WHERE t.channel IS NOT NULL AND t.risk_score IS NOT NULL
RETURN t.channel AS channel,
       count(t) AS tx_count,
       avg(t.risk_score) AS avg_risk,
       sum(CASE WHEN t.risk_category = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count
ORDER BY avg_risk DESC
"""

SHARED_BENEFICIARY_CUSTOMERS = """
MATCH (c1:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(t1:Transaction)-[:TO]->(b:Beneficiary)<-[:TO]-(t2:Transaction)<-[:TRANSFER]-(:CustomerAccount)<-[:OWNS]-(c2:Customer)
WHERE c1.customer_id < c2.customer_id
WITH c1, c2, b,
     count(DISTINCT t1) + count(DISTINCT t2) AS shared_tx_count,
     sum(t1.amount) + sum(t2.amount) AS shared_volume,
     max(CASE WHEN t1.risk_score > t2.risk_score THEN t1.risk_score ELSE t2.risk_score END) AS max_risk
RETURN c1.customer_id AS customer_1,
       c2.customer_id AS customer_2,
       b.beneficiary_account AS shared_beneficiary,
       b.beneficiary_country AS country,
       shared_tx_count,
       shared_volume,
       max_risk
ORDER BY shared_volume DESC
LIMIT 100
"""

NETWORK_DEGREE_METRICS = """
MATCH (c:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
WITH c, collect(DISTINCT b.beneficiary_account) AS beneficiaries,
     count(t) AS tx_count, sum(t.amount) AS total_sent,
     avg(t.risk_score) AS avg_risk,
     max(t.risk_score) AS max_risk
RETURN c.customer_id AS customer_id,
       size(beneficiaries) AS unique_beneficiaries,
       tx_count,
       total_sent,
       avg_risk,
       max_risk,
       CASE WHEN tx_count > 0 THEN total_sent / tx_count ELSE 0 END AS avg_tx_size
ORDER BY unique_beneficiaries DESC
"""

BENEFICIARY_INCOMING_ANALYSIS = """
MATCH (c:Customer)-[:OWNS]->(:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
WITH b, collect(DISTINCT c.customer_id) AS senders,
     count(t) AS tx_count, sum(t.amount) AS total_received,
     avg(t.risk_score) AS avg_risk
RETURN b.beneficiary_account AS beneficiary,
       b.beneficiary_country AS country,
       size(senders) AS unique_senders,
       senders,
       tx_count,
       total_received,
       avg_risk
ORDER BY unique_senders DESC, total_received DESC
LIMIT 50
"""

CURRENCY_BREAKDOWN = """
MATCH (t:Transaction)
WHERE t.currency IS NOT NULL
RETURN t.currency AS currency,
       count(t) AS tx_count,
       sum(t.amount) AS total_volume,
       avg(t.amount) AS avg_amount,
       max(t.amount) AS max_amount,
       sum(CASE WHEN t.is_fraud_transaction IN [true, 'True', 'true'] THEN 1 ELSE 0 END) AS fraud_count,
       sum(CASE WHEN t.is_fraud_transaction IN [true, 'True', 'true'] THEN t.amount ELSE 0 END) AS fraud_volume,
       avg(t.risk_score) AS avg_risk,
       sum(CASE WHEN t.risk_category = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count,
       sum(CASE WHEN t.risk_category = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_count
ORDER BY tx_count DESC
"""

DOMINANT_CURRENCY = """
MATCH (t:Transaction)
WHERE t.currency IS NOT NULL
RETURN t.currency AS currency, count(t) AS cnt
ORDER BY cnt DESC
LIMIT 1
"""

ALL_CUSTOMERS = """
MATCH (c:Customer)
RETURN c.customer_id AS customer_id
ORDER BY c.customer_id
"""

CUSTOMER_SUBGRAPH = """
MATCH (c:Customer {customer_id: $customer_id})-[:OWNS]->(ca:CustomerAccount)-[:TRANSFER]->(t:Transaction)-[:TO]->(b:Beneficiary)
RETURN c.customer_id        AS customer_id,
       ca.customer_account  AS account,
       t.transaction_id     AS tx_id,
       t.amount             AS amount,
       t.currency           AS currency,
       t.channel            AS channel,
       t.risk_score         AS risk_score,
       t.risk_category      AS risk_category,
       t.triggered_rules    AS triggered_rules,
       t.is_fraud_transaction AS is_fraud,
       t.transaction_timestamp AS ts,
       b.beneficiary_account AS beneficiary,
       b.beneficiary_country AS ben_country
ORDER BY t.transaction_timestamp DESC
"""
