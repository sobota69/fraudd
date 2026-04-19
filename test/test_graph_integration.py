"""Integration tests for Neo4jGraphProvider.

Requires a running Neo4j instance.  Skip automatically when the DB is
unreachable so the regular test suite stays green.

    NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=capgemini pytest test/test_graph_integration.py -v
"""

import os
from datetime import datetime, timezone

import pytest

from infrastructure.graph.provider import Neo4jGraphProvider
from infrastructure.graph.cypher_commands import CUSTOMER_SUBGRAPH
from domain.transaction import Transaction

# ── Connection settings (override via env vars) ─────────────────────────────
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "capgemini")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def provider():
    """Create a provider and verify connectivity; skip the whole module if
    Neo4j is not available."""
    p = Neo4jGraphProvider(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        database=NEO4J_DATABASE,
    )
    try:
        p.verify_connectivity()
    except Exception as exc:
        p.close()
        pytest.skip(f"Neo4j not reachable: {exc}")

    # Start each module run with a clean DB
    p.reset_database(include_optional_indexes=False)
    yield p
    p.close()


@pytest.fixture()
def sample_transaction() -> Transaction:
    return Transaction(
        transaction_id="tx-int-0001",
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


# ── Tests ────────────────────────────────────────────────────────────────────

class TestSaveTransaction:
    def test_save_and_retrieve_transaction(self, provider, sample_transaction):
        provider.save_transactions([sample_transaction])

        rows = provider._run_read_many(
            CUSTOMER_SUBGRAPH, customer_id=123,
        )
        assert len(rows) >= 1
        assert rows[0]["tx_id"] == "tx-int-0001"
        assert rows[0]["amount"] == pytest.approx(150.25, rel=1e-2)

    def test_transaction_count_in_time(self, provider, sample_transaction):
        provider.save_transactions([sample_transaction])

        count = provider.get_client_transactions_no_in_time(
            customer_id=123,
            timestamp=datetime.now(timezone.utc),
            minutes=60,
        )
        assert count >= 1


class TestResetDatabase:
    def test_reset_clears_data(self, provider, sample_transaction):
        provider.save_transactions([sample_transaction])
        provider.reset_database(include_optional_indexes=False)

        rows = provider._run_read_many(
            CUSTOMER_SUBGRAPH, customer_id=123,
        )
        assert len(rows) == 0
