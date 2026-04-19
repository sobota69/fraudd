"""Integration tests for Neo4jGraphProvider.

Requires a running Neo4j instance.  Skip automatically when the DB is
unreachable so the regular test suite stays green.

    NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=capgemini pytest test/test_graph_integration.py -v
"""

import os
from datetime import datetime, timezone

import pytest

from infrastructure.graph.provider import Neo4jGraphProvider
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
    def test_save_and_retrieve_avg_amount(self, provider, sample_transaction):
        provider.save_transaction(sample_transaction)

        avg = provider.get_client_30d_avg_amount(customer_id=123)
        assert avg is not None
        assert avg == pytest.approx(150.25, rel=1e-2)

    def test_previous_transaction_returns_saved(self, provider, sample_transaction):
        provider.save_transaction(sample_transaction)

        prev = provider.get_previous_transaction(
            customer_id=123,
            before=datetime.now(timezone.utc),
        )
        assert prev is not None
        assert prev.transaction_id == "tx-int-0001"


class TestResetDatabase:
    def test_reset_clears_data(self, provider, sample_transaction):
        provider.save_transaction(sample_transaction)
        provider.reset_database(include_optional_indexes=False)

        avg = provider.get_client_30d_avg_amount(customer_id=123)
        assert avg is None or avg == 0.0
