# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------
from datetime import datetime, timezone

from provider import Neo4jGraphProvider
from model import Transaction


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