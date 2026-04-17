from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Transaction:
    transaction_id: str
    transaction_timestamp: datetime
    customer_id: int
    customer_account: str
    channel: str
    device_id: str
    amount: float
    currency: str
    is_new_beneficiary: bool
    beneficiary_account: str
    entered_beneficiary_name: str
    official_beneficiary_account_name: str
    customer_account_balance: float
