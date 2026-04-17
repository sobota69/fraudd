from dataclasses import dataclass, field
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

    def __post_init__(self):
        if isinstance(self.transaction_timestamp, str):
            self.transaction_timestamp = datetime.fromisoformat(self.transaction_timestamp)
        if isinstance(self.amount, str):
            self.amount = float(self.amount)
        if isinstance(self.customer_id, str):
            self.customer_id = int(self.customer_id)
        if isinstance(self.customer_account_balance, str):
            self.customer_account_balance = float(self.customer_account_balance)
        if isinstance(self.is_new_beneficiary, str):
            self.is_new_beneficiary = self.is_new_beneficiary.upper() == "TRUE"
