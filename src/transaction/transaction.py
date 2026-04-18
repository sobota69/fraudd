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
    beneficiary_country: Optional[str] = None
    transaction_day_of_week: Optional[int] = None
    transaction_hour_of_day: Optional[int] = None

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
        if not self.beneficiary_country and self.beneficiary_account and isinstance(self.beneficiary_account, str):
            self.beneficiary_country = self.beneficiary_account[:2]
        if not self.transaction_day_of_week and self.transaction_timestamp:
            transaction_day_of_week = self.transaction_timestamp.weekday()
        if not self.transaction_hour_of_day and self.transaction_timestamp:
            transaction_hour_of_day = self.transaction_timestamp.hour
