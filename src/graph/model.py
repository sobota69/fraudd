# from dataclasses import dataclass, field
# from datetime import datetime
# from typing import Optional


# @dataclass(frozen=True, slots=True)
# class Customer:
#     customer_id: int


# @dataclass(frozen=True, slots=True)
# class CustomerAccount:
#     customer_account: str


# @dataclass(frozen=True, slots=True)
# class Beneficiary:
#     beneficiary_account: str
#     official_beneficiary_account_name: Optional[str] = None
#     beneficiary_country: Optional[str] = None


# @dataclass(frozen=True, slots=True)
# class Device:
#     device_id: str


# @dataclass(frozen=True, slots=True)
# class Channel:
#     channel: str


# @dataclass(frozen=True, slots=True)
# class Transaction:
#     transaction_id: str
#     transaction_timestamp: datetime

#     customer_id: int
#     customer_account: str

#     channel: str
#     device_id: str

#     amount: float
#     currency: str

#     is_new_beneficiary: bool
#     beneficiary_account: str
#     entered_beneficiary_name: str
#     official_beneficiary_account_name: Optional[str]

#     customer_account_balance: float

#     # Optional enrichment field
#     beneficiary_country: Optional[str] = None


# @dataclass(frozen=True, slots=True)
# class TransactionSummary:
#     transaction_id: str
#     transaction_timestamp: datetime
#     amount: float
#     currency: str
#     customer_id: int
#     customer_account: str
#     beneficiary_account: str
#     beneficiary_country: Optional[str] = None
#     channel: Optional[str] = None
#     device_id: Optional[str] = None


# @dataclass(frozen=True, slots=True)
# class CountryStat:
#     country: str
#     transaction_count: int
#     total_amount: float


# @dataclass(frozen=True, slots=True)
# class ClientActivityWindow:
#     start_hour: Optional[int]
#     end_hour: Optional[int]
#     hourly_histogram: dict[int, int] = field(default_factory=dict)