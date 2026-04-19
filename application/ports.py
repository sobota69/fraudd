"""Abstract interfaces (ports) for infrastructure dependencies.

The application layer depends on these abstractions, not on concrete
implementations. Infrastructure provides the concrete adapters.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from domain.risk import RiskAssessment
from domain.transaction import Transaction


class GraphRepository(ABC):
    """Port for graph database operations."""

    @abstractmethod
    def reset_database(self) -> None: ...

    @abstractmethod
    def save_transactions(self, transactions: Sequence[Transaction]) -> None: ...

    @abstractmethod
    def update_risk_assessments(self, assessments: Sequence[RiskAssessment]) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class ResultExporter(ABC):
    """Port for exporting risk assessment results."""

    @abstractmethod
    def export(self, assessments: Sequence[RiskAssessment]) -> bytes: ...
