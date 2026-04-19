"""Base class for all FRAML rules."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class Severity(Enum):
    MILD = 1
    STRONG = 2


@dataclass
class RuleResult:
    """Result returned after evaluating a rule on a transaction."""
    rule_id: str
    rule_name: str
    triggered: bool
    severity: Optional[Severity] = None
    score: float = 0.0
    weight: int = 0
    details: dict = field(default_factory=dict)


class BaseRule(ABC):
    """Abstract base for every fraud-detection rule."""

    rule_id: str = ""
    rule_name: str = ""
    category: str = ""
    weight: int = 0
    mandatory: bool = False

    @abstractmethod
    def evaluate(
        self,
        transaction: "Transaction",
        history: Optional[List["Transaction"]] = None,
    ) -> RuleResult:
        """Evaluate the rule against a single transaction.

        Args:
            transaction: A single row from the transactions DataFrame.
            history: Historical transactions (may be needed for velocity/anomaly rules).

        Returns:
            A RuleResult indicating whether the rule was triggered.
        """
        ...
