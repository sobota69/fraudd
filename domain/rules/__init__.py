from .base_rule import BaseRule
from .r10_cross_border_anomaly import R10CrossBorderAnomaly
from .r13_unusual_hour import R13UnusualHour
from .r18_round_amounts_anomaly import R18RoundAmountsAnomaly
from .r21_rapid_account_emptying import R21RapidAccountEmptying

ALL_RULES = [
    R10CrossBorderAnomaly,
    R13UnusualHour,
    R18RoundAmountsAnomaly,
    R21RapidAccountEmptying,
]

__all__ = [
    "BaseRule",
    "ALL_RULES",
    "R10CrossBorderAnomaly",
    "R13UnusualHour",
    "R18RoundAmountsAnomaly",
    "R21RapidAccountEmptying",
]
