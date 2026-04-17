from .base_rule import BaseRule
from .r1_cop_name_mismatch_hard import R1CopNameMismatchHard
from .r2_cop_name_mismatch_soft import R2CopNameMismatchSoft
from .r3_new_beneficiary_cop_mismatch import R3NewBeneficiaryCopMismatch
from .r6_high_amount_spike import R6HighAmountSpike
from .r7_high_frequency_transfers import R7HighFrequencyTransfers
from .r8_new_payees_burst import R8NewPayeesBurst
from .r10_cross_border_anomaly import R10CrossBorderAnomaly
from .r12_zscore_amount import R12ZscoreAmount
from .r13_unusual_hour import R13UnusualHour
from .r17_smurfing_structuring import R17SmurfingStructuring
from .r18_round_amounts_anomaly import R18RoundAmountsAnomaly
from .r21_rapid_account_emptying import R21RapidAccountEmptying
from .r22_absolute_high_value import R22AbsoluteHighValue
from .r24_channel_specific_threshold import R24ChannelSpecificThreshold

ALL_RULES = [
    R1CopNameMismatchHard,
    R2CopNameMismatchSoft,
    R3NewBeneficiaryCopMismatch,
    R6HighAmountSpike,
    R7HighFrequencyTransfers,
    R8NewPayeesBurst,
    R10CrossBorderAnomaly,
    R12ZscoreAmount,
    R13UnusualHour,
    R17SmurfingStructuring,
    R18RoundAmountsAnomaly,
    R21RapidAccountEmptying,
    R22AbsoluteHighValue,
    R24ChannelSpecificThreshold,
]

__all__ = [
    "BaseRule",
    "ALL_RULES",
    "R1CopNameMismatchHard",
    "R2CopNameMismatchSoft",
    "R3NewBeneficiaryCopMismatch",
    "R6HighAmountSpike",
    "R7HighFrequencyTransfers",
    "R8NewPayeesBurst",
    "R10CrossBorderAnomaly",
    "R12ZscoreAmount",
    "R13UnusualHour",
    "R17SmurfingStructuring",
    "R18RoundAmountsAnomaly",
    "R21RapidAccountEmptying",
    "R22AbsoluteHighValue",
    "R24ChannelSpecificThreshold",
]
