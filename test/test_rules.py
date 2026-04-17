"""Unit tests for all FRAML rule services."""

import pytest
import pandas as pd

from src.rules import (
    ALL_RULES,
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
)
from src.rules.base_rule import RuleResult


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_transaction():
    return pd.Series({
        "transaction_id": "TX-000001",
        "transaction_timestamp": "2025-12-19T00:00:00+00:00",
        "customer_id": 2045,
        "customer_account": "PL270398487102963371148627",
        "channel": "Mobile",
        "device_id": "MOB-IOS-21E3BCAD",
        "amount": 32.05,
        "currency": "EUR",
        "is_new_beneficiary": False,
        "beneficiary_account": "DE358075381662562554536117",
        "entered_beneficiary_name": "Justyna Brzykcy",
        "official_beneficiary_account_name": "Justyna Brzykcy",
        "customer_account_balance": 455420,
    })


@pytest.fixture
def sample_history():
    return pd.DataFrame([
        {
            "transaction_id": f"TX-{i:06d}",
            "transaction_timestamp": f"2025-12-{18-i}T10:00:00+00:00",
            "customer_id": 2045,
            "customer_account": "PL270398487102963371148627",
            "channel": "Mobile",
            "amount": 50.0 + i * 10,
            "currency": "EUR",
            "is_new_beneficiary": False,
            "beneficiary_account": "DE358075381662562554536117",
            "entered_beneficiary_name": "Justyna Brzykcy",
            "official_beneficiary_account_name": "Justyna Brzykcy",
            "customer_account_balance": 460000 - i * 100,
        }
        for i in range(1, 11)
    ])


# # ── Generic tests ─────────────────────────────────────────────────────────────

# class TestAllRulesRegistered:
#     def test_all_rules_count(self):
#         assert len(ALL_RULES) == 14

#     def test_all_rules_return_rule_result(self, sample_transaction, sample_history):
#         for RuleClass in ALL_RULES:
#             rule = RuleClass()
#             result = rule.evaluate(sample_transaction, history=sample_history)
#             assert isinstance(result, RuleResult)


# # ── R1 ────────────────────────────────────────────────────────────────────────

# class TestR1CopNameMismatchHard:
#     def test_instantiation(self):
#         rule = R1CopNameMismatchHard()
#         assert rule.rule_id == "R1"
#         assert rule.mandatory is True

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R1CopNameMismatchHard()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_exact_match_should_not_trigger(self, sample_transaction):
#         """TODO: implement once rule logic is added."""
#         pass

#     def test_mismatch_should_trigger(self, sample_transaction):
#         """TODO: implement once rule logic is added."""
#         pass


# # ── R2 ────────────────────────────────────────────────────────────────────────

# class TestR2CopNameMismatchSoft:
#     def test_instantiation(self):
#         rule = R2CopNameMismatchSoft()
#         assert rule.rule_id == "R2"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R2CopNameMismatchSoft()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_soft_warning_range(self, sample_transaction):
#         """TODO: implement – similarity 80-89% should trigger."""
#         pass


# # ── R3 ────────────────────────────────────────────────────────────────────────

# class TestR3NewBeneficiaryCopMismatch:
#     def test_instantiation(self):
#         rule = R3NewBeneficiaryCopMismatch()
#         assert rule.rule_id == "R3"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R3NewBeneficiaryCopMismatch()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_new_beneficiary_with_mismatch(self, sample_transaction):
#         """TODO: implement – new beneficiary + similarity < 90% should trigger."""
#         pass


# # ── R6 ────────────────────────────────────────────────────────────────────────

# class TestR6HighAmountSpike:
#     def test_instantiation(self):
#         rule = R6HighAmountSpike()
#         assert rule.rule_id == "R6"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R6HighAmountSpike()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_spike_above_3x_average(self, sample_transaction, sample_history):
#         """TODO: implement – amount > 3× 30-day avg should trigger."""
#         pass

#     def test_spike_above_10x_strong(self, sample_transaction, sample_history):
#         """TODO: implement – amount > 10× should be STRONG severity."""
#         pass


# # ── R7 ────────────────────────────────────────────────────────────────────────

# class TestR7HighFrequencyTransfers:
#     def test_instantiation(self):
#         rule = R7HighFrequencyTransfers()
#         assert rule.rule_id == "R7"
#         assert rule.mandatory is True

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R7HighFrequencyTransfers()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_five_tx_in_ten_minutes(self, sample_transaction):
#         """TODO: implement – ≥5 tx in 10 min should trigger."""
#         pass


# # ── R8 ────────────────────────────────────────────────────────────────────────

# class TestR8NewPayeesBurst:
#     def test_instantiation(self):
#         rule = R8NewPayeesBurst()
#         assert rule.rule_id == "R8"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R8NewPayeesBurst()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_three_new_payees_in_24h(self, sample_transaction):
#         """TODO: implement – ≥3 new payees in 24h should trigger."""
#         pass


# # ── R10 ───────────────────────────────────────────────────────────────────────

# class TestR10CrossBorderAnomaly:
#     def test_instantiation(self):
#         rule = R10CrossBorderAnomaly()
#         assert rule.rule_id == "R10"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R10CrossBorderAnomaly()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_new_country_high_amount(self, sample_transaction):
#         """TODO: implement – new country + amount > €15k should trigger."""
#         pass


# # ── R12 ───────────────────────────────────────────────────────────────────────

# class TestR12ZscoreAmount:
#     def test_instantiation(self):
#         rule = R12ZscoreAmount()
#         assert rule.rule_id == "R12"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R12ZscoreAmount()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_zscore_above_3(self, sample_transaction, sample_history):
#         """TODO: implement – z-score > 3 should trigger."""
#         pass


# # ── R13 ───────────────────────────────────────────────────────────────────────

# class TestR13UnusualHour:
#     def test_instantiation(self):
#         rule = R13UnusualHour()
#         assert rule.rule_id == "R13"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R13UnusualHour()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_unusual_hour_trigger(self, sample_transaction):
#         """TODO: implement – hour outside 90% window should trigger."""
#         pass

#     def test_insufficient_history(self, sample_transaction):
#         """TODO: implement – < 10 tx history should not trigger."""
#         pass


# # ── R17 ───────────────────────────────────────────────────────────────────────

# class TestR17SmurfingStructuring:
#     def test_instantiation(self):
#         rule = R17SmurfingStructuring()
#         assert rule.rule_id == "R17"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R17SmurfingStructuring()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_five_tx_in_range_2h(self, sample_transaction):
#         """TODO: implement – ≥5 tx €13,500–€14,999 in 2h should trigger."""
#         pass


# # ── R18 ───────────────────────────────────────────────────────────────────────

# class TestR18RoundAmountsAnomaly:
#     def test_instantiation(self):
#         rule = R18RoundAmountsAnomaly()
#         assert rule.rule_id == "R18"
#         assert rule.mandatory is True

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R18RoundAmountsAnomaly()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_three_round_amounts_48h(self, sample_transaction):
#         """TODO: implement – ≥3 round amounts in 48h should trigger."""
#         pass


# # ── R21 ───────────────────────────────────────────────────────────────────────

# class TestR21RapidAccountEmptying:
#     def test_instantiation(self):
#         rule = R21RapidAccountEmptying()
#         assert rule.rule_id == "R21"

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R21RapidAccountEmptying()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_balance_drop_70_percent(self, sample_transaction):
#         """TODO: implement – balance drop > 70% in 1h should trigger."""
#         pass


# # ── R22 ───────────────────────────────────────────────────────────────────────

# class TestR22AbsoluteHighValue:
#     def test_instantiation(self):
#         rule = R22AbsoluteHighValue()
#         assert rule.rule_id == "R22"
#         assert rule.mandatory is True

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R22AbsoluteHighValue()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_amount_above_15000(self, sample_transaction):
#         """TODO: implement – amount > €15,000 should trigger."""
#         pass

#     def test_amount_below_15000(self, sample_transaction):
#         """TODO: implement – amount ≤ €15,000 should not trigger."""
#         pass


# # ── R24 ───────────────────────────────────────────────────────────────────────

# class TestR24ChannelSpecificThreshold:
#     def test_instantiation(self):
#         rule = R24ChannelSpecificThreshold()
#         assert rule.rule_id == "R24"
#         assert rule.mandatory is True

#     def test_evaluate_returns_result(self, sample_transaction, sample_history):
#         rule = R24ChannelSpecificThreshold()
#         result = rule.evaluate(sample_transaction, history=sample_history)
#         assert isinstance(result, RuleResult)

#     def test_mobile_threshold(self, sample_transaction):
#         """TODO: implement – Mobile amount > €2,000 should trigger."""
#         pass

#     def test_web_threshold(self, sample_transaction):
#         """TODO: implement – Web amount > €5,000 should trigger."""
#         pass
