"""
Spark compatibility validation for ruleset metadata.

The base ``RulesetValidator`` enforces the engine's semantic contract. This
validator adds Databricks Spark runtime checks for features that are valid
metadata but intentionally unsupported by the current Spark execution path.
"""

from __future__ import annotations

from rules_engine.enums import (
    AggregateFunction,
    NullInputMode,
    NullResultMode,
    ObjectType,
    ValidationSeverity,
)
from rules_engine.models import AggregateOperand, Condition, ConditionGroup, Operand, Ruleset
from rules_engine.validator import RulesetValidator


class SparkRulesetCompatibilityValidator(RulesetValidator):
    """
    Validate a ruleset for the current Spark DataFrame runtime.
    """

    def validate(self, ruleset: Ruleset):
        """
        Validate base semantics plus Spark runtime compatibility.
        """
        result = super().validate(ruleset)
        for rule in ruleset.rules:
            self._validate_group_for_spark(rule.root_group, result)
        return result.finalize()

    def _validate_group_for_spark(self, group: ConditionGroup, result) -> None:
        for condition in group.conditions:
            self._validate_condition_for_spark(condition, result)
        for nested_group in group.groups:
            self._validate_group_for_spark(nested_group, result)

    def _validate_condition_for_spark(self, condition: Condition, result) -> None:
        self._validate_operand_for_spark(condition.left, condition.condition_id, result)
        if condition.right is not None:
            self._validate_operand_for_spark(condition.right, condition.condition_id, result)

    def _validate_operand_for_spark(self, operand: Operand, condition_id: str, result) -> None:
        if isinstance(operand, AggregateOperand):
            self._validate_aggregate_for_spark(operand, condition_id, result)

    def _validate_aggregate_for_spark(
        self,
        operand: AggregateOperand,
        condition_id: str,
        result,
    ) -> None:
        if operand.function in {AggregateFunction.MEDIAN, AggregateFunction.QUANTILE}:
            self._add_spark_error(
                result,
                "SPARK_EXACT_PERCENTILE_UNSUPPORTED",
                "Spark runtime does not support exact median or quantile in this pass.",
                condition_id,
            )
        if operand.null_input_mode is NullInputMode.ERROR:
            self._add_spark_error(
                result,
                "SPARK_AGGREGATE_NULL_INPUT_ERROR_UNSUPPORTED",
                "Spark runtime does not support aggregate null_input_mode=error.",
                condition_id,
            )
        if operand.null_result_mode is NullResultMode.ERROR:
            self._add_spark_error(
                result,
                "SPARK_AGGREGATE_NULL_RESULT_ERROR_UNSUPPORTED",
                "Spark runtime does not support aggregate null_result_mode=error.",
                condition_id,
            )
        if (
            operand.function in {AggregateFunction.FIRST, AggregateFunction.LAST}
            and operand.null_input_mode is NullInputMode.PROPAGATE
        ):
            self._add_spark_error(
                result,
                "SPARK_FIRST_LAST_PROPAGATE_UNSUPPORTED",
                "Spark runtime does not support first/last with null_input_mode=propagate.",
                condition_id,
            )
        if operand.filter is not None:
            for predicate in operand.filter.predicates:
                if predicate.null_input_mode is NullInputMode.ERROR:
                    self._add_spark_error(
                        result,
                        "SPARK_FILTER_NULL_INPUT_ERROR_UNSUPPORTED",
                        "Spark aggregate filters do not support null_input_mode=error.",
                        condition_id,
                    )
                if predicate.null_result_mode is NullResultMode.ERROR:
                    self._add_spark_error(
                        result,
                        "SPARK_FILTER_NULL_RESULT_ERROR_UNSUPPORTED",
                        "Spark aggregate filters do not support null_result_mode=error.",
                        condition_id,
                    )

    def _add_spark_error(self, result, check_name: str, message: str, condition_id: str) -> None:
        result.add_issue(
            ValidationSeverity.ERROR,
            check_name,
            message,
            ObjectType.CONDITION,
            condition_id,
        )
