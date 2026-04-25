"""
Spark compatibility validation for ruleset metadata.

The base ``RulesetValidator`` enforces the engine's semantic contract. This
validator adds Databricks Spark runtime checks for features that are valid
metadata but intentionally unsupported by the current Spark execution path.
"""

from __future__ import annotations

from rules_engine.enums import ObjectType, ValidationSeverity
from rules_engine.models import (
    AggregateOperand,
    Condition,
    ConditionGroup,
    Operand,
    Ruleset,
    ValidationResult,
)
from rules_engine.spark_constraints import (
    spark_aggregate_compatibility_errors,
    spark_filter_predicate_compatibility_errors,
)
from rules_engine.validator import RulesetValidator


class SparkRulesetCompatibilityValidator(RulesetValidator):
    """
    Validate a ruleset for the current Spark DataFrame runtime.
    """

    def validate(self, ruleset: Ruleset):
        """
        Validate base semantics plus Spark runtime compatibility.
        """
        result = ValidationResult()
        self.populate_result(ruleset, result)
        for rule in ruleset.rules:
            self._validate_group_for_spark(rule.root_group, result)
        return result

    def _validate_group_for_spark(self, group: ConditionGroup, result) -> None:
        """
        Validate every condition in a group tree for Spark compatibility.
        """
        for condition in group.conditions:
            self._validate_condition_for_spark(condition, result)
        for nested_group in group.groups:
            self._validate_group_for_spark(nested_group, result)

    def _validate_condition_for_spark(self, condition: Condition, result) -> None:
        """
        Validate Spark compatibility for operands used by one condition.
        """
        self._validate_operand_for_spark(condition.left, condition.condition_id, result)
        if condition.right is not None:
            self._validate_operand_for_spark(condition.right, condition.condition_id, result)

    def _validate_operand_for_spark(self, operand: Operand, condition_id: str, result) -> None:
        """
        Dispatch Spark compatibility checks for aggregate operands.
        """
        if isinstance(operand, AggregateOperand):
            self._validate_aggregate_for_spark(operand, condition_id, result)

    def _validate_aggregate_for_spark(
        self,
        operand: AggregateOperand,
        condition_id: str,
        result,
    ) -> None:
        """
        Add Spark compatibility errors for unsupported aggregate features.
        """
        for check_name, message in spark_aggregate_compatibility_errors(operand):
            self._add_spark_error(result, check_name, message, condition_id)
        if operand.filter is not None:
            for predicate in operand.filter.predicates:
                for check_name, message in spark_filter_predicate_compatibility_errors(predicate):
                    self._add_spark_error(result, check_name, message, condition_id)

    def _add_spark_error(self, result, check_name: str, message: str, condition_id: str) -> None:
        """
        Append one Spark compatibility validation error.
        """
        result.add_issue(
            ValidationSeverity.ERROR,
            check_name,
            message,
            ObjectType.CONDITION,
            condition_id,
        )
