"""
Ruleset validator.

Validation is intentionally explicit and conservative. The validator enforces
the semantic contract shared by YAML and code-based authoring before metadata
can be published.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from rules_engine.enums import (
    AggregateFunction,
    AggregateScope,
    COLLECTION_LITERAL_OPERATORS,
    ORDER_SENSITIVE_AGGREGATES,
    UNARY_OPERATORS,
    ComparisonOperator,
    NullResultMode,
    ObjectType,
    ValidationSeverity,
)
from rules_engine.models import (
    AggregateOperand,
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    RowFilterPredicate,
    Rule,
    Ruleset,
    ValidationResult,
)
from rules_engine.registry import FunctionRegistry


class RulesetValidator:
    """
    Validate canonical ruleset models.
    """

    def __init__(self, function_registry: FunctionRegistry | None = None) -> None:
        self._function_registry = function_registry

    def validate(self, ruleset: Ruleset) -> ValidationResult:
        """
        Validate a ruleset.

        Parameters
        ----------
        ruleset : Ruleset
            Ruleset to validate.

        Returns
        -------
        ValidationResult
            Structured validation result.
        """
        result = ValidationResult()
        self.populate_result(ruleset, result)
        return result.finalize()

    def populate_result(self, ruleset: Ruleset, result: ValidationResult) -> None:
        """
        Add validation issues for a ruleset into an existing result object.
        """
        self._validate_ruleset(ruleset, result)

    def _validate_ruleset(self, ruleset: Ruleset, result: ValidationResult) -> None:
        if not ruleset.ruleset_id:
            self._add(result, "RULESET_ID_REQUIRED", "ruleset_id is required.", ObjectType.RULESET, "")
        if not ruleset.ruleset_name:
            self._add(
                result,
                "RULESET_NAME_REQUIRED",
                "ruleset_name is required.",
                ObjectType.RULESET,
                ruleset.ruleset_id,
            )
        if not ruleset.rules:
            self._add(
                result,
                "RULESET_RULE_REQUIRED",
                "At least one rule is required.",
                ObjectType.RULESET,
                ruleset.ruleset_id,
            )

        seen_rule_orders: set[int] = set()
        seen_rule_ids: set[str] = set()
        seen_condition_ids: set[str] = set()
        for rule in ruleset.rules:
            if rule.rule_id in seen_rule_ids:
                self._add(
                    result,
                    "RULE_ID_DUPLICATE",
                    f"Duplicate rule_id detected: {rule.rule_id}",
                    ObjectType.RULE,
                    rule.rule_id,
                )
            seen_rule_ids.add(rule.rule_id)
            if rule.rule_order in seen_rule_orders:
                self._add(
                    result,
                    "RULE_ORDER_DUPLICATE",
                    f"Duplicate rule_order detected: {rule.rule_order}",
                    ObjectType.RULE,
                    rule.rule_id,
                )
            seen_rule_orders.add(rule.rule_order)
            self._validate_rule(rule, result, seen_condition_ids)

    def _validate_rule(
        self,
        rule: Rule,
        result: ValidationResult,
        seen_condition_ids: set[str],
    ) -> None:
        if not rule.rule_name:
            self._add(result, "RULE_NAME_REQUIRED", "rule_name is required.", ObjectType.RULE, rule.rule_id)
        self._validate_condition_group(rule.root_group, result, seen_condition_ids)
        if not rule.assignments:
            self._add(
                result,
                "RULE_ASSIGNMENT_REQUIRED",
                "Each rule must define at least one assignment.",
                ObjectType.RULE,
                rule.rule_id,
            )
        seen_assignment_ids: set[str] = set()
        for assignment in rule.assignments:
            if assignment.assignment_id in seen_assignment_ids:
                self._add(
                    result,
                    "ASSIGNMENT_ID_DUPLICATE",
                    f"Duplicate assignment_id detected: {assignment.assignment_id}",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                )
            seen_assignment_ids.add(assignment.assignment_id)
            self._validate_assignment(assignment, result)

    def _validate_condition_group(
        self,
        group: ConditionGroup,
        result: ValidationResult,
        seen_condition_ids: set[str],
    ) -> None:
        if not group.condition_group_id:
            self._add(
                result,
                "CONDITION_GROUP_ID_REQUIRED",
                "condition_group_id is required.",
                ObjectType.CONDITION_GROUP,
                "",
            )
        if not group.conditions and not group.groups:
            self._add(
                result,
                "CONDITION_GROUP_EMPTY",
                "Condition group must contain at least one condition or nested group.",
                ObjectType.CONDITION_GROUP,
                group.condition_group_id,
            )
        for condition in group.conditions:
            if condition.condition_id in seen_condition_ids:
                self._add(
                    result,
                    "CONDITION_ID_DUPLICATE",
                    f"Duplicate condition_id detected: {condition.condition_id}",
                    ObjectType.CONDITION,
                    condition.condition_id,
                )
            seen_condition_ids.add(condition.condition_id)
            self._validate_condition(condition, result)
        for nested_group in group.groups:
            self._validate_condition_group(nested_group, result, seen_condition_ids)

    def _validate_condition(self, condition: Condition, result: ValidationResult) -> None:
        if condition.tolerance_abs < Decimal("0"):
            self._add(
                result,
                "TOLERANCE_NEGATIVE",
                "tolerance_abs must be non-negative.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.null_result_mode is NullResultMode.DEFAULT and condition.null_default_value is None:
            self._add(
                result,
                "NULL_DEFAULT_REQUIRED",
                "null_default_value is required when null_result_mode is default.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        self._validate_operand(condition.left, result, condition.condition_id, in_assignment=False)
        if condition.right is not None:
            self._validate_operand(condition.right, result, condition.condition_id, in_assignment=False)
        self._validate_operator_operands(condition, result)

    def _validate_operator_operands(self, condition: Condition, result: ValidationResult) -> None:
        if condition.operator in UNARY_OPERATORS and condition.right is not None:
            self._add(
                result,
                "UNARY_OPERATOR_RIGHT_FORBIDDEN",
                f"Operator {condition.operator.value} must not define a right operand.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.operator not in UNARY_OPERATORS and condition.right is None:
            self._add(
                result,
                "BINARY_OPERATOR_RIGHT_REQUIRED",
                f"Operator {condition.operator.value} requires a right operand.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.operator in COLLECTION_LITERAL_OPERATORS and isinstance(condition.right, LiteralOperand):
            self._validate_collection_literal(condition, result)
        if (
            condition.operator in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN}
            and condition.tolerance_abs != Decimal("0")
        ):
            self._add(
                result,
                "BETWEEN_TOLERANCE_FORBIDDEN",
                "tolerance_abs must be 0 for between/not_between operators.",
                ObjectType.CONDITION,
                condition.condition_id,
            )

    def _validate_collection_literal(self, condition: Condition, result: ValidationResult) -> None:
        right = condition.right
        if not isinstance(right, LiteralOperand):
            return
        if condition.operator in {ComparisonOperator.IN, ComparisonOperator.NOT_IN}:
            if not isinstance(right.value, (list, tuple, set)):
                self._add(
                    result,
                    "IN_OPERATOR_COLLECTION_REQUIRED",
                    f"Operator {condition.operator.value} requires a collection literal on the right side.",
                    ObjectType.CONDITION,
                    condition.condition_id,
                )
        if condition.operator in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN}:
            if not isinstance(right.value, (list, tuple)) or len(right.value) != 2:
                self._add(
                    result,
                    "BETWEEN_OPERATOR_PAIR_REQUIRED",
                    f"Operator {condition.operator.value} requires exactly two literal values.",
                    ObjectType.CONDITION,
                    condition.condition_id,
                )

    def _validate_assignment(self, assignment: Assignment, result: ValidationResult) -> None:
        if not assignment.target_field:
            self._add(
                result,
                "ASSIGNMENT_TARGET_REQUIRED",
                "target_field is required.",
                ObjectType.ASSIGNMENT,
                assignment.assignment_id,
            )
        self._validate_operand(assignment.value, result, assignment.assignment_id, in_assignment=True)

    def _validate_operand(
        self,
        operand: Operand,
        result: ValidationResult,
        object_id: str,
        *,
        in_assignment: bool,
    ) -> None:
        object_type = ObjectType.ASSIGNMENT if in_assignment else ObjectType.CONDITION
        if isinstance(operand, FieldOperand):
            if not operand.field_name:
                self._add(
                    result,
                    "FIELD_NAME_REQUIRED",
                    "field_name is required.",
                    object_type,
                    object_id,
                )
        elif isinstance(operand, LiteralOperand):
            return
        elif isinstance(operand, AggregateOperand):
            self._validate_aggregate(operand, result, object_id, in_assignment=in_assignment)
        elif isinstance(operand, CustomFunctionOperand):
            self._validate_custom_function(operand, result, object_id, in_assignment=in_assignment)
        else:
            self._add(
                result,
                "OPERAND_KIND_UNSUPPORTED",
                f"Unsupported operand type: {type(operand).__name__}",
                object_type,
                object_id,
            )

    def _validate_aggregate(
        self,
        operand: AggregateOperand,
        result: ValidationResult,
        object_id: str,
        *,
        in_assignment: bool,
    ) -> None:
        if in_assignment:
            self._add(
                result,
                "AGGREGATE_ASSIGNMENT_FORBIDDEN",
                "Aggregate operands are not allowed in assignments in v1.",
                ObjectType.ASSIGNMENT,
                object_id,
            )
        if not operand.field_name:
            self._add(
                result,
                "AGGREGATE_FIELD_REQUIRED",
                "Aggregate field_name is required.",
                ObjectType.CONDITION,
                object_id,
            )
        if operand.scope is AggregateScope.GROUP and not operand.by:
            self._add(
                result,
                "AGGREGATE_GROUP_BY_REQUIRED",
                "scope=group requires non-empty by.",
                ObjectType.CONDITION,
                object_id,
            )
        if operand.scope is AggregateScope.DATASET and operand.by:
            self._add(
                result,
                "AGGREGATE_DATASET_BY_FORBIDDEN",
                "scope=dataset forbids by.",
                ObjectType.CONDITION,
                object_id,
            )
        if operand.function is AggregateFunction.QUANTILE:
            q = operand.args.get("q")
            if not self._valid_quantile(q):
                self._add(
                    result,
                    "QUANTILE_Q_REQUIRED",
                    "quantile requires args.q within [0, 1].",
                    ObjectType.CONDITION,
                    object_id,
                )
        if operand.function in ORDER_SENSITIVE_AGGREGATES and not operand.order_by:
            self._add(
                result,
                "AGGREGATE_ORDER_BY_REQUIRED",
                f"Aggregate {operand.function.value} requires explicit order_by.",
                ObjectType.CONDITION,
                object_id,
            )
        for order in operand.order_by:
            if not order.field:
                self._add(
                    result,
                    "ORDER_BY_FIELD_REQUIRED",
                    "order_by field is required.",
                    ObjectType.CONDITION,
                    object_id,
                )
            if order.direction not in {"asc", "desc"}:
                self._add(
                    result,
                    "ORDER_BY_DIRECTION_INVALID",
                    "order_by direction must be asc or desc.",
                    ObjectType.CONDITION,
                    object_id,
                )
        if operand.null_result_mode is NullResultMode.DEFAULT and operand.null_default_value is None:
            self._add(
                result,
                "AGGREGATE_NULL_DEFAULT_REQUIRED",
                "Aggregate null_default_value is required when null_result_mode is default.",
                ObjectType.CONDITION,
                object_id,
            )
        if operand.filter is not None:
            if not operand.filter.predicates:
                self._add(
                    result,
                    "AGGREGATE_FILTER_EMPTY",
                    "Aggregate filter must contain at least one predicate.",
                    ObjectType.CONDITION,
                    object_id,
                )
            for predicate in operand.filter.predicates:
                self._validate_row_filter_predicate(predicate, result, object_id)

    def _validate_row_filter_predicate(
        self,
        predicate: RowFilterPredicate,
        result: ValidationResult,
        object_id: str,
    ) -> None:
        if predicate.tolerance_abs < Decimal("0"):
            self._add(
                result,
                "FILTER_TOLERANCE_NEGATIVE",
                "Filtered aggregate tolerance_abs must be non-negative.",
                ObjectType.CONDITION,
                object_id,
            )
        if predicate.null_result_mode is NullResultMode.DEFAULT and predicate.null_default_value is None:
            self._add(
                result,
                "FILTER_NULL_DEFAULT_REQUIRED",
                "Filtered aggregate null_default_value is required when null_result_mode is default.",
                ObjectType.CONDITION,
                object_id,
            )
        for side_name, operand in (("left", predicate.left), ("right", predicate.right)):
            if operand is None:
                continue
            if isinstance(operand, AggregateOperand):
                self._add(
                    result,
                    "NESTED_AGGREGATE_FORBIDDEN",
                    f"Aggregate filter {side_name} operand must be row-level.",
                    ObjectType.CONDITION,
                    object_id,
                )
            else:
                self._validate_operand(operand, result, object_id, in_assignment=False)
        pseudo_condition = Condition(
            condition_id=object_id,
            left=predicate.left,
            operator=predicate.operator,
            right=predicate.right,
            tolerance_abs=predicate.tolerance_abs,
            null_input_mode=predicate.null_input_mode,
            null_result_mode=predicate.null_result_mode,
            null_default_value=predicate.null_default_value,
        )
        self._validate_operator_operands(pseudo_condition, result)

    def _validate_custom_function(
        self,
        operand: CustomFunctionOperand,
        result: ValidationResult,
        object_id: str,
        *,
        in_assignment: bool,
    ) -> None:
        object_type = ObjectType.ASSIGNMENT if in_assignment else ObjectType.CONDITION
        if self._function_registry is None:
            self._add(
                result,
                "CUSTOM_FUNCTION_REGISTRY_REQUIRED",
                "Custom function registry is required when custom functions are referenced.",
                object_type,
                object_id,
            )
            return
        try:
            spec = self._function_registry.get_spec(operand.function_name)
        except Exception:
            self._add(
                result,
                "CUSTOM_FUNCTION_UNKNOWN",
                f"Unknown custom function: {operand.function_name}",
                object_type,
                object_id,
            )
            return
        if not spec.active_flag:
            self._add(
                result,
                "CUSTOM_FUNCTION_INACTIVE",
                f"Custom function is inactive: {operand.function_name}",
                object_type,
                object_id,
            )
        if in_assignment and not spec.allowed_in_assignment_flag:
            self._add(
                result,
                "CUSTOM_FUNCTION_ASSIGNMENT_FORBIDDEN",
                f"Custom function is not allowed in assignments: {operand.function_name}",
                object_type,
                object_id,
            )
        if not in_assignment and not spec.allowed_in_condition_flag:
            self._add(
                result,
                "CUSTOM_FUNCTION_CONDITION_FORBIDDEN",
                f"Custom function is not allowed in conditions: {operand.function_name}",
                object_type,
                object_id,
            )
        expected = set(spec.arg_names)
        actual = set(operand.args.keys())
        if actual != expected:
            self._add(
                result,
                "CUSTOM_FUNCTION_ARGS_MISMATCH",
                "Custom function args must exactly match the registered contract.",
                object_type,
                object_id,
                details={
                    "function_name": operand.function_name,
                    "expected": sorted(expected),
                    "actual": sorted(actual),
                },
            )

    def _valid_quantile(self, value: Any) -> bool:
        if value is None:
            return False
        try:
            numeric = Decimal(str(value))
        except Exception:
            return False
        return Decimal("0") <= numeric <= Decimal("1")

    def _add(
        self,
        result: ValidationResult,
        check_name: str,
        message: str,
        object_type: ObjectType,
        object_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        result.add_issue(
            ValidationSeverity.ERROR,
            check_name,
            message,
            object_type,
            object_id,
            details,
        )
