"""
Ruleset validator.

Validation is intentionally explicit and conservative. The validator enforces
the semantic contract shared by YAML and code-based authoring before metadata
can be published.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from rules_engine.enums import (
    COLLECTION_LITERAL_OPERATORS,
    UNARY_OPERATORS,
    ComparisonOperator,
    NullResultMode,
    ObjectType,
    ValidationSeverity,
)
from rules_engine.exceptions import RegistryError
from rules_engine.models import (
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
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
        """
        Create a validator with an optional custom function registry.
        """
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
        return result

    def populate_result(self, ruleset: Ruleset, result: ValidationResult) -> None:
        """
        Add validation issues for a ruleset into an existing result object.
        """
        self._validate_ruleset(ruleset, result)

    def _validate_ruleset(self, ruleset: Ruleset, result: ValidationResult) -> None:
        """
        Validate top-level ruleset identity, ownership, and child rules.
        """
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
        if not ruleset.owner:
            self._add(
                result,
                "RULESET_OWNER_REQUIRED",
                "owner is required.",
                ObjectType.RULESET,
                ruleset.ruleset_id,
            )
        if not ruleset.owner_department:
            self._add(
                result,
                "RULESET_OWNER_DEPARTMENT_REQUIRED",
                "owner_department is required.",
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

        self._validate_expectations(ruleset, result)

        seen_rule_orders: set[int] = set()
        seen_rule_ids: set[str] = set()
        seen_condition_ids: set[str] = set()
        seen_condition_group_ids: set[str] = set()
        seen_assignment_ids: dict[str, str] = {}
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
            self._validate_rule(
                rule,
                result,
                seen_condition_ids,
                seen_condition_group_ids,
                seen_assignment_ids,
                ruleset,
            )

    def _validate_expectations(
        self,
        ruleset: Ruleset,
        result: ValidationResult,
    ) -> None:
        """Validate the stable shape of embedded executable examples."""
        seen_names: set[str] = set()
        assignment_targets = {
            assignment.target_field
            for rule in ruleset.rules
            for assignment in rule.assignments
        }
        reserved_keys = {"matched", "matched_rule_ids", "assign"}
        for expectation in ruleset.expect:
            object_id = expectation.name
            if not expectation.name:
                self._add(
                    result,
                    "EXPECTED_CASE_NAME_REQUIRED",
                    "Expected case name is required.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            elif expectation.name in seen_names:
                self._add(
                    result,
                    "EXPECTED_CASE_NAME_DUPLICATE",
                    f"Duplicate expected case name: {expectation.name}",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            seen_names.add(expectation.name)
            if not isinstance(expectation.given, Mapping):
                self._add(
                    result,
                    "EXPECTED_CASE_GIVEN_MAPPING_REQUIRED",
                    "Expected case given must be a mapping.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            if not isinstance(expectation.then, Mapping) or not expectation.then:
                self._add(
                    result,
                    "EXPECTED_CASE_THEN_REQUIRED",
                    "Expected case then must be a non-empty mapping.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
                continue
            matched = expectation.then.get("matched")
            if matched is not None and not isinstance(matched, bool):
                self._add(
                    result,
                    "EXPECTED_CASE_MATCHED_BOOLEAN_REQUIRED",
                    "Expected matched value must be boolean.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            matched_ids = expectation.then.get("matched_rule_ids")
            if matched_ids is not None and not (
                isinstance(matched_ids, (list, tuple))
                and all(isinstance(item, str) for item in matched_ids)
            ):
                self._add(
                    result,
                    "EXPECTED_CASE_MATCHED_RULE_IDS_REQUIRED",
                    "Expected matched_rule_ids must be a list of strings.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            expected_assign = expectation.then.get("assign")
            if "assign" in expectation.then and not isinstance(
                expected_assign,
                Mapping,
            ):
                self._add(
                    result,
                    "EXPECTED_CASE_ASSIGN_MAPPING_REQUIRED",
                    "Expected assign value must be a mapping.",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                )
            shorthand_keys = set(expectation.then) - reserved_keys
            explicit_assign_keys = (
                set(expected_assign) if isinstance(expected_assign, Mapping) else set()
            )
            unknown_keys = sorted(
                (shorthand_keys | explicit_assign_keys) - assignment_targets,
                key=str,
            )
            if unknown_keys:
                self._add(
                    result,
                    "EXPECTED_CASE_UNKNOWN_KEY",
                    "Expected assignment keys must match a target field declared "
                    f"by the ruleset: {unknown_keys}",
                    ObjectType.EXPECTED_CASE,
                    object_id,
                    details={
                        "unknown_keys": unknown_keys,
                        "known_target_fields": sorted(assignment_targets),
                    },
                )
            self._validate_finite_decimals(
                expectation.given,
                result,
                ObjectType.EXPECTED_CASE,
                object_id,
            )
            self._validate_finite_decimals(
                expectation.then,
                result,
                ObjectType.EXPECTED_CASE,
                object_id,
            )

    def _validate_rule(
        self,
        rule: Rule,
        result: ValidationResult,
        seen_condition_ids: set[str],
        seen_condition_group_ids: set[str],
        seen_assignment_ids: dict[str, str],
        ruleset: Ruleset,
    ) -> None:
        """
        Validate one rule and its condition tree and assignments.
        """
        if not rule.rule_name:
            self._add(result, "RULE_NAME_REQUIRED", "rule_name is required.", ObjectType.RULE, rule.rule_id)
        self._validate_condition_group(
            rule.root_group,
            result,
            seen_condition_ids,
            seen_condition_group_ids,
        )
        if not rule.assignments:
            self._add(
                result,
                "RULE_ASSIGNMENT_REQUIRED",
                "Each rule must define at least one assignment.",
                ObjectType.RULE,
                rule.rule_id,
            )
        assignments_by_target: dict[str, list[str]] = {}
        for assignment in rule.assignments:
            if assignment.assignment_id in seen_assignment_ids:
                first_rule_id = seen_assignment_ids[assignment.assignment_id]
                duplicate_location = (
                    f"more than once in rule {rule.rule_id}"
                    if first_rule_id == rule.rule_id
                    else f"by rules {first_rule_id} and {rule.rule_id}"
                )
                self._add(
                    result,
                    "ASSIGNMENT_ID_DUPLICATE",
                    "assignment_id must be unique within a ruleset version: "
                    f"{assignment.assignment_id} is used {duplicate_location}.",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                    details={
                        "ruleset_id": ruleset.ruleset_id,
                        "version": ruleset.version,
                        "assignment_id": assignment.assignment_id,
                        "rule_ids": (
                            [rule.rule_id]
                            if first_rule_id == rule.rule_id
                            else [first_rule_id, rule.rule_id]
                        ),
                    },
                )
            else:
                seen_assignment_ids[assignment.assignment_id] = rule.rule_id

            conflicting_ids = assignments_by_target.get(assignment.target_field)
            if conflicting_ids is not None:
                assignment_ids = [*conflicting_ids, assignment.assignment_id]
                self._add(
                    result,
                    "ASSIGNMENT_TARGET_DUPLICATE_WITHIN_RULE",
                    f"Rule {rule.rule_id} assigns target field "
                    f"{assignment.target_field!r} more than once. Use one "
                    "assignment or separate ordered rules.",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                    details={
                        "rule_id": rule.rule_id,
                        "target_field": assignment.target_field,
                        "assignment_ids": assignment_ids,
                    },
                )
                conflicting_ids.append(assignment.assignment_id)
            else:
                assignments_by_target[assignment.target_field] = [
                    assignment.assignment_id
                ]
            self._validate_assignment(assignment, result)

    def _validate_condition_group(
        self,
        group: ConditionGroup,
        result: ValidationResult,
        seen_condition_ids: set[str],
        seen_condition_group_ids: set[str],
    ) -> None:
        """
        Validate one condition group and recursively validate child groups.
        """
        if not group.condition_group_id:
            self._add(
                result,
                "CONDITION_GROUP_ID_REQUIRED",
                "condition_group_id is required.",
                ObjectType.CONDITION_GROUP,
                "",
            )
        elif group.condition_group_id in seen_condition_group_ids:
            self._add(
                result,
                "CONDITION_GROUP_ID_DUPLICATE",
                f"Duplicate condition_group_id detected: {group.condition_group_id}",
                ObjectType.CONDITION_GROUP,
                group.condition_group_id,
            )
        seen_condition_group_ids.add(group.condition_group_id)
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
            self._validate_condition_group(
                nested_group,
                result,
                seen_condition_ids,
                seen_condition_group_ids,
            )

    def _validate_condition(self, condition: Condition, result: ValidationResult) -> None:
        """
        Validate one condition's tolerance, null handling, operands, and operator shape.
        """
        if not condition.tolerance_abs.is_finite():
            self._add(
                result,
                "TOLERANCE_FINITE_REQUIRED",
                "tolerance_abs must be finite.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        elif condition.tolerance_abs < Decimal(0):
            self._add(
                result,
                "TOLERANCE_NEGATIVE",
                "tolerance_abs must be non-negative.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.null_result_mode is NullResultMode.DEFAULT:
            if condition.null_default_value is None:
                self._add(
                    result,
                    "NULL_DEFAULT_REQUIRED",
                    "null_default_value is required when null_result_mode is default.",
                    ObjectType.CONDITION,
                    condition.condition_id,
                )
            elif not isinstance(condition.null_default_value, bool):
                self._add(
                    result,
                    "NULL_DEFAULT_BOOLEAN_REQUIRED",
                    "null_default_value must be a YAML boolean when "
                    "null_result_mode is default.",
                    ObjectType.CONDITION,
                    condition.condition_id,
                )
        self._validate_operand(condition.left, result, condition.condition_id, in_assignment=False)
        if condition.right is not None:
            self._validate_operand(condition.right, result, condition.condition_id, in_assignment=False)
        self._validate_operator_operands(condition, result)

    def _validate_operator_operands(self, condition: Condition, result: ValidationResult) -> None:
        """
        Validate operator arity and literal shape requirements.
        """
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
            and condition.tolerance_abs != Decimal(0)
        ):
            self._add(
                result,
                "BETWEEN_TOLERANCE_FORBIDDEN",
                "tolerance_abs must be 0 for between/not_between operators.",
                ObjectType.CONDITION,
                condition.condition_id,
            )

    def _validate_collection_literal(self, condition: Condition, result: ValidationResult) -> None:
        """
        Validate literal collection requirements for collection operators.
        """
        right = condition.right
        if not isinstance(right, LiteralOperand):
            return
        if (
            condition.operator in {ComparisonOperator.IN, ComparisonOperator.NOT_IN}
            and not isinstance(right.value, (list, tuple, set))
        ):
            self._add(
                result,
                "IN_OPERATOR_COLLECTION_REQUIRED",
                f"Operator {condition.operator.value} requires a collection literal on the right side.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if (
            condition.operator
            in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN}
            and (not isinstance(right.value, (list, tuple)) or len(right.value) != 2)
        ):
            self._add(
                result,
                "BETWEEN_OPERATOR_PAIR_REQUIRED",
                f"Operator {condition.operator.value} requires exactly two literal values.",
                ObjectType.CONDITION,
                condition.condition_id,
            )

    def _validate_assignment(self, assignment: Assignment, result: ValidationResult) -> None:
        """
        Validate one assignment target and value operand.
        """
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
        """
        Validate one operand according to its concrete operand type.
        """
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
            self._validate_finite_decimals(
                operand.value,
                result,
                object_type,
                object_id,
            )
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

    def _validate_custom_function(
        self,
        operand: CustomFunctionOperand,
        result: ValidationResult,
        object_id: str,
        *,
        in_assignment: bool,
    ) -> None:
        """
        Validate a custom-function operand against the registered contract.
        """
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
        except RegistryError:
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
        for arg_name, arg_value in operand.args.items():
            if isinstance(arg_value, Operand):
                self._validate_operand(
                    arg_value,
                    result,
                    f"{object_id}.{operand.function_name}.{arg_name}",
                    in_assignment=in_assignment,
                )
            else:
                self._validate_finite_decimals(
                    arg_value,
                    result,
                    object_type,
                    f"{object_id}.{operand.function_name}.{arg_name}",
                )

    def _validate_finite_decimals(
        self,
        value: Any,
        result: ValidationResult,
        object_type: ObjectType,
        object_id: str,
    ) -> None:
        """Reject non-finite numeric values in code-authored literal trees."""
        if isinstance(value, Decimal):
            if not value.is_finite():
                self._add(
                    result,
                    "LITERAL_DECIMAL_FINITE_REQUIRED",
                    "Decimal literal values must be finite.",
                    object_type,
                    object_id,
                )
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                self._add(
                    result,
                    "LITERAL_FLOAT_FINITE_REQUIRED",
                    "Floating-point literal values must be finite.",
                    object_type,
                    object_id,
                )
            return
        if isinstance(value, dict):
            values = value.values()
        elif isinstance(value, (list, tuple, set)):
            values = value
        else:
            return
        for item in values:
            self._validate_finite_decimals(
                item,
                result,
                object_type,
                object_id,
            )

    def _add(
        self,
        result: ValidationResult,
        check_name: str,
        message: str,
        object_type: ObjectType,
        object_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Add one error-severity validation issue to the result.
        """
        result.add_issue(
            ValidationSeverity.ERROR,
            check_name,
            message,
            object_type,
            object_id,
            details,
        )
