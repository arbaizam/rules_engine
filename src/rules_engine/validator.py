"""
Ruleset validator.

Validation is intentionally explicit and conservative. The validator enforces
the semantic contract on compiled YAML metadata before publication.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from rules_engine.enums import (
    COLLECTION_LITERAL_OPERATORS,
    UNARY_OPERATORS,
    ComparisonOperator,
    ObjectType,
)
from rules_engine.exceptions import RegistryError
from rules_engine.models import (
    AssignedOperand,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    LiteralOperand,
    Operand,
    Rule,
    Ruleset,
    ValidationResult,
    iter_nested_operands,
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
        elif not any(rule.active_flag for rule in ruleset.rules):
            self._add(
                result,
                "RULESET_ACTIVE_RULE_REQUIRED",
                "At least one active rule is required.",
                ObjectType.RULESET,
                ruleset.ruleset_id,
            )

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
        self._validate_assigned_references(ruleset, result)

    def _validate_assigned_references(
        self,
        ruleset: Ruleset,
        result: ValidationResult,
    ) -> None:
        """Require every assigned reference to have an earlier active producer."""
        active_rules = sorted(
            (rule for rule in ruleset.rules if rule.active_flag),
            key=lambda rule: rule.rule_order,
        )
        producers: dict[str, list[tuple[int, str, str]]] = {}
        for rule in active_rules:
            for assignment in rule.assignments:
                producers.setdefault(assignment.target_field, []).append(
                    (rule.rule_order, rule.rule_id, assignment.assignment_id)
                )

        for rule in active_rules:
            references: list[tuple[AssignedOperand, ObjectType, str]] = []
            self._collect_group_assigned_references(rule.root_group, references)
            for assignment in rule.assignments:
                self._collect_assigned_references(
                    assignment.value,
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                    references,
                )
            for operand, object_type, object_id in references:
                earlier = [
                    producer
                    for producer in producers.get(operand.target_field, [])
                    if producer[0] < rule.rule_order
                ]
                if earlier:
                    continue
                self._add(
                    result,
                    "ASSIGNED_VALUE_PRIOR_PRODUCER_REQUIRED",
                    f"Assigned value {operand.target_field!r} must be produced "
                    "by an active rule with a lower rule_order.",
                    object_type,
                    object_id,
                    details={
                        "rule_id": rule.rule_id,
                        "rule_order": rule.rule_order,
                        "target_field": operand.target_field,
                    },
                )

    def _collect_group_assigned_references(
        self,
        group: ConditionGroup,
        references: list[tuple[AssignedOperand, ObjectType, str]],
    ) -> None:
        """Collect assigned references from active conditions in one tree."""
        for condition in group.conditions:
            if not condition.active_flag:
                continue
            self._collect_assigned_references(
                condition.left,
                ObjectType.CONDITION,
                condition.condition_id,
                references,
            )
            if condition.right is not None:
                self._collect_assigned_references(
                    condition.right,
                    ObjectType.CONDITION,
                    condition.condition_id,
                    references,
                )
        for nested_group in group.groups:
            self._collect_group_assigned_references(nested_group, references)

    def _collect_assigned_references(
        self,
        operand: Operand,
        object_type: ObjectType,
        object_id: str,
        references: list[tuple[AssignedOperand, ObjectType, str]],
    ) -> None:
        """Collect assigned references recursively through function arguments."""
        if isinstance(operand, AssignedOperand):
            references.append((operand, object_type, object_id))
        elif isinstance(operand, CustomFunctionOperand):
            for argument in operand.args.values():
                for nested_operand in iter_nested_operands(argument):
                    self._collect_assigned_references(
                        nested_operand,
                        object_type,
                        object_id,
                        references,
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
                assignments_by_target[assignment.target_field] = [assignment.assignment_id]
            self._validate_operand(
                assignment.value,
                result,
                assignment.assignment_id,
                in_assignment=True,
            )

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
        if group.condition_group_id in seen_condition_group_ids:
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
        Validate one condition's tolerance, operands, and operator shape.
        """
        if condition.tolerance_abs < Decimal(0):
            self._add(
                result,
                "TOLERANCE_NEGATIVE",
                "tolerance_abs must be non-negative.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.error_on_null and condition.operator in UNARY_OPERATORS:
            self._add(
                result,
                "ERROR_ON_NULL_UNARY_FORBIDDEN",
                "error_on_null is not valid for is_null or is_not_null.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        self._validate_operand(condition.left, result, condition.condition_id, in_assignment=False)
        if condition.right is not None:
            self._validate_operand(
                condition.right, result, condition.condition_id, in_assignment=False
            )
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
        if condition.operator in COLLECTION_LITERAL_OPERATORS and isinstance(
            condition.right, LiteralOperand
        ):
            self._validate_collection_literal(condition, result)
        if condition.operator in {
            ComparisonOperator.BETWEEN,
            ComparisonOperator.NOT_BETWEEN,
        } and condition.tolerance_abs != Decimal(0):
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
        if condition.operator in {
            ComparisonOperator.IN,
            ComparisonOperator.NOT_IN,
        } and not isinstance(right.value, (list, tuple, set)):
            self._add(
                result,
                "IN_OPERATOR_COLLECTION_REQUIRED",
                f"Operator {condition.operator.value} requires a collection literal on the right side.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.operator in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN} and (
            not isinstance(right.value, (list, tuple)) or len(right.value) != 2
        ):
            self._add(
                result,
                "BETWEEN_OPERATOR_PAIR_REQUIRED",
                f"Operator {condition.operator.value} requires exactly two literal values.",
                ObjectType.CONDITION,
                condition.condition_id,
            )

    def _validate_operand(
        self,
        operand: Operand,
        result: ValidationResult,
        object_id: str,
        *,
        in_assignment: bool,
    ) -> None:
        """
        Validate registered functions nested in an operand tree.
        """
        if isinstance(operand, CustomFunctionOperand):
            self._validate_custom_function(operand, result, object_id, in_assignment=in_assignment)

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
        try:
            bound_args = spec.bind_args(operand.args)
        except RegistryError:
            required = {argument.name for argument in spec.arguments if argument.required}
            allowed = set(spec.argument_names)
            actual = set(operand.args)
            self._add(
                result,
                "CUSTOM_FUNCTION_ARGS_MISMATCH",
                "Custom function args do not match the registered contract.",
                object_type,
                object_id,
                details={
                    "function_name": operand.function_name,
                    "required": sorted(required),
                    "optional": sorted(allowed - required),
                    "actual": sorted(actual),
                },
            )
            bound_args = dict(operand.args)
        argument_specs = {argument.name: argument for argument in spec.arguments}
        for arg_name, arg_value in bound_args.items():
            argument_spec = argument_specs.get(arg_name)
            nested_operands = tuple(iter_nested_operands(arg_value))
            if argument_spec is not None:
                if argument_spec.literal_only and nested_operands:
                    self._add(
                        result,
                        "CUSTOM_FUNCTION_ARG_LITERAL_REQUIRED",
                        f"Argument {arg_name!r} for {operand.function_name!r} "
                        "must be a literal value.",
                        object_type,
                        object_id,
                    )
                if not self._argument_matches_type(
                    arg_value,
                    argument_spec.type_hint,
                ):
                    self._add(
                        result,
                        "CUSTOM_FUNCTION_ARG_TYPE_MISMATCH",
                        f"Argument {arg_name!r} for {operand.function_name!r} "
                        f"must have type {argument_spec.type_hint!r}.",
                        object_type,
                        object_id,
                        details={"argument_name": arg_name},
                    )
                if (
                    argument_spec.allowed_values is not None
                    and not nested_operands
                    and arg_value not in argument_spec.allowed_values
                ):
                    self._add(
                        result,
                        "CUSTOM_FUNCTION_ARG_VALUE_INVALID",
                        f"Argument {arg_name!r} for {operand.function_name!r} "
                        "has a value outside its allowed values.",
                        object_type,
                        object_id,
                        details={
                            "argument_name": arg_name,
                            "allowed_values": list(argument_spec.allowed_values),
                            "actual": arg_value,
                        },
                    )
            for nested_operand in nested_operands:
                self._validate_operand(
                    nested_operand,
                    result,
                    f"{object_id}.{operand.function_name}.{arg_name}",
                    in_assignment=in_assignment,
                )

    def _argument_matches_type(self, value: Any, type_hint: str) -> bool:
        """Validate statically knowable literal argument shapes."""
        if value is None:
            return True
        if isinstance(value, Operand):
            return True
        if type_hint == "any":
            return True
        if type_hint == "string":
            return isinstance(value, str)
        if type_hint == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if type_hint == "number":
            return isinstance(value, (int, float, Decimal)) and not isinstance(
                value,
                bool,
            )
        if type_hint == "boolean":
            return isinstance(value, bool)
        if type_hint == "date":
            return isinstance(value, date) and not isinstance(value, datetime)
        if type_hint == "timestamp":
            return isinstance(value, datetime)
        if type_hint == "mapping":
            return isinstance(value, Mapping)
        if type_hint in {
            "sequence",
            "string_sequence",
            "integer_sequence",
            "date_sequence",
        }:
            if not isinstance(value, (list, tuple, set)):
                return False
            item_type = {
                "sequence": "any",
                "string_sequence": "string",
                "integer_sequence": "integer",
                "date_sequence": "date",
            }[type_hint]
            return all(self._argument_matches_type(item, item_type) for item in value)
        return False

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
        Add one validation issue to the result.
        """
        result.add_issue(
            check_name,
            message,
            object_type,
            object_id,
            details,
        )
