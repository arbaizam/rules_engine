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

from rules_engine.canonical_values import validate_literal, validate_string_mapping_keys
from rules_engine.enums import (
    COLLECTION_LITERAL_OPERATORS,
    TOLERANCE_OPERATORS,
    UNARY_OPERATORS,
    ComparisonOperator,
    LogicalOperator,
    ObjectType,
)
from rules_engine.exceptions import RegistryError
from rules_engine.models import (
    AssignedOperand,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Rule,
    Ruleset,
    ValidationResult,
    iter_nested_operands,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.traversal import iter_argument_leaves, iter_conditions, iter_operand_tree


def _is_non_empty_text(value: Any) -> bool:
    """Return whether a value is canonical non-empty text."""
    return isinstance(value, str) and bool(value.strip())


def _diagnostic_id(value: Any) -> str:
    """Return a stable printable identifier for malformed model values."""
    return value if isinstance(value, str) else repr(value)


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
        self._validate_ruleset(ruleset, result)
        return result

    def _validate_ruleset(self, ruleset: Ruleset, result: ValidationResult) -> None:
        """
        Validate top-level ruleset identity, ownership, and child rules.
        """
        ruleset_id = _diagnostic_id(ruleset.ruleset_id)
        if not _is_non_empty_text(ruleset.ruleset_id):
            self._add(
                result,
                "RULESET_ID_INVALID",
                "ruleset_id must be a non-empty string.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if not _is_non_empty_text(ruleset.ruleset_name):
            self._add(
                result,
                "RULESET_NAME_INVALID",
                "ruleset_name must be a non-empty string.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if not _is_non_empty_text(ruleset.version):
            self._add(
                result,
                "RULESET_VERSION_INVALID",
                "version must be a non-empty string.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if ruleset.description is not None and not isinstance(ruleset.description, str):
            self._add(
                result,
                "RULESET_DESCRIPTION_INVALID",
                "description must be a string when provided.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if not _is_non_empty_text(ruleset.owner):
            self._add(
                result,
                "RULESET_OWNER_REQUIRED",
                "owner is required.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if not _is_non_empty_text(ruleset.owner_department):
            self._add(
                result,
                "RULESET_OWNER_DEPARTMENT_REQUIRED",
                "owner_department is required.",
                ObjectType.RULESET,
                ruleset_id,
            )
        if not ruleset.rules:
            self._add(
                result,
                "RULESET_RULE_REQUIRED",
                "At least one rule is required.",
                ObjectType.RULESET,
                ruleset_id,
            )
        elif not any(rule.active_flag is True for rule in ruleset.rules):
            self._add(
                result,
                "RULESET_ACTIVE_RULE_REQUIRED",
                "At least one active rule is required.",
                ObjectType.RULESET,
                ruleset_id,
            )

        seen_rule_orders: set[int] = set()
        seen_rule_ids: set[str] = set()
        seen_condition_ids: set[str] = set()
        seen_condition_group_ids: set[str] = set()
        seen_assignment_ids: dict[str, str] = {}
        for rule in ruleset.rules:
            valid_rule_id = _is_non_empty_text(rule.rule_id)
            if not valid_rule_id:
                self._add(
                    result,
                    "RULE_ID_INVALID",
                    "rule_id must be a non-empty string.",
                    ObjectType.RULE,
                    str(rule.rule_id),
                )
            elif rule.rule_id in seen_rule_ids:
                self._add(
                    result,
                    "RULE_ID_DUPLICATE",
                    f"Duplicate rule_id detected: {rule.rule_id}",
                    ObjectType.RULE,
                    rule.rule_id,
                )
            if valid_rule_id:
                seen_rule_ids.add(rule.rule_id)
            valid_rule_order = isinstance(rule.rule_order, int) and not isinstance(
                rule.rule_order,
                bool,
            )
            if not valid_rule_order:
                self._add(
                    result,
                    "RULE_ORDER_INVALID",
                    "rule_order must be an integer.",
                    ObjectType.RULE,
                    _diagnostic_id(rule.rule_id),
                )
            elif rule.rule_order in seen_rule_orders:
                self._add(
                    result,
                    "RULE_ORDER_DUPLICATE",
                    f"Duplicate rule_order detected: {rule.rule_order}",
                    ObjectType.RULE,
                    _diagnostic_id(rule.rule_id),
                )
            if valid_rule_order:
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
            (
                rule
                for rule in ruleset.rules
                if rule.active_flag is True
                and isinstance(rule.rule_order, int)
                and not isinstance(rule.rule_order, bool)
            ),
            key=lambda rule: rule.rule_order,
        )
        producers: dict[str, list[tuple[int, str, str]]] = {}
        for rule in active_rules:
            for assignment in rule.assignments:
                if _is_non_empty_text(assignment.target_field):
                    producers.setdefault(assignment.target_field, []).append(
                        (
                            rule.rule_order,
                            _diagnostic_id(rule.rule_id),
                            _diagnostic_id(assignment.assignment_id),
                        )
                    )

        for rule in active_rules:
            references: list[tuple[AssignedOperand, ObjectType, str]] = []
            if isinstance(rule.root_group, ConditionGroup):
                self._collect_group_assigned_references(rule.root_group, references)
            for assignment in rule.assignments:
                self._collect_assigned_references(
                    assignment.value,
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                    references,
                )
            for operand, object_type, object_id in references:
                if not _is_non_empty_text(operand.target_field):
                    continue
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
                    _diagnostic_id(object_id),
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
        for condition in iter_conditions(group, active_only=True):
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

    def _collect_assigned_references(
        self,
        operand: Operand,
        object_type: ObjectType,
        object_id: str,
        references: list[tuple[AssignedOperand, ObjectType, str]],
    ) -> None:
        """Collect assigned references recursively through function arguments."""
        for nested in iter_operand_tree(operand, include_defaults=False):
            if isinstance(nested, AssignedOperand):
                references.append((nested, object_type, object_id))

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
        rule_id = _diagnostic_id(rule.rule_id)
        if not _is_non_empty_text(rule.rule_name):
            self._add(
                result,
                "RULE_NAME_INVALID",
                "rule_name must be a non-empty string.",
                ObjectType.RULE,
                rule_id,
            )
        if not isinstance(rule.active_flag, bool):
            self._add(
                result,
                "RULE_ACTIVE_FLAG_INVALID",
                "active_flag must be a boolean.",
                ObjectType.RULE,
                rule_id,
            )
        if not isinstance(rule.stop_on_match, bool):
            self._add(
                result,
                "RULE_STOP_ON_MATCH_INVALID",
                "stop_on_match must be a boolean.",
                ObjectType.RULE,
                rule_id,
            )
        if rule.description is not None and not isinstance(rule.description, str):
            self._add(
                result,
                "RULE_DESCRIPTION_INVALID",
                "description must be a string when provided.",
                ObjectType.RULE,
                rule_id,
            )
        if isinstance(rule.root_group, ConditionGroup):
            self._validate_condition_group(
                rule.root_group,
                result,
                seen_condition_ids,
                seen_condition_group_ids,
            )
        else:
            self._add(
                result,
                "RULE_CONDITION_GROUP_INVALID",
                "root_group must be a ConditionGroup.",
                ObjectType.RULE,
                rule_id,
            )
        if not rule.assignments:
            self._add(
                result,
                "RULE_ASSIGNMENT_REQUIRED",
                "Each rule must define at least one assignment.",
                ObjectType.RULE,
                rule_id,
            )
        assignments_by_target: dict[str, list[str]] = {}
        for assignment in rule.assignments:
            valid_assignment_id = _is_non_empty_text(assignment.assignment_id)
            if not valid_assignment_id:
                self._add(
                    result,
                    "ASSIGNMENT_ID_INVALID",
                    "assignment_id must be a non-empty string.",
                    ObjectType.ASSIGNMENT,
                    str(assignment.assignment_id),
                )
            elif assignment.assignment_id in seen_assignment_ids:
                first_rule_id = seen_assignment_ids[assignment.assignment_id]
                duplicate_location = (
                    f"more than once in rule {rule_id}"
                    if first_rule_id == rule_id
                    else f"by rules {first_rule_id} and {rule_id}"
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
                        "rule_ids": [rule_id]
                        if first_rule_id == rule_id
                        else [first_rule_id, rule_id],
                    },
                )
            elif valid_assignment_id:
                seen_assignment_ids[assignment.assignment_id] = rule_id

            valid_target = _is_non_empty_text(assignment.target_field)
            if not valid_target:
                self._add(
                    result,
                    "ASSIGNMENT_TARGET_FIELD_INVALID",
                    "target_field must be a non-empty string.",
                    ObjectType.ASSIGNMENT,
                    _diagnostic_id(assignment.assignment_id),
                )
            conflicting_ids = (
                assignments_by_target.get(assignment.target_field) if valid_target else None
            )
            if valid_target and conflicting_ids is not None:
                assignment_ids = [*conflicting_ids, assignment.assignment_id]
                self._add(
                    result,
                    "ASSIGNMENT_TARGET_DUPLICATE_WITHIN_RULE",
                    f"Rule {rule_id} assigns target field "
                    f"{assignment.target_field!r} more than once. Use one "
                    "assignment or separate ordered rules.",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                    details={
                        "rule_id": rule_id,
                        "target_field": assignment.target_field,
                        "assignment_ids": assignment_ids,
                    },
                )
                conflicting_ids.append(assignment.assignment_id)
            elif valid_target:
                assignments_by_target[assignment.target_field] = [assignment.assignment_id]
            self._validate_operand(
                assignment.value,
                result,
                _diagnostic_id(assignment.assignment_id),
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
        group_id = _diagnostic_id(group.condition_group_id)
        valid_group_id = _is_non_empty_text(group.condition_group_id)
        if not valid_group_id:
            self._add(
                result,
                "CONDITION_GROUP_ID_INVALID",
                "condition_group_id must be a non-empty string.",
                ObjectType.CONDITION_GROUP,
                group_id,
            )
        elif group.condition_group_id in seen_condition_group_ids:
            self._add(
                result,
                "CONDITION_GROUP_ID_DUPLICATE",
                f"Duplicate condition_group_id detected: {group.condition_group_id}",
                ObjectType.CONDITION_GROUP,
                group_id,
            )
        if valid_group_id:
            seen_condition_group_ids.add(group.condition_group_id)
        if not isinstance(group.logical_operator, LogicalOperator):
            self._add(
                result,
                "LOGICAL_OPERATOR_INVALID",
                "logical_operator must be a LogicalOperator.",
                ObjectType.CONDITION_GROUP,
                group_id,
            )
        if not group.conditions and not group.groups:
            self._add(
                result,
                "CONDITION_GROUP_EMPTY",
                "Condition group must contain at least one condition or nested group.",
                ObjectType.CONDITION_GROUP,
                group_id,
            )
        for condition in group.conditions:
            condition_id = _diagnostic_id(condition.condition_id)
            valid_condition_id = _is_non_empty_text(condition.condition_id)
            if not valid_condition_id:
                self._add(
                    result,
                    "CONDITION_ID_INVALID",
                    "condition_id must be a non-empty string.",
                    ObjectType.CONDITION,
                    condition_id,
                )
            elif condition.condition_id in seen_condition_ids:
                self._add(
                    result,
                    "CONDITION_ID_DUPLICATE",
                    f"Duplicate condition_id detected: {condition.condition_id}",
                    ObjectType.CONDITION,
                    condition_id,
                )
            if valid_condition_id:
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
        condition_id = _diagnostic_id(condition.condition_id)
        valid_operator = isinstance(condition.operator, ComparisonOperator)
        if not valid_operator:
            self._add(
                result,
                "CONDITION_OPERATOR_INVALID",
                "operator must be a ComparisonOperator.",
                ObjectType.CONDITION,
                condition_id,
            )
        valid_tolerance = isinstance(condition.tolerance_abs, Decimal) and (
            condition.tolerance_abs.is_finite()
        )
        if not valid_tolerance:
            self._add(
                result,
                "TOLERANCE_INVALID",
                "tolerance_abs must be a finite Decimal.",
                ObjectType.CONDITION,
                condition_id,
            )
        elif condition.tolerance_abs < Decimal(0):
            self._add(
                result,
                "TOLERANCE_NEGATIVE",
                "tolerance_abs must be non-negative.",
                ObjectType.CONDITION,
                condition_id,
            )
        elif (
            condition.tolerance_abs != Decimal(0)
            and valid_operator
            and condition.operator not in TOLERANCE_OPERATORS
        ):
            between_operator = condition.operator in {
                ComparisonOperator.BETWEEN,
                ComparisonOperator.NOT_BETWEEN,
            }
            self._add(
                result,
                (
                    "BETWEEN_TOLERANCE_FORBIDDEN"
                    if between_operator
                    else "TOLERANCE_OPERATOR_FORBIDDEN"
                ),
                f"tolerance_abs must be 0 for {condition.operator.value} operators.",
                ObjectType.CONDITION,
                condition_id,
            )
        if not isinstance(condition.error_on_null, bool):
            self._add(
                result,
                "ERROR_ON_NULL_INVALID",
                "error_on_null must be a boolean.",
                ObjectType.CONDITION,
                condition_id,
            )
        if not isinstance(condition.active_flag, bool):
            self._add(
                result,
                "CONDITION_ACTIVE_FLAG_INVALID",
                "active_flag must be a boolean.",
                ObjectType.CONDITION,
                condition_id,
            )
        if (
            condition.error_on_null is True
            and valid_operator
            and condition.operator in UNARY_OPERATORS
        ):
            self._add(
                result,
                "ERROR_ON_NULL_UNARY_FORBIDDEN",
                "error_on_null is not valid for is_null or is_not_null.",
                ObjectType.CONDITION,
                condition_id,
            )
        self._validate_operand(condition.left, result, condition.condition_id, in_assignment=False)
        if condition.right is not None:
            self._validate_operand(
                condition.right, result, condition.condition_id, in_assignment=False
            )
        if valid_operator:
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

    def _validate_collection_literal(self, condition: Condition, result: ValidationResult) -> None:
        """
        Validate literal collection requirements for collection operators.
        """
        _, right_value = self._static_literal_value(condition.right)
        if condition.operator in {
            ComparisonOperator.IN,
            ComparisonOperator.NOT_IN,
        } and not isinstance(right_value, (list, tuple, set)):
            self._add(
                result,
                "IN_OPERATOR_COLLECTION_REQUIRED",
                f"Operator {condition.operator.value} requires a collection literal on the right side.",
                ObjectType.CONDITION,
                condition.condition_id,
            )
        if condition.operator in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN} and (
            not isinstance(right_value, (list, tuple)) or len(right_value) != 2
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
        object_type = ObjectType.ASSIGNMENT if in_assignment else ObjectType.CONDITION
        if isinstance(operand, FieldOperand):
            if not _is_non_empty_text(operand.field_name):
                self._add(
                    result,
                    "FIELD_NAME_INVALID",
                    "field_name must be a non-empty string.",
                    object_type,
                    object_id,
                )
        elif isinstance(operand, AssignedOperand):
            if not _is_non_empty_text(operand.target_field):
                self._add(
                    result,
                    "ASSIGNED_TARGET_FIELD_INVALID",
                    "Assigned target_field must be a non-empty string.",
                    object_type,
                    object_id,
                )
        elif isinstance(operand, LiteralOperand):
            self._validate_literal_operand(operand, result, object_type, object_id)
            self._validate_mapping_keys(operand.value, result, object_type, object_id)
        elif isinstance(operand, CustomFunctionOperand):
            self._validate_mapping_keys(operand.args, result, object_type, object_id)
            if not _is_non_empty_text(operand.function_name):
                self._add(
                    result,
                    "CUSTOM_FUNCTION_NAME_INVALID",
                    "function_name must be a non-empty string.",
                    object_type,
                    object_id,
                )
            elif not isinstance(operand.args, Mapping) or not all(
                _is_non_empty_text(name) for name in operand.args
            ):
                self._add(
                    result,
                    "CUSTOM_FUNCTION_ARG_NAME_INVALID",
                    "Custom function argument names must be non-empty strings.",
                    object_type,
                    object_id,
                )
            else:
                self._validate_custom_function(
                    operand,
                    result,
                    object_id,
                    in_assignment=in_assignment,
                )
        else:
            self._add(
                result,
                "OPERAND_INVALID",
                "Operand must use a canonical operand model.",
                object_type,
                object_id,
            )
            return

        default_if_null = operand.default_if_null
        if default_if_null is not None:
            if not isinstance(default_if_null, LiteralOperand):
                self._add(
                    result,
                    "DEFAULT_IF_NULL_INVALID",
                    "default_if_null must be a LiteralOperand.",
                    object_type,
                    object_id,
                )
            else:
                if default_if_null.default_if_null is not None:
                    self._add(
                        result,
                        "DEFAULT_IF_NULL_NESTED_FORBIDDEN",
                        "default_if_null cannot define another default_if_null.",
                        object_type,
                        object_id,
                    )
                self._validate_literal_operand(
                    default_if_null,
                    result,
                    object_type,
                    object_id,
                )
                if default_if_null.value is None:
                    self._add(
                        result,
                        "DEFAULT_IF_NULL_NULL_FORBIDDEN",
                        "default_if_null cannot itself be null.",
                        object_type,
                        object_id,
                    )
                self._validate_mapping_keys(
                    default_if_null.value,
                    result,
                    object_type,
                    object_id,
                )

    def _validate_literal_operand(
        self,
        operand: LiteralOperand,
        result: ValidationResult,
        object_type: ObjectType,
        object_id: str,
    ) -> None:
        """Require type-hint metadata to use the canonical scalar shape."""
        value_type = operand.value_type
        if value_type is not None and not _is_non_empty_text(value_type):
            self._add(
                result,
                "LITERAL_VALUE_TYPE_INVALID",
                "Literal value_type must be a non-empty string when provided.",
                object_type,
                object_id,
            )
            return
        try:
            validate_literal(operand.value, value_type)
        except (ValueError, TypeError, RecursionError) as exc:
            self._add(
                result,
                "LITERAL_VALUE_INVALID",
                str(exc),
                object_type,
                object_id,
            )

    def _validate_mapping_keys(
        self,
        value: Any,
        result: ValidationResult,
        object_type: ObjectType,
        object_id: str,
    ) -> None:
        """Reject mappings whose keys collide in persisted string form."""
        try:
            validate_string_mapping_keys(value)
        except (ValueError, RecursionError) as exc:
            self._add(
                result,
                "MAPPING_KEY_INVALID",
                str(exc),
                object_type,
                object_id,
            )
        collision = self._find_mapping_key_collision(value, set())
        if collision is None:
            return
        first_key, second_key, normalized_key = collision
        self._add(
            result,
            "MAPPING_KEY_NORMALIZATION_COLLISION",
            f"Mapping keys {first_key!r} and {second_key!r} both normalize to {normalized_key!r}.",
            object_type,
            object_id,
            details={
                "first_key": repr(first_key),
                "second_key": repr(second_key),
                "normalized_key": normalized_key,
            },
        )

    def _find_mapping_key_collision(
        self,
        value: Any,
        visited: set[int],
    ) -> tuple[Any, Any, str] | None:
        """Return the first recursive string-key collision, if present."""
        if isinstance(value, LiteralOperand):
            return self._find_mapping_key_collision(value.value, visited)
        if isinstance(value, CustomFunctionOperand):
            return self._find_mapping_key_collision(value.args, visited)
        if isinstance(value, Mapping):
            value_id = id(value)
            if value_id in visited:
                return None
            visited.add(value_id)
            original_keys: dict[str, Any] = {}
            for key, item in value.items():
                normalized_key = str(key)
                if normalized_key in original_keys:
                    return original_keys[normalized_key], key, normalized_key
                original_keys[normalized_key] = key
                nested = self._find_mapping_key_collision(item, visited)
                if nested is not None:
                    return nested
            return None
        if isinstance(value, (list, tuple, set)):
            value_id = id(value)
            if value_id in visited:
                return None
            visited.add(value_id)
            for item in value:
                nested = self._find_mapping_key_collision(item, visited)
                if nested is not None:
                    return nested
        return None

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
            try:
                for leaf in iter_argument_leaves(arg_value):
                    if not isinstance(leaf, Operand):
                        validate_literal(leaf)
            except (ValueError, TypeError, RecursionError) as exc:
                self._add(
                    result,
                    "CUSTOM_FUNCTION_ARG_VALUE_INVALID",
                    f"Argument {arg_name!r} for {operand.function_name!r}: {exc}",
                    object_type,
                    object_id,
                )
                continue
            argument_spec = argument_specs.get(arg_name)
            nested_operands = tuple(iter_nested_operands(arg_value))
            static_literal, effective_value = self._static_literal_value(arg_value)
            if argument_spec is not None:
                if argument_spec.literal_only and not static_literal:
                    self._add(
                        result,
                        "CUSTOM_FUNCTION_ARG_LITERAL_REQUIRED",
                        f"Argument {arg_name!r} for {operand.function_name!r} "
                        "must be a literal value.",
                        object_type,
                        object_id,
                    )
                if not self._argument_matches_type(
                    effective_value if static_literal else arg_value,
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
                    and static_literal
                    and effective_value not in argument_spec.allowed_values
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
                            "actual": effective_value,
                        },
                    )
            for nested_operand in nested_operands:
                self._validate_operand(
                    nested_operand,
                    result,
                    f"{object_id}.{operand.function_name}.{arg_name}",
                    in_assignment=in_assignment,
                )

    def _static_literal_value(self, value: Any) -> tuple[bool, Any]:
        """Unwrap explicit literal operands while rejecting dynamic references."""
        if isinstance(value, LiteralOperand):
            is_static, effective_value = self._static_literal_value(value.value)
            if is_static and effective_value is None and value.default_if_null is not None:
                return self._static_literal_value(value.default_if_null)
            return is_static, effective_value
        if isinstance(value, (FieldOperand, AssignedOperand, CustomFunctionOperand)):
            return False, value
        if isinstance(value, Mapping):
            normalized: dict[Any, Any] = {}
            for key, item in value.items():
                is_static, normalized_item = self._static_literal_value(item)
                if not is_static:
                    return False, value
                normalized[key] = normalized_item
            return True, normalized
        if isinstance(value, (list, tuple, set)):
            normalized_items: list[Any] = []
            for item in value:
                is_static, normalized_item = self._static_literal_value(item)
                if not is_static:
                    return False, value
                normalized_items.append(normalized_item)
            if isinstance(value, tuple):
                return True, tuple(normalized_items)
            if isinstance(value, set):
                return True, set(normalized_items)
            return True, normalized_items
        return True, value

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
        if type_hint == "ordered_sequence":
            return isinstance(value, (list, tuple))
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
