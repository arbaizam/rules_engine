"""Spark schema compatibility validation for ruleset metadata."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping as MappingABC
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import types as T

from rules_engine.enums import ComparisonOperator, ObjectType
from rules_engine.models import (
    AssignedOperand,
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
from rules_engine.spark_types import (
    INTEGRAL_DECIMAL_DIGITS,
    INTEGRAL_LIMITS,
    INTEGRAL_RANK,
    INTEGRAL_TYPES,
    TEMPORAL_TYPES,
    TIMESTAMP_NTZ_TYPE,
    decimal_literal_type,
    decimal_value_fits,
)
from rules_engine.validator import RulesetValidator

SPARK_TYPE_HINTS: dict[str, T.DataType] = {
    "string": T.StringType(),
    "str": T.StringType(),
    "integer": T.LongType(),
    "int": T.LongType(),
    "long": T.LongType(),
    "number": T.DoubleType(),
    "float": T.DoubleType(),
    "double": T.DoubleType(),
    "decimal": T.DecimalType(38, 18),
    "boolean": T.BooleanType(),
    "bool": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
}
if TIMESTAMP_NTZ_TYPE is not None:
    SPARK_TYPE_HINTS["timestamp_ntz"] = TIMESTAMP_NTZ_TYPE()
_TEMPORAL_COMPARISON_OPERATORS = {
    ComparisonOperator.EQ,
    ComparisonOperator.NE,
    ComparisonOperator.GT,
    ComparisonOperator.GE,
    ComparisonOperator.LT,
    ComparisonOperator.LE,
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
    ComparisonOperator.BETWEEN,
    ComparisonOperator.NOT_BETWEEN,
}


class SparkRulesetCompatibilityValidator(RulesetValidator):
    """Validate ruleset metadata and its compatibility with a Spark schema."""

    def validate(
        self,
        ruleset: Ruleset,
        schema: T.StructType | Any | None = None,
    ) -> ValidationResult:
        """Validate base metadata and, when supplied, a Spark input schema."""
        result = super().validate(ruleset)
        if schema is not None:
            self._populate_schema_result(ruleset, self._coerce_schema(schema), result)
        return result

    def assignment_schema(
        self,
        ruleset: Ruleset,
        schema: T.StructType | Any,
    ) -> T.StructType:
        """Return the validated typed assignment payload schema."""
        spark_schema = self._coerce_schema(schema)
        result = ValidationResult()
        field_types = self._resolve_assignment_types(ruleset, spark_schema, result)
        if result.has_errors():
            raise ValueError(result.to_text())
        return T.StructType(
            [
                T.StructField(field_name, data_type, True)
                for field_name, data_type in field_types.items()
            ]
        )

    def _coerce_schema(self, schema: T.StructType | Any) -> T.StructType:
        """Accept either a StructType or an object exposing a StructType schema."""
        if isinstance(schema, T.StructType):
            return schema
        candidate = getattr(schema, "schema", None)
        if isinstance(candidate, T.StructType):
            return candidate
        raise TypeError("schema must be a pyspark.sql.types.StructType or DataFrame.")

    def _populate_schema_result(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
    ) -> None:
        """Add all Spark field-reference and assignment-type issues."""
        self._validate_field_references(ruleset, schema, result)
        assigned_types = self._resolve_assignment_types(ruleset, schema, result)
        self._validate_operand_defaults(
            ruleset,
            schema,
            result,
            assigned_types,
        )
        self._validate_membership_condition_types(
            ruleset,
            schema,
            result,
            assigned_types,
        )
        self._validate_temporal_condition_types(
            ruleset,
            schema,
            result,
            assigned_types,
        )

    def _active_rules(self, ruleset: Ruleset) -> list[Rule]:
        """Return active rules in evaluation order."""
        return sorted(
            (rule for rule in ruleset.rules if rule.active_flag),
            key=lambda rule: rule.rule_order,
        )

    def _validate_field_references(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
    ) -> None:
        """Require every active condition and assignment source field."""
        source_fields = {field.name for field in schema.fields}

        def validate_operand(
            operand: Operand | None,
            *,
            object_type: ObjectType,
            object_id: str,
            check_name: str,
        ) -> None:
            if isinstance(operand, FieldOperand):
                if operand.field_name not in source_fields:
                    self._add(
                        result,
                        check_name,
                        f"Spark input schema does not contain field "
                        f"{operand.field_name!r}.",
                        object_type,
                        object_id,
                        details={"field_name": operand.field_name},
                    )
            elif isinstance(operand, CustomFunctionOperand):
                for argument in operand.args.values():
                    if isinstance(
                        argument,
                        (
                            AssignedOperand,
                            FieldOperand,
                            LiteralOperand,
                            CustomFunctionOperand,
                        ),
                    ):
                        validate_operand(
                            argument,
                            object_type=object_type,
                            object_id=object_id,
                            check_name=check_name,
                        )

        def validate_group(group: ConditionGroup) -> None:
            for condition in group.conditions:
                if condition.active_flag:
                    validate_operand(
                        condition.left,
                        object_type=ObjectType.CONDITION,
                        object_id=condition.condition_id,
                        check_name="SPARK_CONDITION_FIELD_MISSING",
                    )
                    validate_operand(
                        condition.right,
                        object_type=ObjectType.CONDITION,
                        object_id=condition.condition_id,
                        check_name="SPARK_CONDITION_FIELD_MISSING",
                    )
            for child in group.groups:
                validate_group(child)

        for rule in self._active_rules(ruleset):
            validate_group(rule.root_group)
            for assignment in rule.assignments:
                validate_operand(
                    assignment.value,
                    object_type=ObjectType.ASSIGNMENT,
                    object_id=assignment.assignment_id,
                    check_name="SPARK_ASSIGNMENT_SOURCE_FIELD_MISSING",
                )

    def _resolve_assignment_types(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
    ) -> dict[str, T.DataType]:
        """Resolve target types without coercing conflicts to strings."""
        source_fields = {field.name: field.dataType for field in schema.fields}
        assigned_types = self._inferred_assigned_types(ruleset, source_fields)
        assignments_by_target: dict[str, list[tuple[Rule, Assignment]]] = defaultdict(list)
        for rule in self._active_rules(ruleset):
            for assignment in rule.assignments:
                assignments_by_target[assignment.target_field].append((rule, assignment))

        resolved: dict[str, T.DataType] = {}
        for target_field, rule_assignments in assignments_by_target.items():
            target_type = source_fields.get(target_field)
            if isinstance(target_type, T.NullType):
                target_type = None
            inferred_items: list[tuple[Rule, Assignment, T.DataType]] = []
            unresolved_items: list[tuple[Rule, Assignment]] = []
            for rule, assignment in rule_assignments:
                inferred = self._operand_type(
                    assignment.value,
                    source_fields,
                    assigned_types,
                )
                if inferred is None:
                    unresolved_items.append((rule, assignment))
                else:
                    inferred_items.append((rule, assignment, inferred))

            if target_type is not None:
                resolved[target_field] = target_type
                for rule, assignment, inferred in inferred_items:
                    if not self._assignment_is_compatible(
                        assignment,
                        inferred,
                        target_type,
                    ):
                        self._add_target_type_issue(
                            result,
                            rule,
                            assignment,
                            inferred,
                            target_type,
                        )
                self._add_unusable_hint_issues(
                    result,
                    unresolved_items,
                    target_type_known=True,
                    existing_target=True,
                )
                continue

            common_type: T.DataType | None = None
            first_typed: tuple[Rule, Assignment, T.DataType] | None = None
            for rule, assignment, inferred in inferred_items:
                if common_type is None:
                    common_type = inferred
                    first_typed = (rule, assignment, inferred)
                    continue
                merged = self._common_type(common_type, inferred)
                if merged is None:
                    if first_typed is None:
                        raise RuntimeError(
                            "Assignment type resolution lost its first typed value."
                        )
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_TYPE_CONFLICT",
                        f"Assignments to new target field {target_field!r} "
                        "resolve to incompatible Spark types.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                        details={
                            "target_field": target_field,
                            "assignment_ids": [
                                first_typed[1].assignment_id,
                                assignment.assignment_id,
                            ],
                            "spark_types": [
                                first_typed[2].simpleString(),
                                inferred.simpleString(),
                            ],
                        },
                    )
                else:
                    common_type = merged

            self._add_unusable_hint_issues(
                result,
                unresolved_items,
                target_type_known=common_type is not None,
                existing_target=False,
            )
            if common_type is not None:
                resolved[target_field] = common_type
        return resolved

    def _inferred_assigned_types(
        self,
        ruleset: Ruleset,
        source_fields: dict[str, T.DataType],
    ) -> dict[str, T.DataType]:
        """Infer target types in rule order for downstream assigned operands."""
        target_fields = {
            assignment.target_field
            for rule in self._active_rules(ruleset)
            for assignment in rule.assignments
        }
        available = {
            target: data_type
            for target, data_type in source_fields.items()
            if target in target_fields and not isinstance(data_type, T.NullType)
        }
        for rule in self._active_rules(ruleset):
            pending: dict[str, T.DataType] = {}
            for assignment in rule.assignments:
                inferred = self._operand_type(
                    assignment.value,
                    source_fields,
                    available,
                )
                if inferred is None:
                    continue
                current = pending.get(
                    assignment.target_field,
                    available.get(assignment.target_field),
                )
                if current is None:
                    pending[assignment.target_field] = inferred
                    continue
                merged = self._common_type(current, inferred)
                if merged is not None:
                    pending[assignment.target_field] = merged
            available.update(pending)
        return available

    def _validate_operand_defaults(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
        assigned_types: dict[str, T.DataType] | None = None,
    ) -> None:
        """Require each literal null fallback to fit its operand's Spark type."""
        source_fields = {field.name: field.dataType for field in schema.fields}

        def validate_operand(
            operand: Operand | None,
            *,
            object_type: ObjectType,
            object_id: str,
        ) -> None:
            if operand is None:
                return
            default = operand.default_if_null
            if default is not None:
                default_type = self._base_operand_type(default, source_fields)
                if default.value_type and default_type is None:
                    self._add(
                        result,
                        "SPARK_DEFAULT_IF_NULL_VALUE_TYPE_UNSUPPORTED",
                        f"Unsupported default_if_null value_type "
                        f"{default.value_type!r}.",
                        object_type,
                        object_id,
                    )
                operand_type = self._base_operand_type(
                    operand,
                    source_fields,
                    assigned_types,
                )
                if (
                    operand_type is not None
                    and default_type is not None
                    and not self._default_is_compatible(default, operand_type)
                ):
                    self._add(
                        result,
                        "SPARK_DEFAULT_IF_NULL_TYPE_INCOMPATIBLE",
                        f"default_if_null type {default_type.simpleString()} is "
                        f"incompatible with operand type "
                        f"{operand_type.simpleString()}.",
                        object_type,
                        object_id,
                        details={
                            "default_type": default_type.simpleString(),
                            "operand_type": operand_type.simpleString(),
                        },
                    )
            if isinstance(operand, CustomFunctionOperand):
                for argument in operand.args.values():
                    if isinstance(
                        argument,
                        (
                            AssignedOperand,
                            FieldOperand,
                            LiteralOperand,
                            CustomFunctionOperand,
                        ),
                    ):
                        validate_operand(
                            argument,
                            object_type=object_type,
                            object_id=object_id,
                        )

        def validate_group(group: ConditionGroup) -> None:
            for condition in group.conditions:
                if condition.active_flag:
                    validate_operand(
                        condition.left,
                        object_type=ObjectType.CONDITION,
                        object_id=condition.condition_id,
                    )
                    validate_operand(
                        condition.right,
                        object_type=ObjectType.CONDITION,
                        object_id=condition.condition_id,
                    )
            for child in group.groups:
                validate_group(child)

        for rule in self._active_rules(ruleset):
            validate_group(rule.root_group)
            for assignment in rule.assignments:
                validate_operand(
                    assignment.value,
                    object_type=ObjectType.ASSIGNMENT,
                    object_id=assignment.assignment_id,
                )

    def _validate_membership_condition_types(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
        assigned_types: dict[str, T.DataType] | None = None,
    ) -> None:
        """Require statically knowable IN operands to be collection-valued."""
        source_fields = {field.name: field.dataType for field in schema.fields}

        def validate_condition(condition: Condition) -> None:
            if not condition.active_flag or condition.operator not in {
                ComparisonOperator.IN,
                ComparisonOperator.NOT_IN,
            }:
                return
            right_type = self._operand_type(
                condition.right,
                source_fields,
                assigned_types,
            )
            if right_type is None or isinstance(right_type, T.ArrayType):
                return
            self._add(
                result,
                "SPARK_CONDITION_MEMBERSHIP_COLLECTION_REQUIRED",
                f"Condition {condition.condition_id!r} uses "
                f"{condition.operator.value} with non-collection right type "
                f"{right_type.simpleString()}. Use contains/not_contains for "
                "substring checks.",
                ObjectType.CONDITION,
                condition.condition_id,
                details={
                    "operator": condition.operator.value,
                    "right_type": right_type.simpleString(),
                },
            )

        def validate_group(group: ConditionGroup) -> None:
            for condition in group.conditions:
                validate_condition(condition)
            for child in group.groups:
                validate_group(child)

        for rule in self._active_rules(ruleset):
            validate_group(rule.root_group)

    def _validate_temporal_condition_types(
        self,
        ruleset: Ruleset,
        schema: T.StructType,
        result: ValidationResult,
        assigned_types: dict[str, T.DataType] | None = None,
    ) -> None:
        """Reject knowable temporal comparisons that Spark workers cannot align."""
        source_fields = {field.name: field.dataType for field in schema.fields}

        def validate_condition(condition: Condition) -> None:
            if (
                not condition.active_flag
                or condition.operator not in _TEMPORAL_COMPARISON_OPERATORS
            ):
                return
            left_type = self._operand_type(
                condition.left,
                source_fields,
                assigned_types,
            )
            right_type = self._operand_type(
                condition.right,
                source_fields,
                assigned_types,
            )
            if isinstance(right_type, T.ArrayType):
                right_type = right_type.elementType
            if left_type is None:
                return
            if right_type is None:
                collection_types = self._literal_collection_types(condition.right)
                if any(
                    data_type is not None and self._is_temporal_type(data_type)
                    for data_type in collection_types
                ):
                    type_names = sorted(
                        {
                            data_type.simpleString()
                            if data_type is not None
                            else "unknown"
                            for data_type in collection_types
                        }
                    )
                    self._add_temporal_type_issue(
                        condition,
                        left_type.simpleString(),
                        f"array<{','.join(type_names)}>",
                        result,
                    )
                return
            if not (
                self._is_temporal_type(left_type)
                or self._is_temporal_type(right_type)
            ):
                return
            if left_type == right_type:
                return
            self._add_temporal_type_issue(
                condition,
                left_type.simpleString(),
                right_type.simpleString(),
                result,
            )

        def validate_group(group: ConditionGroup) -> None:
            for condition in group.conditions:
                validate_condition(condition)
            for child in group.groups:
                validate_group(child)

        for rule in self._active_rules(ruleset):
            validate_group(rule.root_group)

    def _literal_collection_types(
        self,
        operand: Operand | None,
    ) -> list[T.DataType | None]:
        """Return item types when a literal collection has no common type."""
        if not isinstance(operand, LiteralOperand) or not isinstance(
            operand.value,
            (list, tuple),
        ):
            return []
        return [self._literal_type(item) for item in operand.value]

    def _add_temporal_type_issue(
        self,
        condition: Condition,
        left_type: str,
        right_type: str,
        result: ValidationResult,
    ) -> None:
        """Add an actionable temporal representation mismatch issue."""
        self._add(
            result,
            "SPARK_CONDITION_TEMPORAL_MISMATCH",
            f"Condition {condition.condition_id!r} compares incompatible "
            f"temporal operand types {left_type} and {right_type}. Use an "
            "explicit matching value_type (including timestamp_ntz when "
            "applicable) or to_date for intentional date conversion.",
            ObjectType.CONDITION,
            condition.condition_id,
            details={
                "operator": condition.operator.value,
                "left_type": left_type,
                "right_type": right_type,
            },
        )

    def _add_unusable_hint_issues(
        self,
        result: ValidationResult,
        unresolved_items: list[tuple[Rule, Assignment]],
        *,
        target_type_known: bool,
        existing_target: bool,
    ) -> None:
        """Explain assignment types that could not be inferred."""
        for rule, assignment in unresolved_items:
            operand = assignment.value
            if isinstance(operand, FieldOperand):
                if not target_type_known:
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_TYPE_UNRESOLVED",
                        f"Spark type could not be inferred from source field "
                        f"{operand.field_name!r} for new target field "
                        f"{assignment.target_field!r}.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                    )
                continue
            if isinstance(operand, AssignedOperand):
                if not target_type_known:
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_TYPE_UNRESOLVED",
                        f"Spark type could not be inferred from assigned value "
                        f"{operand.target_field!r} for new target field "
                        f"{assignment.target_field!r}.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                    )
                continue
            if (
                isinstance(operand, LiteralOperand)
                and operand.value_type
                and operand.value_type.lower() not in SPARK_TYPE_HINTS
            ):
                self._add(
                    result,
                    "SPARK_ASSIGNMENT_VALUE_TYPE_UNSUPPORTED",
                    f"Unsupported assignment value_type {operand.value_type!r}.",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                )
                continue
            if isinstance(operand, LiteralOperand) and operand.value is None:
                if not existing_target:
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_NULL_TYPE_REQUIRED",
                        f"Null literal assigned to new target field "
                        f"{assignment.target_field!r} requires value_type.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                        details={
                            "rule_id": rule.rule_id,
                            "target_field": assignment.target_field,
                        },
                    )
                continue
            if isinstance(operand, CustomFunctionOperand):
                hint = self._custom_return_type_hint(operand)
                normalized_hint = hint.lower() if hint is not None else None
                if (
                    normalized_hint is not None
                    and normalized_hint not in SPARK_TYPE_HINTS
                    and normalized_hint != "any"
                ):
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_RETURN_TYPE_UNSUPPORTED",
                        f"Custom assignment function {operand.function_name!r} has "
                        f"unsupported return_type_hint {hint!r}.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                    )
                elif not existing_target:
                    self._add(
                        result,
                        "SPARK_ASSIGNMENT_RETURN_TYPE_REQUIRED",
                        f"Custom assignment function {operand.function_name!r} "
                        "has a polymorphic or missing return type. A concrete "
                        "return_type_hint is required for a new target field.",
                        ObjectType.ASSIGNMENT,
                        assignment.assignment_id,
                        details={
                            "rule_id": rule.rule_id,
                            "target_field": assignment.target_field,
                        },
                    )
                continue
            if not target_type_known:
                self._add(
                    result,
                    "SPARK_ASSIGNMENT_TYPE_UNRESOLVED",
                    f"Spark type could not be inferred for new target field "
                    f"{assignment.target_field!r}.",
                    ObjectType.ASSIGNMENT,
                    assignment.assignment_id,
                )

    def _add_target_type_issue(
        self,
        result: ValidationResult,
        rule: Rule,
        assignment: Assignment,
        proposed_type: T.DataType,
        target_type: T.DataType,
    ) -> None:
        """Add an existing-target compatibility error."""
        self._add(
            result,
            "SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE",
            f"Assignment {assignment.assignment_id!r} in rule {rule.rule_id!r} "
            f"cannot assign {proposed_type.simpleString()} to existing field "
            f"{assignment.target_field!r} of type {target_type.simpleString()}.",
            ObjectType.ASSIGNMENT,
            assignment.assignment_id,
            details={
                "rule_id": rule.rule_id,
                "target_field": assignment.target_field,
                "proposed_type": proposed_type.simpleString(),
                "target_type": target_type.simpleString(),
            },
        )

    def _operand_type(
        self,
        operand: Operand | None,
        source_fields: dict[str, T.DataType],
        assigned_types: dict[str, T.DataType] | None = None,
    ) -> T.DataType | None:
        """Infer an operand Spark type when metadata and schema make it knowable."""
        base_type = self._base_operand_type(
            operand,
            source_fields,
            assigned_types,
        )
        if base_type is not None:
            return base_type
        if operand is None or operand.default_if_null is None:
            return None
        if isinstance(operand, FieldOperand):
            source_type = source_fields.get(operand.field_name)
            return (
                self._base_operand_type(operand.default_if_null, source_fields)
                if isinstance(source_type, T.NullType)
                else None
            )
        if isinstance(operand, AssignedOperand):
            return self._base_operand_type(
                operand.default_if_null,
                source_fields,
                assigned_types,
            )
        if isinstance(operand, LiteralOperand) and operand.value is None:
            return self._base_operand_type(
                operand.default_if_null,
                source_fields,
                assigned_types,
            )
        return None

    def _base_operand_type(
        self,
        operand: Operand | None,
        source_fields: dict[str, T.DataType],
        assigned_types: dict[str, T.DataType] | None = None,
    ) -> T.DataType | None:
        """Infer an operand's type without considering its null fallback."""
        if isinstance(operand, FieldOperand):
            data_type = source_fields.get(operand.field_name)
            return None if isinstance(data_type, T.NullType) else data_type
        if isinstance(operand, AssignedOperand):
            return (assigned_types or {}).get(operand.target_field)
        if isinstance(operand, LiteralOperand):
            if isinstance(operand.value, (list, tuple, set)):
                if operand.value_type:
                    element_type = SPARK_TYPE_HINTS.get(operand.value_type.lower())
                    return (
                        T.ArrayType(element_type, True)
                        if element_type is not None
                        else None
                    )
                return self._literal_type(operand.value)
            if isinstance(operand.value, MappingABC):
                return self._literal_type(operand.value)
            if operand.value_type:
                return SPARK_TYPE_HINTS.get(operand.value_type.lower())
            return self._literal_type(operand.value)
        if isinstance(operand, CustomFunctionOperand):
            hint = self._custom_return_type_hint(operand)
            return SPARK_TYPE_HINTS.get(hint.lower()) if hint else None
        return None

    def _default_is_compatible(
        self,
        default: LiteralOperand,
        operand_type: T.DataType,
    ) -> bool:
        """Return whether a literal fallback fits its operand's Spark type."""
        literal_compatible = self._literal_assignment_is_compatible(
            default,
            operand_type,
        )
        if literal_compatible is not None:
            return literal_compatible
        default_type = self._base_operand_type(default, {})
        if default_type is None:
            return False
        numeric_compatible = self._numeric_assignment_is_compatible(
            default_type,
            operand_type,
        )
        if numeric_compatible is not None:
            return numeric_compatible
        return default_type == operand_type

    def _custom_return_type_hint(
        self,
        operand: CustomFunctionOperand,
    ) -> str | None:
        """Return the registered custom-function type hint when available."""
        if self._function_registry is None:
            return None
        if not self._function_registry.has_spec(operand.function_name):
            return None
        return self._function_registry.get_spec(operand.function_name).return_type_hint

    def _literal_type(self, value: Any) -> T.DataType | None:
        """Infer the Spark type of a non-null Python literal."""
        if value is None:
            return None
        if isinstance(value, bool):
            return T.BooleanType()
        if isinstance(value, int):
            return T.LongType()
        if isinstance(value, float):
            return T.DoubleType()
        if isinstance(value, Decimal):
            return decimal_literal_type(value)
        if isinstance(value, datetime):
            return T.TimestampType()
        if isinstance(value, date):
            return T.DateType()
        if isinstance(value, str):
            return T.StringType()
        if isinstance(value, MappingABC):
            return self._mapping_literal_type(value)
        if isinstance(value, (list, tuple, set)):
            return self._collection_literal_type(value)
        return None

    def _mapping_literal_type(self, value: MappingABC) -> T.StructType | None:
        """Infer a deterministic Spark struct type for a mapping literal."""
        fields: list[T.StructField] = []
        for field_name, field_value in sorted(
            value.items(),
            key=lambda item: str(item[0]),
        ):
            field_type = self._literal_type(field_value)
            if field_type is None:
                return None
            fields.append(T.StructField(str(field_name), field_type, True))
        return T.StructType(fields)

    def _collection_literal_type(
        self,
        value: list[Any] | tuple[Any, ...] | set[Any],
    ) -> T.ArrayType | None:
        """Infer one safe common element type for a collection literal."""
        items = sorted(value, key=repr) if isinstance(value, set) else value
        element_type: T.DataType | None = None
        for item in items:
            item_type = self._literal_type(item)
            if item_type is None:
                return None
            element_type = (
                item_type
                if element_type is None
                else self._common_type(element_type, item_type)
            )
            if element_type is None:
                return None
        return T.ArrayType(element_type, True) if element_type is not None else None

    def _assignment_is_compatible(
        self,
        assignment: Assignment,
        proposed_type: T.DataType,
        target_type: T.DataType,
    ) -> bool:
        """Return whether one assignment can populate an existing target type."""
        operand = assignment.value
        if isinstance(operand, LiteralOperand):
            literal_compatible = self._literal_assignment_is_compatible(
                operand,
                target_type,
            )
            if literal_compatible is not None:
                return literal_compatible
        if (
            isinstance(operand, CustomFunctionOperand)
            and (self._custom_return_type_hint(operand) or "").lower() == "decimal"
            and isinstance(target_type, T.DecimalType)
        ):
            # The concrete precision is data-dependent and is enforced per row.
            return True
        numeric_compatible = self._numeric_assignment_is_compatible(
            proposed_type,
            target_type,
        )
        if numeric_compatible is not None:
            return numeric_compatible
        return proposed_type == target_type

    def _numeric_assignment_is_compatible(
        self,
        proposed_type: T.DataType,
        target_type: T.DataType,
    ) -> bool | None:
        """Return a numeric widening decision, or ``None`` for other types."""
        numeric_types = (*INTEGRAL_TYPES, T.FloatType, T.DoubleType, T.DecimalType)
        if not isinstance(proposed_type, numeric_types) or not isinstance(
            target_type,
            numeric_types,
        ):
            return None
        if isinstance(proposed_type, INTEGRAL_TYPES) and isinstance(
            target_type,
            INTEGRAL_TYPES,
        ):
            return self._integral_rank(proposed_type) <= self._integral_rank(target_type)
        if isinstance(proposed_type, T.FloatType) and isinstance(target_type, T.DoubleType):
            return True
        if isinstance(proposed_type, INTEGRAL_TYPES) and isinstance(
            target_type,
            (T.FloatType, T.DoubleType),
        ):
            return True
        if isinstance(proposed_type, INTEGRAL_TYPES) and isinstance(
            target_type,
            T.DecimalType,
        ):
            return (
                target_type.precision - target_type.scale
                >= INTEGRAL_DECIMAL_DIGITS[type(proposed_type)]
            )
        if isinstance(proposed_type, T.DecimalType) and isinstance(
            target_type,
            T.DecimalType,
        ):
            return (
                proposed_type.scale <= target_type.scale
                and proposed_type.precision - proposed_type.scale
                <= target_type.precision - target_type.scale
            )
        return proposed_type == target_type

    def _literal_assignment_is_compatible(
        self,
        operand: LiteralOperand,
        target_type: T.DataType,
    ) -> bool | None:
        """Return a literal-specific decision, or ``None`` for general typing."""
        value = operand.value
        if value is None:
            return True
        if operand.value_type is None and isinstance(value, bool):
            return isinstance(target_type, T.BooleanType)
        if (
            operand.value_type is None
            and isinstance(value, int)
            and not isinstance(value, bool)
        ):
            for type_class, limits in INTEGRAL_LIMITS.items():
                if isinstance(target_type, type_class):
                    return limits[0] <= value <= limits[1]
            if isinstance(target_type, (T.FloatType, T.DoubleType)):
                return True
            if isinstance(target_type, T.DecimalType):
                return decimal_value_fits(Decimal(value), target_type)
        if operand.value_type is None and isinstance(value, float):
            return isinstance(target_type, (T.FloatType, T.DoubleType))
        if isinstance(value, Decimal):
            return (
                isinstance(target_type, T.DecimalType)
                and decimal_value_fits(value, target_type)
            )
        return None

    def _common_type(
        self,
        left: T.DataType,
        right: T.DataType,
    ) -> T.DataType | None:
        """Return a safe common Spark type, never a string fallback."""
        if left == right:
            return left
        if isinstance(left, INTEGRAL_TYPES) and isinstance(right, INTEGRAL_TYPES):
            return max((left, right), key=self._integral_rank)
        if isinstance(left, (T.FloatType, T.DoubleType)) and isinstance(
            right,
            (T.FloatType, T.DoubleType),
        ):
            return T.DoubleType()
        if (
            isinstance(left, INTEGRAL_TYPES)
            and isinstance(right, (T.FloatType, T.DoubleType))
        ) or (
            isinstance(right, INTEGRAL_TYPES)
            and isinstance(left, (T.FloatType, T.DoubleType))
        ):
            return T.DoubleType()
        if isinstance(left, T.DecimalType) and isinstance(right, T.DecimalType):
            return self._common_decimal_type(left, right)
        if isinstance(left, T.DecimalType) and isinstance(right, INTEGRAL_TYPES):
            return self._common_decimal_integral_type(left, right)
        if isinstance(right, T.DecimalType) and isinstance(left, INTEGRAL_TYPES):
            return self._common_decimal_integral_type(right, left)
        return None

    def _common_decimal_type(
        self,
        left: T.DecimalType,
        right: T.DecimalType,
    ) -> T.DecimalType | None:
        """Return an exact common decimal type when Spark's precision allows it."""
        scale = max(left.scale, right.scale)
        integral_digits = max(
            left.precision - left.scale,
            right.precision - right.scale,
        )
        precision = integral_digits + scale
        return T.DecimalType(precision, scale) if precision <= 38 else None

    def _common_decimal_integral_type(
        self,
        decimal_type: T.DecimalType,
        integral_type: T.DataType,
    ) -> T.DecimalType | None:
        """Return a decimal type that exactly holds a decimal and an integral type."""
        integral_digits = max(
            decimal_type.precision - decimal_type.scale,
            INTEGRAL_DECIMAL_DIGITS[type(integral_type)],
        )
        precision = integral_digits + decimal_type.scale
        return (
            T.DecimalType(precision, decimal_type.scale)
            if precision <= 38
            else None
        )

    def _is_temporal_type(self, data_type: T.DataType) -> bool:
        """Return whether a type is a Spark date or timestamp representation."""
        return isinstance(data_type, TEMPORAL_TYPES)

    def _integral_rank(self, data_type: T.DataType) -> int:
        """Return the widening rank for an integral Spark type."""
        return INTEGRAL_RANK[type(data_type)]
