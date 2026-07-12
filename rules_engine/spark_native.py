"""Native Spark column compiler for field/literal rulesets."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
)
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import (
    Condition,
    ConditionGroup,
    FieldOperand,
    LiteralOperand,
    Operand,
    Rule,
)
from rules_engine.spark_types import (
    CONDITION_TRACE_STRUCT,
    OPERAND_TRACE_STRUCT,
    WINNING_RULE_TRACE_STRUCT,
)


@dataclass(frozen=True)
class _NativeConditionEvaluation:
    """Native expressions produced for one condition."""

    passed: Column
    trace: Column
    explanation: Column


@dataclass(frozen=True)
class _NativeGroupEvaluation:
    """Native expressions produced for one logical condition group."""

    passed: Column
    condition_traces: tuple[Column, ...]
    explanation: Column


def spark_column(name: str) -> Column:
    """Resolve a literal Spark column name, including dots and backticks."""
    return F.col(f"`{name.replace('`', '``')}`")


def _first_matching_value(
    branches: list[tuple[Column, Column]],
    default: Column,
) -> Column:
    """Build one flat first-match Spark CASE expression."""
    if not branches:
        return default
    expression = F.when(*branches[0])
    for condition, value in branches[1:]:
        expression = expression.when(condition, value)
    return expression.otherwise(default)


def mapping_value(value: Mapping[Any, Any], field_name: str) -> Any:
    """Return a mapping item using Spark's stringified field names."""
    if field_name in value:
        return value[field_name]
    for key, item in value.items():
        if str(key) == field_name:
            return item
    return None


def trace_text(value: Any) -> str | None:
    """Convert arbitrary trace values to compact Spark string fields."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, MappingABC):
        return ", ".join(f"{key}={trace_text(item)}" for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return "[" + ", ".join(trace_text(item) or "" for item in value) + "]"
    return str(value)


def non_default(value: Any, default: Any) -> Any | None:
    """Return a trace option only when it differs from its default."""
    return None if value in (None, default) else value


class NativeRulesetCompiler:
    """Compile one compatible ruleset into Spark column expressions."""

    def __init__(
        self,
        source_schema: T.StructType,
        assign_schema: T.StructType,
        rule_formatter: HumanReadableRulesetFormatter,
    ) -> None:
        self._source_types = {
            field.name: field.dataType for field in source_schema.fields
        }
        self._assign_schema = assign_schema
        self._assign_types = {
            field.name: field.dataType for field in assign_schema.fields
        }
        self._rule_formatter = rule_formatter

    def evaluate(
        self,
        df: DataFrame,
        rules: tuple[Rule, ...],
        column_prefix: str,
    ) -> DataFrame:
        """Append native rules engine expressions to a DataFrame."""
        matched_rule_ids = F.array().cast(T.ArrayType(T.StringType(), False))
        eligible = F.lit(True)
        assignments_present = F.lit(False)
        assignment_branches: dict[str, list[tuple[Column, Column]]] = {
            field_name: [] for field_name in self._assign_types
        }
        rule_matches: list[tuple[Rule, Column, _NativeGroupEvaluation]] = []

        for rule in rules:
            group = self._group(rule.root_group)
            matched = eligible & group.passed
            rule_matches.append((rule, matched, group))
            matched_rule_ids = F.concat(
                matched_rule_ids,
                F.when(matched, F.array(F.lit(rule.rule_id))).otherwise(
                    F.array().cast(T.ArrayType(T.StringType(), False))
                ),
            )
            if rule.assignments:
                assignments_present = assignments_present | matched
            for assignment in rule.assignments:
                data_type = self._assign_types[assignment.target_field]
                value = self._assignment_value(assignment.value, data_type)
                assignment_branches[assignment.target_field].append((matched, value))
            if rule.stop_on_match:
                eligible = eligible & ~group.passed

        winning_rule = _first_matching_value(
            [
                (matched, self._rule_trace(rule, group))
                for rule, matched, group in rule_matches
            ],
            F.lit(None).cast(WINNING_RULE_TRACE_STRUCT),
        )
        winning_rule_id = _first_matching_value(
            [(matched, F.lit(rule.rule_id)) for rule, matched, _ in rule_matches],
            F.lit(None).cast(T.StringType()),
        )
        winning_rule_name = _first_matching_value(
            [(matched, F.lit(rule.rule_name)) for rule, matched, _ in rule_matches],
            F.lit(None).cast(T.StringType()),
        )
        winning_explanation = _first_matching_value(
            [(matched, group.explanation) for _, matched, group in rule_matches],
            F.lit(None).cast(T.StringType()),
        )
        assignment_values = {
            field_name: _first_matching_value(
                list(reversed(assignment_branches[field_name])),
                F.lit(None).cast(data_type),
            )
            for field_name, data_type in self._assign_types.items()
        }
        assign_value = self._assignment_struct(
            assignment_values,
            assignments_present,
        )

        return df.withColumns(
            {
                f"{column_prefix}_matched": F.size(matched_rule_ids) > F.lit(0),
                f"{column_prefix}_matched_rule_ids": matched_rule_ids,
                f"{column_prefix}_assign": assign_value,
                f"{column_prefix}_winning_rule": winning_rule,
                f"{column_prefix}_winning_rule_id": winning_rule_id,
                f"{column_prefix}_winning_rule_name": winning_rule_name,
                f"{column_prefix}_winning_rule_explanation": winning_explanation,
                f"{column_prefix}_error": F.lit(None).cast(T.StringType()),
            }
        )

    def _assignment_struct(
        self,
        values: Mapping[str, Column],
        assignments_present: Column,
    ) -> Column:
        """Build the stable assignment result struct."""
        if not values:
            return F.lit(None).cast(self._assign_schema)
        return F.when(
            assignments_present,
            F.struct(
                *[value.alias(field_name) for field_name, value in values.items()]
            ),
        ).otherwise(F.lit(None).cast(self._assign_schema))

    def _group(
        self,
        group: ConditionGroup,
        *,
        nested: bool = False,
    ) -> _NativeGroupEvaluation:
        """Compile one logical group and its descendants."""
        conditions = [self._condition(condition) for condition in group.conditions]
        groups = [self._group(child, nested=True) for child in group.groups]
        child_passes = [item.passed for item in conditions]
        child_passes.extend(item.passed for item in groups)
        if group.logical_operator is LogicalOperator.ALL:
            passed = F.lit(True)
            for child_passed in child_passes:
                passed = passed & child_passed
            joiner = " AND "
        else:
            passed = F.lit(False)
            for child_passed in child_passes:
                passed = passed | child_passed
            joiner = " OR "

        explanation_parts = [item.explanation for item in conditions]
        explanation_parts.extend(item.explanation for item in groups)
        explanation = self._group_explanation(
            passed,
            explanation_parts,
            joiner,
            nested,
        )
        traces = [item.trace for item in conditions]
        for child in groups:
            traces.extend(child.condition_traces)
        return _NativeGroupEvaluation(passed, tuple(traces), explanation)

    def _group_explanation(
        self,
        passed: Column,
        parts: list[Column],
        joiner: str,
        nested: bool,
    ) -> Column:
        """Build the passed-branch explanation for a logical group."""
        if not parts:
            return F.lit(None).cast(T.StringType())
        included_count = F.lit(0)
        for part in parts:
            included_count = included_count + F.when(part.isNotNull(), 1).otherwise(0)
        text = F.concat_ws(joiner, *parts)
        if nested:
            text = F.when(
                included_count > F.lit(1),
                F.concat(F.lit("("), text, F.lit(")")),
            ).otherwise(text)
        return F.when(passed & (included_count > F.lit(0)), text).otherwise(
            F.lit(None).cast(T.StringType())
        )

    def _condition(self, condition: Condition) -> _NativeConditionEvaluation:
        """Compile one condition and its winning-rule trace."""
        if condition.active_flag:
            left = self._operand_value(condition.left)
            right = (
                self._operand_value(condition.right)
                if condition.right is not None
                else None
            )
            comparison_result = self._comparison(condition, left, right)
            if condition.null_result_mode is NullResultMode.DEFAULT:
                passed = F.coalesce(
                    comparison_result,
                    F.lit(bool(condition.null_default_value)),
                )
            else:
                passed = F.coalesce(comparison_result, F.lit(False))
        else:
            comparison_result = F.lit(None).cast(T.BooleanType())
            passed = F.lit(False)

        trace = self._condition_trace(condition, comparison_result, passed)
        explanation = F.when(
            passed,
            F.lit(self._rule_formatter.format_condition(condition)),
        ).otherwise(F.lit(None).cast(T.StringType()))
        return _NativeConditionEvaluation(passed, trace, explanation)

    def _comparison(
        self,
        condition: Condition,
        left: Column,
        right: Column | None,
    ) -> Column:
        """Compile comparison and null handling for one active condition."""
        operator = condition.operator
        if operator is ComparisonOperator.IS_NULL:
            return left.isNull()
        if operator is ComparisonOperator.IS_NOT_NULL:
            return left.isNotNull()
        if right is None:
            raise ValueError(f"Operator {operator.value} requires a right operand.")

        null_input = left.isNull() | right.isNull()
        if condition.null_input_mode is NullInputMode.ZERO:
            left = F.coalesce(left, F.lit(0))
            right = F.coalesce(right, F.lit(0))
        raw = self._operator_result(condition, left, right)
        if condition.null_input_mode in {
            NullInputMode.PROPAGATE,
            NullInputMode.IGNORE,
        }:
            return F.when(
                null_input,
                F.lit(None).cast(T.BooleanType()),
            ).otherwise(raw)
        return raw

    def _operator_result(
        self,
        condition: Condition,
        left: Column,
        right: Column,
    ) -> Column:
        """Compile the non-null comparison operator for one condition."""
        operator = condition.operator
        if operator in {ComparisonOperator.EQ, ComparisonOperator.NE}:
            result = left == right
            return ~result if operator is ComparisonOperator.NE else result
        if operator is ComparisonOperator.GT:
            return left > right
        if operator is ComparisonOperator.GE:
            return left >= right
        if operator is ComparisonOperator.LT:
            return left < right
        if operator is ComparisonOperator.LE:
            return left <= right
        if operator in {ComparisonOperator.IN, ComparisonOperator.NOT_IN}:
            if not isinstance(condition.right, LiteralOperand):
                raise TypeError("Native membership requires a literal right operand.")
            result = left.isin(list(condition.right.value))
            return ~result if operator is ComparisonOperator.NOT_IN else result
        if operator in {ComparisonOperator.BETWEEN, ComparisonOperator.NOT_BETWEEN}:
            if not isinstance(condition.right, LiteralOperand):
                raise TypeError("Native BETWEEN requires a literal right operand.")
            lower, upper = list(condition.right.value)
            result = left.between(F.lit(lower), F.lit(upper))
            return ~result if operator is ComparisonOperator.NOT_BETWEEN else result
        if not isinstance(condition.right, LiteralOperand):
            raise TypeError(
                "Native string comparison requires a literal right operand."
            )
        right_value = condition.right.value
        if operator is ComparisonOperator.CONTAINS:
            return left.cast(T.StringType()).contains(str(right_value))
        if operator is ComparisonOperator.NOT_CONTAINS:
            return ~left.cast(T.StringType()).contains(str(right_value))
        if operator is ComparisonOperator.STARTS_WITH:
            return left.cast(T.StringType()).startswith(str(right_value))
        if operator is ComparisonOperator.ENDS_WITH:
            return left.cast(T.StringType()).endswith(str(right_value))
        if operator is ComparisonOperator.LIKE:
            return left.cast(T.StringType()).like(str(right_value))
        if operator is ComparisonOperator.NOT_LIKE:
            return ~left.cast(T.StringType()).like(str(right_value))
        raise ValueError(f"Unsupported native comparison operator: {operator.value}")

    def _operand_value(self, operand: Operand | None) -> Column:
        """Compile a field or literal operand to a Spark column."""
        if isinstance(operand, FieldOperand):
            return spark_column(operand.field_name)
        if isinstance(operand, LiteralOperand):
            value = (
                list(operand.value)
                if isinstance(operand.value, tuple)
                else operand.value
            )
            column = F.lit(value)
            if operand.value_type is None:
                return column
            data_type = {
                "string": T.StringType(),
                "str": T.StringType(),
                "integer": T.LongType(),
                "int": T.LongType(),
                "long": T.LongType(),
                "float": T.DoubleType(),
                "double": T.DoubleType(),
                "decimal": T.DoubleType(),
                "boolean": T.BooleanType(),
                "bool": T.BooleanType(),
                "date": T.DateType(),
                "timestamp": T.TimestampType(),
            }.get(operand.value_type.lower())
            return column.cast(data_type) if data_type is not None else column
        raise TypeError(f"Unsupported native operand: {type(operand).__name__}")

    def _assignment_value(
        self,
        operand: Operand,
        data_type: T.DataType,
    ) -> Column:
        """Compile an assignment operand to its declared Spark result type."""
        if isinstance(operand, FieldOperand):
            return spark_column(operand.field_name).cast(data_type)
        if not isinstance(operand, LiteralOperand):
            raise TypeError(f"Unsupported native assignment: {type(operand).__name__}")
        if isinstance(data_type, T.StructType):
            if not isinstance(operand.value, MappingABC):
                return F.lit(None).cast(data_type)
            return self._mapping_literal(operand.value, data_type)
        if isinstance(data_type, T.StringType) and not isinstance(operand.value, str):
            return F.lit(trace_text(operand.value))
        return F.lit(operand.value).cast(data_type)

    def _mapping_literal(
        self,
        value: Mapping[Any, Any],
        data_type: T.StructType,
    ) -> Column:
        """Compile a nested mapping literal to a Spark struct expression."""
        fields: list[Column] = []
        for field in data_type.fields:
            item = mapping_value(value, field.name)
            if isinstance(field.dataType, T.StructType) and isinstance(
                item, MappingABC
            ):
                column = self._mapping_literal(item, field.dataType)
            elif isinstance(field.dataType, T.StringType) and not isinstance(item, str):
                column = F.lit(trace_text(item))
            else:
                column = F.lit(item).cast(field.dataType)
            fields.append(column.alias(field.name))
        return F.struct(*fields)

    def _rule_trace(
        self,
        rule: Rule,
        group: _NativeGroupEvaluation,
    ) -> Column:
        """Build a native winning-rule trace struct."""
        assignments = F.array(
            *[F.lit(assignment.target_field) for assignment in rule.assignments]
        ).cast(T.ArrayType(T.StringType(), False))
        conditions = F.array(*group.condition_traces).cast(
            T.ArrayType(CONDITION_TRACE_STRUCT, False)
        )
        return F.struct(
            F.lit(rule.rule_id).alias("rule_id"),
            F.lit(rule.rule_name).alias("rule_name"),
            F.lit(True).alias("matched"),
            assignments.alias("assignments_applied"),
            conditions.alias("conditions"),
        )

    def _condition_trace(
        self,
        condition: Condition,
        comparison_result: Column,
        passed: Column,
    ) -> Column:
        """Build one native condition trace struct."""
        columns = [
            *self._operand_columns(condition.left),
            *self._operand_columns(condition.right),
        ]
        unique_columns = list(dict.fromkeys(columns))
        return F.struct(
            F.array(*[F.lit(name) for name in unique_columns])
            .cast(T.ArrayType(T.StringType(), False))
            .alias("columns"),
            self._operand_trace(
                condition.left,
                evaluated=condition.active_flag,
            ).alias("left"),
            (
                self._operand_trace(
                    condition.right,
                    evaluated=condition.active_flag,
                )
                if condition.right is not None
                else F.lit(None).cast(OPERAND_TRACE_STRUCT)
            ).alias("right"),
            F.lit(condition.operator.value).alias("operator"),
            comparison_result.alias("comparison_result"),
            passed.alias("passed"),
            F.lit(trace_text(non_default(condition.tolerance_abs, Decimal("0"))))
            .cast(T.StringType())
            .alias("tolerance_abs"),
            F.lit(
                non_default(
                    condition.null_input_mode.value,
                    NullInputMode.PROPAGATE.value,
                )
            )
            .cast(T.StringType())
            .alias("null_input_mode"),
            F.lit(
                non_default(
                    condition.null_result_mode.value,
                    NullResultMode.NULL.value,
                )
            )
            .cast(T.StringType())
            .alias("null_result_mode"),
            F.lit(trace_text(condition.null_default_value))
            .cast(T.StringType())
            .alias("null_default_value"),
        )

    def _operand_trace(
        self,
        operand: Operand,
        *,
        evaluated: bool,
    ) -> Column:
        """Build a native Spark operand trace struct."""
        if isinstance(operand, FieldOperand):
            columns = [operand.field_name]
            value = (
                self._trace_value_column(
                    spark_column(operand.field_name),
                    self._source_types[operand.field_name],
                )
                if evaluated
                else F.lit(None).cast(T.StringType())
            )
            column = operand.field_name
            value_type = None
        elif isinstance(operand, LiteralOperand):
            columns = []
            value = F.lit(trace_text(operand.value)).cast(T.StringType())
            column = None
            value_type = operand.value_type
        else:
            raise TypeError(
                f"Unsupported native trace operand: {type(operand).__name__}"
            )
        return F.struct(
            F.lit(operand.kind.value).alias("kind"),
            F.lit(column).cast(T.StringType()).alias("column"),
            value.alias("value"),
            F.lit(value_type).cast(T.StringType()).alias("value_type"),
            F.lit(None).cast(T.StringType()).alias("function_name"),
            F.array(*[F.lit(name) for name in columns])
            .cast(T.ArrayType(T.StringType(), False))
            .alias("source_columns"),
            F.lit(None)
            .cast(T.MapType(T.StringType(), T.StringType(), True))
            .alias("arguments"),
        )

    def _trace_value_column(
        self,
        value: Column,
        data_type: T.DataType,
    ) -> Column:
        """Format a row value with the existing trace string conventions."""
        if isinstance(data_type, T.BooleanType):
            return (
                F.when(value.isNull(), F.lit(None).cast(T.StringType()))
                .when(
                    value,
                    F.lit("True"),
                )
                .otherwise(F.lit("False"))
            )
        return value.cast(T.StringType())

    def _operand_columns(self, operand: Operand | None) -> list[str]:
        """Return source columns referenced by a native operand."""
        if isinstance(operand, FieldOperand):
            return [operand.field_name]
        return []
