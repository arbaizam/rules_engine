"""
Worker-side row evaluation helpers for the Spark runtime.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from json import dumps as json_dumps
from typing import Any

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    OperandKind,
)
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import (
    AssignedOperand,
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    ResolvedConditionTrace,
    Rule,
    RuleExecutionTrace,
    Ruleset,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository


@dataclass(frozen=True)
class OperandResolution:
    """Resolved operand value plus trace-safe metadata."""

    value: Any
    trace: dict[str, Any]


@dataclass(frozen=True)
class AssignedValue:
    """One committed assignment value and the rule that produced it."""

    value: Any
    rule_id: str
    assignment_id: str


class SparkRowEvaluator:
    """
    Row-level evaluator reused inside Spark worker UDFs.
    """

    def __init__(
        self,
        repository: RulesetRepository | None,
        function_registry: FunctionRegistry,
    ) -> None:
        """
        Create a runtime bound to metadata and custom-function registries.
        """
        self._repository = repository
        self._function_registry = function_registry
        self._rule_formatter = HumanReadableRulesetFormatter()

    @classmethod
    def for_embedded_ruleset(
        cls,
        function_registry: FunctionRegistry,
    ) -> SparkRowEvaluator:
        """Create an evaluator that cannot load metadata at row-evaluation time.

        Spark workers and pure-Python expected cases receive an already
        compiled ruleset. Omitting the real Spark/Delta repository prevents an
        active Spark session from being captured in worker serialization.
        """
        return cls(None, function_registry)

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        if self._repository is None:
            raise RuntimeError("This row evaluator cannot load published metadata.")
        return self._repository.load_published(ruleset_name, version)

    def evaluate_row(
        self,
        ruleset: Ruleset,
        row: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Evaluate one Python mapping using production rule semantics.

        This allocation-light API is the semantic core used by embedded
        expected cases. It deliberately returns only stable business results,
        independent of Spark schemas and audit rendering.
        """
        matched_rule_ids: list[str] = []
        assignments: dict[str, Any] = {}
        assigned_values: dict[str, AssignedValue] = {}
        for rule in sorted(ruleset.rules, key=lambda item: item.rule_order):
            if not rule.active_flag or not self._rule_matches(
                rule,
                row,
                assigned_values,
            ):
                continue
            matched_rule_ids.append(rule.rule_id)
            resolved_assignments = self._evaluate_assignments(
                rule.assignments,
                row,
                assigned_values,
            )
            assignments.update(resolved_assignments)
            assigned_values.update(
                {
                    assignment.target_field: AssignedValue(
                        value=resolved_assignments[assignment.target_field],
                        rule_id=rule.rule_id,
                        assignment_id=assignment.assignment_id,
                    )
                    for assignment in rule.assignments
                }
            )
            if rule.stop_on_match:
                break
        return {
            "matched": bool(matched_rule_ids),
            "matched_rule_ids": matched_rule_ids,
            "assign": assignments or None,
        }

    def closest_rule_diagnostic(
        self,
        ruleset: Ruleset,
        row: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Return the closest active rule and its failed active conditions.

        This diagnostic reevaluates every active condition. Custom condition
        functions therefore run again when a caller materializes closest-rule
        coverage rows; only use implementations that are safe to reevaluate.
        """
        candidates: list[tuple[float, int, int, Rule, list[str]]] = []
        assigned_values: dict[str, AssignedValue] = {}
        for rule in sorted(ruleset.rules, key=lambda item: item.rule_order):
            if not rule.active_flag:
                continue
            matched, traces = self._evaluate_rule(rule, row, assigned_values)
            active_traces = [trace for trace in traces if trace.active_flag]
            passed_count = sum(trace.passed for trace in active_traces)
            total_count = len(active_traces)
            score = passed_count / total_count if total_count else 0.0
            failed_ids = [
                trace.condition_id for trace in active_traces if not trace.passed
            ]
            candidates.append(
                (score, passed_count, -rule.rule_order, rule, failed_ids)
            )
            if matched:
                resolved_assignments = self._evaluate_assignments(
                    rule.assignments,
                    row,
                    assigned_values,
                )
                assigned_values.update(
                    {
                        assignment.target_field: AssignedValue(
                            value=resolved_assignments[assignment.target_field],
                            rule_id=rule.rule_id,
                            assignment_id=assignment.assignment_id,
                        )
                        for assignment in rule.assignments
                    }
                )
                if rule.stop_on_match:
                    break
        if not candidates:
            return None
        score, passed_count, _, rule, failed_ids = max(
            candidates,
            key=lambda item: item[:3],
        )
        total_count = passed_count + len(failed_ids)
        return {
            "closest_rule_id": rule.rule_id,
            "closest_rule_name": rule.rule_name,
            "closest_rule_score": score,
            "passed_condition_count": passed_count,
            "condition_count": total_count,
            "failed_condition_ids": failed_ids,
        }

    def _evaluate_rule(
        self,
        rule: Rule,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> tuple[bool, list[ResolvedConditionTrace]]:
        """
        Evaluate one rule against one row and collect condition traces.
        """
        condition_traces: list[ResolvedConditionTrace] = []
        matched = self._evaluate_group(
            rule.root_group,
            row,
            condition_traces,
            assigned_values,
        )
        return matched, condition_traces

    def _rule_matches(
        self,
        rule: Rule,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> bool:
        """Evaluate one rule without constructing trace payloads."""
        return self._group_matches(rule.root_group, row, assigned_values)

    def _group_matches(
        self,
        group: ConditionGroup,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> bool:
        """Evaluate a group without trace allocation while preserving errors."""
        if group.logical_operator is LogicalOperator.ALL:
            matched = True
            for condition in group.conditions:
                if not self._condition_matches(condition, row, assigned_values):
                    matched = False
            for nested_group in group.groups:
                if not self._group_matches(nested_group, row, assigned_values):
                    matched = False
            return matched

        matched = False
        for condition in group.conditions:
            if self._condition_matches(condition, row, assigned_values):
                matched = True
        for nested_group in group.groups:
            if self._group_matches(nested_group, row, assigned_values):
                matched = True
        return matched

    def _condition_matches(
        self,
        condition: Condition,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> bool:
        """Evaluate one condition without resolving trace metadata."""
        if not condition.active_flag:
            return False
        left = self._resolve_operand(condition.left, row, assigned_values)
        right = (
            self._resolve_operand(condition.right, row, assigned_values)
            if condition.right is not None
            else None
        )
        result = self._compare_values(
            left,
            condition.operator,
            right,
            condition.tolerance_abs,
            condition.error_on_null,
        )
        return result is True

    def _evaluate_group(
        self,
        group: ConditionGroup,
        row: Mapping[str, Any],
        condition_traces: list[ResolvedConditionTrace],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> bool:
        """
        Evaluate a logical group and all nested child groups.
        """
        results: list[bool] = []
        for condition in group.conditions:
            condition_trace = self._evaluate_condition(
                condition,
                group,
                row,
                assigned_values,
            )
            condition_traces.append(condition_trace)
            results.append(condition_trace.passed)
        for nested_group in group.groups:
            results.append(
                self._evaluate_group(
                    nested_group,
                    row,
                    condition_traces,
                    assigned_values,
                )
            )
        if group.logical_operator is LogicalOperator.ALL:
            return all(results)
        return any(results)

    def _evaluate_condition(
        self,
        condition: Condition,
        group: ConditionGroup,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> ResolvedConditionTrace:
        """
        Evaluate one active condition after resolving its operands.
        """
        if not condition.active_flag:
            return self._condition_trace(
                condition=condition,
                group=group,
                passed=False,
                left=self._operand_metadata(condition.left),
                right=(
                    self._operand_metadata(condition.right)
                    if condition.right is not None
                    else None
                ),
                comparison_result=None,
            )
        left = self._resolve_operand_resolution(
            condition.left,
            row,
            assigned_values,
        )
        right = (
            self._resolve_operand_resolution(condition.right, row, assigned_values)
            if condition.right is not None
            else None
        )
        result = self._compare_values(
            left.value,
            condition.operator,
            right.value if right is not None else None,
            condition.tolerance_abs,
            condition.error_on_null,
        )
        return self._condition_trace(
            condition=condition,
            group=group,
            passed=result is True,
            left=left.trace,
            right=right.trace if right is not None else None,
            comparison_result=result,
        )

    def _evaluate_assignments(
        self,
        assignments: tuple[Assignment, ...],
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> dict[str, Any]:
        """
        Resolve all assignments for a matched rule into output values.
        """
        return {
            assignment.target_field: self._resolve_operand(
                assignment.value,
                row,
                assigned_values,
            )
            for assignment in assignments
        }

    def _rule_execution_trace(
        self,
        rule: Rule,
        condition_traces: list[ResolvedConditionTrace],
    ) -> RuleExecutionTrace:
        """
        Build the canonical trace for one evaluated rule.
        """
        return RuleExecutionTrace(
            rule_id=rule.rule_id,
            condition_traces=tuple(condition_traces),
            assignments_applied=tuple(
                assignment.target_field for assignment in rule.assignments
            ),
            rule_name=rule.rule_name,
            rule_order=rule.rule_order,
        )

    def _matched_rule_explanation_from_trace(
        self,
        rule: Rule,
        condition_traces: list[ResolvedConditionTrace],
    ) -> str | None:
        """
        Return a readable matched-rule explanation that preserves group logic.
        """
        passed_condition_ids = {
            trace.condition_id
            for trace in condition_traces
            if trace.passed
        }
        return self._rule_formatter.format_matched_rule_explanation(
            rule,
            passed_condition_ids,
        )

    def _operand_trace_summary(self, operand: Any) -> str | None:
        """
        Return a compact resolved-value summary for matched-rule trace arguments.
        """
        if not isinstance(operand, Mapping):
            return None
        kind = operand.get("kind")
        if kind == OperandKind.FIELD.value:
            column = operand.get("column") or operand.get("field_name")
            return f"{column}={self._trace_display_value(operand.get('value'))}"
        if kind == OperandKind.ASSIGNED.value:
            target = operand.get("target_field")
            return f"assigned({target})={self._trace_display_value(operand.get('value'))}"
        if kind == OperandKind.LITERAL.value:
            return self._trace_display_value(operand.get("value"))
        if kind == OperandKind.CUSTOM_FUNCTION.value:
            args = operand.get("args")
            if isinstance(args, Mapping):
                arg_text = ", ".join(
                    f"{name}={self._operand_trace_summary(value)}"
                    for name, value in args.items()
                )
            else:
                arg_text = ", ".join(
                    f"{name}={value}"
                    for name, value in dict(operand.get("arguments") or {}).items()
                )
            return (
                f"{operand.get('function_name')}({arg_text})="
                f"{self._trace_display_value(operand.get('value'))}"
            )
        return self._trace_display_value(operand.get("value"))

    def _trace_display_value(self, value: Any) -> str:
        """
        Return a compact user-facing value string.
        """
        if value is None:
            return "null"
        if isinstance(value, str):
            return value
        return str(value)

    def _condition_trace(
        self,
        *,
        condition: Condition,
        group: ConditionGroup,
        passed: bool,
        left: dict[str, Any],
        right: dict[str, Any] | None,
        comparison_result: bool | None,
    ) -> ResolvedConditionTrace:
        """
        Build one condition trace while preserving explicit condition metadata.
        """
        return ResolvedConditionTrace(
            condition_id=condition.condition_id,
            condition_group_id=group.condition_group_id,
            condition_group_operator=group.logical_operator.value,
            active_flag=condition.active_flag,
            operator=condition.operator.value,
            tolerance_abs=self._trace_value(condition.tolerance_abs),
            left=left,
            right=right,
            comparison_result=comparison_result,
            passed=passed,
        )

    def _resolve_operand(
        self,
        operand: Operand,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> Any:
        """
        Resolve one operand against the current row.
        """
        if isinstance(operand, FieldOperand):
            value = row.get(operand.field_name)
        elif isinstance(operand, AssignedOperand):
            assigned_value = (assigned_values or {}).get(operand.target_field)
            value = assigned_value.value if assigned_value is not None else None
        elif isinstance(operand, LiteralOperand):
            value = operand.value
        elif isinstance(operand, CustomFunctionOperand):
            args = {
                str(key): (
                    self._resolve_operand(value, row, assigned_values)
                    if isinstance(value, Operand)
                    else value
                )
                for key, value in operand.args.items()
            }
            implementation = self._function_registry.get_implementation(
                operand.function_name
            )
            value = implementation(**args)
        else:
            raise TypeError(f"Unsupported operand type: {type(operand).__name__}")
        if value is None and operand.default_if_null is not None:
            return self._resolve_operand(
                operand.default_if_null,
                row,
                assigned_values,
            )
        return value

    def _resolve_operand_resolution(
        self,
        operand: Operand,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> OperandResolution:
        """
        Resolve one operand and return both the value and trace metadata.
        """
        if isinstance(operand, FieldOperand):
            original_value = row.get(operand.field_name)
            trace = {
                "kind": operand.kind.value,
                "columns": [operand.field_name],
                "field_name": operand.field_name,
                "evaluated": True,
            }
        elif isinstance(operand, AssignedOperand):
            assigned_value = (assigned_values or {}).get(operand.target_field)
            original_value = (
                assigned_value.value if assigned_value is not None else None
            )
            trace = {
                "kind": operand.kind.value,
                "columns": [],
                "target_field": operand.target_field,
                "produced_by_rule_id": (
                    assigned_value.rule_id if assigned_value is not None else None
                ),
                "produced_by_assignment_id": (
                    assigned_value.assignment_id
                    if assigned_value is not None
                    else None
                ),
                "evaluated": True,
            }
        elif isinstance(operand, LiteralOperand):
            original_value = operand.value
            trace = {
                "kind": operand.kind.value,
                "columns": [],
                "value_type": operand.value_type,
                "evaluated": True,
            }
        elif isinstance(operand, CustomFunctionOperand):
            args: dict[str, Any] = {}
            arg_traces: dict[str, Any] = {}
            for key, value in operand.args.items():
                arg_key = str(key)
                if isinstance(value, Operand):
                    argument = self._resolve_operand_resolution(
                        value,
                        row,
                        assigned_values,
                    )
                    args[arg_key] = argument.value
                    arg_traces[arg_key] = argument.trace
                else:
                    args[arg_key] = value
                    arg_traces[arg_key] = {
                        "kind": "literal",
                        "columns": [],
                        "value": self._trace_value(value),
                        "evaluated": True,
                    }
            implementation = self._function_registry.get_implementation(operand.function_name)
            original_value = implementation(**args)
            trace = {
                "kind": operand.kind.value,
                "columns": self._unique_strings(
                    column
                    for arg_trace in arg_traces.values()
                    for column in arg_trace.get("columns", [])
                ),
                "function_name": operand.function_name,
                "args": arg_traces,
                "evaluated": True,
            }
        else:
            raise TypeError(f"Unsupported operand type: {type(operand).__name__}")
        default_applied = original_value is None and operand.default_if_null is not None
        value = (
            self._resolve_operand(operand.default_if_null, row, assigned_values)
            if default_applied
            else original_value
        )
        trace.update(
            original_value=self._trace_value(original_value),
            value=self._trace_value(value),
            default_if_null=(
                self._trace_value(operand.default_if_null.value)
                if operand.default_if_null is not None
                else None
            ),
            default_applied=default_applied,
        )
        return OperandResolution(value=value, trace=trace)

    def _operand_metadata(self, operand: Operand) -> dict[str, Any]:
        """
        Return operand metadata without resolving row-dependent values.
        """
        if isinstance(operand, FieldOperand):
            return {
                "kind": operand.kind.value,
                "columns": [operand.field_name],
                "field_name": operand.field_name,
                "default_if_null": self._operand_default_trace_value(operand),
                "default_applied": False,
                "evaluated": False,
            }
        if isinstance(operand, AssignedOperand):
            return {
                "kind": operand.kind.value,
                "columns": [],
                "target_field": operand.target_field,
                "produced_by_rule_id": None,
                "produced_by_assignment_id": None,
                "default_if_null": self._operand_default_trace_value(operand),
                "default_applied": False,
                "evaluated": False,
            }
        if isinstance(operand, LiteralOperand):
            return {
                "kind": operand.kind.value,
                "columns": [],
                "value": self._trace_value(operand.value),
                "value_type": operand.value_type,
                "original_value": self._trace_value(operand.value),
                "default_if_null": self._operand_default_trace_value(operand),
                "default_applied": False,
                "evaluated": False,
            }
        if isinstance(operand, CustomFunctionOperand):
            return {
                "kind": operand.kind.value,
                "columns": self._operand_columns(operand),
                "function_name": operand.function_name,
                "args": {
                    str(key): (
                        self._operand_metadata(value)
                        if isinstance(value, Operand)
                        else {
                            "kind": "literal",
                            "columns": [],
                            "value": self._trace_value(value),
                            "evaluated": False,
                        }
                    )
                    for key, value in operand.args.items()
                },
                "default_if_null": self._operand_default_trace_value(operand),
                "default_applied": False,
                "evaluated": False,
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _operand_default_trace_value(self, operand: Operand) -> Any:
        """Return one configured fallback as a trace-safe value."""
        return (
            self._trace_value(operand.default_if_null.value)
            if operand.default_if_null is not None
            else None
        )

    def _operand_columns(self, operand: Operand | Any) -> list[str]:
        """
        Return all source columns referenced by an operand tree.
        """
        if isinstance(operand, FieldOperand):
            return [operand.field_name]
        if isinstance(operand, AssignedOperand):
            return []
        if isinstance(operand, LiteralOperand):
            return []
        if isinstance(operand, CustomFunctionOperand):
            return self._unique_strings(
                column
                for value in operand.args.values()
                for column in self._operand_columns(value)
            )
        return []

    def _unique_strings(self, values: Iterable[str]) -> list[str]:
        """
        Preserve first-seen order while removing duplicate strings.
        """
        return list(dict.fromkeys(str(value) for value in values))

    def _trace_value(self, value: Any) -> Any:
        """
        Convert a runtime value into a JSON-safe trace value.
        """
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Mapping):
            return {
                str(key): self._trace_value(item)
                for key, item in value.items()
            }
        if isinstance(value, tuple):
            return [self._trace_value(item) for item in value]
        if isinstance(value, list):
            return [self._trace_value(item) for item in value]
        if isinstance(value, set):
            return [
                self._trace_value(item)
                for item in sorted(value, key=lambda item: repr(item))
            ]
        try:
            json_dumps(value)
        except TypeError:
            return str(value)
        return value

    def _compare_values(
        self,
        left: Any,
        operator: ComparisonOperator,
        right: Any,
        tolerance_abs: Decimal,
        error_on_null: bool,
    ) -> bool | None:
        """
        Apply one comparison operator after operand-level null defaults.
        """
        if operator is ComparisonOperator.IS_NULL:
            return left is None
        if operator is ComparisonOperator.IS_NOT_NULL:
            return left is not None

        if left is None or right is None:
            if error_on_null:
                raise ValueError("Null operand encountered with error_on_null=true.")
            return None

        if operator is ComparisonOperator.EQ:
            return self._equals(left, right, tolerance_abs)
        if operator is ComparisonOperator.NE:
            return not self._equals(left, right, tolerance_abs)
        if operator in {
            ComparisonOperator.GT,
            ComparisonOperator.GE,
            ComparisonOperator.LT,
            ComparisonOperator.LE,
        }:
            return self._compare_ordered(
                left,
                operator,
                right,
                tolerance_abs,
            )
        if operator is ComparisonOperator.IN:
            return self._contains(left, right, tolerance_abs)
        if operator is ComparisonOperator.NOT_IN:
            return not self._contains(left, right, tolerance_abs)
        if operator is ComparisonOperator.BETWEEN:
            return self._between(left, right, tolerance_abs)
        if operator is ComparisonOperator.NOT_BETWEEN:
            return not self._between(left, right, tolerance_abs)
        if operator is ComparisonOperator.CONTAINS:
            return str(right) in str(left)
        if operator is ComparisonOperator.NOT_CONTAINS:
            return str(right) not in str(left)
        if operator is ComparisonOperator.STARTS_WITH:
            return str(left).startswith(str(right))
        if operator is ComparisonOperator.ENDS_WITH:
            return str(left).endswith(str(right))
        if operator is ComparisonOperator.LIKE:
            return self._sql_like(str(left), str(right))
        if operator is ComparisonOperator.NOT_LIKE:
            return not self._sql_like(str(left), str(right))
        raise ValueError(f"Unsupported comparison operator at runtime: {operator.value}")

    def _contains(
        self,
        left: Any,
        right: Any,
        tolerance_abs: Decimal,
    ) -> bool:
        """Apply equality semantics consistently to membership operators."""
        if isinstance(right, (str, bytes, Mapping)) or not isinstance(
            right,
            Iterable,
        ):
            raise TypeError(
                "Operators in/not_in require a collection-valued right operand. "
                "Use contains/not_contains for substring checks."
            )
        numeric_left = self._numeric_decimal_or_none(left)
        if numeric_left is not None:
            for item in right:
                numeric_item = self._numeric_decimal_or_none(item)
                if numeric_item is not None:
                    if abs(numeric_left - numeric_item) <= tolerance_abs:
                        return True
                elif self._equals(left, item, tolerance_abs):
                    return True
            return False
        if self._is_temporal(left):
            for item in right:
                temporal_left, temporal_item = self._temporal_pair(
                    left,
                    item,
                    tolerance_abs,
                )
                if temporal_left == temporal_item:
                    return True
            return False
        return any(self._equals(left, item, tolerance_abs) for item in right)

    def _between(
        self,
        left: Any,
        right: Any,
        tolerance_abs: Decimal,
    ) -> bool:
        """Return whether ``left`` falls within the inclusive bound pair."""
        lower, upper = right
        ordered_lower, ordered_left = self._ordered_pair(
            lower,
            left,
            tolerance_abs,
        )
        ordered_left_again, ordered_upper = self._ordered_pair(
            left,
            upper,
            tolerance_abs,
        )
        return ordered_lower <= ordered_left and ordered_left_again <= ordered_upper

    def _equals(self, left: Any, right: Any, tolerance_abs: Decimal) -> bool:
        """
        Compare equality, applying absolute tolerance for numeric values.
        """
        if self._is_numeric(left) and self._is_numeric(right):
            return abs(self._decimal(left) - self._decimal(right)) <= tolerance_abs
        if self._is_temporal(left) or self._is_temporal(right):
            temporal_left, temporal_right = self._temporal_pair(
                left,
                right,
                tolerance_abs,
            )
            return temporal_left == temporal_right
        return left == right

    def _ordered_pair(
        self,
        left: Any,
        right: Any,
        tolerance_abs: Decimal,
    ) -> tuple[Any, Any]:
        """Return a compatible temporal pair or Decimal numeric pair."""
        if self._is_temporal(left) or self._is_temporal(right):
            return self._temporal_pair(left, right, tolerance_abs)
        return self._decimal(left), self._decimal(right)

    def _compare_ordered(
        self,
        left: Any,
        operator: ComparisonOperator,
        right: Any,
        tolerance_abs: Decimal,
    ) -> bool:
        """Apply one ordered comparison to numeric or temporal operands."""
        ordered_left, ordered_right = self._ordered_pair(
            left,
            right,
            tolerance_abs,
        )
        tolerance = self._ordered_tolerance(ordered_left, tolerance_abs)
        if operator is ComparisonOperator.GT:
            return ordered_left > ordered_right + tolerance
        if operator is ComparisonOperator.GE:
            return ordered_left >= ordered_right - tolerance
        if operator is ComparisonOperator.LT:
            return ordered_left < ordered_right - tolerance
        if operator is ComparisonOperator.LE:
            return ordered_left <= ordered_right + tolerance
        raise ValueError(f"Unsupported ordered comparison: {operator.value}")

    def _ordered_tolerance(
        self,
        value: Any,
        tolerance_abs: Decimal,
    ) -> Decimal | timedelta:
        """Return numeric tolerance; temporal comparisons require zero."""
        return timedelta(0) if self._is_temporal(value) else tolerance_abs

    def _temporal_pair(
        self,
        left: Any,
        right: Any,
        tolerance_abs: Decimal,
    ) -> tuple[date | datetime, date | datetime]:
        """Validate a lossless date or timestamp comparison pair."""
        if tolerance_abs != Decimal(0):
            raise ValueError("Date and timestamp comparisons require tolerance_abs=0.")
        left_kind = self._temporal_kind(left)
        right_kind = self._temporal_kind(right)
        if left_kind is None or right_kind is None or left_kind != right_kind:
            raise TypeError(
                "Date comparisons require two dates and timestamp comparisons "
                "require two timestamps. Use to_date for explicit conversion."
            )
        if left_kind == "timestamp":
            left_aware = left.utcoffset() is not None
            right_aware = right.utcoffset() is not None
            if left_aware != right_aware:
                raise TypeError(
                    "Timestamp comparisons cannot mix timezone-aware and naive values."
                )
        return left, right

    def _is_temporal(self, value: Any) -> bool:
        """Return whether a value is a date or timestamp."""
        return self._temporal_kind(value) is not None

    def _temporal_kind(self, value: Any) -> str | None:
        """Return the strict temporal kind, accounting for datetime subclassing date."""
        if isinstance(value, datetime):
            return "timestamp"
        if isinstance(value, date):
            return "date"
        return None

    def _is_numeric(self, value: Any) -> bool:
        """
        Return whether a value can be safely treated as a non-boolean number.
        """
        return self._numeric_decimal_or_none(value) is not None

    def _numeric_decimal_or_none(self, value: Any) -> Decimal | None:
        """Return a finite numeric value, or ``None`` for non-numeric input."""
        if isinstance(value, bool):
            return None
        try:
            converted = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if not converted.is_finite():
            raise ValueError("Numeric comparison values must be finite.")
        return converted

    def _decimal(self, value: Any) -> Decimal:
        """
        Convert a runtime value to ``Decimal`` for numeric comparison.
        """
        converted = Decimal(str(value))
        if not converted.is_finite():
            raise ValueError("Numeric comparison values must be finite.")
        return converted

    def _sql_like(self, value: str, pattern: str) -> bool:
        """
        Match SQL LIKE patterns using ``%`` and ``_`` wildcards.
        """
        regex_parts: list[str] = []
        for character in pattern:
            if character == "%":
                regex_parts.append(".*")
            elif character == "_":
                regex_parts.append(".")
            else:
                regex_parts.append(re.escape(character))
        return re.fullmatch("".join(regex_parts), value, flags=re.DOTALL) is not None
