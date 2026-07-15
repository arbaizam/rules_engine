"""
Worker-side row evaluation helpers for the Spark runtime.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import re
from typing import Any, Iterable, Mapping

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
    OperandKind,
)
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import (
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


class SparkRowEvaluator:
    """
    Row-level evaluator reused inside Spark worker UDFs.
    """

    def __init__(self, repository: RulesetRepository, function_registry: FunctionRegistry) -> None:
        """
        Create a runtime bound to metadata and custom-function registries.
        """
        self._repository = repository
        self._function_registry = function_registry
        self._rule_formatter = HumanReadableRulesetFormatter()

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        return self._repository.load_published(ruleset_name, version)

    def _evaluate_rule(
        self,
        rule: Rule,
        row: Mapping[str, Any],
    ) -> tuple[bool, list[ResolvedConditionTrace]]:
        """
        Evaluate one rule against one row and collect condition traces.
        """
        condition_traces: list[ResolvedConditionTrace] = []
        matched = self._evaluate_group(
            rule.root_group,
            row,
            condition_traces,
        )
        return matched, condition_traces

    def _evaluate_group(
        self,
        group: ConditionGroup,
        row: Mapping[str, Any],
        condition_traces: list[ResolvedConditionTrace],
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
            )
            condition_traces.append(condition_trace)
            results.append(condition_trace.passed)
        for nested_group in group.groups:
            results.append(
                self._evaluate_group(
                    nested_group,
                    row,
                    condition_traces,
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
        left = self._resolve_operand_resolution(condition.left, row)
        right = (
            self._resolve_operand_resolution(condition.right, row)
            if condition.right is not None
            else None
        )
        result = self._compare_values(
            left.value,
            condition.operator,
            right.value if right is not None else None,
            condition.tolerance_abs,
            condition.null_input_mode,
        )
        passed = self._resolve_null_result(
            result,
            condition.null_result_mode,
            condition.null_default_value,
        )
        return self._condition_trace(
            condition=condition,
            group=group,
            passed=passed,
            left=left.trace,
            right=right.trace if right is not None else None,
            comparison_result=result,
        )

    def _evaluate_assignments(
        self,
        assignments: tuple[Assignment, ...],
        row: Mapping[str, Any],
    ) -> dict[str, Any]:
        """
        Resolve all assignments for a matched rule into output values.
        """
        return {
            assignment.target_field: self._resolve_operand(
                assignment.value,
                row,
            )
            for assignment in assignments
        }

    def _rule_execution_trace(
        self,
        rule: Rule,
        matched: bool,
        condition_traces: list[ResolvedConditionTrace],
    ) -> RuleExecutionTrace:
        """
        Build the canonical trace for one evaluated rule.
        """
        return RuleExecutionTrace(
            rule_id=rule.rule_id,
            condition_traces=tuple(condition_traces),
            assignments_applied=(
                tuple(assignment.target_field for assignment in rule.assignments)
                if matched
                else ()
            ),
            matched=matched,
            rule_name=rule.rule_name,
            rule_order=rule.rule_order,
        )

    def _winning_rule_explanation_from_trace(
        self,
        rule: Rule,
        condition_traces: list[ResolvedConditionTrace],
    ) -> str | None:
        """
        Return a readable winning-rule explanation that preserves group logic.
        """
        passed_condition_ids = {
            trace.condition_id
            for trace in condition_traces
            if trace.passed
        }
        return self._rule_formatter.format_winning_rule_explanation(
            rule,
            passed_condition_ids,
        )

    def _operand_trace_summary(self, operand: Any) -> str | None:
        """
        Return a compact resolved-value summary for winning-rule trace arguments.
        """
        if not isinstance(operand, MappingABC):
            return None
        kind = operand.get("kind")
        if kind == OperandKind.FIELD.value:
            column = operand.get("column") or operand.get("field_name")
            return f"{column}={self._trace_display_value(operand.get('value'))}"
        if kind == OperandKind.LITERAL.value:
            return self._trace_display_value(operand.get("value"))
        if kind == OperandKind.CUSTOM_FUNCTION.value:
            args = operand.get("args")
            if isinstance(args, MappingABC):
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
            null_input_mode=condition.null_input_mode.value,
            null_result_mode=condition.null_result_mode.value,
            null_default_value=self._trace_value(condition.null_default_value),
            left=left,
            right=right,
            comparison_result=comparison_result,
            passed=passed,
        )

    def _resolve_operand(
        self,
        operand: Operand,
        row: Mapping[str, Any],
    ) -> Any:
        """
        Resolve one operand against the current row.
        """
        return self._resolve_operand_resolution(operand, row).value

    def _resolve_operand_resolution(
        self,
        operand: Operand,
        row: Mapping[str, Any],
    ) -> OperandResolution:
        """
        Resolve one operand and return both the value and trace metadata.
        """
        if isinstance(operand, FieldOperand):
            value = row.get(operand.field_name)
            return OperandResolution(
                value=value,
                trace={
                    "kind": operand.kind.value,
                    "columns": [operand.field_name],
                    "field_name": operand.field_name,
                    "value": self._trace_value(value),
                    "evaluated": True,
                },
            )
        if isinstance(operand, LiteralOperand):
            return OperandResolution(
                value=operand.value,
                trace={
                    "kind": operand.kind.value,
                    "columns": [],
                    "value": self._trace_value(operand.value),
                    "value_type": operand.value_type,
                    "evaluated": True,
                },
            )
        if isinstance(operand, CustomFunctionOperand):
            args: dict[str, Any] = {}
            arg_traces: dict[str, Any] = {}
            for key, value in operand.args.items():
                arg_key = str(key)
                if isinstance(value, (FieldOperand, LiteralOperand, CustomFunctionOperand)):
                    argument = self._resolve_operand_resolution(value, row)
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
            value = implementation(**args)
            return OperandResolution(
                value=value,
                trace={
                    "kind": operand.kind.value,
                    "columns": self._unique_strings(
                        column
                        for arg_trace in arg_traces.values()
                        for column in arg_trace.get("columns", [])
                    ),
                    "function_name": operand.function_name,
                    "args": arg_traces,
                    "value": self._trace_value(value),
                    "evaluated": True,
                },
            )
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _operand_metadata(self, operand: Operand) -> dict[str, Any]:
        """
        Return operand metadata without resolving row-dependent values.
        """
        if isinstance(operand, FieldOperand):
            return {
                "kind": operand.kind.value,
                "columns": [operand.field_name],
                "field_name": operand.field_name,
                "evaluated": False,
            }
        if isinstance(operand, LiteralOperand):
            return {
                "kind": operand.kind.value,
                "columns": [],
                "value": self._trace_value(operand.value),
                "value_type": operand.value_type,
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
                        if isinstance(value, (FieldOperand, LiteralOperand, CustomFunctionOperand))
                        else {
                            "kind": "literal",
                            "columns": [],
                            "value": self._trace_value(value),
                            "evaluated": False,
                        }
                    )
                    for key, value in operand.args.items()
                },
                "evaluated": False,
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _operand_columns(self, operand: Operand | Any) -> list[str]:
        """
        Return all source columns referenced by an operand tree.
        """
        if isinstance(operand, FieldOperand):
            return [operand.field_name]
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
        if isinstance(value, MappingABC):
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
            json.dumps(value)
        except TypeError:
            return str(value)
        return value

    def _compare_values(
        self,
        left: Any,
        operator: ComparisonOperator,
        right: Any,
        tolerance_abs: Decimal,
        null_input_mode: NullInputMode,
    ) -> bool | None:
        """
        Apply one comparison operator with null-input and tolerance handling.
        """
        if operator is ComparisonOperator.IS_NULL:
            return left is None
        if operator is ComparisonOperator.IS_NOT_NULL:
            return left is not None

        left, right, null_propagated = self._apply_null_input_mode(
            left,
            right,
            null_input_mode,
        )
        if null_propagated:
            return None

        if operator is ComparisonOperator.EQ:
            return self._equals(left, right, tolerance_abs)
        if operator is ComparisonOperator.NE:
            return not self._equals(left, right, tolerance_abs)
        if operator is ComparisonOperator.GT:
            return self._decimal(left) > self._decimal(right) + tolerance_abs
        if operator is ComparisonOperator.GE:
            return self._decimal(left) >= self._decimal(right) - tolerance_abs
        if operator is ComparisonOperator.LT:
            return self._decimal(left) < self._decimal(right) - tolerance_abs
        if operator is ComparisonOperator.LE:
            return self._decimal(left) <= self._decimal(right) + tolerance_abs
        if operator is ComparisonOperator.IN:
            return left in right
        if operator is ComparisonOperator.NOT_IN:
            return left not in right
        if operator is ComparisonOperator.BETWEEN:
            lower, upper = right
            return self._decimal(lower) <= self._decimal(left) <= self._decimal(upper)
        if operator is ComparisonOperator.NOT_BETWEEN:
            lower, upper = right
            return not (self._decimal(lower) <= self._decimal(left) <= self._decimal(upper))
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

    def _apply_null_input_mode(
        self,
        left: Any,
        right: Any,
        null_input_mode: NullInputMode,
    ) -> tuple[Any, Any, bool]:
        """
        Apply configured null-input handling before comparison.
        """
        if left is not None and right is not None:
            return left, right, False
        if null_input_mode is NullInputMode.ERROR:
            raise ValueError("Null input encountered with null_input_mode=error.")
        if null_input_mode is NullInputMode.ZERO:
            return 0 if left is None else left, 0 if right is None else right, False
        return left, right, True

    def _resolve_null_result(
        self,
        result: bool | None,
        null_result_mode: NullResultMode,
        null_default_value: Any | None,
    ) -> bool:
        """
        Convert a nullable comparison result into a final boolean result.
        """
        if result is not None:
            return bool(result)
        if null_result_mode is NullResultMode.ERROR:
            raise ValueError("Null result encountered with null_result_mode=error.")
        if null_result_mode is NullResultMode.DEFAULT:
            return bool(null_default_value)
        return False

    def _equals(self, left: Any, right: Any, tolerance_abs: Decimal) -> bool:
        """
        Compare equality, applying absolute tolerance for numeric values.
        """
        if self._is_numeric(left) and self._is_numeric(right):
            return abs(self._decimal(left) - self._decimal(right)) <= tolerance_abs
        return left == right

    def _is_numeric(self, value: Any) -> bool:
        """
        Return whether a value can be safely treated as a non-boolean number.
        """
        try:
            Decimal(str(value))
        except Exception:
            return False
        return not isinstance(value, bool)

    def _decimal(self, value: Any) -> Decimal:
        """
        Convert a runtime value to ``Decimal`` for numeric comparison.
        """
        return Decimal(str(value))

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
