"""
Worker-side row evaluation helpers for the Spark runtime.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from json import dumps as json_dumps
from typing import Any

from rules_engine.decimal_math import subtract_exact
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
from rules_engine.registry import CustomFunction, FunctionRegistry
from rules_engine.traversal import iter_conditions, iter_nested_operands, iter_rules


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


@dataclass
class _FunctionBinding:
    """Static arguments and callable resolved for one custom-function operand."""

    operand: CustomFunctionOperand
    bound_args: Mapping[str, Any]
    implementation: CustomFunction | None


@dataclass(frozen=True)
class _PreparedRuleset:
    """Ruleset metadata reused across repeated row evaluations."""

    source: Ruleset
    active_rules: tuple[Rule, ...]
    assignment_targets: tuple[str, ...]


@dataclass(frozen=True)
class RowExecutionResult:
    """Business outcomes produced by the shared row execution loop."""

    matched_rule_ids: list[str]
    assignments: dict[str, Any]


class SparkRowEvaluator:
    """
    Row-level evaluator reused inside Spark worker UDFs.
    """

    def __init__(
        self,
        function_registry: FunctionRegistry,
    ) -> None:
        """
        Create a runtime bound to a custom-function registry.
        """
        self._function_registry = function_registry
        self._rule_formatter = HumanReadableRulesetFormatter()
        self._prepared_ruleset: _PreparedRuleset | None = None
        self._prepared_source: Ruleset | None = None
        self._function_bindings: list[_FunctionBinding] = []
        self._function_bindings_by_id: dict[int, _FunctionBinding] | None = {}

    def __getstate__(self) -> dict[str, Any]:
        """Rebuild process-local operand identity indexes after every pickle."""
        state = dict(self.__dict__)
        state["_function_bindings_by_id"] = None
        return state

    def evaluate_row(
        self,
        ruleset: Ruleset,
        row: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Evaluate one Python mapping using production rule semantics.

        This allocation-light API exposes the same stable business results as
        the Spark worker, independent of Spark schemas and audit rendering.
        """
        prepared = self._prepare_ruleset(ruleset)
        execution = self._execute_prepared(prepared, row)
        return {
            "matched": bool(execution.matched_rule_ids),
            "matched_rule_ids": execution.matched_rule_ids,
            "assign": {
                target_field: {
                    "applied": target_field in execution.assignments,
                    "value": execution.assignments.get(target_field),
                }
                for target_field in prepared.assignment_targets
            },
        }

    def _execute_prepared(
        self,
        prepared: _PreparedRuleset,
        row: Mapping[str, Any],
        *,
        full_audit: bool = False,
        normalize_assignment: Callable[[Assignment, Any], Any] | None = None,
        on_rule_matched: Callable[[Rule, list[ResolvedConditionTrace]], None] | None = None,
        on_assignment: Callable[[Rule, Assignment, Any, Any], None] | None = None,
    ) -> RowExecutionResult:
        """Evaluate ordered rules and commit each matched rule atomically.

        Adapters may normalize proposed values and observe completed work. Rule
        matching, assignment visibility, and stop handling have one owner. The
        compact path does not construct condition traces or audit events.
        """
        matched_rule_ids: list[str] = []
        assignments: dict[str, Any] = {}
        assigned_values: dict[str, AssignedValue] = {}
        for rule in prepared.active_rules:
            if full_audit:
                matched, condition_traces = self._evaluate_rule(rule, row, assigned_values)
            else:
                matched = self._rule_matches(rule, row, assigned_values)
                condition_traces = None
            if not matched:
                continue
            matched_rule_ids.append(rule.rule_id)
            if on_rule_matched is not None:
                on_rule_matched(rule, condition_traces or [])
            resolved_assignments: list[tuple[Assignment, Any]] = []
            for assignment in rule.assignments:
                value = self._copy_collection(
                    self._resolve_operand(assignment.value, row, assigned_values)
                )
                if normalize_assignment is not None:
                    value = normalize_assignment(assignment, value)
                resolved_assignments.append((assignment, value))
            if on_assignment is not None:
                for assignment, value in resolved_assignments:
                    previous = assigned_values.get(assignment.target_field)
                    old_value = (
                        previous.value if previous is not None else row.get(assignment.target_field)
                    )
                    on_assignment(rule, assignment, old_value, value)
            for assignment, value in resolved_assignments:
                assignments[assignment.target_field] = value
                assigned_values[assignment.target_field] = AssignedValue(
                    value=value,
                    rule_id=rule.rule_id,
                    assignment_id=assignment.assignment_id,
                )
            if rule.stop_on_match:
                break
        return RowExecutionResult(matched_rule_ids, assignments)

    def _prepare_ruleset(self, ruleset: Ruleset) -> _PreparedRuleset:
        """Bind static function metadata and cache ordered active rules."""
        prepared = self._prepared_ruleset
        if prepared is not None and self._prepared_source is ruleset:
            return prepared
        self._prepared_ruleset = None
        self._prepared_source = None
        self._function_bindings = []
        self._function_bindings_by_id = {}
        snapshot = deepcopy(ruleset)
        active_rules = tuple(iter_rules(snapshot, active_only=True, ordered=True))
        for rule in active_rules:
            self._prepare_rule(rule)
        prepared = _PreparedRuleset(
            source=snapshot,
            active_rules=active_rules,
            assignment_targets=tuple(
                dict.fromkeys(
                    assignment.target_field
                    for rule in active_rules
                    for assignment in rule.assignments
                )
            ),
        )
        self._prepared_ruleset = prepared
        self._prepared_source = ruleset
        return prepared

    def _prepare_rule(self, rule: Rule) -> None:
        """Bind static custom-function metadata in one active rule."""
        self._prepare_group(rule.root_group)
        for assignment in rule.assignments:
            self._prepare_operand(assignment.value)

    def _prepare_group(self, group: ConditionGroup) -> None:
        """Bind static custom-function metadata in a condition-group tree."""
        for condition in iter_conditions(group):
            self._prepare_operand(
                condition.left,
                resolve_implementation=condition.active_flag,
            )
            if condition.right is not None:
                self._prepare_operand(
                    condition.right,
                    resolve_implementation=condition.active_flag,
                )

    def _prepare_operand(
        self,
        operand: Operand,
        *,
        resolve_implementation: bool = True,
    ) -> None:
        """Bind one custom-function operand and any nested function operands."""
        if not isinstance(operand, CustomFunctionOperand):
            return
        bindings_by_id = self._function_bindings_by_id
        if bindings_by_id is None:
            bindings_by_id = {id(binding.operand): binding for binding in self._function_bindings}
            self._function_bindings_by_id = bindings_by_id
        existing = bindings_by_id.get(id(operand))
        if existing is not None and existing.operand is operand:
            if resolve_implementation and existing.implementation is None:
                existing.implementation = self._function_registry.get_implementation(
                    operand.function_name
                )
            return
        spec = self._function_registry.get_spec(operand.function_name)
        binding = _FunctionBinding(
            operand=operand,
            bound_args=spec.bind_args(operand.args),
            implementation=(
                self._function_registry.get_implementation(operand.function_name)
                if resolve_implementation
                else None
            ),
        )
        self._function_bindings.append(binding)
        bindings_by_id[id(operand)] = binding
        for value in binding.bound_args.values():
            for nested_operand in iter_nested_operands(value):
                self._prepare_operand(
                    nested_operand,
                    resolve_implementation=resolve_implementation,
                )

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
                    self._operand_metadata(condition.right) if condition.right is not None else None
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
            assignments_applied=tuple(assignment.target_field for assignment in rule.assignments),
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
        passed_condition_ids = {trace.condition_id for trace in condition_traces if trace.passed}
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
            column = operand.get("field_name")
            return f"{column}={self._trace_display_value(operand.get('value'))}"
        if kind == OperandKind.ASSIGNED.value:
            target = operand.get("target_field")
            return f"assigned({target})={self._trace_display_value(operand.get('value'))}"
        if kind == OperandKind.LITERAL.value:
            return self._trace_display_value(operand.get("value"))
        if kind == OperandKind.CUSTOM_FUNCTION.value:
            args = operand.get("args") or {}
            arg_text = ", ".join(
                f"{name}={self._operand_trace_summary(value)}" for name, value in args.items()
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
                str(key): self._resolve_function_argument(
                    value,
                    row,
                    assigned_values,
                )
                for key, value in self._bound_function_args(operand).items()
            }
            value = self._function_implementation(operand)(**args)
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
            original_value = assigned_value.value if assigned_value is not None else None
            trace = {
                "kind": operand.kind.value,
                "columns": [],
                "target_field": operand.target_field,
                "produced_by_rule_id": (
                    assigned_value.rule_id if assigned_value is not None else None
                ),
                "produced_by_assignment_id": (
                    assigned_value.assignment_id if assigned_value is not None else None
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
            for key, value in self._bound_function_args(operand).items():
                arg_key = str(key)
                argument = self._resolve_function_argument_resolution(
                    value,
                    row,
                    assigned_values,
                )
                args[arg_key] = argument.value
                arg_traces[arg_key] = argument.trace
            original_value = self._function_implementation(operand)(**args)
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
                    str(key): self._function_argument_metadata(value)
                    for key, value in self._bound_function_args(operand).items()
                },
                "default_if_null": self._operand_default_trace_value(operand),
                "default_applied": False,
                "evaluated": False,
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _bound_function_args(self, operand: CustomFunctionOperand) -> Mapping[str, Any]:
        """Return driver-bound arguments, binding raw operands on demand."""
        return self._function_binding(operand).bound_args

    def _function_implementation(self, operand: CustomFunctionOperand) -> CustomFunction:
        """Return a driver-bound implementation, resolving raw operands on demand."""
        binding = self._function_binding(operand)
        if binding.implementation is not None:
            return binding.implementation
        return self._function_registry.get_implementation(operand.function_name)

    def _function_binding(self, operand: CustomFunctionOperand) -> _FunctionBinding:
        """Return the identity-bound runtime metadata for one operand."""
        bindings_by_id = self._function_bindings_by_id
        if bindings_by_id is None:
            bindings_by_id = {id(binding.operand): binding for binding in self._function_bindings}
            self._function_bindings_by_id = bindings_by_id
        binding = bindings_by_id.get(id(operand))
        if binding is not None and binding.operand is operand:
            return binding
        self._prepare_operand(operand)
        refreshed_bindings = self._function_bindings_by_id
        assert refreshed_bindings is not None
        return refreshed_bindings[id(operand)]

    def _resolve_function_argument(
        self,
        value: Any,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None,
    ) -> Any:
        """Resolve operands recursively inside one function argument."""
        if isinstance(value, Operand):
            resolved = self._resolve_operand(value, row, assigned_values)
            return self._copy_collection(resolved)
        if isinstance(value, Mapping):
            return {
                str(key): self._resolve_function_argument(
                    item,
                    row,
                    assigned_values,
                )
                for key, item in value.items()
            }
        if isinstance(value, tuple):
            return tuple(
                self._resolve_function_argument(item, row, assigned_values) for item in value
            )
        if isinstance(value, list):
            return [self._resolve_function_argument(item, row, assigned_values) for item in value]
        if isinstance(value, set):
            return {self._resolve_function_argument(item, row, assigned_values) for item in value}
        return value

    def _resolve_function_argument_resolution(
        self,
        value: Any,
        row: Mapping[str, Any],
        assigned_values: Mapping[str, AssignedValue] | None,
    ) -> OperandResolution:
        """Resolve a nested argument and retain its source-column metadata."""
        if isinstance(value, Operand):
            resolution = self._resolve_operand_resolution(value, row, assigned_values)
            return OperandResolution(
                value=self._copy_collection(resolution.value),
                trace=resolution.trace,
            )
        resolved = self._resolve_function_argument(value, row, assigned_values)
        return OperandResolution(
            value=resolved,
            trace={
                "kind": "literal",
                "columns": self._operand_columns(value),
                "value": self._trace_value(resolved),
                "evaluated": True,
            },
        )

    def _copy_collection(self, value: Any) -> Any:
        """Isolate callable arguments and assignment snapshots, keeping scalars cheap.

        A literal can reach a callable through a prior assignment as well as a
        direct argument. Copying at both boundaries prevents mutation of prepared
        metadata, original row values, or an earlier committed assignment.
        """
        if isinstance(value, (Mapping, list, tuple, set)):
            return deepcopy(value)
        return value

    def _function_argument_metadata(self, value: Any) -> dict[str, Any]:
        """Return trace metadata for a possibly nested function argument."""
        if isinstance(value, Operand):
            return self._operand_metadata(value)
        return {
            "kind": "literal",
            "columns": self._operand_columns(value),
            "value": self._trace_value(value),
            "evaluated": False,
        }

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
                column for value in operand.args.values() for column in self._operand_columns(value)
            )
        if isinstance(operand, Mapping):
            return self._unique_strings(
                column for value in operand.values() for column in self._operand_columns(value)
            )
        if isinstance(operand, (list, tuple, set)):
            values = sorted(operand, key=repr) if isinstance(operand, set) else operand
            return self._unique_strings(
                column for value in values for column in self._operand_columns(value)
            )
        return []

    def _unique_strings(self, values: Iterable[str]) -> list[str]:
        """
        Preserve first-seen order while removing duplicate strings.
        """
        return list(dict.fromkeys(str(value) for value in values))

    def _trace_value(self, value: Any, seen: set[int] | None = None) -> Any:
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
        if isinstance(value, (Mapping, tuple, list, set)):
            seen = set() if seen is None else seen
            identity = id(value)
            if identity in seen:
                return "<recursive>"
            seen.add(identity)
            try:
                if isinstance(value, Mapping):
                    normalized_keys = [str(key) for key in value]
                    if len(normalized_keys) != len(set(normalized_keys)):
                        return repr(value)
                    return {str(key): self._trace_value(item, seen) for key, item in value.items()}
                items = sorted(value, key=repr) if isinstance(value, set) else value
                return [self._trace_value(item, seen) for item in items]
            finally:
                seen.remove(identity)
        try:
            json_dumps(value)
        except (TypeError, ValueError):
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
        return any(self._equals(left, item, tolerance_abs) for item in right)

    def _between(
        self,
        left: Any,
        right: Any,
        tolerance_abs: Decimal,
    ) -> bool:
        """Return whether ``left`` falls within the inclusive bound pair."""
        if not isinstance(right, (list, tuple)) or len(right) != 2:
            raise TypeError(
                "Operators between/not_between require a two-item list or tuple "
                f"right operand, found {right!r}."
            )
        lower, upper = right
        ordered_lower, ordered_left = self._ordered_pair(
            lower,
            left,
            tolerance_abs,
        )
        ordered_left_for_upper, ordered_upper = self._ordered_pair(
            left,
            upper,
            tolerance_abs,
        )
        return ordered_lower <= ordered_left and ordered_left_for_upper <= ordered_upper

    def _equals(self, left: Any, right: Any, tolerance_abs: Decimal) -> bool:
        """
        Compare equality, applying absolute tolerance for numeric values.
        """
        if self._has_numeric_runtime_type(left) or self._has_numeric_runtime_type(right):
            numeric_left = self._numeric_decimal_or_none(left)
            numeric_right = self._numeric_decimal_or_none(right)
            if numeric_left is not None and numeric_right is not None:
                if tolerance_abs == 0:
                    return numeric_left == numeric_right
                return subtract_exact(numeric_left, numeric_right).copy_abs() <= tolerance_abs
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
        comparison_left = ordered_left
        upper_bound = lower_bound = ordered_right
        if tolerance_abs != 0:
            comparison_left = subtract_exact(ordered_left, ordered_right)
            upper_bound = tolerance_abs
            lower_bound = tolerance_abs.copy_negate()
        if operator is ComparisonOperator.GT:
            return comparison_left > upper_bound
        if operator is ComparisonOperator.GE:
            return comparison_left >= lower_bound
        if operator is ComparisonOperator.LT:
            return comparison_left < lower_bound
        if operator is ComparisonOperator.LE:
            return comparison_left <= upper_bound
        raise ValueError(f"Unsupported ordered comparison: {operator.value}")

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
                raise TypeError("Timestamp comparisons cannot mix timezone-aware and naive values.")
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

    def _has_numeric_runtime_type(self, value: Any) -> bool:
        """Return whether Spark supplies ``value`` as a concrete numeric type."""
        return not isinstance(value, bool) and isinstance(value, (int, float, Decimal))

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
        try:
            converted = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                "Ordered comparison requires numeric or matching temporal operands; "
                f"found {value!r}."
            ) from exc
        if not converted.is_finite():
            raise ValueError(f"Numeric comparison values must be finite; found {value!r}.")
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
