"""
Runtime facade and pure-Python evaluator.

The evaluator operates on the incoming row set exactly as provided. It does
not deduplicate, reshape, filter globally, or retain cross-run state.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import logging
import re
from statistics import mean, median, pstdev, pvariance
from typing import Any, Iterable, Mapping

from rules_engine.aggregate_key import aggregate_key
from rules_engine.enums import (
    AggregateFunction,
    AggregateScope,
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
    OperandKind,
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
    ResolvedConditionTrace,
    RowFilterPredicate,
    Rule,
    RuleExecutionTrace,
    Ruleset,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OperandResolution:
    """Resolved operand value plus trace-safe metadata."""

    value: Any
    trace: dict[str, Any]


class RulesEngineRuntime:
    """
    Runtime facade for published ruleset metadata.
    """

    def __init__(self, repository: RulesetRepository, function_registry: FunctionRegistry) -> None:
        """
        Create a runtime bound to metadata and custom-function registries.
        """
        self._repository = repository
        self._function_registry = function_registry

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        return self._repository.load_published(ruleset_name, version)

    def evaluate(
        self,
        rows: Iterable[Mapping[str, Any]],
        ruleset: Ruleset,
    ) -> tuple[list[dict[str, Any]], list[RuleExecutionTrace]]:
        """
        Evaluate rows against a published ruleset.

        Parameters
        ----------
        rows : Iterable[Mapping[str, Any]]
            Incoming row set. Aggregates operate on this materialized row set
            exactly as supplied.
        ruleset : Ruleset
            Ruleset metadata to evaluate.

        Returns
        -------
        tuple[list[dict[str, Any]], list[RuleExecutionTrace]]
            Output rows and flattened rule execution traces.
        """
        materialized_rows = [dict(row) for row in rows]
        logger.info(
            "Evaluating ruleset in Python runtime: ruleset_id=%s ruleset_name=%s version=%s row_count=%s rule_count=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            len(materialized_rows),
            len(ruleset.rules),
        )
        aggregate_cache = AggregateContext(materialized_rows, self)
        output_rows: list[dict[str, Any]] = []
        traces: list[RuleExecutionTrace] = []

        active_rules = sorted(
            (rule for rule in ruleset.rules if rule.active_flag),
            key=lambda item: item.rule_order,
        )
        for row_index, row in enumerate(materialized_rows):
            matched_rule_ids: list[str] = []
            assignments: dict[str, Any] = {}
            rule_results: list[dict[str, Any]] = []
            for rule in active_rules:
                matched, condition_traces = self._evaluate_rule(
                    rule,
                    row,
                    row_index,
                    aggregate_cache,
                )
                traces.append(
                    trace := self._rule_execution_trace(rule, matched, condition_traces)
                )
                rule_results.append(self._rule_trace_payload(trace))
                if matched:
                    matched_rule_ids.append(rule.rule_id)
                    assignments.update(
                        self._evaluate_assignments(rule.assignments, row, row_index, aggregate_cache)
                    )
                    if rule.stop_on_match:
                        break

            output_rows.append(
                {
                    "row": row,
                    "matched": bool(matched_rule_ids),
                    "matched_rule_ids": matched_rule_ids,
                    "assign": assignments if assignments else None,
                    "rule_results": rule_results,
                }
            )
        matched_count = sum(1 for row in output_rows if row["matched"])
        logger.info(
            "Python runtime evaluation complete: ruleset_id=%s version=%s row_count=%s matched_count=%s",
            ruleset.ruleset_id,
            ruleset.version,
            len(output_rows),
            matched_count,
        )
        return output_rows, traces

    def _evaluate_rule(
        self,
        rule: Rule,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> tuple[bool, list[ResolvedConditionTrace]]:
        """
        Evaluate one rule against one row and collect condition traces.
        """
        condition_traces: list[ResolvedConditionTrace] = []
        matched = self._evaluate_group(
            rule.root_group,
            row,
            row_index,
            aggregate_cache,
            condition_traces,
        )
        return matched, condition_traces

    def _evaluate_group(
        self,
        group: ConditionGroup,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
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
                row_index,
                aggregate_cache,
            )
            condition_traces.append(condition_trace)
            results.append(condition_trace.passed)
        for nested_group in group.groups:
            results.append(
                self._evaluate_group(
                    nested_group,
                    row,
                    row_index,
                    aggregate_cache,
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
        row_index: int,
        aggregate_cache: "AggregateContext",
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
        left = self._resolve_operand_resolution(condition.left, row, row_index, aggregate_cache)
        right = (
            self._resolve_operand_resolution(condition.right, row, row_index, aggregate_cache)
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
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> dict[str, Any]:
        """
        Resolve all assignments for a matched rule into output values.
        """
        return {
            assignment.target_field: self._resolve_operand(
                assignment.value,
                row,
                row_index,
                aggregate_cache,
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

    def _rule_trace_payload(self, trace: RuleExecutionTrace) -> dict[str, Any]:
        """
        Convert one rule trace to the runtime result payload.
        """
        payload: dict[str, Any] = {
            "rule_id": trace.rule_id,
            "rule_name": trace.rule_name,
            "matched": trace.matched,
            "conditions": [
                self._condition_trace_payload(condition_trace)
                for condition_trace in trace.condition_traces
            ],
        }
        if trace.assignments_applied:
            payload["assignments_applied"] = list(trace.assignments_applied)
        return payload

    def _condition_trace_payload(self, trace: ResolvedConditionTrace) -> dict[str, Any]:
        """
        Convert one condition trace to the compact audit result payload.
        """
        left = self._operand_trace_payload(trace.left)
        right = self._operand_trace_payload(trace.right)
        columns = self._unique_strings(
            [
                *self._trace_columns(trace.left),
                *self._trace_columns(trace.right),
            ]
        )
        payload: dict[str, Any] = {
            "left": left,
            "operator": trace.operator,
            "comparison_result": trace.comparison_result,
            "passed": trace.passed,
        }
        self._add_present(payload, "columns", columns)
        if right is not None:
            payload["right"] = right
        if trace.tolerance_abs not in (None, "0"):
            payload["tolerance_abs"] = trace.tolerance_abs
        if trace.null_input_mode not in (None, NullInputMode.PROPAGATE.value):
            payload["null_input_mode"] = trace.null_input_mode
        if trace.null_result_mode not in (None, NullResultMode.NULL.value):
            payload["null_result_mode"] = trace.null_result_mode
        if trace.null_default_value is not None:
            payload["null_default_value"] = trace.null_default_value
        return payload

    def _operand_trace_payload(self, trace: Mapping[str, Any] | None) -> dict[str, Any] | None:
        """
        Convert an operand trace to compact audit fields.
        """
        if trace is None:
            return None
        kind = trace.get("kind")
        if kind == OperandKind.FIELD.value:
            payload = {
                "kind": kind,
                "column": trace.get("field_name"),
            }
            if "value" in trace:
                payload["value"] = trace["value"]
            return payload
        if kind == OperandKind.LITERAL.value:
            payload = {
                "kind": kind,
                "value": trace.get("value"),
            }
            if trace.get("value_type") is not None:
                payload["value_type"] = trace["value_type"]
            return payload
        if kind == OperandKind.AGGREGATE.value:
            payload = {
                "kind": kind,
                "function": trace.get("function"),
                "scope": trace.get("scope"),
                "source_columns": list(trace.get("columns", [])),
            }
            self._add_present(payload, "group_key", trace.get("group_key"))
            self._add_present(payload, "arguments", trace.get("args"))
            self._add_present(payload, "filter", self._aggregate_filter_trace_payload(trace.get("filter")))
            self._add_present(payload, "order_by", trace.get("order_by"))
            if "value" in trace:
                payload["value"] = trace["value"]
            if trace.get("null_input_mode") not in (None, NullInputMode.IGNORE.value):
                payload["null_input_mode"] = trace["null_input_mode"]
            if trace.get("null_result_mode") not in (None, NullResultMode.NULL.value):
                payload["null_result_mode"] = trace["null_result_mode"]
            self._add_present(payload, "null_default_value", trace.get("null_default_value"))
            return payload
        if kind == OperandKind.CUSTOM_FUNCTION.value:
            payload = {
                "kind": kind,
                "function_name": trace.get("function_name"),
            }
            self._add_present(
                payload,
                "args",
                {
                    str(key): self._operand_trace_payload(value)
                    for key, value in dict(trace.get("args", {})).items()
                },
            )
            self._add_present(payload, "source_columns", trace.get("columns"))
            if "value" in trace:
                payload["value"] = trace["value"]
            return payload
        payload = {"kind": kind}
        if "value" in trace:
            payload["value"] = trace["value"]
        return payload

    def _aggregate_filter_trace_payload(self, trace: Any) -> dict[str, Any] | None:
        """
        Convert aggregate-filter metadata to compact audit fields.
        """
        if not trace:
            return None
        return {
            "logical_operator": trace["logical_operator"],
            "predicates": [
                self._filter_predicate_trace_payload(predicate)
                for predicate in trace["predicates"]
            ],
        }

    def _filter_predicate_trace_payload(self, trace: Mapping[str, Any]) -> dict[str, Any]:
        """
        Convert one aggregate-filter predicate to compact audit fields.
        """
        payload: dict[str, Any] = {
            "columns": list(trace.get("columns", [])),
            "left": self._operand_trace_payload(trace.get("left")),
            "operator": trace.get("operator"),
        }
        right = self._operand_trace_payload(trace.get("right"))
        if right is not None:
            payload["right"] = right
        if trace.get("tolerance_abs") not in (None, "0"):
            payload["tolerance_abs"] = trace["tolerance_abs"]
        if trace.get("null_input_mode") not in (None, NullInputMode.PROPAGATE.value):
            payload["null_input_mode"] = trace["null_input_mode"]
        if trace.get("null_result_mode") not in (None, NullResultMode.NULL.value):
            payload["null_result_mode"] = trace["null_result_mode"]
        self._add_present(payload, "null_default_value", trace.get("null_default_value"))
        return payload

    def _trace_columns(self, trace: Mapping[str, Any] | None) -> list[str]:
        """
        Return source columns from an operand trace.
        """
        if trace is None:
            return []
        return list(trace.get("columns", []))

    def _add_present(self, payload: dict[str, Any], key: str, value: Any) -> None:
        """
        Add non-empty values to a compact trace payload.
        """
        if value in (None, {}, []):
            return
        payload[key] = value

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
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> Any:
        """
        Resolve one operand against the current row or aggregate context.
        """
        return self._resolve_operand_resolution(operand, row, row_index, aggregate_cache).value

    def _resolve_operand_resolution(
        self,
        operand: Operand,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
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
        if isinstance(operand, AggregateOperand):
            value = self._resolve_aggregate_operand_value(
                operand,
                row,
                row_index,
                aggregate_cache,
            )
            trace = self._aggregate_operand_metadata(operand, row)
            trace["value"] = self._trace_value(value)
            trace["evaluated"] = True
            return OperandResolution(value=value, trace=trace)
        if isinstance(operand, CustomFunctionOperand):
            args: dict[str, Any] = {}
            arg_traces: dict[str, Any] = {}
            for key, value in operand.args.items():
                arg_key = str(key)
                if isinstance(value, (FieldOperand, LiteralOperand, AggregateOperand, CustomFunctionOperand)):
                    argument = self._resolve_operand_resolution(value, row, row_index, aggregate_cache)
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

    def _resolve_aggregate_operand_value(
        self,
        operand: AggregateOperand,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> Any:
        """
        Resolve an aggregate operand value for the current runtime.
        """
        if aggregate_cache is None:
            raise RuntimeError("Aggregate cache is required to resolve aggregate operands.")
        return aggregate_cache.resolve(operand, row_index)

    def _resolve_custom_function_arg(
        self,
        value: Any,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> Any:
        """
        Resolve operand-valued custom function args against the current row.
        """
        if isinstance(value, (FieldOperand, LiteralOperand, AggregateOperand, CustomFunctionOperand)):
            return self._resolve_operand(value, row, row_index, aggregate_cache)
        return value

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
        if isinstance(operand, AggregateOperand):
            trace = self._aggregate_operand_metadata(operand, None)
            trace["evaluated"] = False
            return trace
        if isinstance(operand, CustomFunctionOperand):
            return {
                "kind": operand.kind.value,
                "columns": self._operand_columns(operand),
                "function_name": operand.function_name,
                "args": {
                    str(key): (
                        self._operand_metadata(value)
                        if isinstance(value, (FieldOperand, LiteralOperand, AggregateOperand, CustomFunctionOperand))
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

    def _aggregate_operand_metadata(
        self,
        operand: AggregateOperand,
        row: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Return trace metadata for an aggregate operand.
        """
        return {
            "kind": operand.kind.value,
            "columns": self._operand_columns(operand),
            "function": operand.function.value,
            "field_name": operand.field_name,
            "scope": operand.scope.value,
            "by": list(operand.by),
            "group_key": (
                {field_name: self._trace_value(row.get(field_name)) for field_name in operand.by}
                if row is not None and operand.scope is AggregateScope.GROUP
                else None
            ),
            "args": self._trace_value(dict(operand.args)),
            "filter": self._aggregate_filter_metadata(operand.filter),
            "order_by": [
                {
                    "field": order.field,
                    "direction": order.direction,
                }
                for order in operand.order_by
            ],
            "null_input_mode": operand.null_input_mode.value,
            "null_result_mode": operand.null_result_mode.value,
            "null_default_value": self._trace_value(operand.null_default_value),
        }

    def _aggregate_filter_metadata(self, filter_: Any | None) -> dict[str, Any] | None:
        """
        Return trace metadata for an aggregate filter definition.
        """
        if filter_ is None:
            return None
        return {
            "logical_operator": filter_.logical_operator.value,
            "predicates": [
                self._row_filter_predicate_metadata(predicate)
                for predicate in filter_.predicates
            ],
        }

    def _row_filter_predicate_metadata(self, predicate: RowFilterPredicate) -> dict[str, Any]:
        """
        Return trace metadata for one aggregate-filter predicate.
        """
        return {
            "columns": self._unique_strings(
                [
                    *self._operand_columns(predicate.left),
                    *(
                        self._operand_columns(predicate.right)
                        if predicate.right is not None
                        else []
                    ),
                ]
            ),
            "operator": predicate.operator.value,
            "tolerance_abs": self._trace_value(predicate.tolerance_abs),
            "null_input_mode": predicate.null_input_mode.value,
            "null_result_mode": predicate.null_result_mode.value,
            "null_default_value": self._trace_value(predicate.null_default_value),
            "left": self._operand_metadata(predicate.left),
            "right": (
                self._operand_metadata(predicate.right)
                if predicate.right is not None
                else None
            ),
        }

    def _operand_columns(self, operand: Operand | Any) -> list[str]:
        """
        Return all source columns referenced by an operand tree.
        """
        if isinstance(operand, FieldOperand):
            return [operand.field_name]
        if isinstance(operand, LiteralOperand):
            return []
        if isinstance(operand, AggregateOperand):
            columns: list[str] = [operand.field_name, *operand.by]
            columns.extend(order.field for order in operand.order_by)
            if operand.filter is not None:
                for predicate in operand.filter.predicates:
                    columns.extend(self._operand_columns(predicate.left))
                    if predicate.right is not None:
                        columns.extend(self._operand_columns(predicate.right))
            return self._unique_strings(columns)
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


class AggregateContext:
    """
    Per-run aggregate resolver.

    Aggregates are cached by operand shape and group key for the materialized
    incoming row set. The cache is scoped to one call to ``evaluate`` only.
    """

    def __init__(self, rows: list[dict[str, Any]], runtime: RulesEngineRuntime) -> None:
        """
        Create a per-evaluation aggregate context over materialized rows.
        """
        self._rows = rows
        self._runtime = runtime
        self._dataset_cache: dict[str, Any] = {}
        self._group_cache: dict[str, dict[tuple[Any, ...], Any]] = {}

    def resolve(self, operand: AggregateOperand, row_index: int) -> Any:
        """
        Resolve an aggregate operand for one input row.
        """
        cache_key = self._cache_key(operand)
        if operand.scope is AggregateScope.DATASET:
            if cache_key not in self._dataset_cache:
                logger.debug(
                    "Calculating dataset aggregate: function=%s field=%s row_count=%s",
                    operand.function.value,
                    operand.field_name,
                    len(self._rows),
                )
                self._dataset_cache[cache_key] = self._calculate(operand, self._rows)
            return self._dataset_cache[cache_key]

        if cache_key not in self._group_cache:
            logger.debug(
                "Calculating group aggregate: function=%s field=%s by=%s row_count=%s",
                operand.function.value,
                operand.field_name,
                list(operand.by),
                len(self._rows),
            )
            grouped_rows: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
            for row in self._rows:
                grouped_rows[self._group_key(operand, row)].append(row)
            self._group_cache[cache_key] = {
                key: self._calculate(operand, group_rows)
                for key, group_rows in grouped_rows.items()
            }
        current_key = self._group_key(operand, self._rows[row_index])
        return self._group_cache[cache_key].get(current_key)

    def _calculate(self, operand: AggregateOperand, rows: list[dict[str, Any]]) -> Any:
        """
        Calculate one aggregate over a scoped row subset.
        """
        filtered_rows = self._filter_rows(operand, rows)
        ordered_rows = self._order_rows(operand, filtered_rows)
        values = self._extract_values(operand, ordered_rows)
        if values is None:
            return self._resolve_aggregate_null(operand)
        if operand.function is AggregateFunction.COUNT:
            return len(values)
        if operand.function is AggregateFunction.COUNT_DISTINCT:
            return len(set(values))
        if not values:
            return self._resolve_aggregate_null(operand)
        if operand.function is AggregateFunction.SUM:
            return sum(values)
        if operand.function is AggregateFunction.MEAN:
            return mean(values)
        if operand.function is AggregateFunction.MIN:
            return min(values)
        if operand.function is AggregateFunction.MAX:
            return max(values)
        if operand.function is AggregateFunction.MEDIAN:
            return median(values)
        if operand.function is AggregateFunction.STDDEV:
            return pstdev(values)
        if operand.function is AggregateFunction.VARIANCE:
            return pvariance(values)
        if operand.function is AggregateFunction.QUANTILE:
            return self._quantile(values, Decimal(str(operand.args["q"])))
        if operand.function is AggregateFunction.FIRST:
            return values[0]
        if operand.function is AggregateFunction.LAST:
            return values[-1]
        raise ValueError(f"Unsupported aggregate function: {operand.function.value}")

    def _filter_rows(
        self,
        operand: AggregateOperand,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Apply an aggregate's optional row-level filter to candidate rows.
        """
        if operand.filter is None:
            return rows
        filtered: list[dict[str, Any]] = []
        for row in rows:
            predicate_results = [
                self._evaluate_filter_predicate(predicate, row)
                for predicate in operand.filter.predicates
            ]
            if operand.filter.logical_operator is LogicalOperator.ALL:
                include = all(predicate_results)
            else:
                include = any(predicate_results)
            if include:
                filtered.append(row)
        return filtered

    def _evaluate_filter_predicate(
        self,
        predicate: RowFilterPredicate,
        row: Mapping[str, Any],
    ) -> bool:
        """
        Evaluate one aggregate-filter predicate against one row.
        """
        if isinstance(predicate.left, AggregateOperand) or isinstance(predicate.right, AggregateOperand):
            raise RuntimeError("Nested aggregate in filter predicate at runtime.")
        left = self._runtime._resolve_operand(predicate.left, row, 0, self)
        right = (
            self._runtime._resolve_operand(predicate.right, row, 0, self)
            if predicate.right is not None
            else None
        )
        result = self._runtime._compare_values(
            left,
            predicate.operator,
            right,
            predicate.tolerance_abs,
            predicate.null_input_mode,
        )
        return self._runtime._resolve_null_result(
            result,
            predicate.null_result_mode,
            predicate.null_default_value,
        )

    def _order_rows(
        self,
        operand: AggregateOperand,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Return rows ordered for order-sensitive aggregate functions.
        """
        ordered_rows = list(rows)
        for order in reversed(operand.order_by):
            ordered_rows.sort(
                key=lambda row, field=order.field: (
                    row.get(field) is None,
                    self._sortable_value(row.get(field)),
                ),
            )
            if order.direction == "desc":
                non_null_rows = [row for row in ordered_rows if row.get(order.field) is not None]
                null_rows = [row for row in ordered_rows if row.get(order.field) is None]
                ordered_rows = list(reversed(non_null_rows)) + null_rows
        return ordered_rows

    def _sortable_value(self, value: Any) -> Any:
        """
        Normalize None for Python sort keys while preserving non-null values.
        """
        return "" if value is None else value

    def _extract_values(
        self,
        operand: AggregateOperand,
        rows: list[dict[str, Any]],
    ) -> list[Any] | None:
        """
        Extract aggregate input values while applying null-input behavior.
        """
        values: list[Any] = []
        for row in rows:
            value = row.get(operand.field_name)
            if value is None:
                if operand.null_input_mode is NullInputMode.ERROR:
                    raise ValueError("Null aggregate input encountered with null_input_mode=error.")
                if operand.null_input_mode is NullInputMode.ZERO:
                    values.append(0)
                elif operand.null_input_mode is NullInputMode.PROPAGATE:
                    return None
                else:
                    continue
            else:
                values.append(value)
        return values

    def _resolve_aggregate_null(self, operand: AggregateOperand) -> Any:
        """
        Resolve an empty or null-propagated aggregate result.
        """
        if operand.null_result_mode is NullResultMode.ERROR:
            raise ValueError("Null aggregate result encountered with null_result_mode=error.")
        if operand.null_result_mode is NullResultMode.DEFAULT:
            return operand.null_default_value
        return None

    def _quantile(self, values: list[Any], q: Decimal) -> Any:
        """
        Calculate an exact linear-interpolated quantile.
        """
        sorted_values = sorted(values)
        if len(sorted_values) == 1:
            return sorted_values[0]
        position = float(q) * (len(sorted_values) - 1)
        lower_index = int(position)
        upper_index = min(lower_index + 1, len(sorted_values) - 1)
        fraction = position - lower_index
        lower = sorted_values[lower_index]
        upper = sorted_values[upper_index]
        return lower + (upper - lower) * fraction

    def _group_key(self, operand: AggregateOperand, row: Mapping[str, Any]) -> tuple[Any, ...]:
        """
        Build the group cache key for a group-scoped aggregate.
        """
        return tuple(row.get(field_name) for field_name in operand.by)

    def _cache_key(self, operand: AggregateOperand) -> str:
        """
        Build the aggregate operand cache key.
        """
        return aggregate_key(operand)
