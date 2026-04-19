"""
Runtime facade and pure-Python evaluator.

The evaluator operates on the incoming row set exactly as provided. It does
not deduplicate, reshape, filter globally, or retain cross-run state.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
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


class RulesEngineRuntime:
    """
    Runtime facade for published ruleset metadata.
    """

    def __init__(self, repository: RulesetRepository, function_registry: FunctionRegistry) -> None:
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
                    RuleExecutionTrace(
                        rule_id=rule.rule_id,
                        condition_traces=tuple(condition_traces),
                        assignments_applied=(
                            tuple(assignment.target_field for assignment in rule.assignments)
                            if matched
                            else ()
                        ),
                        matched=matched,
                    )
                )
                rule_results.append({"rule_id": rule.rule_id, "matched": matched})
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
        return output_rows, traces

    def _evaluate_rule(
        self,
        rule: Rule,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> tuple[bool, list[ResolvedConditionTrace]]:
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
        results: list[bool] = []
        for condition in group.conditions:
            passed = self._evaluate_condition(condition, row, row_index, aggregate_cache)
            condition_traces.append(
                ResolvedConditionTrace(condition_id=condition.condition_id, passed=passed)
            )
            results.append(passed)
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
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> bool:
        if not condition.active_flag:
            return False
        left = self._resolve_operand(condition.left, row, row_index, aggregate_cache)
        right = (
            self._resolve_operand(condition.right, row, row_index, aggregate_cache)
            if condition.right is not None
            else None
        )
        result = self._compare_values(
            left,
            condition.operator,
            right,
            condition.tolerance_abs,
            condition.null_input_mode,
        )
        return self._resolve_null_result(
            result,
            condition.null_result_mode,
            condition.null_default_value,
        )

    def _evaluate_assignments(
        self,
        assignments: tuple[Assignment, ...],
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> dict[str, Any]:
        return {
            assignment.target_field: self._resolve_operand(
                assignment.value,
                row,
                row_index,
                aggregate_cache,
            )
            for assignment in assignments
        }

    def _resolve_operand(
        self,
        operand: Operand,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: "AggregateContext",
    ) -> Any:
        if isinstance(operand, FieldOperand):
            return row.get(operand.field_name)
        if isinstance(operand, LiteralOperand):
            return operand.value
        if isinstance(operand, AggregateOperand):
            return aggregate_cache.resolve(operand, row_index)
        if isinstance(operand, CustomFunctionOperand):
            implementation = self._function_registry.get_implementation(operand.function_name)
            return implementation(**dict(operand.args))
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _compare_values(
        self,
        left: Any,
        operator: ComparisonOperator,
        right: Any,
        tolerance_abs: Decimal,
        null_input_mode: NullInputMode,
    ) -> bool | None:
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
        if result is not None:
            return bool(result)
        if null_result_mode is NullResultMode.ERROR:
            raise ValueError("Null result encountered with null_result_mode=error.")
        if null_result_mode is NullResultMode.DEFAULT:
            return bool(null_default_value)
        return False

    def _equals(self, left: Any, right: Any, tolerance_abs: Decimal) -> bool:
        if self._is_numeric(left) and self._is_numeric(right):
            return abs(self._decimal(left) - self._decimal(right)) <= tolerance_abs
        return left == right

    def _is_numeric(self, value: Any) -> bool:
        try:
            Decimal(str(value))
        except Exception:
            return False
        return not isinstance(value, bool)

    def _decimal(self, value: Any) -> Decimal:
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
                self._dataset_cache[cache_key] = self._calculate(operand, self._rows)
            return self._dataset_cache[cache_key]

        if cache_key not in self._group_cache:
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
        return "" if value is None else value

    def _extract_values(
        self,
        operand: AggregateOperand,
        rows: list[dict[str, Any]],
    ) -> list[Any] | None:
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
        if operand.null_result_mode is NullResultMode.ERROR:
            raise ValueError("Null aggregate result encountered with null_result_mode=error.")
        if operand.null_result_mode is NullResultMode.DEFAULT:
            return operand.null_default_value
        return None

    def _quantile(self, values: list[Any], q: Decimal) -> Any:
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
        return tuple(row.get(field_name) for field_name in operand.by)

    def _cache_key(self, operand: AggregateOperand) -> str:
        return aggregate_key(operand)
