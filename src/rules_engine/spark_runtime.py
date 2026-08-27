"""
Spark-facing rules engine runtime.

The runtime evaluates each input row with a Python UDF and returns Spark-native
struct columns so downstream Spark jobs can select nested fields directly.
Rules use original row fields, committed lower-order assignments, literals, or
registered custom functions.
"""

from __future__ import annotations

import logging
import math
import traceback
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import uuid4

from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.dataframe_evaluation import DataFrameEvaluation
from rules_engine.exceptions import ValidationFailedError
from rules_engine.models import (
    Assignment,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    Operand,
    ResolvedConditionTrace,
    Rule,
    RuleExecutionTrace,
    Ruleset,
    iter_nested_operands,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository
from rules_engine.runtime import AssignedValue, SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_types import (
    INTEGRAL_LIMITS,
    INTEGRAL_TYPES,
    TIMESTAMP_TYPES,
    decimal_value_fits,
)
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.version import __version__

logger = logging.getLogger(__name__)


def required_source_columns(ruleset: Ruleset) -> tuple[str, ...]:
    """Return source columns required to evaluate active rules.

    Columns are returned once, in rule evaluation and operand traversal order.
    Inactive rules and conditions are excluded because their values are never
    resolved against an input row.
    """
    columns: list[str] = []

    def add_operand(operand: Operand | None) -> None:
        if isinstance(operand, FieldOperand):
            columns.append(operand.field_name)
        elif isinstance(operand, CustomFunctionOperand):
            for argument in operand.args.values():
                for nested_operand in iter_nested_operands(argument):
                    add_operand(nested_operand)

    def add_group(group: ConditionGroup) -> None:
        for condition in group.conditions:
            if condition.active_flag:
                add_operand(condition.left)
                add_operand(condition.right)
        for nested_group in group.groups:
            add_group(nested_group)

    for rule in sorted(
        (item for item in ruleset.rules if item.active_flag),
        key=lambda item: item.rule_order,
    ):
        add_group(rule.root_group)
        for assignment in rule.assignments:
            add_operand(assignment.value)
    return tuple(dict.fromkeys(columns))


def _source_column(column_name: str):
    """Return a top-level Spark column with its literal source name preserved."""
    escaped_name = column_name.replace("`", "``")
    return F.col(f"`{escaped_name}`").alias(column_name)


OPERAND_TRACE_STRUCT = T.StructType(
    [
        T.StructField("kind", T.StringType(), True),
        T.StructField("column", T.StringType(), True),
        T.StructField("target_field", T.StringType(), True),
        T.StructField("original_value", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("value_type", T.StringType(), True),
        T.StructField("default_if_null", T.StringType(), True),
        T.StructField("default_applied", T.BooleanType(), False),
        T.StructField("function_name", T.StringType(), True),
        T.StructField("produced_by_rule_id", T.StringType(), True),
        T.StructField("produced_by_assignment_id", T.StringType(), True),
        T.StructField("source_columns", T.ArrayType(T.StringType(), False), True),
        T.StructField("arguments", T.MapType(T.StringType(), T.StringType(), True), True),
    ]
)

CONDITION_TRACE_STRUCT = T.StructType(
    [
        T.StructField("condition_id", T.StringType(), False),
        T.StructField("condition_group_id", T.StringType(), False),
        T.StructField("condition_group_operator", T.StringType(), False),
        T.StructField("active_flag", T.BooleanType(), False),
        T.StructField("columns", T.ArrayType(T.StringType(), False), True),
        T.StructField("left", OPERAND_TRACE_STRUCT, True),
        T.StructField("right", OPERAND_TRACE_STRUCT, True),
        T.StructField("operator", T.StringType(), True),
        T.StructField("comparison_result", T.BooleanType(), True),
        T.StructField("passed", T.BooleanType(), True),
        T.StructField("tolerance_abs", T.StringType(), True),
    ]
)

MATCHED_RULE_TRACE_STRUCT = T.StructType(
    [
        T.StructField("rule_id", T.StringType(), True),
        T.StructField("rule_name", T.StringType(), True),
        T.StructField("rule_order", T.LongType(), True),
        T.StructField("explanation", T.StringType(), True),
        T.StructField("assignments_applied", T.ArrayType(T.StringType(), False), True),
        T.StructField("conditions", T.ArrayType(CONDITION_TRACE_STRUCT, False), True),
    ]
)

ASSIGNMENT_RESULT_STRUCT = T.StructType(
    [
        T.StructField("assignment_id", T.StringType(), True),
        T.StructField("rule_id", T.StringType(), True),
        T.StructField("rule_name", T.StringType(), True),
        T.StructField("rule_order", T.LongType(), True),
        T.StructField("target_field", T.StringType(), True),
        T.StructField("authored_expression", T.StringType(), False),
        T.StructField("old_value", T.StringType(), True),
        T.StructField("proposed_value", T.StringType(), True),
        T.StructField("changed", T.BooleanType(), False),
        T.StructField("overridden_by_rule_id", T.StringType(), True),
        T.StructField("overridden_by_assignment_id", T.StringType(), True),
        T.StructField("effective", T.BooleanType(), False),
        T.StructField("final_winning_rule_id", T.StringType(), False),
        T.StructField("final_winning_assignment_id", T.StringType(), False),
    ]
)


@dataclass(frozen=True)
class _PreparedRule:
    """Driver-precomputed rule metadata captured by the worker evaluator."""

    rule: Rule
    assignment_specs: tuple[tuple[Assignment, str | None], ...]


def _require_bool(name: str, value: Any) -> None:
    """Reject truthy non-booleans for public boolean options."""
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, not {type(value).__name__}.")


_COMPACT_RESULT_FIELDS_BEFORE_ASSIGN = (
    T.StructField("error", T.StringType(), True),
    T.StructField("matched", T.BooleanType(), False),
    T.StructField("matched_rule_ids", T.ArrayType(T.StringType(), False), False),
)
_FULL_AUDIT_ONLY_RESULT_FIELDS = (
    T.StructField(
        "matched_rules",
        T.ArrayType(MATCHED_RULE_TRACE_STRUCT, False),
        False,
    ),
    T.StructField(
        "assignment_results",
        T.ArrayType(ASSIGNMENT_RESULT_STRUCT, False),
        False,
    ),
)


def _result_struct(
    assign_schema: T.StructType,
    *,
    full_audit: bool = False,
) -> T.StructType:
    """Build the Spark UDF result schema for the requested audit detail."""
    _require_bool("full_audit", full_audit)
    compact_fields = [
        *_COMPACT_RESULT_FIELDS_BEFORE_ASSIGN,
        T.StructField("assign", _assignment_outcome_schema(assign_schema), False),
    ]
    return T.StructType(
        compact_fields + (list(_FULL_AUDIT_ONLY_RESULT_FIELDS) if full_audit else [])
    )


def _assignment_outcome_schema(assign_schema: T.StructType) -> T.StructType:
    """Wrap every typed assignment target with explicit application state."""
    return T.StructType(
        [
            T.StructField(
                field.name,
                T.StructType(
                    [
                        T.StructField("applied", T.BooleanType(), False),
                        T.StructField("value", field.dataType, True),
                    ]
                ),
                False,
            )
            for field in assign_schema.fields
        ]
    )


COMPACT_RESULT_FIELD_NAMES = tuple(_result_struct(T.StructType()).fieldNames())
FULL_AUDIT_RESULT_FIELD_NAMES = tuple(_result_struct(T.StructType(), full_audit=True).fieldNames())
FULL_AUDIT_ONLY_RESULT_FIELD_NAMES = tuple(field.name for field in _FULL_AUDIT_ONLY_RESULT_FIELDS)


def result_field_names(
    *,
    full_audit: bool = False,
) -> tuple[str, ...]:
    """Return result field names emitted for the requested audit detail."""
    _require_bool("full_audit", full_audit)
    return FULL_AUDIT_RESULT_FIELD_NAMES if full_audit else COMPACT_RESULT_FIELD_NAMES


class SparkRulesEngineRuntime:
    """Spark DataFrame runtime for ruleset evaluation."""

    def __init__(
        self,
        repository: RulesetRepository,
        function_registry: FunctionRegistry,
        compatibility_validator: SparkRulesetCompatibilityValidator | None = None,
    ) -> None:
        """Initialize the Spark runtime with metadata and function registries."""
        self._repository = repository
        self._function_registry = function_registry
        self._compatibility_validator = (
            compatibility_validator or SparkRulesetCompatibilityValidator(function_registry)
        )

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Load published metadata through the configured repository."""
        return self._repository.load_published(ruleset_name, version)

    def evaluate_dataframe(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        key_columns: Sequence[str] | None = None,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> DataFrameEvaluation:
        """
        Evaluate keyed Spark rows and return separate lazy result projections.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Incoming rows. Any cross-row facts must already exist as columns.
        ruleset : Ruleset
            Ruleset metadata to evaluate. This method performs semantic and
            incoming-schema validation before building the UDF.
        key_columns : sequence of str, optional
            Existing, immutable columns that identify rows in ``results_df``.
            When omitted, all input columns are used in their existing order.
            The caller guarantees their values are non-null and unique; this
            method does not start a hidden Spark action to prove that contract.
        column_prefix : str, default "rules_engine"
            Prefix for result columns.
        fail_on_error : bool, default True
            Raise from the worker during the caller's first materializing Spark
            action when a row-level evaluator error is produced. Building the
            returned evaluation does not start a hidden validation job.
        include_error_traceback : bool, default False
            Include full Python tracebacks in row error payloads. Keep disabled
            for production data because tracebacks substantially enlarge rows.
        full_audit : bool, default False
            Add a detailed trace for every matched rule plus assignment
            provenance. Immutable ruleset/engine identity is always appended
            as driver-side literal columns.

        Returns
        -------
        DataFrameEvaluation
            Shared lazy plan exposing keyed results and applied business rows.
        """
        normalized_keys = self._validate_key_columns(df, ruleset, key_columns)
        evaluated, assign_schema = self._evaluate_attached_dataframe(
            df,
            ruleset,
            column_prefix=column_prefix,
            fail_on_error=fail_on_error,
            include_error_traceback=include_error_traceback,
            full_audit=full_audit,
        )
        return DataFrameEvaluation(
            evaluated,
            source_columns=df.columns,
            key_columns=normalized_keys,
            result_columns=self._output_column_names(
                column_prefix,
                full_audit=full_audit,
            ),
            assignment_fields=assign_schema.fields,
            assign_column=f"{column_prefix}_assign",
        )

    def _evaluate_attached_dataframe(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> tuple[DataFrame, T.StructType]:
        """Build the internal source-plus-results plan without starting a job."""
        _require_bool("full_audit", full_audit)
        if not column_prefix:
            raise ValueError("column_prefix must be non-empty.")
        output_names = {
            *{f"{column_prefix}_{field_name}" for field_name in FULL_AUDIT_RESULT_FIELD_NAMES},
            f"{column_prefix}_ruleset",
            f"{column_prefix}_engine_version",
        }
        conflicts = sorted(output_names & set(df.columns))
        if conflicts:
            raise ValueError(
                f"Input contains rules-engine output columns for prefix "
                f"{column_prefix!r}: {conflicts}"
            )
        logger.info(
            "Evaluating ruleset in Spark runtime: ruleset_id=%s ruleset_name=%s version=%s rule_count=%s fail_on_error=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            len(ruleset.rules),
            fail_on_error,
        )
        validation = self._compatibility_validator.validate(
            ruleset,
            df.schema,
        )
        if validation.has_errors():
            raise ValidationFailedError(
                "Ruleset validation failed for Spark evaluation.\n" + validation.to_text()
            )
        assign_schema = self._assignment_schema(ruleset, df.schema)
        assign_field_names = [field.name for field in assign_schema.fields]
        assign_field_types = {field.name: field.dataType for field in assign_schema.fields}

        row_evaluator = self._build_row_evaluator(
            ruleset,
            assign_field_names,
            assign_field_types,
            raise_on_error=fail_on_error,
            include_error_traceback=include_error_traceback,
            full_audit=full_audit,
        )
        self.validate_worker_serializable(row_evaluator)
        result_udf = F.udf(
            row_evaluator,
            _result_struct(assign_schema, full_audit=full_audit),
            # This is a row-oriented, nested-struct UDF rather than a vectorized UDF.
            useArrow=False,
        )
        available_columns = set(df.columns)
        assignment_target_columns = (
            [
                assignment.target_field
                for rule in sorted(ruleset.rules, key=lambda item: item.rule_order)
                if rule.active_flag
                for assignment in rule.assignments
            ]
            if full_audit
            else []
        )
        serialized_columns = [
            column_name
            for column_name in dict.fromkeys(
                (*required_source_columns(ruleset), *assignment_target_columns)
            )
            if column_name in available_columns
        ]
        row_struct = F.struct(
            *(
                [_source_column(column_name) for column_name in serialized_columns]
                or [F.lit(None).alias("__rules_engine_empty")]
            )
        )
        result_col = f"__rules_engine_result_{uuid4().hex}"
        evaluated = df.withColumn(result_col, result_udf(row_struct))
        output = self._append_output_columns(
            evaluated,
            result_col=result_col,
            column_prefix=column_prefix,
            ruleset=ruleset,
            full_audit=full_audit,
        )
        logger.info(
            "Spark runtime evaluation DataFrame built: ruleset_id=%s version=%s output_prefix=%s",
            ruleset.ruleset_id,
            ruleset.version,
            column_prefix,
        )
        return output, assign_schema

    @staticmethod
    def _validate_key_columns(
        df: DataFrame,
        ruleset: Ruleset,
        key_columns: Sequence[str] | None,
    ) -> tuple[str, ...]:
        """Validate lazy row-identity metadata without scanning key values."""
        if key_columns is None:
            key_columns = df.columns
        if isinstance(key_columns, (str, bytes)):
            raise TypeError("key_columns must be a sequence of column names, not a string.")
        try:
            normalized = tuple(key_columns)
        except TypeError as exc:
            raise TypeError("key_columns must be a sequence of column names.") from exc
        if not normalized:
            raise ValueError("key_columns must contain at least one column name.")
        invalid = [
            column_name
            for column_name in normalized
            if not isinstance(column_name, str) or not column_name
        ]
        if invalid:
            raise ValueError("key_columns must contain only non-empty strings.")
        duplicates = sorted(
            {column_name for column_name in normalized if normalized.count(column_name) > 1}
        )
        if duplicates:
            raise ValueError(f"key_columns contains duplicate names: {duplicates}")
        missing = sorted(set(normalized) - set(df.columns))
        if missing:
            raise ValueError(f"key_columns are missing from the input DataFrame: {missing}")
        ambiguous = sorted(
            column_name for column_name in normalized if df.columns.count(column_name) > 1
        )
        if ambiguous:
            raise ValueError(f"key_columns are ambiguous in the input DataFrame: {ambiguous}")
        assignment_targets = {
            assignment.target_field
            for rule in ruleset.rules
            if rule.active_flag
            for assignment in rule.assignments
        }
        assigned_keys = sorted(set(normalized) & assignment_targets)
        if assigned_keys:
            raise ValueError(
                f"Assignment targets cannot modify immutable key columns: {assigned_keys}"
            )
        return normalized

    @staticmethod
    def _output_column_names(
        column_prefix: str,
        *,
        full_audit: bool,
    ) -> tuple[str, ...]:
        """Return the public result columns in contract order."""
        return (
            *(
                f"{column_prefix}_{field_name}"
                for field_name in result_field_names(full_audit=full_audit)
            ),
            f"{column_prefix}_ruleset",
            f"{column_prefix}_engine_version",
        )

    @staticmethod
    def _append_output_columns(
        evaluated: DataFrame,
        *,
        result_col: str,
        column_prefix: str,
        ruleset: Ruleset,
        full_audit: bool,
    ) -> DataFrame:
        """Append the public result contract in its documented column order."""
        result = F.col(result_col)
        output_columns = {
            f"{column_prefix}_{field_name}": result.getField(field_name)
            for field_name in result_field_names(full_audit=full_audit)
        }
        output_columns[f"{column_prefix}_ruleset"] = F.struct(
            F.lit(ruleset.ruleset_id).alias("id"),
            F.lit(ruleset.version).alias("version"),
            F.lit(DeltaRowSerializer().content_hash(ruleset)).alias("content_hash"),
        )
        output_columns[f"{column_prefix}_engine_version"] = F.lit(__version__)
        return evaluated.withColumns(output_columns).drop(result_col)

    def validate_worker_serializable(self, evaluator: Any) -> None:
        """Fail before job submission when a UDF closure cannot be serialized."""
        try:
            CloudPickleSerializer().dumps(evaluator)
        except Exception as exc:
            raise ValidationFailedError(
                "Rules engine custom function implementations must be "
                "Spark-worker-serializable. Register top-level callables and "
                "avoid captured objects that cloudpickle cannot serialize. "
                f"Serialization failed with {type(exc).__name__}: {exc}"
            ) from exc

    def _assignment_schema(self, ruleset: Ruleset, source_schema: T.StructType) -> T.StructType:
        """Build a ruleset-specific assignment result struct."""
        return self._compatibility_validator.assignment_schema(
            ruleset,
            source_schema,
        )

    def _build_row_evaluator(
        self,
        ruleset: Ruleset,
        assign_field_names: list[str],
        assign_field_types: Mapping[str, T.DataType],
        *,
        raise_on_error: bool = False,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ):
        """Build the serializable Python callable used by the Spark UDF."""
        _require_bool("full_audit", full_audit)
        runtime = _SparkRowUdfEvaluator.without_repository(self._function_registry)
        ordered_rules = sorted(ruleset.rules, key=lambda item: item.rule_order)
        active_rules: list[_PreparedRule] = []
        for rule in (item for item in ordered_rules if item.active_flag):
            assignment_specs = tuple(
                (
                    assignment,
                    (
                        runtime._rule_formatter.format_assignment_expression(assignment)
                        if full_audit
                        else None
                    ),
                )
                for assignment in rule.assignments
            )
            active_rules.append(
                _PreparedRule(
                    rule=rule,
                    assignment_specs=assignment_specs,
                )
            )
        base_payload_template = runtime._base_payload(
            assign_field_names,
            full_audit=full_audit,
        )

        def evaluate(row: Any) -> dict[str, Any]:
            """Evaluate one Spark row struct and return the declared result struct."""
            try:
                row_dict = row.asDict(recursive=True)
                matched_rule_ids: list[str] = []
                matched_rules: list[dict[str, Any]] = []
                assignments: dict[str, Any] = {}
                assigned_values: dict[str, AssignedValue] = {}
                assignment_events: list[dict[str, Any]] = []
                for prepared_rule in active_rules:
                    rule = prepared_rule.rule
                    if full_audit:
                        matched, condition_traces = runtime._evaluate_rule(
                            rule,
                            row_dict,
                            assigned_values,
                        )
                    else:
                        matched = runtime._rule_matches(
                            rule,
                            row_dict,
                            assigned_values,
                        )
                        condition_traces = []
                    if matched:
                        matched_rule_ids.append(rule.rule_id)
                        if full_audit:
                            explanation = runtime._matched_rule_explanation_from_trace(
                                rule,
                                condition_traces,
                            )
                            matched_rules.append(
                                runtime._spark_rule_trace(
                                    runtime._rule_execution_trace(
                                        rule,
                                        condition_traces,
                                    ),
                                    explanation=explanation,
                                )
                            )
                        resolved_rule_assignments: list[tuple[Assignment, Any]] = []
                        for assignment, authored_expression in prepared_rule.assignment_specs:
                            if full_audit:
                                event = runtime._typed_assignment_event(
                                    rule,
                                    assignment,
                                    authored_expression,
                                    row_dict,
                                    assign_field_types[assignment.target_field],
                                    assigned_values,
                                )
                                proposed_value = event["proposed_value"]
                                assignment_events.append(event)
                            else:
                                proposed_value = runtime._spark_assignment_value(
                                    runtime._resolve_operand(
                                        assignment.value,
                                        row_dict,
                                        assigned_values,
                                    ),
                                    assign_field_types[assignment.target_field],
                                )
                            resolved_rule_assignments.append((assignment, proposed_value))
                        for assignment, proposed_value in resolved_rule_assignments:
                            assignments[assignment.target_field] = proposed_value
                            assigned_values[assignment.target_field] = AssignedValue(
                                value=proposed_value,
                                rule_id=rule.rule_id,
                                assignment_id=assignment.assignment_id,
                            )
                        if rule.stop_on_match:
                            break
                assignment_results = (
                    runtime._assignment_results(assignment_events) if full_audit else []
                )
                assign_payload = (
                    {
                        field_name: {
                            "applied": field_name in assignments,
                            "value": assignments.get(field_name),
                        }
                        for field_name in assign_field_names
                    }
                    if assignments
                    else base_payload_template["assign"]
                )
                return runtime._success_payload(
                    matched_rule_ids=matched_rule_ids,
                    matched_rules=matched_rules,
                    assign_payload=assign_payload,
                    assignment_results=assignment_results,
                    full_audit=full_audit,
                    base_payload_template=base_payload_template,
                )
            except Exception as exc:
                if raise_on_error:
                    raise RuntimeError(
                        f"Rules engine row evaluation failed with {type(exc).__name__}: {exc}"
                    ) from exc
                return runtime._error_payload(
                    exc,
                    include_traceback=include_error_traceback,
                    full_audit=full_audit,
                    base_payload_template=base_payload_template,
                )

        return evaluate


class _SparkRowUdfEvaluator(SparkRowEvaluator):
    """Row evaluator plus Spark-schema trace normalization helpers."""

    def _typed_assignment_event(
        self,
        rule: Rule,
        assignment: Assignment,
        authored_expression: str,
        row: Mapping[str, Any],
        data_type: T.DataType,
        assigned_values: Mapping[str, AssignedValue] | None = None,
    ) -> dict[str, Any]:
        """Resolve and normalize both sides of one assignment audit event."""
        proposed_value = self._resolve_operand(
            assignment.value,
            row,
            assigned_values,
        )
        assigned_value = (assigned_values or {}).get(assignment.target_field)
        old_value = (
            assigned_value.value if assigned_value is not None else row.get(assignment.target_field)
        )
        return {
            "assignment_id": assignment.assignment_id,
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "rule_order": rule.rule_order,
            "target_field": assignment.target_field,
            "authored_expression": authored_expression,
            "old_value": self._spark_assignment_value(old_value, data_type),
            "proposed_value": self._spark_assignment_value(
                proposed_value,
                data_type,
            ),
        }

    def _assignment_results(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build ordered immediate-override and final-winner assignment provenance."""
        effective_event_indexes = {
            event["target_field"]: index for index, event in enumerate(events)
        }
        next_event_indexes: dict[int, int] = {}
        next_index_by_target: dict[str, int] = {}
        for index in range(len(events) - 1, -1, -1):
            target_field = events[index]["target_field"]
            if target_field in next_index_by_target:
                next_event_indexes[index] = next_index_by_target[target_field]
            next_index_by_target[target_field] = index

        results: list[dict[str, Any]] = []
        for index, event in enumerate(events):
            effective_index = effective_event_indexes[event["target_field"]]
            effective_event = events[effective_index]
            next_event_index = next_event_indexes.get(index)
            next_event = events[next_event_index] if next_event_index is not None else None
            effective = index == effective_index
            results.append(
                {
                    "assignment_id": event["assignment_id"],
                    "rule_id": event["rule_id"],
                    "rule_name": event["rule_name"],
                    "rule_order": event["rule_order"],
                    "target_field": event["target_field"],
                    "authored_expression": event["authored_expression"],
                    "old_value": self._trace_text(event["old_value"]),
                    "proposed_value": self._trace_text(event["proposed_value"]),
                    "changed": self._values_changed(
                        event["old_value"],
                        event["proposed_value"],
                    ),
                    "overridden_by_rule_id": (
                        next_event["rule_id"] if next_event is not None else None
                    ),
                    "overridden_by_assignment_id": (
                        next_event["assignment_id"] if next_event is not None else None
                    ),
                    "effective": effective,
                    "final_winning_rule_id": effective_event["rule_id"],
                    "final_winning_assignment_id": effective_event["assignment_id"],
                }
            )
        return results

    def _success_payload(
        self,
        *,
        matched_rule_ids: list[str],
        matched_rules: list[dict[str, Any]],
        assign_payload: dict[str, Any],
        assignment_results: list[dict[str, Any]],
        base_payload_template: Mapping[str, Any],
        full_audit: bool = False,
    ) -> dict[str, Any]:
        """Build the stable Spark result payload."""
        payload = dict(base_payload_template)
        payload.update(
            matched=bool(matched_rule_ids),
            matched_rule_ids=matched_rule_ids,
            assign=assign_payload,
        )
        if full_audit:
            payload.update(
                matched_rules=matched_rules,
                assignment_results=assignment_results,
            )
        return payload

    def _error_payload(
        self,
        exc: Exception,
        *,
        include_traceback: bool,
        base_payload_template: Mapping[str, Any],
        full_audit: bool = False,
    ) -> dict[str, Any]:
        """Build a compact production error, optionally with a debug traceback."""
        error = f"{type(exc).__name__}: {exc}"
        if include_traceback:
            error = f"{error}\n{traceback.format_exc()}"
        payload = dict(base_payload_template)
        payload.update(error=error, matched_rule_ids=[])
        if full_audit:
            payload.update(matched_rules=[], assignment_results=[])
        return payload

    def _base_payload(
        self,
        assign_field_names: Sequence[str],
        *,
        full_audit: bool = False,
    ) -> dict[str, Any]:
        """Return an immutable-by-convention template for result payloads."""
        payload = dict.fromkeys(result_field_names(full_audit=full_audit))
        payload.update(
            matched=False,
            matched_rule_ids=[],
            assign={
                field_name: {"applied": False, "value": None} for field_name in assign_field_names
            },
        )
        if full_audit:
            payload.update(matched_rules=[], assignment_results=[])
        return payload

    def _spark_assignment_value(self, value: Any, data_type: T.DataType) -> Any:
        """Return an assignment value compatible with the declared Spark type."""
        if value is None:
            return value
        if isinstance(data_type, T.StringType):
            return self._trace_text(value)
        if isinstance(data_type, INTEGRAL_TYPES):
            return self._spark_integral_value(value, data_type)
        if isinstance(data_type, (T.FloatType, T.DoubleType)):
            return self._spark_float_value(value)
        if isinstance(data_type, T.DecimalType):
            return self._spark_decimal_value(value, data_type)
        if isinstance(data_type, T.BooleanType):
            return self._spark_boolean_value(value)
        if isinstance(data_type, TIMESTAMP_TYPES):
            return datetime.fromisoformat(value) if isinstance(value, str) else value
        if isinstance(data_type, T.DateType):
            if isinstance(value, datetime):
                return value.date()
            return date.fromisoformat(value) if isinstance(value, str) else value
        if isinstance(data_type, T.ArrayType):
            if isinstance(value, set):
                value = sorted(value, key=repr)
            return [self._spark_assignment_value(item, data_type.elementType) for item in value]
        if isinstance(data_type, T.StructType):
            return self._spark_struct_value(value, data_type)
        return value

    def _spark_integral_value(
        self,
        value: Any,
        data_type: T.DataType,
    ) -> int:
        """Return a lossless, in-range Spark integral value."""
        if isinstance(value, bool):
            raise TypeError("Boolean assignment values are not integers.")
        converted = int(value)
        if isinstance(value, (float, Decimal)) and value != converted:
            raise ValueError(
                f"Assignment value {value!r} cannot be converted to an integer "
                "without losing its fractional component."
            )
        limits = INTEGRAL_LIMITS[type(data_type)]
        if not limits[0] <= converted <= limits[1]:
            raise OverflowError(
                f"Assignment value {value!r} is outside the range for {data_type.simpleString()}."
            )
        return converted

    def _spark_float_value(self, value: Any) -> float:
        """Return a finite Spark floating-point assignment value."""
        converted = float(value)
        if not math.isfinite(converted):
            raise ValueError("Floating-point assignment values must be finite.")
        return converted

    def _spark_decimal_value(
        self,
        value: Any,
        data_type: T.DecimalType,
    ) -> Decimal:
        """Return a Decimal that fits the declared Spark precision and scale."""
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal_value_fits(converted, data_type):
            raise ValueError(
                f"Assignment value {value!r} does not fit "
                f"{data_type.simpleString()} without rounding or overflow."
            )
        return converted

    def _spark_boolean_value(self, value: Any) -> bool:
        """Return a canonical Spark boolean assignment value."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.lower() == "true"
        raise TypeError(
            f"Assignment value {value!r} is not a boolean or a canonical 'true'/'false' string."
        )

    def _spark_struct_value(
        self,
        value: Any,
        data_type: T.StructType,
    ) -> dict[str, Any]:
        """Return a recursively coerced Spark struct assignment value."""
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Assignment value {value!r} must be a mapping for {data_type.simpleString()}."
            )
        return {
            field.name: self._spark_assignment_value(
                self._mapping_value(value, field.name),
                field.dataType,
            )
            for field in data_type.fields
        }

    def _values_changed(self, old_value: Any, proposed_value: Any) -> bool:
        """Compare audit values with null-safe equality semantics."""
        if old_value is None or proposed_value is None:
            return (old_value is None) != (proposed_value is None)
        return old_value != proposed_value

    def _mapping_value(self, value: Mapping[str, Any], field_name: str) -> Any:
        """Return a mapping value using Spark's stringified struct field name."""
        if field_name in value:
            return value[field_name]
        for key, item in value.items():
            if str(key) == field_name:
                return item
        return None

    def _spark_rule_trace(
        self,
        trace: RuleExecutionTrace,
        *,
        explanation: str | None,
    ) -> dict[str, Any]:
        """Convert a rule trace to the declared Spark struct schema."""
        return {
            "rule_id": trace.rule_id,
            "rule_name": trace.rule_name,
            "rule_order": trace.rule_order,
            "explanation": explanation,
            "assignments_applied": list(trace.assignments_applied),
            "conditions": [
                self._spark_condition_trace(condition) for condition in trace.condition_traces
            ],
        }

    def _spark_condition_trace(self, trace: ResolvedConditionTrace) -> dict[str, Any]:
        """Convert a condition trace to the declared Spark struct schema."""
        return {
            "condition_id": trace.condition_id,
            "condition_group_id": trace.condition_group_id,
            "condition_group_operator": trace.condition_group_operator,
            "active_flag": trace.active_flag,
            "columns": self._spark_condition_columns(trace),
            "left": self._spark_operand_trace(trace.left),
            "right": self._spark_operand_trace(trace.right),
            "operator": trace.operator,
            "comparison_result": trace.comparison_result,
            "passed": trace.passed,
            "tolerance_abs": self._non_default_trace_text(trace.tolerance_abs, "0"),
        }

    def _spark_condition_columns(self, trace: ResolvedConditionTrace) -> list[str]:
        """Return source columns referenced by a condition trace."""
        return self._unique_strings(
            [
                *self._operand_trace_columns(trace.left),
                *self._operand_trace_columns(trace.right),
            ]
        )

    def _operand_trace_columns(self, trace: Mapping[str, Any] | None) -> list[str]:
        """Return source columns from one operand trace."""
        if trace is None:
            return []
        return list(trace.get("columns", []))

    def _spark_operand_trace(self, trace: Any) -> dict[str, Any] | None:
        """Convert one operand trace to the declared Spark struct schema."""
        if not isinstance(trace, Mapping):
            return None
        column = trace.get("field_name")
        source_columns = trace.get("columns")
        if source_columns is None and column is not None:
            source_columns = [column]
        return {
            "kind": trace.get("kind"),
            "column": column,
            "target_field": trace.get("target_field"),
            "original_value": self._trace_text(trace.get("original_value")),
            "value": self._trace_text(trace.get("value")),
            "value_type": trace.get("value_type"),
            "default_if_null": self._trace_text(trace.get("default_if_null")),
            "default_applied": bool(trace.get("default_applied", False)),
            "function_name": trace.get("function_name"),
            "produced_by_rule_id": trace.get("produced_by_rule_id"),
            "produced_by_assignment_id": trace.get("produced_by_assignment_id"),
            "source_columns": [str(column) for column in source_columns or []],
            "arguments": self._trace_arguments(trace),
        }

    def _trace_arguments(self, payload: Mapping[str, Any]) -> dict[str, str | None] | None:
        """Return compact string arguments for custom-function operands."""
        args = payload.get("args")
        if not isinstance(args, Mapping) or not args:
            return None
        return {
            str(name): self._operand_trace_summary(value) or self._trace_text(value)
            for name, value in args.items()
        }

    def _non_default_trace_text(self, value: Any, default: Any) -> str | None:
        """Return trace text only when a trace value differs from its default."""
        if value in (None, default):
            return None
        return self._trace_text(value)

    def _trace_text(self, value: Any) -> str | None:
        """Convert arbitrary trace values to compact Spark string fields."""
        if value is None:
            return None
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, Enum):
            return str(value.value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Mapping):
            return ", ".join(f"{key}={self._trace_text(item)}" for key, item in value.items())
        if isinstance(value, set):
            value = sorted(value, key=repr)
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(self._trace_text(item) or "" for item in value) + "]"
        return str(value)
