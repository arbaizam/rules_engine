"""
Spark-facing rules engine runtime.

The runtime evaluates each input row with a Python UDF and returns Spark-native
struct columns so downstream Spark jobs can select nested fields directly.
Rules use original row fields, committed lower-order assignments, literals, or
registered custom functions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import struct
import traceback
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import uuid4

from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.dataframe_evaluation import DataFrameEvaluation, top_level_column
from rules_engine.exceptions import ValidationFailedError
from rules_engine.models import (
    Assignment,
    FieldOperand,
    ResolvedConditionTrace,
    Rule,
    RuleExecutionTrace,
    Ruleset,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_types import (
    INTEGRAL_LIMITS,
    INTEGRAL_TYPES,
    TIMESTAMP_NTZ_TYPE,
    TIMESTAMP_TYPES,
    decimal_value_fits,
)
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.traversal import iter_conditions, iter_operand_tree, iter_rules
from rules_engine.version import __version__

logger = logging.getLogger(__name__)


def required_source_columns(ruleset: Ruleset) -> tuple[str, ...]:
    """Return source columns required to evaluate active rules.

    Columns are returned once, in rule evaluation and operand traversal order.
    Inactive rules and conditions are excluded because their values are never
    resolved against an input row.
    """
    columns: list[str] = []
    for rule in iter_rules(ruleset, active_only=True, ordered=True):
        roots = [
            operand
            for condition in iter_conditions(rule.root_group, active_only=True)
            for operand in (condition.left, condition.right)
            if operand is not None
        ]
        roots.extend(assignment.value for assignment in rule.assignments)
        for root in roots:
            columns.extend(
                operand.field_name
                for operand in iter_operand_tree(root)
                if isinstance(operand, FieldOperand)
            )
    return tuple(dict.fromkeys(columns))


def _source_column(column_name: str) -> Column:
    """Return a top-level Spark column with its literal source name preserved."""
    return top_level_column(column_name).alias(column_name)


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
        evaluated, assign_schema, manifest = self._evaluate_attached_dataframe(
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
            function_dependencies=manifest,
        )

    def evaluate_attached_dataframe(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> tuple[DataFrame, T.StructType]:
        """Build a source-plus-results plan for package integrations."""
        evaluated, assign_schema, _ = self._evaluate_attached_dataframe(
            df,
            ruleset,
            column_prefix=column_prefix,
            fail_on_error=fail_on_error,
            include_error_traceback=include_error_traceback,
            full_audit=full_audit,
        )
        return evaluated, assign_schema

    def _evaluate_attached_dataframe(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        column_prefix: str,
        fail_on_error: bool,
        include_error_traceback: bool,
        full_audit: bool,
    ) -> tuple[DataFrame, T.StructType, str]:
        """Build one plan and retain its canonical function manifest on the driver."""
        _require_bool("full_audit", full_audit)
        _require_bool("fail_on_error", fail_on_error)
        _require_bool("include_error_traceback", include_error_traceback)
        if not column_prefix:
            raise ValueError("column_prefix must be non-empty.")
        output_names = {
            *{f"{column_prefix}_{field_name}" for field_name in FULL_AUDIT_RESULT_FIELD_NAMES},
            f"{column_prefix}_ruleset",
            f"{column_prefix}_engine_version",
        }
        output_names_by_case = {name.casefold() for name in output_names}
        conflicts = sorted(
            {
                column_name
                for column_name in df.columns
                if column_name.casefold() in output_names_by_case
            }
        )
        if conflicts:
            raise ValueError(
                f"Input contains rules-engine output columns for prefix "
                f"{column_prefix!r}: {conflicts}"
            )
        logger.info(
            "Evaluating ruleset in Spark runtime: ruleset_id=%s ruleset_name=%s "
            "version=%s rule_count=%s fail_on_error=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            len(ruleset.rules),
            fail_on_error,
        )
        prepared_schema = self._compatibility_validator.prepare(
            ruleset,
            df.schema,
        )
        validation = prepared_schema.validation
        if validation.has_errors():
            raise ValidationFailedError(
                "Ruleset validation failed for Spark evaluation.\n" + validation.to_text()
            )
        assign_schema = prepared_schema.assignment_schema
        assign_field_names = [field.name for field in assign_schema.fields]
        assign_field_types = {field.name: field.dataType for field in assign_schema.fields}

        row_evaluator = self._build_row_evaluator(
            ruleset,
            assign_field_names,
            assign_field_types,
            raise_on_error=fail_on_error,
            include_error_traceback=include_error_traceback,
            full_audit=full_audit,
            source_schema=df.schema,
        )
        self.validate_worker_serializable(row_evaluator)
        result_udf = F.udf(
            row_evaluator,
            _result_struct(assign_schema, full_audit=full_audit),
            # This is a row-oriented, nested-struct UDF rather than a vectorized UDF.
            useArrow=False,
        )
        available_columns = set(df.columns)
        serialized_columns = [
            column_name
            for column_name in dict.fromkeys(
                (
                    *prepared_schema.required_source_columns,
                    *(assign_field_names if full_audit else ()),
                )
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
        manifest = json.dumps(
            self._function_registry.dependency_manifest(ruleset),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        output = self._append_output_columns(
            evaluated,
            result_col=result_col,
            column_prefix=column_prefix,
            ruleset=ruleset,
            full_audit=full_audit,
            function_dependencies=manifest,
        )
        logger.info(
            "Spark runtime evaluation DataFrame built: ruleset_id=%s version=%s output_prefix=%s",
            ruleset.ruleset_id,
            ruleset.version,
            column_prefix,
        )
        return output, assign_schema, manifest

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
        normalized_counts = Counter(column_name.casefold() for column_name in normalized)
        duplicates = sorted(
            {
                column_name
                for column_name in normalized
                if normalized_counts[column_name.casefold()] > 1
            }
        )
        if duplicates:
            raise ValueError(f"key_columns contains duplicate names: {duplicates}")
        input_counts = Counter(column_name.casefold() for column_name in df.columns)
        ambiguous_source_columns = sorted(
            {column_name for column_name in df.columns if input_counts[column_name.casefold()] > 1}
        )
        if ambiguous_source_columns:
            raise ValueError(
                "Input DataFrame contains ambiguous column names under Spark's "
                f"case-insensitive resolver: {ambiguous_source_columns}"
            )
        missing = sorted(set(normalized) - set(df.columns))
        if missing:
            raise ValueError(f"key_columns are missing from the input DataFrame: {missing}")
        assignment_targets_by_case = {
            assignment.target_field.casefold()
            for rule in ruleset.rules
            if rule.active_flag
            for assignment in rule.assignments
        }
        assigned_keys = sorted(
            column_name
            for column_name in normalized
            if column_name.casefold() in assignment_targets_by_case
        )
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
        function_dependencies: str,
    ) -> DataFrame:
        """Append the public result contract in its documented column order."""
        result = F.col(result_col)
        output_columns = {
            f"{column_prefix}_{field_name}": result.getField(field_name)
            for field_name in result_field_names(full_audit=full_audit)
        }
        manifest = function_dependencies
        output_columns[f"{column_prefix}_ruleset"] = F.struct(
            F.lit(ruleset.ruleset_id).alias("id"),
            F.lit(ruleset.version).alias("version"),
            F.lit(DeltaRowSerializer().content_hash(ruleset)).alias("content_hash"),
            *([F.lit(manifest).alias("function_dependencies")] if full_audit else []),
            F.lit(hashlib.sha256(manifest.encode("utf-8")).hexdigest()).alias(
                "function_dependencies_hash"
            ),
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

    def _build_row_evaluator(
        self,
        ruleset: Ruleset,
        assign_field_names: list[str],
        assign_field_types: Mapping[str, T.DataType],
        *,
        raise_on_error: bool = False,
        include_error_traceback: bool = False,
        full_audit: bool = False,
        source_schema: T.StructType | None = None,
    ):
        """Adapt the shared row executor to Spark's input and result schemas."""
        _require_bool("full_audit", full_audit)
        _require_bool("raise_on_error", raise_on_error)
        _require_bool("include_error_traceback", include_error_traceback)
        runtime = _SparkRowUdfEvaluator(self._function_registry)
        prepared = runtime._prepare_ruleset(ruleset)
        # Retain only the snapshot and referenced bindings in the worker closure.
        runtime._prepared_ruleset = None
        runtime._prepared_source = None
        runtime._function_bindings_by_id = None
        runtime._function_registry = FunctionRegistry()
        authored_expressions = (
            {
                assignment.assignment_id: runtime._rule_formatter.format_assignment_expression(
                    assignment
                )
                for rule in prepared.active_rules
                for assignment in rule.assignments
            }
            if full_audit
            else {}
        )
        source_types = {
            field.name: field.dataType for field in (source_schema or T.StructType()).fields
        }
        base_payload_template = runtime._base_payload(assign_field_names, full_audit=full_audit)

        def normalize_assignment(assignment: Assignment, value: Any) -> Any:
            return runtime._spark_assignment_value(
                value, assign_field_types[assignment.target_field]
            )

        def evaluate(row: Any) -> dict[str, Any]:
            """Convert inputs, execute once, and capture all output conversion failures."""
            try:
                row_dict = {
                    name: runtime._spark_input_value(value, source_types.get(name))
                    for name, value in row.asDict(recursive=True).items()
                }
                matched_rules: list[dict[str, Any]] = []
                assignment_events: list[dict[str, Any]] = []

                def on_rule_matched(rule: Rule, condition_traces: Any) -> None:
                    matched_rules.append(
                        runtime._spark_rule_trace(
                            runtime._rule_execution_trace(rule, condition_traces),
                            explanation=runtime._matched_rule_explanation_from_trace(
                                rule, condition_traces
                            ),
                        )
                    )

                def on_assignment(
                    rule: Rule, assignment: Assignment, old_value: Any, proposed_value: Any
                ) -> None:
                    target_type = assign_field_types[assignment.target_field]
                    if isinstance(target_type, T.DateType) and isinstance(old_value, datetime):
                        old_value = old_value.date()
                    assignment_events.append(
                        {
                            "assignment_id": assignment.assignment_id,
                            "rule_id": rule.rule_id,
                            "rule_name": rule.rule_name,
                            "rule_order": rule.rule_order,
                            "target_field": assignment.target_field,
                            "authored_expression": authored_expressions[assignment.assignment_id],
                            # The existing source is evidence, not a new assignment. In
                            # particular, auditing NaN must not prevent a valid overwrite.
                            # Render immediately so later user code cannot mutate
                            # historical evidence through a shared list or mapping.
                            "old_value": runtime._trace_text(old_value),
                            "proposed_value": runtime._trace_text(proposed_value),
                            "changed": runtime._values_changed(old_value, proposed_value),
                        }
                    )

                result = runtime._execute_prepared(
                    prepared,
                    row_dict,
                    full_audit=full_audit,
                    normalize_assignment=normalize_assignment,
                    on_rule_matched=on_rule_matched if full_audit else None,
                    on_assignment=on_assignment if full_audit else None,
                )
                return runtime._success_payload(
                    matched_rule_ids=result.matched_rule_ids,
                    matched_rules=matched_rules,
                    assign_payload={
                        field_name: {
                            "applied": field_name in result.assignments,
                            "value": result.assignments.get(field_name),
                        }
                        for field_name in assign_field_names
                    },
                    assignment_results=(
                        runtime._assignment_results(assignment_events) if full_audit else []
                    ),
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
                    "old_value": event["old_value"],
                    "proposed_value": event["proposed_value"],
                    "changed": event["changed"],
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
        payload.update(
            error=error,
            matched_rule_ids=[],
            assign={
                field_name: dict(assignment)
                for field_name, assignment in base_payload_template["assign"].items()
            },
        )
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

    def _spark_input_value(self, value: Any, data_type: T.DataType | None) -> Any:
        """Restore timestamp instants lost from the naive Python UDF representation.

        Spark's TimestampType decoder uses datetime.fromtimestamp in the worker's
        local timezone. astimezone reverses that exact convention, including DST
        folds. TimestampNTZ values remain wall-clock datetimes without a timezone.
        """
        if value is None:
            return None
        if isinstance(data_type, T.TimestampType):
            return value.astimezone(timezone.utc)
        if isinstance(data_type, T.StructType):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return {
                field.name: self._spark_input_value(value.get(field.name), field.dataType)
                for field in data_type.fields
            }
        if isinstance(data_type, T.ArrayType):
            return [self._spark_input_value(item, data_type.elementType) for item in value]
        if isinstance(data_type, T.MapType):
            return {
                self._hashable_map_key(
                    self._spark_input_value(key, data_type.keyType), data_type.keyType
                ): self._spark_input_value(item, data_type.valueType)
                for key, item in value.items()
            }
        return value

    def _spark_assignment_value(
        self, value: Any, data_type: T.DataType, *, nullable: bool = True
    ) -> Any:
        """Return an assignment value compatible with the declared Spark type."""
        if value is None:
            if not nullable:
                raise ValueError(
                    f"Null assignment value is forbidden for {data_type.simpleString()}."
                )
            return value
        if isinstance(data_type, T.StringType):
            return self._trace_text(value)
        if isinstance(data_type, INTEGRAL_TYPES):
            return self._spark_integral_value(value, data_type)
        if isinstance(data_type, (T.FloatType, T.DoubleType)):
            return self._spark_float_value(value, data_type)
        if isinstance(data_type, T.DecimalType):
            return self._spark_decimal_value(value, data_type)
        if isinstance(data_type, T.BooleanType):
            return self._spark_boolean_value(value)
        if isinstance(data_type, (*TIMESTAMP_TYPES, T.DateType)):
            return self._spark_temporal_value(value, data_type)
        if isinstance(data_type, T.ArrayType):
            if not isinstance(value, (list, tuple, set)):
                raise TypeError("Array assignment values must be a list, tuple, or set.")
            if isinstance(value, set):
                value = sorted(value, key=repr)
            return [
                self._spark_assignment_value(
                    item, data_type.elementType, nullable=data_type.containsNull
                )
                for item in value
            ]
        if isinstance(data_type, T.StructType):
            return self._spark_struct_value(value, data_type)
        if isinstance(data_type, T.MapType):
            return self._spark_map_value(value, data_type)
        return self._spark_other_value(value, data_type)

    def _spark_temporal_value(self, value: Any, data_type: T.DataType) -> date | datetime:
        """Validate temporal values before Spark's external serializer sees them."""
        if isinstance(data_type, T.DateType):
            if isinstance(value, str):
                value = date.fromisoformat(value)
            if isinstance(value, datetime):
                value = value.date()
            if not isinstance(value, date):
                raise TypeError("Date assignment values must be dates or ISO date strings.")
            return value
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise TypeError(
                "Timestamp assignment values must be datetimes or ISO timestamp strings."
            )
        if TIMESTAMP_NTZ_TYPE is not None and isinstance(data_type, TIMESTAMP_NTZ_TYPE):
            if value.utcoffset() is not None:
                raise TypeError("TimestampNTZ assignments cannot contain timezone-aware values.")
            return value
        if value.utcoffset() is None:
            raise TypeError(
                "Timestamp assignments require a UTC offset; use TimestampNTZ for wall-clock values."
            )
        return value.astimezone(timezone.utc)

    def _spark_map_value(self, value: Any, data_type: T.MapType) -> dict[Any, Any]:
        """Validate both sides of map entries, including non-null map keys."""
        if not isinstance(value, Mapping):
            raise TypeError("Map assignment values must be mappings.")
        converted: dict[Any, Any] = {}
        for key, item in value.items():
            converted_key = self._hashable_map_key(
                self._spark_assignment_value(key, data_type.keyType, nullable=False),
                data_type.keyType,
            )
            if converted_key in converted:
                raise ValueError("Map assignment keys collide after Spark type conversion.")
            converted[converted_key] = self._spark_assignment_value(
                item, data_type.valueType, nullable=data_type.valueContainsNull
            )
        return converted

    def _hashable_map_key(self, value: Any, data_type: T.DataType) -> Any:
        """Keep composite map keys hashable while retaining Spark's schema order.

        Row.asDict does not recurse into map keys. Struct keys therefore arrive
        as Rows; normalization must restore a Row after converting its fields.
        Arrays and binary fields nested inside keys use tuples and bytes, which
        are accepted by Spark's converters and remain hashable inside a Row.
        """
        if value is None:
            return None
        if isinstance(data_type, T.StructType):
            if isinstance(value, Row):
                value = value.asDict(recursive=True)
            return Row(*data_type.fieldNames())(
                *(
                    self._hashable_map_key(value[field.name], field.dataType)
                    for field in data_type.fields
                )
            )
        if isinstance(data_type, T.ArrayType):
            return tuple(self._hashable_map_key(item, data_type.elementType) for item in value)
        if isinstance(data_type, T.BinaryType):
            return bytes(value)
        if isinstance(data_type, T.MapType):
            raise TypeError("Spark map keys cannot contain map values.")
        return value

    def _spark_other_value(self, value: Any, data_type: T.DataType) -> Any:
        """Check less common Spark types without leaving conversion errors to Spark."""
        if isinstance(data_type, T.BinaryType):
            if not isinstance(value, (bytes, bytearray)):
                raise TypeError("Binary assignment values must be bytes or bytearrays.")
            return value
        if isinstance(data_type, T.DayTimeIntervalType):
            if not isinstance(value, timedelta):
                raise TypeError("Day-time interval assignment values must be timedeltas.")
        elif data_type.typeName() == "time":
            if not isinstance(value, time) or value.utcoffset() is not None:
                raise TypeError("Time assignment values must be timezone-free times.")
        elif isinstance(data_type, T.UserDefinedType):
            if getattr(value, "__UDT__", None) != data_type:
                raise TypeError(f"Assignment value does not implement {data_type.simpleString()}.")
        elif not data_type.needConversion():
            raise TypeError(f"Unsupported assignment value type: {data_type.simpleString()}.")
        # Specialized Spark types own their conversion checks. Run these inside
        # our error boundary; Spark will repeat conversion when writing the UDF.
        internal_value = data_type.toInternal(value)
        if isinstance(data_type, T.DayTimeIntervalType):
            if not -(2**63) <= internal_value <= 2**63 - 1:
                raise OverflowError(
                    "Day-time interval assignments must fit signed 64-bit microseconds."
                )
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

    def _spark_float_value(self, value: Any, data_type: T.DataType) -> float:
        """Return a finite value, rounding FloatType assignments to binary32."""
        try:
            converted = float(value)
        except OverflowError as exc:
            raise OverflowError(
                f"Assignment value {value!r} is outside the range for {data_type.simpleString()}."
            ) from exc
        if not math.isfinite(converted):
            raise ValueError("Floating-point assignment values must be finite.")
        if isinstance(data_type, T.FloatType):
            if abs(converted) > float.fromhex("0x1.fffffep+127"):
                raise OverflowError(f"Assignment value {value!r} is outside the range for float.")
            # Use the same binary32 value downstream rules will observe in Spark.
            converted = struct.unpack("!f", struct.pack("!f", converted))[0]
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
        if isinstance(value, Row):
            value = value.asDict(recursive=True)
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Assignment value {value!r} must be a mapping for {data_type.simpleString()}."
            )
        normalized_names = [str(key) for key in value]
        if len(set(normalized_names)) != len(normalized_names):
            raise ValueError("Struct assignment field names collide after string conversion.")
        unexpected = sorted(set(normalized_names) - set(data_type.fieldNames()))
        if unexpected:
            raise ValueError(f"Struct assignment has unexpected fields: {unexpected}.")
        return {
            field.name: self._spark_assignment_value(
                self._mapping_value(value, field.name),
                field.dataType,
                nullable=field.nullable,
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
