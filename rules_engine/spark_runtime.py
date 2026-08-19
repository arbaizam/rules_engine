"""
Spark-facing rules engine runtime.

The runtime evaluates each input row with a Python UDF and returns Spark-native
struct columns so downstream Spark jobs can select nested fields directly.
Rules must reference row-level fields, literals, or registered custom
functions. Aggregate-style facts should be precomputed upstream and exposed as
ordinary input columns.
"""

from __future__ import annotations

import logging
import math
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.exceptions import ValidationFailedError
from rules_engine.models import (
    Assignment,
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
from rules_engine.runtime import SparkRowEvaluator
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
                if isinstance(
                    argument,
                    (FieldOperand, LiteralOperand, CustomFunctionOperand),
                ):
                    add_operand(argument)

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
        T.StructField("value", T.StringType(), True),
        T.StructField("value_type", T.StringType(), True),
        T.StructField("function_name", T.StringType(), True),
        T.StructField("source_columns", T.ArrayType(T.StringType(), False), True),
        T.StructField("arguments", T.MapType(T.StringType(), T.StringType(), True), True),
    ]
)

CONDITION_TRACE_STRUCT = T.StructType(
    [
        T.StructField("columns", T.ArrayType(T.StringType(), False), True),
        T.StructField("left", OPERAND_TRACE_STRUCT, True),
        T.StructField("right", OPERAND_TRACE_STRUCT, True),
        T.StructField("operator", T.StringType(), True),
        T.StructField("comparison_result", T.BooleanType(), True),
        T.StructField("passed", T.BooleanType(), True),
        T.StructField("tolerance_abs", T.StringType(), True),
        T.StructField("null_input_mode", T.StringType(), True),
        T.StructField("null_result_mode", T.StringType(), True),
        T.StructField("null_default_value", T.StringType(), True),
    ]
)

FIRST_MATCHED_RULE_TRACE_STRUCT = T.StructType(
    [
        T.StructField("rule_id", T.StringType(), True),
        T.StructField("rule_name", T.StringType(), True),
        T.StructField("rule_order", T.LongType(), True),
        T.StructField("explanation", T.StringType(), True),
        T.StructField("assignments_applied", T.ArrayType(T.StringType(), False), True),
        T.StructField("conditions", T.ArrayType(CONDITION_TRACE_STRUCT, False), True),
    ]
)

MATCHED_RULE_SUMMARY_STRUCT = T.StructType(
    [
        T.StructField("rule_id", T.StringType(), True),
        T.StructField("rule_name", T.StringType(), True),
        T.StructField("rule_order", T.LongType(), True),
        T.StructField("human_readable_condition", T.StringType(), True),
        T.StructField("human_readable_assignment", T.StringType(), True),
        T.StructField("assigned_fields", T.ArrayType(T.StringType(), False), False),
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
        T.StructField("effective", T.BooleanType(), False),
        T.StructField("overridden_by_rule_id", T.StringType(), True),
        T.StructField("overridden_by_assignment_id", T.StringType(), True),
    ]
)


@dataclass(frozen=True)
class _PreparedRule:
    """Driver-precomputed rule metadata captured by the worker evaluator."""

    rule: Rule
    has_custom_condition: bool
    assignment_specs: tuple[tuple[Assignment, str | None], ...]
    summary: dict[str, Any] | None


COMPACT_RESULT_FIELD_NAMES = (
    "error",
    "matched",
    "matched_rule_ids",
    "assign",
)
FULL_AUDIT_ONLY_RESULT_FIELD_NAMES = (
    "matched_rules",
    "first_matched_rule_trace",
    "assignment_results",
)
FULL_AUDIT_RESULT_FIELD_NAMES = (
    *COMPACT_RESULT_FIELD_NAMES,
    *FULL_AUDIT_ONLY_RESULT_FIELD_NAMES,
)


def _result_struct(
    assign_schema: T.StructType,
    *,
    full_audit: bool = False,
) -> T.StructType:
    """Build the Spark UDF result schema for the requested audit detail."""
    _require_bool("full_audit", full_audit)
    compact_fields = [
        T.StructField("error", T.StringType(), True),
        T.StructField("matched", T.BooleanType(), False),
        T.StructField("matched_rule_ids", T.ArrayType(T.StringType(), False), False),
        T.StructField("assign", assign_schema, True),
    ]
    full_audit_only_fields = [
        T.StructField(
            "matched_rules",
            T.ArrayType(MATCHED_RULE_SUMMARY_STRUCT, False),
            False,
        ),
        T.StructField(
            "first_matched_rule_trace",
            FIRST_MATCHED_RULE_TRACE_STRUCT,
            True,
        ),
        T.StructField(
            "assignment_results",
            T.ArrayType(ASSIGNMENT_RESULT_STRUCT, False),
            False,
        ),
    ]
    return T.StructType(
        compact_fields + (full_audit_only_fields if full_audit else [])
    )


def _require_bool(name: str, value: Any) -> None:
    """Reject truthy non-booleans for public boolean options."""
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, not {type(value).__name__}.")


def result_field_names(
    *,
    full_audit: bool = False,
) -> tuple[str, ...]:
    """Return result field names emitted for the requested audit detail."""
    _require_bool("full_audit", full_audit)
    return (
        FULL_AUDIT_RESULT_FIELD_NAMES
        if full_audit
        else COMPACT_RESULT_FIELD_NAMES
    )


EMPTY_ASSIGN_STRUCT = T.StructType(
    [
        T.StructField("__empty", T.StringType(), True),
    ]
)

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
            compatibility_validator
            or SparkRulesetCompatibilityValidator(function_registry)
        )

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Load published metadata through the configured repository."""
        return self._repository.load_published(ruleset_name, version)

    def evaluate_dataframe(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> DataFrame:
        """
        Evaluate a Spark DataFrame against a ruleset.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Incoming rows. Any cross-row facts must already exist as columns.
        ruleset : Ruleset
            Ruleset metadata to evaluate. Direct runtime callers are responsible
            for base metadata validation; published rulesets loaded through the
            service have already passed that gate. This method always performs
            incoming-schema compatibility validation before building the UDF.
        column_prefix : str, default "rules_engine"
            Prefix for appended output columns.
        fail_on_error : bool, default True
            Raise from the worker during the caller's first materializing Spark
            action when a row-level evaluator error is produced. Building the
            returned DataFrame does not start a hidden validation job.
        include_error_traceback : bool, default False
            Include full Python tracebacks in row error payloads. Keep disabled
            for production data because tracebacks substantially enlarge rows.
        full_audit : bool, default False
            Add ordered match summaries, the detailed first-match trace, and
            assignment provenance. Immutable ruleset/engine identity is always
            appended as driver-side literal columns.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with rules engine result columns appended.
        """
        _require_bool("full_audit", full_audit)
        if not column_prefix:
            raise ValueError("column_prefix must be non-empty.")
        output_names = {
            *{
                f"{column_prefix}_{field_name}"
                for field_name in FULL_AUDIT_RESULT_FIELD_NAMES
            },
            f"{column_prefix}_ruleset",
            f"{column_prefix}_engine_version",
            f"{column_prefix}_result",
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
        compatibility = self._compatibility_validator.validate_schema(
            ruleset,
            df.schema,
        )
        if compatibility.has_errors():
            raise ValidationFailedError(
                "Ruleset is incompatible with the incoming Spark schema.\n"
                + compatibility.to_text()
            )
        assign_schema = self._assignment_schema(ruleset, df.schema)
        assign_field_names = [field.name for field in assign_schema.fields]
        assign_field_types = {
            field.name: field.dataType
            for field in assign_schema.fields
        }

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
        result_col = f"{column_prefix}_result"
        evaluated = df.withColumn(result_col, result_udf(row_struct))
        result = F.col(result_col)
        output = evaluated.withColumns(
            {
                f"{column_prefix}_{field_name}": result.getField(field_name)
                for field_name in result_field_names(full_audit=full_audit)
            }
            | {
                f"{column_prefix}_ruleset": F.struct(
                    F.lit(ruleset.ruleset_id).alias("id"),
                    F.lit(ruleset.version).alias("version"),
                    F.lit(DeltaRowSerializer().content_hash(ruleset)).alias(
                        "content_hash"
                    ),
                ),
                f"{column_prefix}_engine_version": F.lit(__version__),
            }
        )
        output = output.drop(result_col)
        logger.info(
            "Spark runtime evaluation DataFrame built: ruleset_id=%s version=%s output_prefix=%s",
            ruleset.ruleset_id,
            ruleset.version,
            column_prefix,
        )
        return output

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
        assign_schema = self._compatibility_validator.assignment_schema(
            ruleset,
            source_schema,
        )
        if not assign_schema.fields:
            return EMPTY_ASSIGN_STRUCT
        return assign_schema

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
        runtime = _SparkRowUdfEvaluator.for_embedded_ruleset(
            self._function_registry
        )
        ordered_rules = sorted(ruleset.rules, key=lambda item: item.rule_order)
        active_rules: list[_PreparedRule] = []
        for rule in (item for item in ordered_rules if item.active_flag):
            description = (
                runtime._rule_formatter.describe_rule(rule)
                if full_audit
                else None
            )
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
            summary = None
            if description is not None:
                summary = {
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "rule_order": rule.rule_order,
                    "human_readable_condition": description["rule_logic"],
                    "human_readable_assignment": description["match_payload"],
                    "assigned_fields": [
                        assignment.target_field
                        for assignment in rule.assignments
                    ],
                }
            active_rules.append(
                _PreparedRule(
                    rule=rule,
                    has_custom_condition=runtime._rule_has_custom_condition(rule),
                    assignment_specs=assignment_specs,
                    summary=summary,
                )
            )
        base_payload_template = runtime._base_payload(full_audit=full_audit)

        def evaluate(row: Any) -> dict[str, Any]:
            """Evaluate one Spark row struct and return the declared result struct."""
            try:
                row_dict = row.asDict(recursive=True)
                matched_rule_ids: list[str] = []
                matched_rules: list[dict[str, Any]] = []
                assignments: dict[str, Any] = {}
                assignment_events: list[dict[str, Any]] = []
                first_matched_rule_trace: dict[str, Any] | None = None
                for prepared_rule in active_rules:
                    rule = prepared_rule.rule
                    condition_traces = None
                    # Every active condition remains eager until stop_on_match,
                    # preserving row errors. Full audit reevaluates a pure first
                    # match only; custom first matches stay single-pass.
                    if (
                        full_audit
                        and first_matched_rule_trace is None
                        and prepared_rule.has_custom_condition
                    ):
                        matched, condition_traces = runtime._evaluate_rule(
                            rule,
                            row_dict,
                        )
                    else:
                        matched = runtime._rule_matches(rule, row_dict)
                    if matched:
                        matched_rule_ids.append(rule.rule_id)
                        if full_audit:
                            matched_rules.append(prepared_rule.summary)
                        if (
                            full_audit
                            and first_matched_rule_trace is None
                        ):
                            if condition_traces is None:
                                _, condition_traces = runtime._evaluate_rule(
                                    rule,
                                    row_dict,
                                )
                            explanation = runtime._matched_rule_explanation_from_trace(
                                rule,
                                condition_traces,
                            )
                            first_matched_rule_trace = runtime._spark_rule_trace(
                                runtime._rule_execution_trace(
                                    rule,
                                    matched,
                                    condition_traces,
                                ),
                                explanation=explanation,
                            )
                        for assignment, authored_expression in (
                            prepared_rule.assignment_specs
                        ):
                            if full_audit:
                                event = runtime._typed_assignment_event(
                                    rule,
                                    assignment,
                                    authored_expression,
                                    row_dict,
                                    assign_field_types[assignment.target_field],
                                )
                                assignments[assignment.target_field] = event[
                                    "proposed_value"
                                ]
                                assignment_events.append(event)
                            else:
                                assignments[assignment.target_field] = (
                                    runtime._spark_assignment_value(
                                        runtime._resolve_operand(assignment.value, row_dict),
                                        assign_field_types[assignment.target_field],
                                    )
                                )
                        if rule.stop_on_match:
                            break
                assignment_results = (
                    runtime._assignment_results(assignment_events)
                    if full_audit
                    else []
                )
                assign_payload = (
                    {
                        field_name: assignments.get(field_name)
                        for field_name in assign_field_names
                    }
                    if assignments
                    else None
                )
                return runtime._success_payload(
                    matched_rule_ids=matched_rule_ids,
                    matched_rules=matched_rules,
                    assign_payload=assign_payload,
                    first_matched_rule_trace=first_matched_rule_trace,
                    assignment_results=assignment_results,
                    full_audit=full_audit,
                    base_payload_template=base_payload_template,
                )
            except Exception as exc:
                if raise_on_error:
                    raise RuntimeError(
                        f"Rules engine row evaluation failed with "
                        f"{type(exc).__name__}: {exc}"
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
    ) -> dict[str, Any]:
        """Resolve and normalize both sides of one assignment audit event."""
        proposed_value = self._resolve_operand(assignment.value, row)
        return {
            "assignment_id": assignment.assignment_id,
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "rule_order": rule.rule_order,
            "target_field": assignment.target_field,
            "authored_expression": authored_expression,
            "old_value": self._spark_assignment_value(
                row.get(assignment.target_field),
                data_type,
            ),
            "proposed_value": self._spark_assignment_value(
                proposed_value,
                data_type,
            ),
        }

    def _assignment_results(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build ordered provenance and override metadata for assignments."""
        effective_event_indexes = {
            event["target_field"]: index for index, event in enumerate(events)
        }
        results: list[dict[str, Any]] = []
        for index, event in enumerate(events):
            effective_index = effective_event_indexes[event["target_field"]]
            effective_event = events[effective_index]
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
                    "effective": effective,
                    "overridden_by_rule_id": (
                        None if effective else effective_event["rule_id"]
                    ),
                    "overridden_by_assignment_id": (
                        None if effective else effective_event["assignment_id"]
                    ),
                }
            )
        return results

    def _success_payload(
        self,
        *,
        matched_rule_ids: list[str],
        matched_rules: list[dict[str, Any]],
        assign_payload: dict[str, Any] | None,
        first_matched_rule_trace: dict[str, Any] | None,
        assignment_results: list[dict[str, Any]],
        full_audit: bool = False,
        base_payload_template: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the stable Spark result payload."""
        payload = dict(
            base_payload_template
            if base_payload_template is not None
            else self._base_payload(full_audit=full_audit)
        )
        payload.update(
            matched=bool(matched_rule_ids),
            matched_rule_ids=matched_rule_ids,
            assign=assign_payload,
        )
        if full_audit:
            payload.update(
                matched_rules=matched_rules,
                first_matched_rule_trace=first_matched_rule_trace,
                assignment_results=assignment_results,
            )
        return payload

    def _error_payload(
        self,
        exc: Exception,
        *,
        include_traceback: bool,
        full_audit: bool = False,
        base_payload_template: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a compact production error, optionally with a debug traceback."""
        error = f"{type(exc).__name__}: {exc}"
        if include_traceback:
            error = f"{error}\n{traceback.format_exc()}"
        payload = dict(
            base_payload_template
            if base_payload_template is not None
            else self._base_payload(full_audit=full_audit)
        )
        for field_name in (
            "matched_rule_ids",
            "matched_rules",
            "assignment_results",
        ):
            if field_name in payload:
                payload[field_name] = list(payload[field_name])
        payload["error"] = error
        return payload

    def _base_payload(
        self,
        *,
        full_audit: bool = False,
    ) -> dict[str, Any]:
        """Return schema-derived defaults shared by success and error rows."""
        payload = dict.fromkeys(result_field_names(full_audit=full_audit))
        payload.update(
            matched=False,
            matched_rule_ids=[],
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
            return (
                datetime.fromisoformat(value)
                if isinstance(value, str)
                else value
            )
        if isinstance(data_type, T.DateType):
            if isinstance(value, datetime):
                return value.date()
            return date.fromisoformat(value) if isinstance(value, str) else value
        if isinstance(data_type, T.ArrayType):
            if isinstance(value, set):
                value = sorted(value, key=repr)
            return [
                self._spark_assignment_value(item, data_type.elementType)
                for item in value
            ]
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
                f"Assignment value {value!r} is outside the range for "
                f"{data_type.simpleString()}."
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
            f"Assignment value {value!r} is not a boolean or a canonical "
            "'true'/'false' string."
        )

    def _spark_struct_value(
        self,
        value: Any,
        data_type: T.StructType,
    ) -> dict[str, Any]:
        """Return a recursively coerced Spark struct assignment value."""
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Assignment value {value!r} must be a mapping for "
                f"{data_type.simpleString()}."
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
                self._spark_condition_trace(condition)
                for condition in trace.condition_traces
            ],
        }

    def _spark_condition_trace(self, trace: ResolvedConditionTrace) -> dict[str, Any]:
        """Convert a condition trace to the declared Spark struct schema."""
        return {
            "columns": self._spark_condition_columns(trace),
            "left": self._spark_operand_trace(trace.left),
            "right": self._spark_operand_trace(trace.right),
            "operator": trace.operator,
            "comparison_result": trace.comparison_result,
            "passed": trace.passed,
            "tolerance_abs": self._non_default_trace_text(trace.tolerance_abs, "0"),
            "null_input_mode": self._non_default_trace_value(
                trace.null_input_mode,
                "propagate",
            ),
            "null_result_mode": self._non_default_trace_value(
                trace.null_result_mode,
                "null",
            ),
            "null_default_value": self._trace_text(trace.null_default_value),
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
        column = trace.get("column") or trace.get("field_name")
        source_columns = trace.get("source_columns") or trace.get("columns")
        if source_columns is None and column is not None:
            source_columns = [column]
        return {
            "kind": trace.get("kind"),
            "column": column,
            "value": self._trace_text(trace.get("value")),
            "value_type": trace.get("value_type"),
            "function_name": trace.get("function_name"),
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

    def _non_default_trace_value(self, value: Any, default: Any) -> Any | None:
        """Return a trace value only when it differs from its default."""
        if value in (None, default):
            return None
        return value

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
            return ", ".join(
                f"{key}={self._trace_text(item)}"
                for key, item in value.items()
            )
        if isinstance(value, set):
            value = sorted(value, key=repr)
        if isinstance(value, (list, tuple)):
            return "[" + ", ".join(self._trace_text(item) or "" for item in value) + "]"
        return str(value)
