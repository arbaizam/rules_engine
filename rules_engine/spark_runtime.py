"""
Spark-facing rules engine runtime.

The runtime evaluates each input row with a Python UDF and returns Spark-native
struct columns so downstream Spark jobs can select nested fields directly.
Rules must reference row-level fields, literals, or registered custom
functions. Aggregate-style facts should be precomputed upstream and exposed as
ordinary input columns.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import logging
import traceback
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.models import (
    Assignment,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Ruleset,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository
from rules_engine.runtime import SparkRowEvaluator


logger = logging.getLogger(__name__)


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

WINNING_RULE_TRACE_STRUCT = T.StructType(
    [
        T.StructField("rule_id", T.StringType(), True),
        T.StructField("rule_name", T.StringType(), True),
        T.StructField("matched", T.BooleanType(), True),
        T.StructField("assignments_applied", T.ArrayType(T.StringType(), False), True),
        T.StructField("conditions", T.ArrayType(CONDITION_TRACE_STRUCT, False), True),
    ]
)


def _result_struct(assign_schema: T.StructType) -> T.StructType:
    """Build the Spark UDF result schema for one ruleset."""
    return T.StructType(
        [
            T.StructField("matched", T.BooleanType(), False),
            T.StructField("matched_rule_ids", T.ArrayType(T.StringType(), False), False),
            T.StructField("assign", assign_schema, True),
            T.StructField("winning_rule", WINNING_RULE_TRACE_STRUCT, True),
            T.StructField("winning_rule_id", T.StringType(), True),
            T.StructField("winning_rule_name", T.StringType(), True),
            T.StructField("winning_rule_explanation", T.StringType(), True),
            T.StructField("error", T.StringType(), True),
        ]
    )


FUNCTION_RETURN_TYPES = {
    "string": T.StringType(),
    "str": T.StringType(),
    "integer": T.LongType(),
    "int": T.LongType(),
    "long": T.LongType(),
    "number": T.DoubleType(),
    "float": T.DoubleType(),
    "double": T.DoubleType(),
    "decimal": T.DoubleType(),
    "boolean": T.BooleanType(),
    "bool": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
}


EMPTY_ASSIGN_STRUCT = T.StructType(
    [
        T.StructField("__empty", T.StringType(), True),
    ]
)


class _SparkRowNoOpRepository:
    """
    Minimal repository placeholder for Spark worker-side row evaluation.

    The row evaluator never loads metadata from the repository because the
    ruleset is already serialized into the UDF closure. Passing the real
    Spark/Delta repository would also serialize the active Spark session into
    the Python worker, which fails under Databricks Spark Connect.
    """

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Reject metadata loading from inside the Spark row UDF."""
        raise RuntimeError("Spark row UDF cannot load published metadata.")


class SparkRulesEngineRuntime:
    """Spark DataFrame runtime for ruleset evaluation."""

    def __init__(self, repository: RulesetRepository, function_registry: FunctionRegistry) -> None:
        """Initialize the Spark runtime with metadata and function registries."""
        self._repository = repository
        self._function_registry = function_registry

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
    ) -> DataFrame:
        """
        Evaluate a Spark DataFrame against a ruleset.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Incoming rows. Any cross-row facts must already exist as columns.
        ruleset : Ruleset
            Ruleset metadata to evaluate.
        column_prefix : str, default "rules_engine"
            Prefix for appended output columns.
        fail_on_error : bool, default True
            Raise when any row-level evaluator error is produced.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame with rules engine result columns appended.
        """
        logger.info(
            "Evaluating ruleset in Spark runtime: ruleset_id=%s ruleset_name=%s version=%s rule_count=%s fail_on_error=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            len(ruleset.rules),
            fail_on_error,
        )
        assign_schema = self._assignment_schema(ruleset, df.schema)
        assign_field_names = [field.name for field in assign_schema.fields]
        assign_field_types = {
            field.name: field.dataType
            for field in assign_schema.fields
        }

        result_udf = F.udf(
            self._build_row_evaluator(
                ruleset,
                assign_field_names,
                assign_field_types,
            ),
            _result_struct(assign_schema),
        )
        row_struct = F.struct(*[F.col(column_name) for column_name in df.columns])
        result_col = f"{column_prefix}_result"
        evaluated = df.withColumn(result_col, result_udf(row_struct))

        output = (
            evaluated.withColumn(f"{column_prefix}_matched", F.col(f"{result_col}.matched"))
            .withColumn(
                f"{column_prefix}_matched_rule_ids",
                F.col(f"{result_col}.matched_rule_ids"),
            )
            .withColumn(f"{column_prefix}_assign", F.col(f"{result_col}.assign"))
            .withColumn(f"{column_prefix}_winning_rule", F.col(f"{result_col}.winning_rule"))
            .withColumn(f"{column_prefix}_winning_rule_id", F.col(f"{result_col}.winning_rule_id"))
            .withColumn(f"{column_prefix}_winning_rule_name", F.col(f"{result_col}.winning_rule_name"))
            .withColumn(
                f"{column_prefix}_winning_rule_explanation",
                F.col(f"{result_col}.winning_rule_explanation"),
            )
            .withColumn(f"{column_prefix}_error", F.col(f"{result_col}.error"))
            .drop(result_col)
        )
        if fail_on_error:
            self._raise_if_row_errors(output, f"{column_prefix}_error")
        logger.info(
            "Spark runtime evaluation DataFrame built: ruleset_id=%s version=%s output_prefix=%s",
            ruleset.ruleset_id,
            ruleset.version,
            column_prefix,
        )
        return output

    def _raise_if_row_errors(self, df: DataFrame, error_column: str) -> None:
        """Collect a small sample of row-level errors and raise when any exist."""
        error_rows = (
            df.where(F.col(error_column).isNotNull())
            .select(error_column)
            .limit(5)
            .collect()
        )
        if error_rows:
            samples = [row[error_column] for row in error_rows]
            logger.error(
                "Spark runtime row-level errors detected: error_column=%s sample_count=%s",
                error_column,
                len(samples),
            )
            raise RuntimeError(
                "Rules engine Spark evaluation produced row-level errors: "
                + "; ".join(samples)
            )

    def _assignment_schema(self, ruleset: Ruleset, source_schema: T.StructType) -> T.StructType:
        """Build a ruleset-specific assignment result struct."""
        field_types: dict[str, T.DataType] = {}
        for rule in ruleset.rules:
            for assignment in rule.assignments:
                inferred = self._assignment_data_type(assignment, source_schema)
                existing = field_types.get(assignment.target_field)
                if existing is None:
                    field_types[assignment.target_field] = inferred
                elif existing != inferred:
                    field_types[assignment.target_field] = T.StringType()

        if not field_types:
            return EMPTY_ASSIGN_STRUCT
        return T.StructType(
            [
                T.StructField(field_name, data_type, True)
                for field_name, data_type in field_types.items()
            ]
        )

    def _assignment_data_type(
        self,
        assignment: Assignment,
        source_schema: T.StructType,
    ) -> T.DataType:
        """Infer a Spark output type for one assignment target."""
        return self._operand_data_type(assignment.value, source_schema)

    def _operand_data_type(self, operand: Operand, source_schema: T.StructType) -> T.DataType:
        """Infer a Spark output type for an assignment operand."""
        if isinstance(operand, LiteralOperand):
            return self._literal_data_type(operand.value, operand.value_type)
        if isinstance(operand, FieldOperand):
            for field in source_schema.fields:
                if field.name == operand.field_name:
                    return field.dataType
            return T.StringType()
        if isinstance(operand, CustomFunctionOperand):
            if self._function_registry.has_spec(operand.function_name):
                hint = self._function_registry.get_spec(operand.function_name).return_type_hint
                return self._return_hint_data_type(hint)
            return T.StringType()
        return T.StringType()

    def _literal_data_type(self, value: Any, value_type: str | None = None) -> T.DataType:
        """Infer a Spark data type for a literal assignment value."""
        if value_type:
            hinted = self._return_hint_data_type(value_type)
            if not isinstance(hinted, T.StringType) or value_type.lower() in {"string", "str"}:
                return hinted
        if isinstance(value, bool):
            return T.BooleanType()
        if isinstance(value, int):
            return T.LongType()
        if isinstance(value, (float, Decimal)):
            return T.DoubleType()
        if isinstance(value, datetime):
            return T.TimestampType()
        if isinstance(value, date):
            return T.DateType()
        if isinstance(value, MappingABC):
            return T.StructType(
                [
                    T.StructField(str(field_name), self._literal_data_type(field_value), True)
                    for field_name, field_value in value.items()
                ]
            )
        return T.StringType()

    def _return_hint_data_type(self, return_type_hint: str | None) -> T.DataType:
        """Map registry return-type hints to Spark output types."""
        if return_type_hint is None:
            return T.StringType()
        return FUNCTION_RETURN_TYPES.get(return_type_hint.lower(), T.StringType())

    def _build_row_evaluator(
        self,
        ruleset: Ruleset,
        assign_field_names: list[str],
        assign_field_types: Mapping[str, T.DataType],
    ):
        """Build the serializable Python callable used by the Spark UDF."""
        runtime = _SparkRowUdfEvaluator(
            _SparkRowNoOpRepository(),
            self._function_registry,
        )

        def evaluate(row: Any) -> dict[str, Any]:
            """Evaluate one Spark row struct and return the declared result struct."""
            try:
                row_dict = row.asDict(recursive=True)
                matched_rule_ids: list[str] = []
                assignments: dict[str, Any] = {}
                winning_rule: dict[str, Any] | None = None
                for rule in sorted(
                    (item for item in ruleset.rules if item.active_flag),
                    key=lambda item: item.rule_order,
                ):
                    matched, condition_traces = runtime._evaluate_rule(rule, row_dict)
                    if matched:
                        matched_rule_ids.append(rule.rule_id)
                        if winning_rule is None:
                            winning_rule = runtime._spark_rule_trace_payload(
                                runtime._rule_trace_payload(
                                    runtime._rule_execution_trace(
                                        rule,
                                        matched,
                                        condition_traces,
                                    )
                                )
                            )
                        assignments.update(runtime._evaluate_assignments(rule.assignments, row_dict))
                        if rule.stop_on_match:
                            break
                assign_payload = (
                    {
                        field_name: runtime._spark_assignment_value(
                            assignments.get(field_name),
                            assign_field_types[field_name],
                        )
                        for field_name in assign_field_names
                    }
                    if assignments
                    else None
                )
                return {
                    "matched": bool(matched_rule_ids),
                    "matched_rule_ids": matched_rule_ids,
                    "assign": assign_payload,
                    "winning_rule": winning_rule,
                    "winning_rule_id": winning_rule.get("rule_id") if winning_rule else None,
                    "winning_rule_name": winning_rule.get("rule_name") if winning_rule else None,
                    "winning_rule_explanation": runtime._winning_rule_explanation(winning_rule),
                    "error": None,
                }
            except Exception as exc:
                return {
                    "matched": False,
                    "matched_rule_ids": [],
                    "assign": None,
                    "winning_rule": None,
                    "winning_rule_id": None,
                    "winning_rule_name": None,
                    "winning_rule_explanation": None,
                    "error": f"{exc}\n{traceback.format_exc()}",
                }

        return evaluate


class _SparkRowUdfEvaluator(SparkRowEvaluator):
    """Row evaluator plus Spark-schema trace normalization helpers."""

    def _spark_assignment_value(self, value: Any, data_type: T.DataType) -> Any:
        """Return an assignment value compatible with the declared Spark type."""
        if value is None:
            return value
        if isinstance(data_type, T.StringType):
            return self._trace_text(value)
        if isinstance(data_type, T.StructType):
            if not isinstance(value, MappingABC):
                return None
            return {
                field.name: self._spark_assignment_value(
                    self._mapping_value(value, field.name),
                    field.dataType,
                )
                for field in data_type.fields
            }
        return value

    def _mapping_value(self, value: Mapping[str, Any], field_name: str) -> Any:
        """Return a mapping value using Spark's stringified struct field name."""
        if field_name in value:
            return value[field_name]
        for key, item in value.items():
            if str(key) == field_name:
                return item
        return None

    def _spark_rule_trace_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Normalize a rule trace payload to the declared Spark struct schema."""
        return {
            "rule_id": payload.get("rule_id"),
            "rule_name": payload.get("rule_name"),
            "matched": payload.get("matched"),
            "assignments_applied": list(payload.get("assignments_applied") or []),
            "conditions": [
                self._spark_condition_trace_payload(condition)
                for condition in payload.get("conditions", [])
            ],
        }

    def _spark_condition_trace_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Normalize a condition trace payload to the declared Spark struct schema."""
        return {
            "columns": [str(column) for column in payload.get("columns", [])],
            "left": self._spark_operand_trace_payload(payload.get("left")),
            "right": self._spark_operand_trace_payload(payload.get("right")),
            "operator": payload.get("operator"),
            "comparison_result": payload.get("comparison_result"),
            "passed": payload.get("passed"),
            "tolerance_abs": self._trace_text(payload.get("tolerance_abs")),
            "null_input_mode": payload.get("null_input_mode"),
            "null_result_mode": payload.get("null_result_mode"),
            "null_default_value": self._trace_text(payload.get("null_default_value")),
        }

    def _spark_operand_trace_payload(self, payload: Any) -> dict[str, Any] | None:
        """Normalize one operand trace payload to the declared Spark struct schema."""
        if not isinstance(payload, MappingABC):
            return None
        source_columns = payload.get("source_columns")
        if source_columns is None and payload.get("column") is not None:
            source_columns = [payload.get("column")]
        return {
            "kind": payload.get("kind"),
            "column": payload.get("column"),
            "value": self._trace_text(payload.get("value")),
            "value_type": payload.get("value_type"),
            "function_name": payload.get("function_name"),
            "source_columns": [str(column) for column in source_columns or []],
            "arguments": self._trace_arguments(payload),
        }

    def _trace_arguments(self, payload: Mapping[str, Any]) -> dict[str, str | None] | None:
        """Return compact string arguments for custom-function operands."""
        args = payload.get("args")
        if not isinstance(args, MappingABC):
            return None
        return {
            str(name): self._operand_explanation(value) or self._trace_text(value)
            for name, value in args.items()
        }

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
        if isinstance(value, MappingABC):
            return ", ".join(
                f"{key}={self._trace_text(item)}"
                for key, item in value.items()
            )
        if isinstance(value, (list, tuple, set)):
            return "[" + ", ".join(self._trace_text(item) or "" for item in value) + "]"
        return str(value)
