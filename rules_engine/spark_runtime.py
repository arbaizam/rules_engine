"""Spark-facing rules engine runtime.

Field/literal rules compile to native Spark column expressions. Rules that
require Python custom functions or row-level error capture use the compatible
Python UDF path. Aggregate-style facts must be precomputed upstream and exposed
as ordinary input columns.
"""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from datetime import date, datetime
from decimal import Decimal
import logging
import traceback
from typing import Any, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.enums import (
    ComparisonOperator,
    NullInputMode,
    NullResultMode,
)
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.literal_types import literal_value_type_issue
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
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_native import (
    NativeRulesetCompiler,
    mapping_value,
    non_default,
    spark_column,
    trace_text,
)
from rules_engine.spark_types import WINNING_RULE_TRACE_STRUCT


logger = logging.getLogger(__name__)


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
        self._rule_formatter = HumanReadableRulesetFormatter()

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
        require_native: bool = False,
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
        require_native : bool, default False
            Raise during planning instead of using the Python UDF compatibility
            path when the ruleset cannot be represented as native Spark columns.

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
        native_issue = self._native_compatibility_issue(
            ruleset,
            df.schema,
            assign_schema,
        )
        if native_issue is None:
            output = self._evaluate_natively(
                df,
                ruleset,
                assign_schema,
                column_prefix,
            )
            execution_mode = "native"
        else:
            if require_native:
                raise ValueError(
                    "Ruleset cannot use native Spark execution: " + native_issue
                )
            logger.warning(
                "Using Python UDF compatibility path: ruleset_id=%s reason=%s",
                ruleset.ruleset_id,
                native_issue,
            )
            output = self._evaluate_with_udf(
                df,
                ruleset,
                assign_schema,
                column_prefix,
            )
            execution_mode = "python_udf"
            if fail_on_error:
                self._raise_if_row_errors(output, f"{column_prefix}_error")

        logger.info(
            "Spark runtime evaluation DataFrame built: ruleset_id=%s version=%s output_prefix=%s execution_mode=%s",
            ruleset.ruleset_id,
            ruleset.version,
            column_prefix,
            execution_mode,
        )
        return output

    def _evaluate_with_udf(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        assign_schema: T.StructType,
        column_prefix: str,
    ) -> DataFrame:
        """Evaluate rules that require Python behavior through the UDF path."""
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
        row_struct = F.struct(*[spark_column(column_name) for column_name in df.columns])
        result_col = f"{column_prefix}_result"
        evaluated = df.withColumn(result_col, result_udf(row_struct))
        result = spark_column(result_col)
        return evaluated.withColumns(
            {
                f"{column_prefix}_matched": result.getField("matched"),
                f"{column_prefix}_matched_rule_ids": result.getField(
                    "matched_rule_ids"
                ),
                f"{column_prefix}_assign": result.getField("assign"),
                f"{column_prefix}_winning_rule": result.getField("winning_rule"),
                f"{column_prefix}_winning_rule_id": result.getField(
                    "winning_rule_id"
                ),
                f"{column_prefix}_winning_rule_name": result.getField(
                    "winning_rule_name"
                ),
                f"{column_prefix}_winning_rule_explanation": result.getField(
                    "winning_rule_explanation"
                ),
                f"{column_prefix}_error": result.getField("error"),
            }
        ).drop(result_col)

    def _native_compatibility_issue(
        self,
        ruleset: Ruleset,
        source_schema: T.StructType,
        assign_schema: T.StructType,
    ) -> str | None:
        """Return why a ruleset needs the UDF path, or ``None`` when native."""
        source_types = {field.name: field.dataType for field in source_schema.fields}
        assign_types = {field.name: field.dataType for field in assign_schema.fields}
        for rule in self._active_rules(ruleset):
            for assignment in rule.assignments:
                issue = self._native_operand_issue(assignment.value, source_types)
                if issue:
                    return f"assignment {assignment.assignment_id}: {issue}"
                if (
                    isinstance(assignment.value, FieldOperand)
                    and isinstance(
                        assign_types[assignment.target_field],
                        T.StringType,
                    )
                    and not isinstance(
                        source_types[assignment.value.field_name],
                        T.StringType,
                    )
                ):
                    return (
                        f"assignment {assignment.assignment_id}: mixed field types "
                        "require Python string formatting"
                    )
            issue = self._native_group_issue(rule.root_group, source_types)
            if issue:
                return f"rule {rule.rule_id}: {issue}"
        return None

    def _native_group_issue(
        self,
        group: ConditionGroup,
        source_types: Mapping[str, T.DataType],
    ) -> str | None:
        """Return the first native-compatibility issue in a condition group."""
        for condition in group.conditions:
            issue = self._native_condition_issue(condition, source_types)
            if issue:
                return f"condition {condition.condition_id}: {issue}"
        for child in group.groups:
            issue = self._native_group_issue(child, source_types)
            if issue:
                return issue
        return None

    def _native_condition_issue(
        self,
        condition: Condition,
        source_types: Mapping[str, T.DataType],
    ) -> str | None:
        """Return why one condition cannot preserve behavior natively."""
        # Inactive operands still appear in the winning trace, so validate them
        # before returning for an inactive condition.
        for operand in (condition.left, condition.right):
            if operand is None:
                continue
            issue = self._native_operand_issue(operand, source_types)
            if issue:
                return issue
            if isinstance(operand, LiteralOperand) and isinstance(
                operand.value,
                MappingABC,
            ):
                return "mapping comparisons require Python semantics"

        if not condition.active_flag:
            return None
        if condition.null_input_mode is NullInputMode.ERROR:
            return "null_input_mode=error requires row-level error capture"
        if condition.null_result_mode is NullResultMode.ERROR:
            return "null_result_mode=error requires row-level error capture"

        operator = condition.operator
        left_type = self._native_operand_type(condition.left, source_types)
        right_type = (
            self._native_operand_type(condition.right, source_types)
            if condition.right is not None
            else None
        )
        if condition.null_input_mode is NullInputMode.ZERO and not (
            self._is_numeric_type(left_type) and self._is_numeric_type(right_type)
        ):
            return "null_input_mode=zero is native only for numeric operands"
        if operator in {
            ComparisonOperator.GT,
            ComparisonOperator.GE,
            ComparisonOperator.LT,
            ComparisonOperator.LE,
        } and not (
            self._is_numeric_type(left_type) and self._is_numeric_type(right_type)
        ):
            return f"operator={operator.value} requires numeric Spark operands"
        if operator in {
            ComparisonOperator.GT,
            ComparisonOperator.GE,
            ComparisonOperator.LT,
            ComparisonOperator.LE,
        } and not self._native_numeric_types_compatible(left_type, right_type):
            return f"operator={operator.value} has unsafe numeric type coercion"
        if condition.tolerance_abs != Decimal("0"):
            return "nonzero tolerance requires Python decimal semantics"
        if operator in {
            ComparisonOperator.EQ,
            ComparisonOperator.NE,
        } and not self._native_types_compatible(left_type, right_type):
            return f"operator={operator.value} has incompatible Spark operand types"
        if operator in {
            ComparisonOperator.IN,
            ComparisonOperator.NOT_IN,
            ComparisonOperator.BETWEEN,
            ComparisonOperator.NOT_BETWEEN,
        }:
            if not isinstance(condition.right, LiteralOperand) or not isinstance(
                condition.right.value,
                (list, tuple),
            ):
                return f"operator={operator.value} requires a literal collection"
            if any(item is None for item in condition.right.value):
                return "collections containing null require Python membership semantics"
            if operator in {
                ComparisonOperator.BETWEEN,
                ComparisonOperator.NOT_BETWEEN,
            } and not (
                self._is_numeric_type(left_type)
                and all(
                    self._native_numeric_types_compatible(
                        left_type,
                        self._literal_data_type(item),
                    )
                    for item in condition.right.value
                )
            ):
                return f"operator={operator.value} requires numeric Spark operands"
            if operator in {
                ComparisonOperator.IN,
                ComparisonOperator.NOT_IN,
            } and not all(
                self._native_types_compatible(
                    left_type,
                    self._literal_data_type(item),
                )
                for item in condition.right.value
            ):
                return f"operator={operator.value} has incompatible collection values"
        if operator in {
            ComparisonOperator.CONTAINS,
            ComparisonOperator.NOT_CONTAINS,
            ComparisonOperator.STARTS_WITH,
            ComparisonOperator.ENDS_WITH,
            ComparisonOperator.LIKE,
            ComparisonOperator.NOT_LIKE,
        }:
            if not isinstance(condition.right, LiteralOperand):
                return f"operator={operator.value} requires a literal right operand"
            if not (
                isinstance(left_type, T.StringType)
                and isinstance(right_type, T.StringType)
            ):
                return f"operator={operator.value} requires string Spark operands"
            if (
                operator in {ComparisonOperator.LIKE, ComparisonOperator.NOT_LIKE}
                and "\\" in str(condition.right.value)
            ):
                return "LIKE patterns containing backslashes require Python semantics"
        return None

    def _native_operand_issue(
        self,
        operand: Operand,
        source_types: Mapping[str, T.DataType],
    ) -> str | None:
        """Return why an operand cannot be expressed as a Spark column."""
        if isinstance(operand, CustomFunctionOperand):
            return f"custom function {operand.function_name!r} requires Python"
        if isinstance(operand, FieldOperand) and operand.field_name not in source_types:
            return f"source field {operand.field_name!r} is missing"
        if isinstance(operand, LiteralOperand):
            return literal_value_type_issue(operand.value, operand.value_type)
        return None

    def _native_operand_type(
        self,
        operand: Operand | None,
        source_types: Mapping[str, T.DataType],
    ) -> T.DataType | None:
        """Return the Spark type available to a native condition operand."""
        if isinstance(operand, FieldOperand):
            return source_types.get(operand.field_name)
        if isinstance(operand, LiteralOperand):
            return self._literal_data_type(operand.value, operand.value_type)
        return None

    def _is_numeric_type(self, data_type: T.DataType | None) -> bool:
        """Return whether a Spark data type has numeric comparison semantics."""
        return isinstance(data_type, T.NumericType)

    def _native_types_compatible(
        self,
        left: T.DataType | None,
        right: T.DataType | None,
    ) -> bool:
        """Return whether Spark comparison preserves the row evaluator contract."""
        if self._is_numeric_type(left) and self._is_numeric_type(right):
            return self._native_numeric_types_compatible(left, right)
        atomic_types = (
            T.StringType,
            T.BooleanType,
            T.DateType,
            T.TimestampType,
            T.BinaryType,
        )
        return (
            isinstance(left, atomic_types)
            and isinstance(right, atomic_types)
            and type(left) is type(right)
        )

    def _native_numeric_types_compatible(
        self,
        left: T.DataType | None,
        right: T.DataType | None,
    ) -> bool:
        """Reject numeric coercions that lose precision relative to Python."""
        integral_types = (
            T.ByteType,
            T.ShortType,
            T.IntegerType,
            T.LongType,
        )
        floating_types = (T.FloatType, T.DoubleType)
        exact_types = (*integral_types, T.DecimalType)
        return (
            isinstance(left, integral_types) and isinstance(right, integral_types)
        ) or (
            isinstance(left, floating_types) and isinstance(right, floating_types)
        ) or (
            isinstance(left, exact_types) and isinstance(right, exact_types)
        )

    def _evaluate_natively(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        assign_schema: T.StructType,
        column_prefix: str,
    ) -> DataFrame:
        """Compile a compatible ruleset into native Spark column expressions."""
        compiler = NativeRulesetCompiler(
            df.schema,
            assign_schema,
            self._rule_formatter,
        )
        return compiler.evaluate(
            df,
            self._active_rules(ruleset),
            column_prefix,
        )

    def _active_rules(self, ruleset: Ruleset) -> tuple[Rule, ...]:
        """Return active rules once in deterministic execution order."""
        return tuple(
            sorted(
                (rule for rule in ruleset.rules if rule.active_flag),
                key=lambda rule: rule.rule_order,
            )
        )

    def _raise_if_row_errors(self, df: DataFrame, error_column: str) -> None:
        """Collect a small sample of row-level errors and raise when any exist."""
        error_rows = (
            df.where(spark_column(error_column).isNotNull())
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
        for rule in self._active_rules(ruleset):
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
        if value_type and value_type.lower() not in {"number", "list", "any"}:
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
        active_rules = self._active_rules(ruleset)

        def evaluate(row: Any) -> dict[str, Any]:
            """Evaluate one Spark row struct and return the declared result struct."""
            try:
                row_dict = row.asDict(recursive=True)
                matched_rule_ids: list[str] = []
                assignments: dict[str, Any] = {}
                winning_rule: dict[str, Any] | None = None
                winning_rule_explanation: str | None = None
                for rule in active_rules:
                    matched, condition_traces = runtime._evaluate_rule(rule, row_dict)
                    if matched:
                        matched_rule_ids.append(rule.rule_id)
                        if winning_rule is None:
                            winning_rule = runtime._spark_rule_trace(
                                runtime._rule_execution_trace(
                                    rule,
                                    matched,
                                    condition_traces,
                                )
                            )
                            winning_rule_explanation = runtime._winning_rule_explanation_from_trace(
                                rule,
                                condition_traces,
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
                    "winning_rule_explanation": winning_rule_explanation,
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
                    mapping_value(value, field.name),
                    field.dataType,
                )
                for field in data_type.fields
            }
        return value

    def _spark_rule_trace(self, trace: RuleExecutionTrace) -> dict[str, Any]:
        """Convert a rule trace to the declared Spark struct schema."""
        return {
            "rule_id": trace.rule_id,
            "rule_name": trace.rule_name,
            "matched": trace.matched,
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
            "tolerance_abs": self._trace_text(
                non_default(trace.tolerance_abs, "0")
            ),
            "null_input_mode": non_default(trace.null_input_mode, "propagate"),
            "null_result_mode": non_default(trace.null_result_mode, "null"),
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
        if not isinstance(trace, MappingABC):
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
        if not isinstance(args, MappingABC) or not args:
            return None
        return {
            str(name): self._operand_trace_summary(value) or self._trace_text(value)
            for name, value in args.items()
        }

    def _trace_text(self, value: Any) -> str | None:
        """Convert arbitrary trace values to compact Spark string fields."""
        return trace_text(value)
