"""
Spark-facing rules engine runtime.

This facade keeps Spark execution separate from the pure-Python runtime. It
precomputes aggregate operands with Spark DataFrame operations, joins those
values back to the incoming rows, and then uses a Python UDF for final rule
evaluation. This preserves the Python runtime semantics for predicates,
assignments, custom functions, null handling, and tolerance checks.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
    RowFilterPredicate,
    Rule,
    Ruleset,
)
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesetRepository
from rules_engine.runtime import RulesEngineRuntime


logger = logging.getLogger(__name__)


RESULT_STRUCT = T.StructType(
    [
        T.StructField("matched", T.BooleanType(), False),
        T.StructField("matched_rule_ids", T.ArrayType(T.StringType(), False), False),
        T.StructField("assign", T.StringType(), True),
        T.StructField("rule_results", T.StringType(), False),
        T.StructField("error", T.StringType(), True),
    ]
)


@dataclass(frozen=True)
class AggregateBinding:
    """
    Internal binding between an aggregate operand and a precomputed column.
    """

    operand: AggregateOperand
    column_name: str


class _SparkRowNoOpRepository:
    """
    Minimal repository placeholder for Spark worker-side row evaluation.

    The row evaluator never loads metadata from the repository because the
    ruleset is already serialized into the UDF closure. Passing the real
    Spark/Delta repository would also serialize the active Spark session into
    the Python worker, which fails under Databricks Spark Connect.
    """

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Reject metadata loading from inside the Spark row UDF.
        """
        raise RuntimeError("Spark row UDF cannot load published metadata.")


class SparkRulesEngineRuntime:
    """
    Spark DataFrame runtime for ruleset evaluation.
    """

    def __init__(self, repository: RulesetRepository, function_registry: FunctionRegistry) -> None:
        """
        Initialize the Spark runtime with metadata and function registries.
        """
        self._repository = repository
        self._function_registry = function_registry

    def load_published_ruleset(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load published metadata through the configured repository.
        """
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
            Incoming rows. Aggregate operands operate on this DataFrame exactly
            as supplied to the method.
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
        bindings = self._discover_aggregate_bindings(ruleset)
        logger.info(
            "Evaluating ruleset in Spark runtime: ruleset_id=%s ruleset_name=%s version=%s rule_count=%s aggregate_binding_count=%s fail_on_error=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            len(ruleset.rules),
            len(bindings),
            fail_on_error,
        )
        augmented = self._with_aggregate_columns(df, bindings)
        aggregate_lookup = {
            aggregate_key(binding.operand): binding.column_name
            for binding in bindings
        }

        source_columns = augmented.columns
        result_udf = F.udf(
            self._build_row_evaluator(ruleset, aggregate_lookup, source_columns),
            RESULT_STRUCT,
        )
        row_struct = F.struct(*[F.col(column_name) for column_name in source_columns])
        result_col = f"{column_prefix}_result"
        evaluated = augmented.withColumn(result_col, result_udf(row_struct))

        output = (
            evaluated.withColumn(f"{column_prefix}_matched", F.col(f"{result_col}.matched"))
            .withColumn(
                f"{column_prefix}_matched_rule_ids",
                F.col(f"{result_col}.matched_rule_ids"),
            )
            .withColumn(f"{column_prefix}_assign", F.col(f"{result_col}.assign"))
            .withColumn(f"{column_prefix}_rule_results", F.col(f"{result_col}.rule_results"))
            .withColumn(f"{column_prefix}_error", F.col(f"{result_col}.error"))
            .drop(result_col)
        )
        output = output.drop(*[binding.column_name for binding in bindings])
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
        """
        Collect a small sample of row-level errors and raise when any exist.
        """
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

    def _build_row_evaluator(
        self,
        ruleset: Ruleset,
        aggregate_lookup: dict[str, str],
        source_columns: list[str],
    ):
        """
        Build the serializable Python callable used by the Spark UDF.
        """
        runtime = SparkRowRuntime(
            _SparkRowNoOpRepository(),
            self._function_registry,
            aggregate_lookup,
        )

        def evaluate(row: Any) -> dict[str, Any]:
            """
            Evaluate one Spark row struct and return the declared result struct.
            """
            try:
                row_dict = row.asDict(recursive=True)
                matched_rule_ids: list[str] = []
                assignments: dict[str, Any] = {}
                rule_results: list[dict[str, Any]] = []
                for rule in sorted(
                    (item for item in ruleset.rules if item.active_flag),
                    key=lambda item: item.rule_order,
                ):
                    matched, _ = runtime._evaluate_rule(
                        rule,
                        row_dict,
                        0,
                        None,
                    )
                    rule_results.append({"rule_id": rule.rule_id, "matched": matched})
                    if matched:
                        matched_rule_ids.append(rule.rule_id)
                        assignments.update(
                            runtime._evaluate_assignments(rule.assignments, row_dict, 0, None)
                        )
                        if rule.stop_on_match:
                            break
                return {
                    "matched": bool(matched_rule_ids),
                    "matched_rule_ids": matched_rule_ids,
                    "assign": json.dumps(assignments, sort_keys=True) if assignments else None,
                    "rule_results": json.dumps(rule_results, sort_keys=True),
                    "error": None,
                }
            except Exception as exc:
                return {
                    "matched": False,
                    "matched_rule_ids": [],
                    "assign": None,
                    "rule_results": "[]",
                    "error": str(exc),
                }

        return evaluate

    def _discover_aggregate_bindings(self, ruleset: Ruleset) -> list[AggregateBinding]:
        """
        Discover unique aggregate operands that need Spark precompute columns.
        """
        operands: dict[str, AggregateOperand] = {}
        for rule in ruleset.rules:
            self._collect_group_aggregates(rule.root_group, operands)
        return [
            AggregateBinding(
                operand=operand,
                column_name=f"__rules_engine_agg_{index}",
            )
            for index, operand in enumerate(operands.values(), start=1)
        ]

    def _collect_group_aggregates(
        self,
        group: ConditionGroup,
        operands: dict[str, AggregateOperand],
    ) -> None:
        """
        Recursively collect aggregate operands from a condition group tree.
        """
        for condition in group.conditions:
            self._collect_operand_aggregate(condition.left, operands)
            if condition.right is not None:
                self._collect_operand_aggregate(condition.right, operands)
        for nested_group in group.groups:
            self._collect_group_aggregates(nested_group, operands)

    def _collect_operand_aggregate(
        self,
        operand: Operand,
        operands: dict[str, AggregateOperand],
    ) -> None:
        """
        Add an operand to the aggregate map when it is aggregate-backed.
        """
        if isinstance(operand, AggregateOperand):
            operands[aggregate_key(operand)] = operand

    def _with_aggregate_columns(
        self,
        df: DataFrame,
        bindings: list[AggregateBinding],
    ) -> DataFrame:
        """
        Add all required aggregate precompute columns to a DataFrame.
        """
        output = df
        for binding in bindings:
            output = self._with_one_aggregate_column(output, binding)
        return output

    def _with_one_aggregate_column(self, df: DataFrame, binding: AggregateBinding) -> DataFrame:
        """
        Add one aggregate precompute column for one aggregate binding.
        """
        operand = binding.operand
        self._validate_spark_supported_aggregate(operand)
        logger.debug(
            "Adding Spark aggregate column: column=%s function=%s field=%s scope=%s",
            binding.column_name,
            operand.function.value,
            operand.field_name,
            operand.scope.value,
        )
        if operand.function in {AggregateFunction.FIRST, AggregateFunction.LAST}:
            return self._with_order_sensitive_aggregate(df, binding)

        filtered = self._filtered_frame(df, operand)
        aggregate_expr = self._aggregate_expr(operand).alias(binding.column_name)
        if operand.scope is AggregateScope.DATASET:
            aggregate_df = filtered.agg(aggregate_expr)
            return df.crossJoin(aggregate_df)

        aggregate_df = filtered.groupBy(*[F.col(field_name) for field_name in operand.by]).agg(
            aggregate_expr
        )
        joined = df.join(aggregate_df, on=list(operand.by), how="left")
        return self._apply_joined_aggregate_default(joined, binding)

    def _with_order_sensitive_aggregate(
        self,
        df: DataFrame,
        binding: AggregateBinding,
    ) -> DataFrame:
        """
        Precompute FIRST or LAST aggregates with Spark window functions.
        """
        operand = binding.operand
        filtered = self._filtered_frame(df, operand)
        if operand.null_input_mode is NullInputMode.IGNORE:
            filtered = filtered.where(F.col(operand.field_name).isNotNull())
        value_expr = self._order_sensitive_value_expr(operand).alias(binding.column_name)
        order_columns = self._order_columns(operand)
        if operand.function is AggregateFunction.LAST:
            order_columns = [self._reverse_order_column(item) for item in operand.order_by]

        if operand.scope is AggregateScope.DATASET:
            window = Window.orderBy(*order_columns)
            selected = F.when(
                F.col("__rules_engine_row_number") == 1,
                self._order_sensitive_value_expr(operand),
            )
            result_expr = F.first(selected, ignorenulls=True)
            if operand.null_result_mode is NullResultMode.DEFAULT:
                result_expr = F.coalesce(result_expr, F.lit(operand.null_default_value))
            aggregate_df = (
                filtered.withColumn("__rules_engine_row_number", F.row_number().over(window))
                .agg(result_expr.alias(binding.column_name))
            )
            return df.crossJoin(aggregate_df)

        window = Window.partitionBy(*[F.col(field_name) for field_name in operand.by]).orderBy(
            *order_columns
        )
        aggregate_df = (
            filtered.withColumn("__rules_engine_row_number", F.row_number().over(window))
            .where(F.col("__rules_engine_row_number") == 1)
            .select(
                *[F.col(field_name) for field_name in operand.by],
                value_expr,
            )
        )
        joined = df.join(aggregate_df, on=list(operand.by), how="left")
        return self._apply_joined_aggregate_default(joined, binding)

    def _filtered_frame(self, df: DataFrame, operand: AggregateOperand) -> DataFrame:
        """
        Apply an aggregate filter to the DataFrame before aggregation.
        """
        if operand.filter is None:
            return df
        if not operand.filter.predicates:
            raise ValueError("Aggregate filter must contain at least one predicate.")
        predicates = [self._row_filter_expr(predicate) for predicate in operand.filter.predicates]
        if operand.filter.logical_operator is LogicalOperator.ALL:
            combined = predicates[0]
            for predicate in predicates[1:]:
                combined = combined & predicate
        else:
            combined = predicates[0]
            for predicate in predicates[1:]:
                combined = combined | predicate
        return df.where(combined)

    def _row_filter_expr(self, predicate: RowFilterPredicate):
        """
        Convert one aggregate-filter predicate into a Spark boolean expression.
        """
        if predicate.null_input_mode is NullInputMode.ERROR:
            raise ValueError(
                "Spark aggregate filters do not support null_input_mode=error in this pass."
            )
        if predicate.null_result_mode is NullResultMode.ERROR:
            raise ValueError(
                "Spark aggregate filters do not support null_result_mode=error in this pass."
            )
        left = self._spark_operand_expr(predicate.left)
        right = self._spark_operand_value(predicate.right) if predicate.right is not None else None
        expression = self._spark_compare_expr(
            left,
            predicate.operator,
            right,
            predicate.tolerance_abs,
            predicate.null_input_mode,
        )
        return self._spark_null_result_expr(
            expression,
            predicate.null_result_mode,
            predicate.null_default_value,
        )

    def _spark_operand_expr(self, operand: Operand | None):
        """
        Convert a row-level operand into a Spark expression.
        """
        if operand is None:
            return None
        if isinstance(operand, FieldOperand):
            return F.col(operand.field_name)
        if isinstance(operand, LiteralOperand):
            return F.lit(operand.value)
        if isinstance(operand, CustomFunctionOperand):
            raise ValueError("Custom function operands are not supported in Spark aggregate filters.")
        if isinstance(operand, AggregateOperand):
            raise ValueError("Nested aggregate operands are not supported in Spark aggregate filters.")
        raise TypeError(f"Unsupported operand type in Spark expression: {type(operand).__name__}")

    def _spark_operand_value(self, operand: Operand | None):
        """
        Return a literal value or expression for Spark predicate comparison.
        """
        if isinstance(operand, LiteralOperand):
            return operand.value
        return self._spark_operand_expr(operand)

    def _spark_compare_expr(
        self,
        left,
        operator: ComparisonOperator,
        right,
        tolerance_abs: Decimal,
        null_input_mode: NullInputMode,
    ):
        """
        Build a Spark comparison expression for a canonical operator.
        """
        if operator is ComparisonOperator.IS_NULL:
            return left.isNull()
        if operator is ComparisonOperator.IS_NOT_NULL:
            return left.isNotNull()

        right_col = F.lit(right) if not hasattr(right, "_jc") else right

        if null_input_mode is NullInputMode.ERROR:
            left_value = left
            right_value = right_col
        elif null_input_mode is NullInputMode.ZERO:
            left_value = F.coalesce(left, F.lit(0))
            right_value = F.coalesce(right_col, F.lit(0))
        else:
            left_value = left
            right_value = right_col

        if operator is ComparisonOperator.EQ:
            if tolerance_abs == Decimal("0"):
                return left_value == right_value
            return F.abs(left_value.cast("double") - right_value.cast("double")) <= float(tolerance_abs)
        if operator is ComparisonOperator.NE:
            if tolerance_abs == Decimal("0"):
                return left_value != right_value
            return F.abs(left_value.cast("double") - right_value.cast("double")) > float(tolerance_abs)
        if operator is ComparisonOperator.GT:
            return left_value.cast("double") > (right_value.cast("double") + float(tolerance_abs))
        if operator is ComparisonOperator.GE:
            return left_value.cast("double") >= (right_value.cast("double") - float(tolerance_abs))
        if operator is ComparisonOperator.LT:
            return left_value.cast("double") < (right_value.cast("double") - float(tolerance_abs))
        if operator is ComparisonOperator.LE:
            return left_value.cast("double") <= (right_value.cast("double") + float(tolerance_abs))
        if operator is ComparisonOperator.IN:
            return left_value.isin(*right)
        if operator is ComparisonOperator.NOT_IN:
            return ~left_value.isin(*right)
        if operator is ComparisonOperator.BETWEEN:
            return left_value.between(right[0], right[1])
        if operator is ComparisonOperator.NOT_BETWEEN:
            return ~left_value.between(right[0], right[1])
        if operator is ComparisonOperator.CONTAINS:
            return left_value.contains(right)
        if operator is ComparisonOperator.NOT_CONTAINS:
            return ~left_value.contains(right)
        if operator is ComparisonOperator.STARTS_WITH:
            return left_value.startswith(right)
        if operator is ComparisonOperator.ENDS_WITH:
            return left_value.endswith(right)
        if operator is ComparisonOperator.LIKE:
            return left_value.like(right)
        if operator is ComparisonOperator.NOT_LIKE:
            return ~left_value.like(right)
        raise ValueError(f"Unsupported Spark comparison operator: {operator.value}")

    def _spark_null_result_expr(
        self,
        expression,
        null_result_mode: NullResultMode,
        null_default_value: Any | None,
    ):
        """
        Apply Spark-side null-result handling to a predicate expression.
        """
        if null_result_mode is NullResultMode.ERROR:
            raise ValueError("Spark expression path does not support null_result_mode=error.")
        if null_result_mode is NullResultMode.DEFAULT:
            return F.coalesce(expression, F.lit(bool(null_default_value)))
        return F.coalesce(expression, F.lit(False))

    def _aggregate_expr(self, operand: AggregateOperand):
        """
        Build a Spark aggregate expression for a supported aggregate function.
        """
        value_col = self._aggregate_value_col(operand)
        if operand.function is AggregateFunction.SUM:
            return self._aggregate_result_expr(operand, F.sum(value_col))
        if operand.function is AggregateFunction.MEAN:
            return self._aggregate_result_expr(operand, F.avg(value_col))
        if operand.function is AggregateFunction.MIN:
            return self._aggregate_result_expr(operand, F.min(value_col))
        if operand.function is AggregateFunction.MAX:
            return self._aggregate_result_expr(operand, F.max(value_col))
        if operand.function is AggregateFunction.COUNT:
            return self._aggregate_result_expr(operand, F.count(value_col))
        if operand.function is AggregateFunction.COUNT_DISTINCT:
            return self._aggregate_result_expr(operand, F.count_distinct(value_col))
        if operand.function is AggregateFunction.STDDEV:
            return self._aggregate_result_expr(operand, F.stddev_pop(value_col))
        if operand.function is AggregateFunction.VARIANCE:
            return self._aggregate_result_expr(operand, F.var_pop(value_col))
        raise ValueError(f"Unsupported aggregate function for Spark aggregation: {operand.function.value}")

    def _aggregate_value_col(self, operand: AggregateOperand):
        """
        Return an aggregate value column with null-input mode applied.
        """
        value_col = F.col(operand.field_name)
        if operand.null_input_mode is NullInputMode.ERROR:
            return value_col
        if operand.null_input_mode is NullInputMode.ZERO:
            return F.coalesce(value_col, F.lit(0))
        if operand.null_input_mode is NullInputMode.PROPAGATE:
            return value_col
        return value_col

    def _aggregate_result_expr(self, operand: AggregateOperand, base_expr):
        """
        Apply aggregate null-result behavior to a Spark aggregate expression.
        """
        expr = base_expr
        if operand.null_input_mode is NullInputMode.PROPAGATE:
            null_count = F.sum(
                F.when(F.col(operand.field_name).isNull(), F.lit(1)).otherwise(F.lit(0))
            )
            expr = F.when(null_count > 0, F.lit(None)).otherwise(base_expr)
        if operand.null_result_mode is NullResultMode.DEFAULT:
            return F.coalesce(expr, F.lit(operand.null_default_value))
        return expr

    def _order_sensitive_value_expr(self, operand: AggregateOperand):
        """
        Return the value expression used for FIRST/LAST precompute.
        """
        value_expr = self._aggregate_value_col(operand)
        if operand.null_result_mode is NullResultMode.DEFAULT:
            return F.coalesce(value_expr, F.lit(operand.null_default_value))
        return value_expr

    def _apply_joined_aggregate_default(
        self,
        df: DataFrame,
        binding: AggregateBinding,
    ) -> DataFrame:
        """
        Apply default null-result handling after joining group aggregates.
        """
        if binding.operand.null_result_mode is NullResultMode.DEFAULT:
            return df.withColumn(
                binding.column_name,
                F.coalesce(F.col(binding.column_name), F.lit(binding.operand.null_default_value)),
            )
        return df

    def _order_columns(self, operand: AggregateOperand) -> list:
        """
        Convert order-by specs into Spark columns with nulls last.
        """
        columns = []
        for order in operand.order_by:
            column = F.col(order.field)
            columns.append(column.asc_nulls_last() if order.direction == "asc" else column.desc_nulls_last())
        return columns

    def _reverse_order_column(self, order):
        """
        Reverse an order-by direction for LAST aggregate calculation.
        """
        column = F.col(order.field)
        return column.desc_nulls_last() if order.direction == "asc" else column.asc_nulls_last()

    def _validate_spark_supported_aggregate(self, operand: AggregateOperand) -> None:
        """
        Raise when an aggregate uses a Spark-unsupported feature.
        """
        if operand.function in {AggregateFunction.MEDIAN, AggregateFunction.QUANTILE}:
            raise ValueError(
                "Spark aggregate precompute does not support exact median/quantile in this pass."
            )
        if operand.null_input_mode is NullInputMode.ERROR:
            raise ValueError(
                "Spark aggregate precompute does not support null_input_mode=error in this pass."
            )
        if operand.null_result_mode is NullResultMode.ERROR:
            raise ValueError(
                "Spark aggregate precompute does not support null_result_mode=error in this pass."
            )
        if (
            operand.function in {AggregateFunction.FIRST, AggregateFunction.LAST}
            and operand.null_input_mode is NullInputMode.PROPAGATE
        ):
            raise ValueError(
                "Spark first/last aggregate precompute does not support null_input_mode=propagate in this pass."
            )



class SparkRowRuntime(RulesEngineRuntime):
    """
    Row evaluator that resolves aggregate operands from precomputed columns.
    """

    def __init__(
        self,
        repository: RulesetRepository,
        function_registry: FunctionRegistry,
        aggregate_lookup: dict[str, str],
    ) -> None:
        """
        Initialize the row runtime with aggregate lookup column names.
        """
        super().__init__(repository, function_registry)
        self._aggregate_lookup = aggregate_lookup

    def _resolve_operand(
        self,
        operand: Operand,
        row: Mapping[str, Any],
        row_index: int,
        aggregate_cache: Any,
    ) -> Any:
        """
        Resolve precomputed aggregates from row columns before normal operands.
        """
        if isinstance(operand, AggregateOperand):
            return row.get(self._aggregate_lookup[aggregate_key(operand)])
        return super()._resolve_operand(operand, row, row_index, aggregate_cache)
