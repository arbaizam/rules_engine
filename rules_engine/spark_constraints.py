"""
Shared Spark runtime compatibility checks.
"""

from __future__ import annotations

from collections.abc import Iterable

from rules_engine.enums import AggregateFunction, NullInputMode, NullResultMode
from rules_engine.models import AggregateOperand, RowFilterPredicate


def spark_aggregate_compatibility_errors(
    operand: AggregateOperand,
) -> Iterable[tuple[str, str]]:
    """
    Yield Spark compatibility errors for one aggregate operand.
    """
    if operand.function in {AggregateFunction.MEDIAN, AggregateFunction.QUANTILE}:
        yield (
            "SPARK_EXACT_PERCENTILE_UNSUPPORTED",
            "Spark runtime does not support exact median/quantile in this pass.",
        )
    if operand.null_input_mode is NullInputMode.ERROR:
        yield (
            "SPARK_AGGREGATE_NULL_INPUT_ERROR_UNSUPPORTED",
            "Spark runtime does not support aggregate null_input_mode=error.",
        )
    if operand.null_result_mode is NullResultMode.ERROR:
        yield (
            "SPARK_AGGREGATE_NULL_RESULT_ERROR_UNSUPPORTED",
            "Spark runtime does not support aggregate null_result_mode=error.",
        )
    if (
        operand.function in {AggregateFunction.FIRST, AggregateFunction.LAST}
        and operand.null_input_mode is NullInputMode.PROPAGATE
    ):
        yield (
            "SPARK_FIRST_LAST_PROPAGATE_UNSUPPORTED",
            "Spark runtime does not support first/last with null_input_mode=propagate.",
        )


def spark_filter_predicate_compatibility_errors(
    predicate: RowFilterPredicate,
) -> Iterable[tuple[str, str]]:
    """
    Yield Spark compatibility errors for one aggregate filter predicate.
    """
    if predicate.null_input_mode is NullInputMode.ERROR:
        yield (
            "SPARK_FILTER_NULL_INPUT_ERROR_UNSUPPORTED",
            "Spark aggregate filters do not support null_input_mode=error.",
        )
    if predicate.null_result_mode is NullResultMode.ERROR:
        yield (
            "SPARK_FILTER_NULL_RESULT_ERROR_UNSUPPORTED",
            "Spark aggregate filters do not support null_result_mode=error.",
        )
