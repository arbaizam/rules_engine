"""
Shared aggregate operand key generation.
"""

from __future__ import annotations

from rules_engine.models import AggregateOperand


def aggregate_key(operand: AggregateOperand) -> str:
    """
    Return a stable in-process key for one aggregate operand shape.

    The key is used by the Python aggregate cache and by the Spark runtime to
    bind precomputed aggregate columns back to row-level evaluation.
    """
    return repr(
        (
            operand.function.value,
            operand.field_name,
            operand.scope.value,
            operand.by,
            sorted(operand.args.items()),
            operand.filter,
            operand.order_by,
            operand.null_input_mode.value,
            operand.null_result_mode.value,
            operand.null_default_value,
        )
    )
