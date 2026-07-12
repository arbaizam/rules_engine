"""Shared Spark result schemas for rules engine evaluation."""

from pyspark.sql import types as T


OPERAND_TRACE_STRUCT = T.StructType(
    [
        T.StructField("kind", T.StringType(), True),
        T.StructField("column", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("value_type", T.StringType(), True),
        T.StructField("function_name", T.StringType(), True),
        T.StructField("source_columns", T.ArrayType(T.StringType(), False), True),
        T.StructField(
            "arguments",
            T.MapType(T.StringType(), T.StringType(), True),
            True,
        ),
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
        T.StructField(
            "assignments_applied",
            T.ArrayType(T.StringType(), False),
            True,
        ),
        T.StructField(
            "conditions",
            T.ArrayType(CONDITION_TRACE_STRUCT, False),
            True,
        ),
    ]
)
