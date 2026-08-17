"""Order-independent Spark DataFrame parity checks for regression testing.

The comparison is serverless-safe: it uses only public DataFrame APIs and
never accesses ``_jdf`` or the Spark driver JVM. Rows are compared as a
multiset, so duplicate counts matter while physical row order does not.

Notebook example::

    from tools.spark_dataframe_parity import assert_spark_dataframes_equal

    assert_spark_dataframes_equal(
        expected_df,
        actual_df,
        ignore_columns={"processed_at"},
        float_round=10,
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

_ROW_COLUMN = "__parity_row_json"


@dataclass(frozen=True)
class RowDifference:
    """One canonical row whose duplicate count differs between DataFrames."""

    occurrences: int
    row_json: str


@dataclass(frozen=True)
class DataFrameParityResult:
    """Structured result returned by :func:`compare_spark_dataframes`."""

    matches: bool
    left_row_count: int | None
    right_row_count: int | None
    left_only_count: int | None
    right_only_count: int | None
    left_only_columns: tuple[str, ...] = ()
    right_only_columns: tuple[str, ...] = ()
    schema_differences: tuple[str, ...] = ()
    left_only_sample: tuple[RowDifference, ...] = ()
    right_only_sample: tuple[RowDifference, ...] = ()

    def to_text(self) -> str:
        """Render a concise report suitable for a notebook assertion failure."""
        lines = ["DataFrame parity: " + ("PASS" if self.matches else "FAIL")]
        if self.left_only_columns:
            lines.append(f"Columns only on left: {list(self.left_only_columns)}")
        if self.right_only_columns:
            lines.append(f"Columns only on right: {list(self.right_only_columns)}")
        lines.extend(f"Schema: {difference}" for difference in self.schema_differences)
        if self.left_row_count is not None:
            lines.append(
                "Rows: "
                f"left={self.left_row_count}, right={self.right_row_count}, "
                f"left_only={self.left_only_count}, right_only={self.right_only_count}"
            )
        lines.extend(_sample_lines("Left-only", self.left_only_sample))
        lines.extend(_sample_lines("Right-only", self.right_only_sample))
        return "\n".join(lines)


def compare_spark_dataframes(
    left: DataFrame,
    right: DataFrame,
    *,
    ignore_columns: set[str] | tuple[str, ...] | list[str] = (),
    check_column_order: bool = False,
    check_nullability: bool = False,
    strict_schema: bool = True,
    float_round: int | None = None,
    sample_limit: int = 10,
) -> DataFrameParityResult:
    """Compare two Spark DataFrames without depending on their row order.

    Columns are aligned by name unless ``check_column_order`` is enabled.
    Nested maps are normalized by key before comparison. Set ``float_round``
    when upstream floating-point calculations need a fixed decimal tolerance;
    decimal values remain exact.
    """
    if sample_limit < 0:
        raise ValueError("sample_limit must be non-negative.")
    if float_round is not None and float_round < 0:
        raise ValueError("float_round must be non-negative when provided.")

    ignored = set(ignore_columns)
    left_columns = tuple(column for column in left.columns if column not in ignored)
    right_columns = tuple(column for column in right.columns if column not in ignored)
    left_only_columns = tuple(sorted(set(left_columns) - set(right_columns)))
    right_only_columns = tuple(sorted(set(right_columns) - set(left_columns)))

    schema_differences = _schema_differences(
        left,
        right,
        columns=left_columns,
        check_column_order=check_column_order,
        check_nullability=check_nullability,
    )
    structural_mismatch = bool(
        left_only_columns
        or right_only_columns
        or (strict_schema and schema_differences)
    )
    if structural_mismatch:
        return DataFrameParityResult(
            matches=False,
            left_row_count=None,
            right_row_count=None,
            left_only_count=None,
            right_only_count=None,
            left_only_columns=left_only_columns,
            right_only_columns=right_only_columns,
            schema_differences=schema_differences,
        )

    left_rows = _canonical_rows(left, left_columns, float_round=float_round)
    right_rows = _canonical_rows(right, left_columns, float_round=float_round)
    differences = _row_differences(left_rows, right_rows).persist()
    try:
        counts = differences.agg(
            F.coalesce(F.sum("__left_count"), F.lit(0)).cast("long").alias("left_rows"),
            F.coalesce(F.sum("__right_count"), F.lit(0)).cast("long").alias("right_rows"),
            F.coalesce(F.sum("__left_only"), F.lit(0)).cast("long").alias("left_only"),
            F.coalesce(F.sum("__right_only"), F.lit(0)).cast("long").alias("right_only"),
        ).collect()[0]
        left_sample = _difference_sample(
            differences,
            count_column="__left_only",
            sample_limit=sample_limit,
        )
        right_sample = _difference_sample(
            differences,
            count_column="__right_only",
            sample_limit=sample_limit,
        )
    finally:
        differences.unpersist()

    left_only_count = int(counts["left_only"])
    right_only_count = int(counts["right_only"])
    return DataFrameParityResult(
        matches=not left_only_count
        and not right_only_count
        and (not strict_schema or not schema_differences),
        left_row_count=int(counts["left_rows"]),
        right_row_count=int(counts["right_rows"]),
        left_only_count=left_only_count,
        right_only_count=right_only_count,
        schema_differences=schema_differences,
        left_only_sample=left_sample,
        right_only_sample=right_sample,
    )


def assert_spark_dataframes_equal(
    left: DataFrame,
    right: DataFrame,
    **options: Any,
) -> DataFrameParityResult:
    """Assert parity and return the successful structured comparison result."""
    result = compare_spark_dataframes(left, right, **options)
    if not result.matches:
        raise AssertionError(result.to_text())
    return result


def _schema_differences(
    left: DataFrame,
    right: DataFrame,
    *,
    columns: tuple[str, ...],
    check_column_order: bool,
    check_nullability: bool,
) -> tuple[str, ...]:
    """Return human-readable differences for columns present in both inputs."""
    differences: list[str] = []
    if check_column_order:
        right_columns = tuple(column for column in right.columns if column in columns)
        if columns != right_columns:
            differences.append(
                f"column order differs: left={list(columns)}, right={list(right_columns)}"
            )

    left_fields = {field.name: field for field in left.schema.fields}
    right_fields = {field.name: field for field in right.schema.fields}
    for column in columns:
        if column not in right_fields:
            continue
        left_field = left_fields[column]
        right_field = right_fields[column]
        if left_field.dataType != right_field.dataType:
            differences.append(
                f"{column!r} type differs: "
                f"left={left_field.dataType.simpleString()}, "
                f"right={right_field.dataType.simpleString()}"
            )
        if check_nullability and left_field.nullable != right_field.nullable:
            differences.append(
                f"{column!r} nullability differs: "
                f"left={left_field.nullable}, right={right_field.nullable}"
            )
    return tuple(differences)


def _canonical_rows(
    dataframe: DataFrame,
    columns: tuple[str, ...],
    *,
    float_round: int | None,
) -> DataFrame:
    """Return one deterministic JSON value per source row."""
    fields = {field.name: field for field in dataframe.schema.fields}
    values = [
        _canonical_value(_column(column), fields[column].dataType, float_round).alias(column)
        for column in columns
    ]
    return dataframe.select(
        F.to_json(
            F.struct(*values),
            options={"ignoreNullFields": "false"},
        ).alias(_ROW_COLUMN)
    )


def _canonical_value(
    column: Column,
    data_type: T.DataType,
    float_round: int | None,
) -> Column:
    """Normalize nested values before converting the complete row to JSON."""
    if isinstance(data_type, T.StructType):
        value = F.struct(
            *[
                _canonical_value(column[field.name], field.dataType, float_round).alias(
                    field.name
                )
                for field in data_type.fields
            ]
        )
        return F.when(column.isNull(), F.lit(None)).otherwise(value)
    if isinstance(data_type, T.ArrayType):
        return F.transform(
            column,
            lambda item: _canonical_value(item, data_type.elementType, float_round),
        )
    if isinstance(data_type, T.MapType):
        entries = F.array_sort(
            F.map_entries(column),
            lambda left, right: (
                F.when(left["key"] == right["key"], F.lit(0))
                .when(left["key"] < right["key"], F.lit(-1))
                .otherwise(F.lit(1))
            ),
        )
        return F.transform(
            entries,
            lambda entry: F.struct(
                _canonical_value(entry["key"], data_type.keyType, float_round).alias("key"),
                _canonical_value(entry["value"], data_type.valueType, float_round).alias(
                    "value"
                ),
            ),
        )
    if float_round is not None and isinstance(data_type, (T.FloatType, T.DoubleType)):
        return F.round(column, float_round)
    return column


def _row_differences(left_rows: DataFrame, right_rows: DataFrame) -> DataFrame:
    """Return canonical rows with multiplicity deltas."""
    left_counts = left_rows.groupBy(_ROW_COLUMN).count().withColumnRenamed(
        "count", "__left_count"
    )
    right_counts = right_rows.groupBy(_ROW_COLUMN).count().withColumnRenamed(
        "count", "__right_count"
    )
    return (
        left_counts.join(right_counts, on=_ROW_COLUMN, how="full")
        .fillna(0, subset=["__left_count", "__right_count"])
        .withColumn(
            "__left_only",
            F.greatest(F.col("__left_count") - F.col("__right_count"), F.lit(0)),
        )
        .withColumn(
            "__right_only",
            F.greatest(F.col("__right_count") - F.col("__left_count"), F.lit(0)),
        )
    )


def _difference_sample(
    differences: DataFrame,
    *,
    count_column: str,
    sample_limit: int,
) -> tuple[RowDifference, ...]:
    """Collect a bounded deterministic sample of unequal rows."""
    if sample_limit == 0:
        return ()
    rows = (
        differences.where(F.col(count_column) > 0)
        .select(_ROW_COLUMN, count_column)
        .orderBy(_ROW_COLUMN)
        .limit(sample_limit)
        .collect()
    )
    return tuple(
        RowDifference(
            occurrences=int(row[count_column]),
            row_json=str(row[_ROW_COLUMN]),
        )
        for row in rows
    )


def _column(name: str) -> Column:
    """Return a safely quoted top-level column reference."""
    return F.col(f"`{name.replace('`', '``')}`")


def _sample_lines(label: str, sample: tuple[RowDifference, ...]) -> list[str]:
    """Render bounded mismatch samples."""
    if not sample:
        return []
    lines = [f"{label} sample:"]
    lines.extend(
        f"  occurrences={difference.occurrences}: {difference.row_json}"
        for difference in sample
    )
    return lines
