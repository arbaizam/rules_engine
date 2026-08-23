"""Lazy Spark projections produced by one rules-engine evaluation."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _top_level_column(column_name: str) -> Column:
    """Return a top-level Spark column without interpreting dots as paths."""
    escaped_name = column_name.replace("`", "``")
    return F.col(f"`{escaped_name}`")


class DataFrameEvaluation:
    """Own the shared lazy plan for keyed results and applied business rows.

    Instances are created by ``evaluate_dataframe``. ``results_df`` projects
    only caller-declared keys and rules-engine outputs. ``apply_assignments``
    projects the original business columns with effective assignments applied.
    Both projections use the same evaluated plan and never join rows by key.
    """

    def __init__(
        self,
        evaluated_df: DataFrame,
        *,
        source_columns: Sequence[str],
        key_columns: Sequence[str],
        result_columns: Sequence[str],
        assignment_fields: Sequence[T.StructField],
        assign_column: str,
    ) -> None:
        """Create an evaluation from runtime-owned plan metadata."""
        self._evaluated_df = evaluated_df
        self._source_columns = tuple(source_columns)
        self._key_columns = tuple(key_columns)
        self._result_columns = tuple(result_columns)
        self._assignment_fields = tuple(assignment_fields)
        self._assign_column = assign_column

    @property
    def key_columns(self) -> tuple[str, ...]:
        """Return the caller-declared immutable row-identity columns."""
        return self._key_columns

    @property
    def result_columns(self) -> tuple[str, ...]:
        """Return the ordered rules-engine output columns."""
        return self._result_columns

    @property
    def results_df(self) -> DataFrame:
        """Return keys plus rules-engine results, without business payload columns."""
        return self._evaluated_df.select(
            *(
                _top_level_column(column_name).alias(column_name)
                for column_name in (*self._key_columns, *self._result_columns)
            )
        )

    def apply_assignments(self) -> DataFrame:
        """Return business columns with effective top-level assignments applied.

        Existing targets are replaced in place. New targets follow the original
        business columns in ruleset assignment order. Rules-engine result
        columns are excluded because they remain available through ``results_df``.
        """
        source_names = set(self._source_columns)
        assignment_by_name = {field.name: field for field in self._assignment_fields}
        business_columns: list[Column] = []
        for column_name in self._source_columns:
            assignment_field = assignment_by_name.get(column_name)
            if assignment_field is None:
                business_columns.append(_top_level_column(column_name).alias(column_name))
                continue
            business_columns.append(
                self._applied_value(
                    assignment_field,
                    otherwise=_top_level_column(column_name),
                ).alias(column_name)
            )
        for assignment_field in self._assignment_fields:
            if assignment_field.name in source_names:
                continue
            business_columns.append(
                self._applied_value(
                    assignment_field,
                    otherwise=F.lit(None).cast(assignment_field.dataType),
                ).alias(assignment_field.name)
            )
        return self._evaluated_df.select(*business_columns)

    def persist(
        self,
        storage_level: StorageLevel | None = None,
    ) -> DataFrameEvaluation:
        """Persist the shared evaluated plan and return this evaluation."""
        if storage_level is None:
            self._evaluated_df.persist()
        else:
            self._evaluated_df.persist(storage_level)
        return self

    def unpersist(self, *, blocking: bool = False) -> DataFrameEvaluation:
        """Remove the shared evaluated plan from cache and return this evaluation."""
        self._evaluated_df.unpersist(blocking=blocking)
        return self

    def _applied_value(
        self,
        assignment_field: T.StructField,
        *,
        otherwise: Column,
    ) -> Column:
        """Choose the typed assigned value only when the target was applied."""
        outcome = _top_level_column(self._assign_column).getField(assignment_field.name)
        return F.when(
            outcome.getField("applied"),
            outcome.getField("value"),
        ).otherwise(otherwise)
