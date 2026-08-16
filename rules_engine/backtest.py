"""Same-tape baseline/candidate Spark backtesting."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rules_engine.enums import AuditLevel
from rules_engine.models import Ruleset
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_runtime import SparkRulesEngineRuntime
from rules_engine.version import __version__


@dataclass(frozen=True)
class RulesetIdentity:
    """Immutable identity carried into a backtest report."""

    ruleset_id: str
    ruleset_name: str
    version: str
    content_hash: str
    engine_version: str


@dataclass(frozen=True)
class BacktestReport:
    """Observed output differences between baseline and candidate rulesets."""

    baseline: RulesetIdentity
    candidate: RulesetIdentity
    key: str
    compare_field: str | None
    total_row_count: int
    changed_row_count: int
    unchanged_row_count: int
    change_rate: float
    change_matrix: DataFrame
    changed_rows: DataFrame
    sample_rows: DataFrame


class RulesetBacktester:
    """Evaluate two rulesets against one keyed input DataFrame."""

    _BASELINE_PREFIX = "__rules_engine_backtest_baseline"
    _CANDIDATE_PREFIX = "__rules_engine_backtest_candidate"

    def __init__(self, runtime: SparkRulesEngineRuntime) -> None:
        self._runtime = runtime

    def analyze(
        self,
        df: DataFrame,
        baseline: Ruleset,
        candidate: Ruleset,
        *,
        key: str,
        compare_field: str | None = None,
        sample_size: int = 100,
    ) -> BacktestReport:
        """Run a keyed baseline/candidate comparison on the same tape."""
        if key not in df.columns:
            raise ValueError(f"Backtest key column not found: {key}")
        if sample_size < 0:
            raise ValueError("sample_size must be non-negative.")
        reserved_prefixes = (self._BASELINE_PREFIX, self._CANDIDATE_PREFIX)
        reserved_names = {
            "baseline_value",
            "candidate_value",
            *{
                f"{label}_{name}"
                for label in ("baseline", "candidate")
                for name in (
                    "matched",
                    "matched_rule_ids",
                    "assign",
                    "error",
                    "ruleset_id",
                    "ruleset_version",
                    "content_hash",
                    "engine_version",
                )
            },
        }
        if any(
            column.startswith(prefix)
            for column in df.columns
            for prefix in reserved_prefixes
        ):
            raise ValueError("Input contains reserved backtest output columns.")
        conflicts = sorted(set(df.columns) & reserved_names)
        if conflicts:
            raise ValueError(
                f"Input contains reserved backtest output columns: {conflicts}"
            )
        key_counts = df.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(F.when(F.col(key).isNull(), 1).otherwise(0)).alias("null_count"),
            F.countDistinct(F.col(key)).alias("distinct_count"),
        ).collect()[0]
        row_count = int(key_counts["row_count"] or 0)
        null_count = int(key_counts["null_count"] or 0)
        distinct_count = int(key_counts["distinct_count"] or 0)
        if null_count:
            raise ValueError(
                f"Backtest key {key!r} contains {null_count} null value(s)."
            )
        if distinct_count != row_count:
            raise ValueError(
                f"Backtest key {key!r} must be unique; found {row_count} rows "
                f"and {distinct_count} distinct keys."
            )
        baseline_targets = self._assignment_targets(baseline)
        candidate_targets = self._assignment_targets(candidate)
        all_targets = tuple(sorted(baseline_targets | candidate_targets))
        if compare_field is not None and compare_field not in all_targets:
            raise ValueError(
                f"compare_field {compare_field!r} is not assigned by either ruleset."
            )
        baseline_evaluated = self._runtime.evaluate_dataframe(
            df,
            baseline,
            column_prefix=self._BASELINE_PREFIX,
            audit_level=AuditLevel.MINIMAL,
        )
        candidate_evaluated = self._runtime.evaluate_dataframe(
            df,
            candidate,
            column_prefix=self._CANDIDATE_PREFIX,
            audit_level=AuditLevel.MINIMAL,
        )
        baseline_result = self._result_columns(
            baseline_evaluated,
            self._BASELINE_PREFIX,
            "baseline",
            key,
        )
        candidate_result = self._result_columns(
            candidate_evaluated,
            self._CANDIDATE_PREFIX,
            "candidate",
            key,
        )
        joined = df.join(baseline_result, key).join(candidate_result, key)
        baseline_value = self._comparison_json(
            "baseline_assign",
            baseline_targets,
            all_targets,
            compare_field,
        )
        candidate_value = self._comparison_json(
            "candidate_assign",
            candidate_targets,
            all_targets,
            compare_field,
        )
        compared = joined.withColumns(
            {
                "baseline_value": baseline_value,
                "candidate_value": candidate_value,
            }
        )
        changed = ~F.col("baseline_value").eqNullSafe(F.col("candidate_value"))
        change_counts = compared.agg(
            F.sum(F.when(changed, 1).otherwise(0)).alias("changed_count")
        ).collect()[0]
        changed_count = int(change_counts["changed_count"] or 0)
        change_matrix = (
            compared.groupBy("baseline_value", "candidate_value")
            .count()
            .withColumnRenamed("count", "row_count")
            .orderBy(F.desc("row_count"), "baseline_value", "candidate_value")
        )
        changed_rows = compared.filter(changed)
        return BacktestReport(
            baseline=self._identity(baseline),
            candidate=self._identity(candidate),
            key=key,
            compare_field=compare_field,
            total_row_count=row_count,
            changed_row_count=changed_count,
            unchanged_row_count=row_count - changed_count,
            change_rate=changed_count / row_count if row_count else 0.0,
            change_matrix=change_matrix,
            changed_rows=changed_rows,
            sample_rows=changed_rows.limit(sample_size),
        )

    def _result_columns(
        self,
        evaluated: DataFrame,
        prefix: str,
        label: str,
        key: str,
    ) -> DataFrame:
        names = (
            "matched",
            "matched_rule_ids",
            "assign",
            "error",
            "ruleset_id",
            "ruleset_version",
            "content_hash",
            "engine_version",
        )
        return evaluated.select(
            F.col(key),
            *[
                F.col(f"{prefix}_{name}").alias(f"{label}_{name}")
                for name in names
            ],
        )

    def _comparison_json(
        self,
        assign_column: str,
        available_targets: set[str],
        all_targets: tuple[str, ...],
        compare_field: str | None,
    ):
        targets = (compare_field,) if compare_field is not None else all_targets
        if not targets:
            return F.lit("{}")
        assign = F.col(assign_column)
        values = [
            (
                assign.getField(target)
                if target in available_targets
                else F.lit(None)
            ).alias(target)
            for target in targets
        ]
        return F.to_json(
            F.struct(*values),
            options={"ignoreNullFields": "false"},
        )

    def _assignment_targets(self, ruleset: Ruleset) -> set[str]:
        return {
            assignment.target_field
            for rule in ruleset.rules
            if rule.active_flag
            for assignment in rule.assignments
        }

    def _identity(self, ruleset: Ruleset) -> RulesetIdentity:
        return RulesetIdentity(
            ruleset_id=ruleset.ruleset_id,
            ruleset_name=ruleset.ruleset_name,
            version=ruleset.version,
            content_hash=DeltaRowSerializer().content_hash(ruleset),
            engine_version=__version__,
        )
