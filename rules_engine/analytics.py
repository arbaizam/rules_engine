"""Spark coverage diagnostics for published or candidate rulesets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine.enums import AuditLevel
from rules_engine.models import Ruleset
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_runtime import SparkRulesEngineRuntime, required_source_columns


@dataclass(frozen=True)
class RuleCoverage:
    """Observed match coverage for one rule."""

    rule_id: str
    rule_name: str
    rule_order: int
    match_count: int
    first_match_count: int
    match_rate: float
    dead: bool
    suspiciously_broad: bool


@dataclass(frozen=True)
class CoverageReport:
    """Aggregate coverage plus clean no-match diagnostics."""

    total_row_count: int
    no_match_count: int
    error_count: int
    rules: tuple[RuleCoverage, ...]
    first_match_distribution: dict[str, int]
    no_match_rows: DataFrame

    @property
    def dead_rule_ids(self) -> tuple[str, ...]:
        return tuple(rule.rule_id for rule in self.rules if rule.dead)

    @property
    def suspiciously_broad_rule_ids(self) -> tuple[str, ...]:
        return tuple(
            rule.rule_id for rule in self.rules if rule.suspiciously_broad
        )


_CLOSEST_RULE_STRUCT = T.StructType(
    [
        T.StructField("closest_rule_id", T.StringType(), True),
        T.StructField("closest_rule_name", T.StringType(), True),
        T.StructField("closest_rule_score", T.DoubleType(), True),
        T.StructField("passed_condition_count", T.LongType(), True),
        T.StructField("condition_count", T.LongType(), True),
        T.StructField(
            "failed_condition_ids",
            T.ArrayType(T.StringType(), False),
            True,
        ),
    ]
)


class RulesetCoverageAnalyzer:
    """Build a coverage report using the production Spark evaluator."""

    DEFAULT_PREFIX = "rules_engine_coverage"

    def __init__(
        self,
        runtime: SparkRulesEngineRuntime,
        function_registry: FunctionRegistry,
    ) -> None:
        self._runtime = runtime
        self._function_registry = function_registry

    def analyze(
        self,
        df: DataFrame,
        ruleset: Ruleset,
        *,
        broad_match_threshold: float = 0.40,
        column_prefix: str = DEFAULT_PREFIX,
    ) -> CoverageReport:
        """Evaluate a tape and report match/dead/no-match behavior."""
        if not 0 <= broad_match_threshold <= 1:
            raise ValueError("broad_match_threshold must be between 0 and 1.")
        if not column_prefix:
            raise ValueError("column_prefix must be non-empty.")
        if any(column.startswith(f"{column_prefix}_") for column in df.columns):
            raise ValueError(
                "Input contains reserved coverage columns beginning "
                f"{column_prefix}_"
            )
        evaluated = self._runtime.evaluate_dataframe(
            df,
            ruleset,
            column_prefix=column_prefix,
            fail_on_error=False,
            audit_level=AuditLevel.MINIMAL,
        )
        matched_col = F.col(f"{column_prefix}_matched")
        matched_ids_col = F.col(f"{column_prefix}_matched_rule_ids")
        error_col = F.col(f"{column_prefix}_error")
        clean_no_match = (~matched_col) & error_col.isNull()
        active_rules = tuple(
            sorted(
                (rule for rule in ruleset.rules if rule.active_flag),
                key=lambda rule: rule.rule_order,
            )
        )
        aggregates = [
            F.count(F.lit(1)).alias("total_row_count"),
            F.sum(F.when(clean_no_match, 1).otherwise(0)).alias("no_match_count"),
            F.sum(F.when(error_col.isNotNull(), 1).otherwise(0)).alias("error_count"),
        ]
        for index, rule in enumerate(active_rules):
            aggregates.extend(
                [
                    F.sum(
                        F.when(F.array_contains(matched_ids_col, rule.rule_id), 1)
                        .otherwise(0)
                    ).alias(f"match_{index}"),
                    F.sum(
                        F.when(
                            F.try_element_at(matched_ids_col, F.lit(1))
                            == rule.rule_id,
                            1,
                        ).otherwise(0)
                    ).alias(f"first_{index}"),
                ]
            )
        counts = evaluated.agg(*aggregates).collect()[0].asDict()
        total_count = int(counts["total_row_count"] or 0)
        rule_coverage: list[RuleCoverage] = []
        first_distribution: dict[str, int] = {}
        for index, rule in enumerate(active_rules):
            match_count = int(counts[f"match_{index}"] or 0)
            first_count = int(counts[f"first_{index}"] or 0)
            match_rate = match_count / total_count if total_count else 0.0
            rule_coverage.append(
                RuleCoverage(
                    rule_id=rule.rule_id,
                    rule_name=rule.rule_name,
                    rule_order=rule.rule_order,
                    match_count=match_count,
                    first_match_count=first_count,
                    match_rate=match_rate,
                    dead=match_count == 0,
                    suspiciously_broad=(
                        total_count > 0 and match_rate >= broad_match_threshold
                    ),
                )
            )
            first_distribution[rule.rule_id] = first_count
        no_match_rows = self._with_closest_rule(
            evaluated.filter(clean_no_match),
            ruleset,
            column_prefix,
        )
        return CoverageReport(
            total_row_count=total_count,
            no_match_count=int(counts["no_match_count"] or 0),
            error_count=int(counts["error_count"] or 0),
            rules=tuple(rule_coverage),
            first_match_distribution=first_distribution,
            no_match_rows=no_match_rows,
        )

    def _with_closest_rule(
        self,
        no_match_rows: DataFrame,
        ruleset: Ruleset,
        column_prefix: str,
    ) -> DataFrame:
        evaluator = SparkRowEvaluator.for_embedded_ruleset(
            self._function_registry
        )

        def diagnose(row: Any) -> dict[str, Any] | None:
            return evaluator.closest_rule_diagnostic(
                ruleset,
                row.asDict(recursive=True),
            )

        self._runtime.validate_worker_serializable(diagnose)
        diagnostic_udf = F.udf(diagnose, _CLOSEST_RULE_STRUCT)
        source_columns = required_source_columns(ruleset)
        row_struct = F.struct(
            *(
                [F.col(f"`{name.replace('`', '``')}`").alias(name) for name in source_columns]
                or [F.lit(None).alias("__rules_engine_empty")]
            )
        )
        diagnostic_col = f"{column_prefix}_closest_rule"
        result = no_match_rows.withColumn(diagnostic_col, diagnostic_udf(row_struct))
        return result.withColumns(
            {
                f"{column_prefix}_{field.name}": F.col(diagnostic_col).getField(
                    field.name
                )
                for field in _CLOSEST_RULE_STRUCT.fields
            }
        ).drop(diagnostic_col)
