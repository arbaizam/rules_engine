# Databricks notebook source
# MAGIC %md
# MAGIC # Rules Engine Serverless Performance Benchmark
# MAGIC
# MAGIC This notebook measures complete Spark actions on Databricks serverless.
# MAGIC It does not use Spark cache, persistence, checkpoints, or RDD APIs. Each
# MAGIC timed case writes a unique Delta table, and validation reads only that
# MAGIC materialized output so the rules UDF is not executed again.
# MAGIC
# MAGIC Supply parameters as job/notebook widgets or define matching globals
# MAGIC before running the notebook. See
# MAGIC `docs/rules_engine_serverless_performance.md` for the parameter contract.

# COMMAND ----------
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import random
import re
import time
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine import RulesEngineService
from rules_engine.models import (
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Ruleset,
)

try:
    from rules_engine import required_source_columns as _runtime_source_columns
except (AttributeError, ImportError):
    _runtime_source_columns = None


def _parameter(name: str, default: str = "") -> str:
    """Read a global or Databricks widget parameter without creating state."""
    if name in globals():
        return str(globals()[name])
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


def _as_bool(value: str) -> bool:
    """Parse one benchmark boolean parameter."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Expected boolean parameter, found {value!r}.")


def _safe_name(value: str) -> str:
    """Return a lowercase identifier fragment for generated table names."""
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    return normalized or "benchmark"


def _quoted_identifier(value: str) -> str:
    """Quote a two- or three-part Spark identifier."""
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split("."))


def _literal_column(column_name: str):
    """Return a top-level Spark column whose name may contain dots."""
    escaped_name = column_name.replace("`", "``")
    return F.col(f"`{escaped_name}`")


def _ruleset_dependencies(ruleset: Ruleset) -> tuple[str, ...]:
    """Return active field dependencies across baseline and optimized versions."""
    columns = []

    def add_operand(operand: Operand | None) -> None:
        if isinstance(operand, FieldOperand):
            columns.append(operand.field_name)
        elif isinstance(operand, CustomFunctionOperand):
            for argument in operand.args.values():
                if isinstance(
                    argument,
                    (FieldOperand, LiteralOperand, CustomFunctionOperand),
                ):
                    add_operand(argument)

    def add_group(group: ConditionGroup) -> None:
        for condition in group.conditions:
            if condition.active_flag:
                add_operand(condition.left)
                add_operand(condition.right)
        for nested_group in group.groups:
            add_group(nested_group)

    for rule in sorted(
        (item for item in ruleset.rules if item.active_flag),
        key=lambda item: item.rule_order,
    ):
        add_group(rule.root_group)
        for assignment in rule.assignments:
            add_operand(assignment.value)
    return tuple(dict.fromkeys(columns))


PERF_RULES_ENGINE_SCHEMA = _parameter("PERF_RULES_ENGINE_SCHEMA")
PERF_SOURCE_TABLE = _parameter("PERF_SOURCE_TABLE")
PERF_RULESET_NAME = _parameter("PERF_RULESET_NAME")
PERF_RULESET_VERSION = _parameter("PERF_RULESET_VERSION") or None
PERF_OUTPUT_SCHEMA = _parameter(
    "PERF_OUTPUT_SCHEMA",
    PERF_RULES_ENGINE_SCHEMA,
)
PERF_VARIANT = _parameter("PERF_VARIANT", "working_tree")
PERF_COMMIT_SHA = _parameter("PERF_COMMIT_SHA", "unknown")
PERF_ASSIGNMENT_FIELD = _parameter("PERF_ASSIGNMENT_FIELD", "leaf_key")
PERF_WHERE_SQL = _parameter("PERF_WHERE_SQL")
PERF_ROW_LIMIT = int(_parameter("PERF_ROW_LIMIT", "0"))
PERF_REPETITIONS = int(_parameter("PERF_REPETITIONS", "5"))
PERF_WARMUP_REPETITIONS = int(_parameter("PERF_WARMUP_REPETITIONS", "1"))
PERF_RANDOM_SEED = int(_parameter("PERF_RANDOM_SEED", "20260715"))
PERF_INCLUDE_FAIL_ON_ERROR = _as_bool(_parameter("PERF_INCLUDE_FAIL_ON_ERROR", "true"))
PERF_CLEANUP_OUTPUTS = _as_bool(_parameter("PERF_CLEANUP_OUTPUTS", "true"))
PERF_METRICS_TABLE = _parameter(
    "PERF_METRICS_TABLE",
    f"{PERF_OUTPUT_SCHEMA}.rules_engine_performance_results",
)
PERF_OUTPUT_PREFIX = _safe_name(_parameter("PERF_OUTPUT_PREFIX", "rules_engine_perf"))

assert PERF_RULES_ENGINE_SCHEMA, "Set PERF_RULES_ENGINE_SCHEMA."
assert PERF_SOURCE_TABLE, "Set PERF_SOURCE_TABLE."
assert PERF_RULESET_NAME, "Set PERF_RULESET_NAME."
assert PERF_OUTPUT_SCHEMA, "Set PERF_OUTPUT_SCHEMA."
assert PERF_REPETITIONS >= 1, "PERF_REPETITIONS must be at least 1."
assert PERF_WARMUP_REPETITIONS >= 0, "PERF_WARMUP_REPETITIONS cannot be negative."
assert PERF_ROW_LIMIT >= 0, "PERF_ROW_LIMIT cannot be negative."

service = RulesEngineService.from_schema(
    spark=spark,
    schema=PERF_RULES_ENGINE_SCHEMA,
)
ruleset = service.load_published(
    PERF_RULESET_NAME,
    version=PERF_RULESET_VERSION,
)
source_schema = spark.table(PERF_SOURCE_TABLE).schema
source_column_names = [field.name for field in source_schema.fields]
source_column_set = set(source_column_names)
dependency_columns = _ruleset_dependencies(ruleset)
serialized_columns = (
    _runtime_source_columns(ruleset)
    if _runtime_source_columns is not None
    else tuple(source_column_names)
)
missing_columns = [
    column_name
    for column_name in dependency_columns
    if column_name not in source_column_set
]
assert not missing_columns, (
    "Benchmark source is missing required ruleset columns: "
    + ", ".join(missing_columns)
)

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "serverless")

print("Rules Engine Serverless Performance Benchmark")
print("-" * 80)
print(f"Run ID:                  {run_id}")
print(f"Variant:                 {PERF_VARIANT}")
print(f"Commit:                  {PERF_COMMIT_SHA}")
print(f"Source table:            {PERF_SOURCE_TABLE}")
print(f"Ruleset:                 {ruleset.ruleset_name} {ruleset.version}")
print(f"Rule count:              {len(ruleset.rules)}")
print(f"Source column count:     {len(source_column_names)}")
print(f"Dependency column count: {len(dependency_columns)}")
print(f"Dependency columns:      {dependency_columns}")
print(f"Serialized column count: {len(serialized_columns)}")
print(f"Serialized columns:      {serialized_columns}")
print(f"Optimized projection:    {_runtime_source_columns is not None}")
print(f"Where clause:            {PERF_WHERE_SQL or '<none>'}")
print(f"Row limit:               {PERF_ROW_LIMIT or '<none>'}")
print(f"Measured repetitions:    {PERF_REPETITIONS}")
print(f"Warm-up repetitions:     {PERF_WARMUP_REPETITIONS}")
print(f"Metrics table:           {PERF_METRICS_TABLE}")

# COMMAND ----------
CASE_NAMES = ["input_floor", "assignment_only", "full_output"]
if PERF_INCLUDE_FAIL_ON_ERROR:
    CASE_NAMES.append("assignment_only_fail_on_error")


def _source_dataframe():
    """Build a fresh logical source plan for one benchmark case."""
    source = spark.table(PERF_SOURCE_TABLE)
    if PERF_WHERE_SQL:
        source = source.where(PERF_WHERE_SQL)
    if PERF_ROW_LIMIT:
        source = source.limit(PERF_ROW_LIMIT)
    return source


def _input_floor(source):
    """Force required-column reads and write one fixed-width value per row."""
    if serialized_columns:
        return source.select(
            F.xxhash64(
                *[_literal_column(column_name) for column_name in serialized_columns]
            ).alias("source_hash")
        )
    return source.select(F.lit(1).cast("long").alias("source_hash"))


def _evaluated_output(source, *, fail_on_error: bool, full_audit: bool):
    """Build the rules-engine output used by measured cases."""
    return service.evaluate_dataframe(
        source,
        ruleset=ruleset,
        fail_on_error=fail_on_error,
        full_audit=full_audit,
    )


def _benchmark_dataframe(case_name: str):
    """Build one lazy benchmark DataFrame without executing an action."""
    source = _source_dataframe()
    if case_name == "input_floor":
        return _input_floor(source)

    fail_on_error = case_name == "assignment_only_fail_on_error"
    evaluated = _evaluated_output(
        source,
        fail_on_error=fail_on_error,
        full_audit=case_name == "full_output",
    )
    if case_name in {"assignment_only", "assignment_only_fail_on_error"}:
        assignment = F.col("rules_engine_assign")
        if PERF_ASSIGNMENT_FIELD:
            assignment = assignment.getField(PERF_ASSIGNMENT_FIELD)
        return evaluated.select(assignment.alias("assignment_value"))
    if case_name == "full_output":
        engine_output_columns = [
            column_name
            for column_name in evaluated.columns
            if column_name.startswith("rules_engine_")
        ]
        assert engine_output_columns, "Evaluation emitted no rules_engine_ columns."
        return evaluated.select(*engine_output_columns)
    raise ValueError(f"Unsupported benchmark case: {case_name}")


schedule = []
for warmup_index in range(PERF_WARMUP_REPETITIONS):
    for case_name in CASE_NAMES:
        schedule.append((case_name, warmup_index + 1, True))

measured_schedule = [
    (case_name, repetition, False)
    for repetition in range(1, PERF_REPETITIONS + 1)
    for case_name in CASE_NAMES
]
random.Random(PERF_RANDOM_SEED).shuffle(measured_schedule)
schedule.extend(measured_schedule)

print("Execution schedule")
print("------------------")
for case_name, repetition, is_warmup in schedule:
    label = "warm-up" if is_warmup else "measured"
    print(f"{label}: {case_name} repetition={repetition}")

# COMMAND ----------
METRICS_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("recorded_at", T.TimestampType(), False),
        T.StructField("variant", T.StringType(), False),
        T.StructField("commit_sha", T.StringType(), False),
        T.StructField("case_name", T.StringType(), False),
        T.StructField("repetition", T.IntegerType(), False),
        T.StructField("is_warmup", T.BooleanType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("duration_seconds", T.DoubleType(), False),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("error_count", T.LongType(), True),
        T.StructField("assignment_counts_json", T.StringType(), True),
        T.StructField("first_match_counts_json", T.StringType(), True),
        T.StructField("failure_text", T.StringType(), True),
        T.StructField("output_table", T.StringType(), False),
        T.StructField("source_table", T.StringType(), False),
        T.StructField("ruleset_name", T.StringType(), False),
        T.StructField("ruleset_version", T.StringType(), False),
        T.StructField("rule_count", T.IntegerType(), False),
        T.StructField("source_column_count", T.IntegerType(), False),
        T.StructField("dependency_column_count", T.IntegerType(), False),
        T.StructField("dependency_columns_json", T.StringType(), False),
        T.StructField("serialized_column_count", T.IntegerType(), False),
        T.StructField("serialized_columns_json", T.StringType(), False),
        T.StructField("spark_version", T.StringType(), False),
        T.StructField("runtime_version", T.StringType(), False),
    ]
)


def _output_table(case_name: str, repetition: int, is_warmup: bool) -> str:
    """Return a unique managed Delta table name for one timed action."""
    phase = "warmup" if is_warmup else "run"
    table_name = "_".join(
        (
            PERF_OUTPUT_PREFIX,
            _safe_name(PERF_VARIANT),
            _safe_name(run_id),
            _safe_name(case_name),
            phase,
            str(repetition),
        )
    )
    return f"{PERF_OUTPUT_SCHEMA}.{table_name}"


def _materialized_metrics(output_table: str):
    """Read validation metrics from materialized output without rerunning rules."""
    materialized = spark.table(output_table)
    row_count = materialized.count()
    error_count = None
    assignment_counts_json = None
    first_match_counts_json = None
    if "rules_engine_error" in materialized.columns:
        error_count = materialized.where(
            F.col("rules_engine_error").isNotNull()
        ).count()
    if "rules_engine_assign" in materialized.columns:
        assignment_counts = {
            (row["assignment_json"] or "<unassigned>"): row["count"]
            for row in materialized.select(
                F.to_json("rules_engine_assign").alias("assignment_json")
            )
            .groupBy("assignment_json")
            .count()
            .collect()
        }
        assignment_counts_json = json.dumps(assignment_counts, sort_keys=True)
    if "rules_engine_matched_rule_ids" in materialized.columns:
        first_match_counts = {
            (row["first_match_rule_id"] or "<unmatched>"): row["count"]
            for row in materialized.withColumn(
                "first_match_rule_id",
                F.try_element_at("rules_engine_matched_rule_ids", F.lit(1)),
            )
            .groupBy("first_match_rule_id")
            .count()
            .collect()
        }
        first_match_counts_json = json.dumps(first_match_counts, sort_keys=True)
    return row_count, error_count, assignment_counts_json, first_match_counts_json


metrics = []
output_tables = []
for case_name, repetition, is_warmup in schedule:
    output_table = _output_table(case_name, repetition, is_warmup)
    output_tables.append(output_table)
    print("")
    print(
        f"START case={case_name} repetition={repetition} "
        f"warmup={is_warmup} output={output_table}"
    )
    started = time.perf_counter()
    status = "passed"
    failure_text = None
    row_count = None
    error_count = None
    assignment_counts_json = None
    first_match_counts_json = None
    try:
        benchmark_df = _benchmark_dataframe(case_name)
        benchmark_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
    except Exception as exc:
        status = "failed"
        failure_text = str(exc)[:8000]
    duration_seconds = time.perf_counter() - started

    if status == "passed":
        (
            row_count,
            error_count,
            assignment_counts_json,
            first_match_counts_json,
        ) = _materialized_metrics(output_table)

    metrics.append(
        {
            "run_id": run_id,
            "recorded_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "variant": PERF_VARIANT,
            "commit_sha": PERF_COMMIT_SHA,
            "case_name": case_name,
            "repetition": repetition,
            "is_warmup": is_warmup,
            "status": status,
            "duration_seconds": duration_seconds,
            "row_count": row_count,
            "error_count": error_count,
            "assignment_counts_json": assignment_counts_json,
            "first_match_counts_json": first_match_counts_json,
            "failure_text": failure_text,
            "output_table": output_table,
            "source_table": PERF_SOURCE_TABLE,
            "ruleset_name": ruleset.ruleset_name,
            "ruleset_version": ruleset.version,
            "rule_count": len(ruleset.rules),
            "source_column_count": len(source_column_names),
            "dependency_column_count": len(dependency_columns),
            "dependency_columns_json": json.dumps(dependency_columns),
            "serialized_column_count": len(serialized_columns),
            "serialized_columns_json": json.dumps(serialized_columns),
            "spark_version": spark.version,
            "runtime_version": runtime_version,
        }
    )
    print(
        f"END status={status} duration_seconds={duration_seconds:.3f} "
        f"rows={row_count} errors={error_count}"
    )
    if failure_text:
        print(f"Failure: {failure_text}")

# COMMAND ----------
metrics_df = spark.createDataFrame(metrics, schema=METRICS_SCHEMA)
metrics_df.write.format("delta").mode("append").saveAsTable(PERF_METRICS_TABLE)

run_metrics = spark.table(PERF_METRICS_TABLE).where(F.col("run_id") == run_id)
summary = (
    run_metrics.where(~F.col("is_warmup"))
    .groupBy("variant", "commit_sha", "case_name", "status")
    .agg(
        F.count("*").alias("runs"),
        F.min("duration_seconds").alias("min_seconds"),
        F.expr("percentile_approx(duration_seconds, 0.5)").alias("median_seconds"),
        F.max("duration_seconds").alias("max_seconds"),
    )
    .orderBy("case_name", "status")
)

display(summary)
display(run_metrics.orderBy("is_warmup", "case_name", "repetition"))

failed_runs = run_metrics.where(F.col("status") == "failed").count()
assert failed_runs == 0, (
    f"Benchmark run {run_id} had {failed_runs} failed cases. "
    f"Review {PERF_METRICS_TABLE}."
)

if PERF_CLEANUP_OUTPUTS:
    for output_table in output_tables:
        spark.sql(f"DROP TABLE IF EXISTS {_quoted_identifier(output_table)}")
    print(f"Dropped {len(output_tables)} temporary benchmark output tables.")
else:
    print("Temporary benchmark output tables were retained.")

print(f"PASS: Benchmark metrics appended to {PERF_METRICS_TABLE} for run {run_id}.")
