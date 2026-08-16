# Rules Engine Serverless Performance Testing

Use `notebooks/rules_engine_serverless_performance.py` to measure rules-engine
changes on Databricks serverless. The notebook intentionally avoids `cache`,
`persist`, `unpersist`, `localCheckpoint`, `clearCache`, and RDD APIs.

## Measurement Contract

Every timed case performs an actual Delta write to a unique managed table.
Post-run row counts, error counts, assignment distributions, and winner
distributions read that materialized output and therefore do not execute the
rules UDF again. Measured case order is randomized, warm-up runs are recorded
separately, and durable results are appended to `PERF_METRICS_TABLE` before
temporary tables are dropped.

The cases are:

| Case | Purpose |
| --- | --- |
| `input_floor` | Reads every column the tested runtime serializes, computes one `xxhash64` value per row, and writes it. The baseline runtime reads all source columns; the optimized runtime reads only rule dependencies. This is context, not a value to subtract mechanically from other cases. |
| `assignment_only` | Evaluates the ruleset with `fail_on_error=False` and writes only the configured assignment field. This most closely represents leaf-key production use. |
| `full_output` | Evaluates once and writes every documented rules-engine output, including the winning trace. Materialized output supplies error and winner-distribution evidence. |
| `assignment_only_fail_on_error` | Runs assignment-only output with the lazy single-pass `fail_on_error=True` contract. Its difference from `assignment_only` measures worker exception-checking overhead on clean data. |

## Required Parameters

Supply parameters as Databricks job/notebook widgets or define matching Python
globals before running the notebook.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `PERF_RULES_ENGINE_SCHEMA` | Yes |  | Schema containing rules-engine metadata tables. |
| `PERF_SOURCE_TABLE` | Yes |  | Immutable Delta input snapshot used by every case. |
| `PERF_RULESET_NAME` | Yes |  | Published ruleset name. |
| `PERF_RULESET_VERSION` | No | published-name resolution | Explicit published version. Prefer setting it. |
| `PERF_OUTPUT_SCHEMA` | No | rules-engine schema | Writable schema for metrics and temporary Delta tables. |
| `PERF_VARIANT` | No | `working_tree` | Stable comparison label such as `baseline_32fb617` or `optimized_054130a`. |
| `PERF_COMMIT_SHA` | No | `unknown` | Exact tested Git commit. |
| `PERF_ASSIGNMENT_FIELD` | No | `leaf_key` | Child field selected from `rules_engine_assign` in assignment-only cases. Use an empty value to write the complete assignment struct. |
| `PERF_WHERE_SQL` | No |  | Optional identical source filter applied to every case. |
| `PERF_ROW_LIMIT` | No | `0` | Optional row limit. Use `0` for the complete source snapshot. |
| `PERF_REPETITIONS` | No | `5` | Number of randomized measured runs per case. |
| `PERF_WARMUP_REPETITIONS` | No | `1` | Warm-up runs per case, retained in metrics but excluded from summaries. |
| `PERF_RANDOM_SEED` | No | `20260715` | Reproducible measured-case ordering. |
| `PERF_INCLUDE_FAIL_ON_ERROR` | No | `true` | Include the fail-fast worker case. Use only clean benchmark data. |
| `PERF_CLEANUP_OUTPUTS` | No | `true` | Drop temporary result tables after durable metrics are written. |
| `PERF_METRICS_TABLE` | No | `<output_schema>.rules_engine_performance_results` | Durable Delta metrics table. |
| `PERF_OUTPUT_PREFIX` | No | `rules_engine_perf` | Prefix for temporary managed tables. |

## Comparison Procedure

1. Use one immutable Delta source table version and one explicit published
   ruleset version for all variants.
2. Run the notebook from the exact commit under test. Record that commit in
   `PERF_COMMIT_SHA` and use a unique `PERF_VARIANT`.
3. Keep source filter, row limit, repetitions, assignment field, serverless
   environment, and notebook parameters identical.
4. Compare medians rather than the fastest run. Review the complete duration
   spread and Databricks query profile for skew, retries, input bytes, and task
   time.
5. Confirm `row_count`, `error_count`, `assignment_counts_json`, and
   `winner_counts_json` before accepting a faster variant. Winner position
   determines how much losing-rule work each row performs.
6. Treat `input_floor` as context only. Delta output shapes differ across cases,
   so durations are not algebraically interchangeable.

To compare commit `32fb617` with `054130a`, retain a workspace copy of this
benchmark notebook outside the Git folder before switching the Git folder to
the older commit. Run both variants against the same source and metrics table.
Do not rebuild or refresh the source snapshot between variants. The notebook
contains its own dependency inspection for compatibility with `32fb617`; it
records all source columns as serialized for that baseline and the dependency
subset for optimized versions that expose `required_source_columns`.

## Acceptance Guidance

Accept an optimization only when:

- the median `assignment_only` action improves across both a shorter ruleset
  and a 100-plus-rule ruleset;
- no measured case fails;
- materialized row counts and error counts match;
- assignment and winner distributions match exactly; and
- the query profile does not reveal increased retries, spill, or skew that the
  wall-clock median conceals.

`fail_on_error=True` now raises from the UDF during the notebook's materializing
write, so it does not launch a separate validation action or require caching.
`fail_on_error=False` remains the appropriate case for measuring a governed
write-once/quarantine workflow.
