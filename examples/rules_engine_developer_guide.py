# Databricks notebook source
# ruff: noqa: E402
# MAGIC %md
# MAGIC # Rules Engine Developer Guide
# MAGIC
# MAGIC This notebook supplements `README.md`. It is written as a Databricks
# MAGIC Python notebook source file so it can be imported into Databricks or copied
# MAGIC cell by cell into a workspace notebook.
# MAGIC
# MAGIC The notebook demonstrates the core workflows:
# MAGIC
# MAGIC 1. Author canonical YAML.
# MAGIC 2. Compile YAML into immutable dataclasses.
# MAGIC 3. Validate semantic rules.
# MAGIC 4. Export canonical YAML for round-trip review.
# MAGIC 5. Prepare sample input data.
# MAGIC 6. Run Spark compatibility validation.
# MAGIC 7. Create Delta metadata tables.
# MAGIC 8. Test, publish, load, evaluate, and retire a ruleset.
# MAGIC 9. Inspect audit identity and rule coverage.
# MAGIC
# MAGIC This guide intentionally uses small examples. Production workflows should
# MAGIC add environment-specific catalog names, permissions, logging, and approval
# MAGIC controls.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports
# MAGIC
# MAGIC In Databricks, install or copy the package before running this notebook.
# MAGIC The global `spark` variable is provided by Databricks.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Imports the public package APIs used throughout the guide.
# MAGIC - Imports the Spark/Delta repository classes used to persist metadata.
# MAGIC - Imports standard-library helpers used only by this notebook.
# MAGIC
# MAGIC What this cell should prove:
# MAGIC
# MAGIC - Databricks can find the copied or installed `rules_engine` package.
# MAGIC - The notebook is using the package location you expect.
# MAGIC
# MAGIC If this cell fails, stop and fix `sys.path`, cluster library installation,
# MAGIC or repository checkout placement before continuing.

# COMMAND ----------

import os
import re
import sys
from pathlib import Path

root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / "databricks.yml").exists()),
    None,
)
if root:
    src_path = os.path.normpath(root / "src")
    if src_path not in sys.path:
        print(f"Adding to sys.path: {src_path}")
        sys.path.append(src_path)

import rules_engine
from rules_engine import (
    FunctionRegistry,
    RulesEngineService,
    RulesetValidator,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
)
from rules_engine.exceptions import RepositoryError

print(f"rules_engine version: {rules_engine.__version__}")
print(f"rules_engine package: {rules_engine.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Canonical YAML Authoring
# MAGIC
# MAGIC The authoring format is intentionally explicit:
# MAGIC
# MAGIC - canonical operator names only,
# MAGIC - null operands do not match unless an operand defines `default_if_null`,
# MAGIC - `field` reads the original row while `assigned` explicitly reads a
# MAGIC   value committed by a matched lower-order rule,
# MAGIC - absolute tolerance only,
# MAGIC - no expression DSL,
# MAGIC - no aliases.
# MAGIC
# MAGIC This example contains:
# MAGIC
# MAGIC - a row-level condition,
# MAGIC - a numeric row-level threshold,
# MAGIC - an assignment emitted when the rule matches.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Creates a canonical YAML document as a Python string.
# MAGIC - Defines one published ruleset named `Account Review Rules`.
# MAGIC - Defines one rule evaluated by `rule_order = 1`.
# MAGIC - Uses an `all` condition group, meaning every condition must pass.
# MAGIC - Adds explicit condition IDs so metadata and diagnostics are readable.
# MAGIC - Uses the default null behavior: an unresolved null makes its condition fail.
# MAGIC
# MAGIC The rule means:
# MAGIC
# MAGIC - `status` must equal `OPEN`.
# MAGIC - `amount` must be greater than `50`.
# MAGIC - when all conditions pass, assign `review_bucket = high_value_open`.
# MAGIC
# MAGIC What this cell does not do:
# MAGIC
# MAGIC - It does not compile the YAML.
# MAGIC - It does not validate the YAML.
# MAGIC - It does not create Delta metadata rows.
# MAGIC - It does not evaluate business data.

# COMMAND ----------

ruleset_yaml = """
ruleset_id: account_review_rules
ruleset_name: Account Review Rules
version: "1"
owner: Rules Team
owner_department: ALM Engineering
description: Example ruleset used by the developer guide.
rules:
  - rule_id: high_value_open_account
    rule_name: High Value Open Account
    rule_order: 1
    active_flag: true
    stop_on_match: false
    description: Match open rows whose amount is high.
    when:
      all:
        - condition_id: c_status_open
          left: { field: status }
          operator: eq
          right: { literal: OPEN, value_type: string }
          tolerance_abs: "0"
        - condition_id: c_amount_gt_50
          left:
            field: amount
          operator: gt
          right: { literal: 50, value_type: number }
          tolerance_abs: "0"
    assign:
      review_bucket: high_value_open
"""

print(ruleset_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compile And Validate
# MAGIC
# MAGIC `YamlRulesetCompiler` performs shape checks and enum parsing.
# MAGIC `RulesetValidator` enforces semantic validity.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. `compile_text()` parses YAML and builds immutable dataclasses.
# MAGIC 2. The compiler rejects malformed YAML shape and unsupported enum values.
# MAGIC 3. `RulesetValidator.validate()` checks the ruleset's semantic contract.
# MAGIC 4. `validation.to_text()` renders validation output for humans.
# MAGIC
# MAGIC Important distinction:
# MAGIC
# MAGIC - compilation answers "Can this YAML become a ruleset model?"
# MAGIC - validation answers "Does this ruleset obey the semantic contract?"
# MAGIC
# MAGIC No Delta tables are touched in this cell.

# COMMAND ----------

compiler = YamlRulesetCompiler()
ruleset = compiler.compile_text(ruleset_yaml)

semantic_validation = RulesetValidator(FunctionRegistry()).validate(ruleset)
print(semantic_validation.to_text())

if semantic_validation.has_errors():
    raise ValueError(semantic_validation.to_text())

ruleset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Export YAML For Round-Trip Governance
# MAGIC
# MAGIC Exported YAML uses canonical vocabulary and includes explicit generated
# MAGIC identifiers. The exported YAML should compile back into the same canonical
# MAGIC dataclasses.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Converts the compiled dataclass model back into canonical YAML.
# MAGIC - Recompiles the exported YAML.
# MAGIC - Asserts the recompiled model equals the compiled model.
# MAGIC
# MAGIC Why this matters:
# MAGIC
# MAGIC - Governance workflows can compare YAML artifacts rather than relying only
# MAGIC   on in-memory objects.
# MAGIC - Exported YAML includes IDs so round-trip equality is preserved, not just
# MAGIC   semantic similarity.

# COMMAND ----------

exporter = YamlRulesetExporter()
exported_yaml = exporter.export_text(ruleset)
round_tripped = compiler.compile_text(exported_yaml)

assert round_tripped == ruleset
print(exported_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prepare Sample Input Data
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Creates a tiny list of Python dictionaries as input rows.
# MAGIC - Reuses those rows later to build a Spark DataFrame.
# MAGIC - Does not evaluate or persist anything yet.

# COMMAND ----------

input_rows = [
    {"row_id": 1, "account": "A", "status": "OPEN", "amount": 60},
    {"row_id": 2, "account": "A", "status": "OPEN", "amount": 50},
    {"row_id": 3, "account": "B", "status": "OPEN", "amount": 10},
    {"row_id": 4, "account": "C", "status": "CLOSED", "amount": 500},
]

input_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Spark Compatibility Preflight
# MAGIC
# MAGIC The Spark compatibility validator runs the semantic contract and checks
# MAGIC active field references and assignment types against an incoming schema.
# MAGIC
# MAGIC Use it before publishing metadata intended for Databricks execution.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Runs the Spark-specific validation gate against the compiled ruleset.
# MAGIC - Raises before metadata promotion if the ruleset violates the supported
# MAGIC   row-level contract.
# MAGIC
# MAGIC This is a preflight check. It does not write metadata and does not evaluate
# MAGIC input data.

# COMMAND ----------

spark_validation = SparkRulesetCompatibilityValidator(FunctionRegistry()).validate(
    ruleset,
    spark.createDataFrame(input_rows).schema,
)
print(spark_validation.to_text())

if spark_validation.has_errors():
    raise ValueError(spark_validation.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Configure Delta Metadata Tables
# MAGIC
# MAGIC Choose a catalog/schema/table naming convention that fits your environment.
# MAGIC This example uses a dedicated guide schema with the standard rules engine
# MAGIC table names.
# MAGIC
# MAGIC The next cell drops and recreates the guide schema for a clean run. Do not
# MAGIC point `DATABASE` at production metadata. The reset is blocked unless
# MAGIC `RULES_ENGINE_GUIDE_RESET_CONFIRMATION` exactly equals the resulting
# MAGIC `catalog.schema`, making the destructive target visible before execution.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Sets the target catalog/schema path for guide metadata tables.
# MAGIC 2. Drops the guide schema with `CASCADE` so reruns start cleanly.
# MAGIC 3. Recreates the schema.
# MAGIC 4. Builds the standard service facade.
# MAGIC 5. Creates empty Delta tables with explicit schemas.
# MAGIC
# MAGIC Tables created:
# MAGIC
# MAGIC - ruleset_versions
# MAGIC - function_registry
# MAGIC
# MAGIC This cell is intentionally destructive for the guide schema only.

# COMMAND ----------

CATALOG = globals().get("RULES_ENGINE_GUIDE_CATALOG", "main")
SCHEMA = globals().get("RULES_ENGINE_GUIDE_SCHEMA", "rules_engine_guide")
DATABASE = f"{CATALOG}.{SCHEMA}"
RESET_CONFIRMATION = globals().get("RULES_ENGINE_GUIDE_RESET_CONFIRMATION", "")

identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
if not identifier.fullmatch(CATALOG) or not identifier.fullmatch(SCHEMA):
    raise ValueError("Guide catalog and schema must be unquoted Spark identifiers.")
if RESET_CONFIRMATION != DATABASE:
    raise ValueError(
        "This guide resets its dedicated schema. Set "
        f"RULES_ENGINE_GUIDE_RESET_CONFIRMATION={DATABASE!r} to confirm the exact target."
    )

cleanup_sql = f"DROP SCHEMA IF EXISTS {DATABASE} CASCADE"
spark.sql(cleanup_sql)

create_sql = f"CREATE SCHEMA IF NOT EXISTS {DATABASE}"
spark.sql(create_sql)

service = RulesEngineService.from_schema(spark, DATABASE)
service.create_tables(mode="error")
table_names = service.table_names

table_names

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Publish Metadata
# MAGIC
# MAGIC `RulesEngineService` orchestrates validation and direct publication
# MAGIC through the standard public facade.
# MAGIC
# MAGIC Published metadata is immutable by both `(ruleset_id, version)` and
# MAGIC `(ruleset_name, version)`. To rerun this cell, use a new version or reset
# MAGIC the dedicated guide tables.
# MAGIC
# MAGIC `owner` and `owner_department` come from the YAML ruleset metadata.
# MAGIC `published_by` is an optional lifecycle actor field. When omitted,
# MAGIC persisted actor metadata uses `system`, which fits dedicated production
# MAGIC cluster execution.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Uses the configured `RulesEngineService`.
# MAGIC 2. Calls `service.publish(ruleset)`.
# MAGIC 3. Runs metadata validation.
# MAGIC 4. Writes one published row into `ruleset_versions`.
# MAGIC
# MAGIC Tables affected by this cell:
# MAGIC
# MAGIC - `ruleset_versions`: one authoritative metadata row. After publish,
# MAGIC   `status` should be `published`, `owner` should reflect the authored
# MAGIC   owner, `published_by` should be `system`, `published_at` should be
# MAGIC   populated, and `payload_json` should contain the full canonical ruleset.
# MAGIC   Lifecycle status is
# MAGIC   authoritative in the table row, not duplicated inside the payload.
# MAGIC
# MAGIC This cell does **not** evaluate input business data. It only promotes rule
# MAGIC metadata into an auditable, published state. Row evaluation happens in the
# MAGIC next section.

# COMMAND ----------

service.publish(ruleset)

display(spark.table(table_names.ruleset_versions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Load Published Metadata And Evaluate A Spark DataFrame
# MAGIC
# MAGIC Runtime execution should load published metadata from the repository.
# MAGIC `fail_on_error=True` is the default. Tape-cleaning workflows may use
# MAGIC `fail_on_error=False` only when they durably quarantine and monitor every
# MAGIC row with `rules_engine_error`.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Loads the published ruleset from Delta metadata.
# MAGIC 2. Creates a Spark input DataFrame from `input_rows`.
# MAGIC 3. Instantiates the Spark runtime.
# MAGIC 4. Evaluates the DataFrame against the published ruleset.
# MAGIC 5. Displays keyed engine results and applied business rows separately.
# MAGIC
# MAGIC Runtime execution details:
# MAGIC
# MAGIC - `load_published()` reads only `status = published` metadata.
# MAGIC - A Python UDF evaluates final condition and assignment logic per row.
# MAGIC - Building the evaluation and either DataFrame projection is lazy and
# MAGIC   starts no hidden error-check action.
# MAGIC - With `fail_on_error=True`, a row error raises from the UDF during the
# MAGIC   materializing `display` action below.
# MAGIC
# MAGIC For this guide data, row 1 should match. Rows 2 through 4 should not.
# MAGIC This cell does not modify metadata or write result rows to Delta.

# COMMAND ----------

input_df = spark.createDataFrame(input_rows)
evaluation = service.evaluate_dataframe(
    input_df,
    ruleset_name="Account Review Rules",
    version="1",
    key_columns=["row_id"],
    fail_on_error=True,
    full_audit=True,
).persist()
result_df = evaluation.results_df
applied_df = evaluation.apply_assignments()

display(result_df.orderBy("row_id"))
display(applied_df.orderBy("row_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Inspect Output Columns
# MAGIC
# MAGIC `results_df` contains the declared key columns followed by:
# MAGIC
# MAGIC - `rules_engine_error`
# MAGIC - `rules_engine_matched`
# MAGIC - `rules_engine_matched_rule_ids`
# MAGIC - `rules_engine_assign`
# MAGIC - `rules_engine_matched_rules`
# MAGIC - `rules_engine_assignment_results`
# MAGIC - `rules_engine_ruleset`
# MAGIC - `rules_engine_engine_version`
# MAGIC - `rules_engine_audit_schema_version`
# MAGIC
# MAGIC The detailed trace columns and audit-schema marker are present only because
# MAGIC this example sets `full_audit=True`. Assignment output, trace detail, matched rules, and
# MAGIC per-assignment provenance are Spark-native structs and arrays.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Collects the small guide result set to the driver for display.
# MAGIC - Prints key runtime output fields per row.
# MAGIC - Asserts that no row-level errors occurred.
# MAGIC - Asserts that exactly two rows matched.
# MAGIC
# MAGIC Output column meaning:
# MAGIC
# MAGIC - `rules_engine_matched`: whether at least one rule matched the row.
# MAGIC - `rules_engine_matched_rule_ids`: ordered list of matched rule IDs.
# MAGIC - `rules_engine_assign`: one `{applied, value}` outcome per assignment
# MAGIC   target. `applied=false` means keep the business value; `applied=true`
# MAGIC   means use `value`, including when that value is null.
# MAGIC - `rules_engine_matched_rules`: every matched rule in order, including its
# MAGIC   explanation and condition-level source columns, values, and pass/fail state.
# MAGIC - `rules_engine_assignment_results`: every proposed assignment plus its
# MAGIC   authored expression, original value, changed/effective flags, immediate
# MAGIC   override, and final-winner provenance.
# MAGIC - `rules_engine_error`: row-level evaluator error text, null when clean.
# MAGIC - `rules_engine_ruleset`: immutable ruleset ID, version, and content hash.
# MAGIC - `rules_engine_engine_version`: worker evaluator version, verified against the driver.
# MAGIC - `rules_engine_audit_schema_version`: version of the persisted full-audit contract.
# MAGIC
# MAGIC `apply_assignments()` returns only business columns. Existing targets are
# MAGIC replaced in place, new targets are appended, and unmatched targets keep
# MAGIC their current values. Struct targets are replaced as whole values; we do
# MAGIC not merge individual nested fields.
# MAGIC
# MAGIC In production, avoid collecting large DataFrames. Use aggregations,
# MAGIC displays, or writes instead.

# COMMAND ----------

rows = result_df.orderBy("row_id").collect()
for row in rows:
    matched_rule_explanations = [
        trace["explanation"] for trace in row["rules_engine_matched_rules"]
    ]
    print(
        row["row_id"],
        row["rules_engine_matched"],
        row["rules_engine_matched_rule_ids"],
        row["rules_engine_assign"],
        matched_rule_explanations,
        row["rules_engine_assignment_results"],
        row["rules_engine_error"],
    )

assert result_df.where("rules_engine_error IS NOT NULL").count() == 0
assert result_df.where("rules_engine_matched").count() == 2
assert [row["row_id"] for row in rows if row["rules_engine_matched"]] == [1, 2]
assert all(
    row["rules_engine_ruleset"]["id"] == ruleset.ruleset_id
    and row["rules_engine_ruleset"]["version"] == ruleset.version
    and row["rules_engine_ruleset"]["content_hash"]
    and row["rules_engine_engine_version"] == rules_engine.__version__
    and row["rules_engine_audit_schema_version"] == rules_engine.AUDIT_SCHEMA_VERSION
    for row in rows
)
evaluation.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Inspect Coverage
# MAGIC
# MAGIC Coverage reports expose dead, broad, and clean no-match behavior on
# MAGIC representative data. Coverage starts one Spark action for aggregate
# MAGIC counts; the returned `no_match_rows` is a filtered view of the same
# MAGIC evaluation.

# COMMAND ----------

coverage = service.coverage_report(
    input_df,
    ruleset=ruleset,
    broad_match_threshold=0.40,
)
assert coverage.total_row_count == 4
assert coverage.no_match_count == 2
assert coverage.error_count == 0
assert coverage.suspiciously_broad_rule_ids == ("high_value_open_account",)
display(coverage.no_match_rows.orderBy("row_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Query Persisted Metadata
# MAGIC
# MAGIC Metadata is deliberately relational where fields are stable and JSON-shaped
# MAGIC only where operand/function payloads vary.
# MAGIC
# MAGIC Clean validation runs write an explicit `INFO / VALIDATION_PASSED` row, so
# MAGIC the payload columns preserve the exact canonical metadata that was published.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Displays each metadata table created by the repository.
# MAGIC - Lets engineers inspect the authoritative ruleset version payload and
# MAGIC   function registry table.
# MAGIC
# MAGIC What to inspect:
# MAGIC
# MAGIC - `ruleset_versions.status` should be `published`.
# MAGIC - `ruleset_versions.owner` should reflect the authored owner.
# MAGIC - `ruleset_versions.owner_department` should reflect the authored department.
# MAGIC - `ruleset_versions.published_by` should be `system` after publish.
# MAGIC - `ruleset_versions.content_hash` should be populated.
# MAGIC - `ruleset_versions.payload_json` should contain the full canonical ruleset.
# MAGIC - `ruleset_versions.rule_count` and `condition_count` should be populated.
# MAGIC
# MAGIC This cell is read-only.

# COMMAND ----------

display(spark.table(table_names.ruleset_versions))
display(spark.table(table_names.function_registry))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Retire Published Metadata
# MAGIC
# MAGIC Retired metadata should no longer load through `load_published`.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Calls `service.retire(ruleset_id, version, retired_by=...)`.
# MAGIC 2. Updates the `ruleset_versions` row to `status = retired`.
# MAGIC 3. Attempts to load the same ruleset through `load_published()`.
# MAGIC 4. Expects `RepositoryError`, because retired metadata is not published.
# MAGIC 5. Displays the `ruleset_versions` table so the lifecycle transition is visible.
# MAGIC
# MAGIC Retire does not delete metadata. It changes lifecycle status so prior
# MAGIC metadata remains auditable but is no longer returned by runtime
# MAGIC `load_published()`.

# COMMAND ----------

service.retire("account_review_rules", "1", retired_by="developer_guide")

try:
    service.load_published("Account Review Rules", version="1")
except RepositoryError as exc:
    print(f"Expected load failure after retire: {exc}")
else:
    raise AssertionError("Retired ruleset was still loadable as published.")

display(spark.table(table_names.ruleset_versions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Production Checklist
# MAGIC
# MAGIC Before production use:
# MAGIC
# MAGIC - run unit tests,
# MAGIC - run Spark tests on the target Databricks runtime,
# MAGIC - run Databricks validation against a disposable schema,
# MAGIC - validate representative production-like rulesets,
# MAGIC - review metadata table permissions and retention,
# MAGIC - use `fail_on_error=True`, or write and monitor a governed quarantine
# MAGIC   when tape-cleaning operations require `fail_on_error=False`,
# MAGIC - keep `include_error_traceback=False` outside controlled debugging,
# MAGIC
# MAGIC Recommended promotion path:
# MAGIC
# MAGIC 1. Author and review canonical YAML.
# MAGIC 2. Compile, validate, and export YAML.
# MAGIC 3. Publish into non-production metadata tables.
# MAGIC 4. Run small hand-verified DataFrame tests.
# MAGIC 5. Run representative volume/performance tests.
# MAGIC 6. Promote the exact package version and YAML artifact together.
