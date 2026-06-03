# Databricks notebook source
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
# MAGIC 4. Normalize metadata for persistence.
# MAGIC 5. Export canonical YAML for round-trip governance.
# MAGIC 6. Run the pure-Python runtime.
# MAGIC 7. Run Spark compatibility validation.
# MAGIC 8. Create Delta metadata tables.
# MAGIC 9. Save, publish, load, evaluate, and retire a ruleset.
# MAGIC 10. Translate a reconciliation CSV spec into canonical YAML.
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
# MAGIC - Imports the reconciliation translation utility, which is intentionally
# MAGIC   outside the runtime package.
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

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from rules_engine import (
    FunctionRegistry,
    RulesEngineRuntime,
    RulesetNormalizer,
    RulesEngineService,
    RulesetValidator,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
)
from rules_engine.exceptions import RepositoryError
from tools.recon_spec_translation.audit import write_audit
from tools.recon_spec_translation.reader_csv import read_reconciliation_csv
from tools.recon_spec_translation.translator import ReconciliationSpecTranslator
from tools.recon_spec_translation.writer_yaml import write_yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Canonical YAML Authoring
# MAGIC
# MAGIC The authoring format is intentionally explicit:
# MAGIC
# MAGIC - canonical operator names only,
# MAGIC - explicit null behavior,
# MAGIC - absolute tolerance only,
# MAGIC - explicit aggregate scope,
# MAGIC - no expression DSL,
# MAGIC - no aliases.
# MAGIC
# MAGIC This example contains:
# MAGIC
# MAGIC - a row-level condition,
# MAGIC - a group aggregate,
# MAGIC - a filtered dataset aggregate,
# MAGIC - an assignment emitted when the rule matches.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Creates a canonical YAML document as a Python string.
# MAGIC - Defines one published ruleset named `Account Review Rules`.
# MAGIC - Defines one rule evaluated by `rule_order = 1`.
# MAGIC - Uses an `all` condition group, meaning every condition must pass.
# MAGIC - Adds explicit condition IDs so metadata and diagnostics are readable.
# MAGIC - Uses `null_result_mode: "null"` with quotes because unquoted YAML `null`
# MAGIC   is parsed as Python `None`, not as the canonical string value.
# MAGIC
# MAGIC The rule means:
# MAGIC
# MAGIC - `status` must equal `OPEN`.
# MAGIC - the sum of `amount` within the current `account` group must be greater
# MAGIC   than `100`.
# MAGIC - the dataset-level sum of `amount` for rows where `status == OPEN` must
# MAGIC   be greater than `100`.
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
    description: Match open accounts whose account-level amount total is high.
    when:
      all:
        - condition_id: c_status_open
          left: { field: status }
          operator: eq
          right: { literal: OPEN, value_type: string }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c_group_sum_gt_100
          left:
            aggregate:
              function: sum
              field: amount
              scope: group
              by: [account]
              args: {}
              order_by: []
              null_input_mode: ignore
              null_result_mode: "null"
          operator: gt
          right: { literal: 100, value_type: number }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c_open_dataset_sum_gt_100
          left:
            aggregate:
              function: sum
              field: amount
              scope: dataset
              args: {}
              order_by: []
              filter:
                all:
                  - left: { field: status }
                    operator: eq
                    right: { literal: OPEN, value_type: string }
                    tolerance_abs: "0"
                    null_input_mode: propagate
                    null_result_mode: "null"
              null_input_mode: ignore
              null_result_mode: "null"
          operator: gt
          right: { literal: 100, value_type: number }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket: high_value_open
"""

print(ruleset_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compile, Validate, Normalize
# MAGIC
# MAGIC `YamlRulesetCompiler` performs shape checks and enum parsing.
# MAGIC `RulesetValidator` enforces semantic validity.
# MAGIC `RulesetNormalizer` materializes explicit persisted defaults.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. `compile_text()` parses YAML and builds immutable dataclasses.
# MAGIC 2. The compiler rejects malformed YAML shape and unsupported enum values.
# MAGIC 3. `RulesetValidator.validate()` checks semantic rules that apply to both
# MAGIC    YAML-authored and code-authored rulesets.
# MAGIC 4. `validation.to_text()` renders validation output for humans.
# MAGIC 5. `RulesetNormalizer.normalize_ruleset()` materializes publish-ready
# MAGIC    explicit values.
# MAGIC
# MAGIC Important distinction:
# MAGIC
# MAGIC - compilation answers "Can this YAML become a ruleset model?"
# MAGIC - validation answers "Does this ruleset obey the semantic contract?"
# MAGIC - normalization answers "Is this ruleset fully explicit for persistence
# MAGIC   and runtime?"
# MAGIC
# MAGIC No Delta tables are touched in this cell.

# COMMAND ----------

compiler = YamlRulesetCompiler()
ruleset = compiler.compile_text(ruleset_yaml)

semantic_validation = RulesetValidator(FunctionRegistry()).validate(ruleset)
print(semantic_validation.to_text())

if semantic_validation.has_errors():
    raise ValueError(semantic_validation.to_text())

normalized_ruleset = RulesetNormalizer().normalize_ruleset(ruleset)
normalized_ruleset

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
# MAGIC - Converts the normalized dataclass model back into canonical YAML.
# MAGIC - Recompiles the exported YAML.
# MAGIC - Asserts the recompiled model equals the normalized model.
# MAGIC
# MAGIC Why this matters:
# MAGIC
# MAGIC - Engineers can author or refine rules through code, then export canonical
# MAGIC   YAML for review and source control.
# MAGIC - Governance workflows can compare YAML artifacts rather than relying only
# MAGIC   on in-memory objects.
# MAGIC - Exported YAML includes IDs so round-trip equality is preserved, not just
# MAGIC   semantic similarity.

# COMMAND ----------

exporter = YamlRulesetExporter()
exported_yaml = exporter.export_text(normalized_ruleset)
round_tripped = compiler.compile_text(exported_yaml)

assert round_tripped == normalized_ruleset
print(exported_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pure-Python Runtime
# MAGIC
# MAGIC The pure-Python runtime is useful for local unit tests, small fixtures, and
# MAGIC semantic parity checks. It evaluates aggregates over the incoming row set
# MAGIC exactly as provided.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Creates a tiny list of Python dictionaries as input rows.
# MAGIC - Evaluates the normalized ruleset directly in Python.
# MAGIC - Produces output rows and compact execution traces.
# MAGIC
# MAGIC Runtime semantics demonstrated here:
# MAGIC
# MAGIC - aggregates are computed over the input row list exactly as supplied,
# MAGIC - no rows are deduplicated or filtered outside explicit aggregate filters,
# MAGIC - assignments appear only when a rule matches,
# MAGIC - traces show which rules matched.
# MAGIC
# MAGIC What this cell does not do:
# MAGIC
# MAGIC - It does not use Spark.
# MAGIC - It does not read published metadata from Delta.
# MAGIC - It does not persist output.

# COMMAND ----------

class NotebookDummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("This example passes rulesets directly.")


input_rows = [
    {"row_id": 1, "account": "A", "status": "OPEN", "amount": 60},
    {"row_id": 2, "account": "A", "status": "OPEN", "amount": 50},
    {"row_id": 3, "account": "B", "status": "OPEN", "amount": 10},
    {"row_id": 4, "account": "C", "status": "CLOSED", "amount": 500},
]

python_runtime = RulesEngineRuntime(NotebookDummyRepository(), FunctionRegistry())
python_output, python_traces = python_runtime.evaluate(input_rows, normalized_ruleset)

for row in python_output:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Spark Compatibility Preflight
# MAGIC
# MAGIC The Spark compatibility validator catches rules that are valid metadata but
# MAGIC intentionally unsupported by the current Spark runtime.
# MAGIC
# MAGIC Use it before publishing metadata intended for Databricks execution.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Runs the Spark-specific validation gate against the normalized ruleset.
# MAGIC - Raises before metadata promotion if Spark execution would fail or weaken
# MAGIC   semantics.
# MAGIC
# MAGIC Examples of rules this validator rejects:
# MAGIC
# MAGIC - `median` and `quantile`, because exact Spark implementation is not enabled.
# MAGIC - aggregate `null_input_mode=error`.
# MAGIC - aggregate `null_result_mode=error`.
# MAGIC - aggregate-filter error null modes.
# MAGIC - `first` or `last` with aggregate `null_input_mode=propagate`.
# MAGIC
# MAGIC This is a preflight check. It does not write metadata and does not evaluate
# MAGIC input data.

# COMMAND ----------

spark_validation = SparkRulesetCompatibilityValidator(FunctionRegistry()).validate(
    normalized_ruleset
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
# MAGIC point `DATABASE` at production metadata.
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

CATALOG = "main"
SCHEMA = "rules_engine_guide"
DATABASE = f"{CATALOG}.{SCHEMA}"

cleanup_sql = f"DROP SCHEMA IF EXISTS {DATABASE} CASCADE"
spark.sql(cleanup_sql)

create_sql = f"CREATE SCHEMA IF NOT EXISTS {DATABASE}"
spark.sql(create_sql)

service = RulesEngineService.from_schema(spark, DATABASE)
service.create_tables(mode="overwrite")
table_names = service.table_names

table_names

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Publish Metadata
# MAGIC
# MAGIC `RulesEngineService` orchestrates normalization, validation, and direct
# MAGIC publication through the standard public facade.
# MAGIC
# MAGIC Published metadata is immutable by `(ruleset_id, version)`. If you rerun
# MAGIC this cell after publication, retire or overwrite the development tables first.
# MAGIC
# MAGIC `owner` and `owner_department` come from the YAML/Python ruleset metadata.
# MAGIC `published_by` is an optional lifecycle actor field. When omitted,
# MAGIC persisted actor metadata uses `system`, which fits dedicated production
# MAGIC cluster execution.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Uses the configured `RulesEngineService`.
# MAGIC 2. Calls `service.publish(ruleset)`.
# MAGIC 3. Normalizes the ruleset so persistence-ready fields are explicit.
# MAGIC 4. Runs semantic validation plus Spark compatibility validation.
# MAGIC 5. Writes one published row into `ruleset_versions`.
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
# MAGIC `fail_on_error=True` is the default and should remain enabled for regulated
# MAGIC workflows unless downstream controls explicitly inspect `rules_engine_error`.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Loads the published ruleset from Delta metadata.
# MAGIC 2. Creates a Spark input DataFrame from `input_rows`.
# MAGIC 3. Instantiates the Spark runtime.
# MAGIC 4. Evaluates the DataFrame against the published ruleset.
# MAGIC 5. Displays output ordered by `row_id`.
# MAGIC
# MAGIC Runtime execution details:
# MAGIC
# MAGIC - `load_published()` reads only `status = published` metadata.
# MAGIC - Aggregate operands are discovered from the ruleset.
# MAGIC - Spark precomputes group and dataset aggregates.
# MAGIC - Aggregate values are joined back to the original input rows.
# MAGIC - A Python UDF evaluates final condition and assignment logic per row.
# MAGIC - Temporary aggregate columns are dropped from the returned DataFrame.
# MAGIC - `fail_on_error=True` performs an error check and raises if any row has
# MAGIC   `rules_engine_error`.
# MAGIC
# MAGIC For this guide data, rows 1 and 2 should match. Rows 3 and 4 should not.
# MAGIC This cell does not modify metadata or write result rows to Delta.

# COMMAND ----------

input_df = spark.createDataFrame(input_rows)
result_df = service.evaluate_dataframe(
    input_df,
    ruleset_name="Account Review Rules",
    version="1",
    fail_on_error=True,
)

display(result_df.orderBy("row_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Inspect Output Columns
# MAGIC
# MAGIC The Spark runtime appends:
# MAGIC
# MAGIC - `rules_engine_matched`
# MAGIC - `rules_engine_matched_rule_ids`
# MAGIC - `rules_engine_assign`
# MAGIC - `rules_engine_rule_results`
# MAGIC - `rules_engine_winning_rule`
# MAGIC - `rules_engine_winning_rule_id`
# MAGIC - `rules_engine_winning_rule_name`
# MAGIC - `rules_engine_winning_rule_explanation`
# MAGIC - `rules_engine_error`
# MAGIC
# MAGIC Assignment, rule result, and winning-rule payloads are JSON strings.
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
# MAGIC - `rules_engine_assign`: JSON object containing assignments from matched
# MAGIC   rules, or null when no rule matched.
# MAGIC - `rules_engine_rule_results`: compact JSON array of per-rule traces with
# MAGIC   condition-level source columns, evaluated values, and pass/fail state.
# MAGIC - `rules_engine_winning_rule`: JSON object for the first matched rule, or
# MAGIC   null when no rule matched.
# MAGIC - `rules_engine_winning_rule_id`: ID of the first matched rule.
# MAGIC - `rules_engine_winning_rule_name`: name of the first matched rule.
# MAGIC - `rules_engine_winning_rule_explanation`: readable summary of the passed
# MAGIC   conditions from the first matched rule.
# MAGIC - `rules_engine_error`: row-level evaluator error text, null when clean.
# MAGIC
# MAGIC In production, avoid collecting large DataFrames. Use aggregations,
# MAGIC displays, or writes instead.

# COMMAND ----------

rows = result_df.orderBy("row_id").collect()
for row in rows:
    print(
        row["row_id"],
        row["rules_engine_matched"],
        row["rules_engine_matched_rule_ids"],
        row["rules_engine_assign"],
        row["rules_engine_winning_rule_explanation"],
        row["rules_engine_error"],
    )

assert result_df.where("rules_engine_error IS NOT NULL").count() == 0
assert result_df.where("rules_engine_matched").count() == 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Query Persisted Metadata
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
# MAGIC ## 12. Retire Published Metadata
# MAGIC
# MAGIC Retired metadata should no longer load through `load_published`.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC 1. Calls `repository.retire(ruleset_id, version, retired_by=...)`.
# MAGIC 2. Updates the `ruleset_versions` row to `status = retired`.
# MAGIC 3. Attempts to load the same ruleset through `load_published()`.
# MAGIC 4. Expects `RepositoryError`, because retired metadata is not published.
# MAGIC 5. Displays the `ruleset_versions` table so the lifecycle transition is visible.
# MAGIC
# MAGIC Retire does not delete metadata. It changes lifecycle status so prior
# MAGIC metadata remains auditable but is no longer returned by runtime
# MAGIC `load_published()`.

# COMMAND ----------

repository.retire("account_review_rules", "1", retired_by="developer_guide")

try:
    repository.load_published("Account Review Rules", version="1")
except RepositoryError as exc:
    print(f"Expected load failure after retire: {exc}")
else:
    raise AssertionError("Retired ruleset was still loadable as published.")

display(spark.table(table_names.ruleset_versions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Reconciliation CSV Translation
# MAGIC
# MAGIC The reconciliation translator is outside runtime execution. It converts a
# MAGIC flat source CSV spec into canonical YAML and writes an audit artifact.
# MAGIC
# MAGIC Source columns:
# MAGIC
# MAGIC - `MatchRuleName`
# MAGIC - `GroupSequence`
# MAGIC - `GroupJoinOperator`
# MAGIC - `CriteriaSequence`
# MAGIC - `FieldName`
# MAGIC - `ValueOperator`
# MAGIC - `Value`
# MAGIC - `JoinType`
# MAGIC
# MAGIC `JoinType` and `GroupJoinOperator` are folded left-to-right.
# MAGIC Translated rules default to `stop_on_match: true`, so the first matching
# MAGIC rule by `rule_order` wins. Treat the translated YAML as a first-pass
# MAGIC artifact for manual refinement before publish.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Creates a small CSV reconciliation spec in a temporary directory.
# MAGIC - Reads source rows with `read_reconciliation_csv()`.
# MAGIC - Translates source rows into canonical rules engine YAML payload.
# MAGIC - Writes a translation audit JSON artifact.
# MAGIC - Fails if any source pattern is unsupported or ambiguous.
# MAGIC - Writes translated YAML.
# MAGIC - Prints both YAML and audit output.
# MAGIC
# MAGIC Translator behavior:
# MAGIC
# MAGIC - Groups rows by `MatchRuleName`.
# MAGIC - Orders criteria by `GroupSequence` and `CriteriaSequence`.
# MAGIC - Maps supported source operators to canonical operators.
# MAGIC - Emits `assign.translated_match_rule_name = MatchRuleName`.
# MAGIC - Emits `stop_on_match: true` by default.
# MAGIC
# MAGIC The translator is not a runtime dependency. Its output should be reviewed
# MAGIC and refined manually before publication.

# COMMAND ----------

csv_text = """MatchRuleName,GroupSequence,GroupJoinOperator,CriteriaSequence,FieldName,ValueOperator,Value,JoinType
Rule A,1,,1,status,TextEquals,OPEN,And
Rule A,1,,2,account,TextContains,A,
Rule B,1,Or,1,status,TextEquals,CLOSED,
Rule B,2,,1,amount,NumericGreaterThan,100,
"""

with TemporaryDirectory() as temp_dir:
    temp_path = Path(temp_dir)
    source_csv = temp_path / "source_spec.csv"
    yaml_path = temp_path / "translated_rules.yaml"
    audit_path = temp_path / "translation_audit.json"
    source_csv.write_text(csv_text, encoding="utf-8")

    source_rows = read_reconciliation_csv(source_csv)
    translation = ReconciliationSpecTranslator(
        assignment_target_field="translated_match_rule_name"
    ).translate(
        source_rows,
        owner="Rules Team",
        owner_department="ALM Engineering",
    )

    write_audit(translation.audit_records, audit_path)
    if any(record.failures for record in translation.audit_records):
        print(audit_path.read_text(encoding="utf-8"))
        raise ValueError("Translation failed.")

    write_yaml(translation.payload, yaml_path)
    translated_yaml = yaml_path.read_text(encoding="utf-8")
    translated_audit = json.loads(audit_path.read_text(encoding="utf-8"))

print(translated_yaml)
print(json.dumps(translated_audit, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Compile Translated YAML
# MAGIC
# MAGIC Translation output should compile and validate like any other canonical
# MAGIC ruleset.
# MAGIC
# MAGIC What this cell does:
# MAGIC
# MAGIC - Compiles the YAML produced by the translation utility.
# MAGIC - Runs the standard semantic validator.
# MAGIC - Raises if translated YAML violates the engine contract.
# MAGIC
# MAGIC This step proves that translation output is a valid authoring artifact.
# MAGIC It does not publish the translated ruleset and does not execute it against
# MAGIC business data.

# COMMAND ----------

translated_ruleset = compiler.compile_text(translated_yaml)
translated_validation = RulesetValidator(FunctionRegistry()).validate(translated_ruleset)

print(translated_validation.to_text())
if translated_validation.has_errors():
    raise ValueError(translated_validation.to_text())

translated_ruleset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Production Checklist
# MAGIC
# MAGIC Before production use:
# MAGIC
# MAGIC - run unit tests,
# MAGIC - run Spark tests on the target Databricks runtime,
# MAGIC - run the system test against a disposable schema,
# MAGIC - validate representative production-like rulesets,
# MAGIC - compare translated reconciliation output against known-good results,
# MAGIC - review metadata table permissions and retention,
# MAGIC - define package deployment and versioning controls,
# MAGIC - keep `fail_on_error=True` unless there is explicit downstream error
# MAGIC   handling.
# MAGIC
# MAGIC Recommended promotion path:
# MAGIC
# MAGIC 1. Translate source specs to YAML where helpful.
# MAGIC 2. Manually refine YAML for semantics not captured in source specs.
# MAGIC 3. Compile, validate, normalize, and export YAML.
# MAGIC 4. Publish into non-production metadata tables.
# MAGIC 5. Run small hand-verified DataFrame tests.
# MAGIC 6. Compare against known-good legacy output where available.
# MAGIC 7. Run representative volume/performance tests.
# MAGIC 8. Decide packaging and release tagging.
# MAGIC 9. Promote the exact package version and YAML artifact together.
