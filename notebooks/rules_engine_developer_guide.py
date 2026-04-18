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

# COMMAND ----------

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from rules_engine import (
    FunctionRegistry,
    PublishService,
    RulesEngineRuntime,
    RulesetNormalizer,
    RulesetValidator,
    SparkRulesEngineRuntime,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
)
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
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

# COMMAND ----------

ruleset_yaml = """
ruleset_id: account_review_rules
ruleset_name: Account Review Rules
version: "1"
status: draft
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
          null_result_mode: null
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
              null_result_mode: null
          operator: gt
          right: { literal: 100, value_type: number }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: null
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
                    null_result_mode: null
              null_input_mode: ignore
              null_result_mode: null
          operator: gt
          right: { literal: 100, value_type: number }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: null
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
# MAGIC This example uses `default` and a notebook-specific prefix.
# MAGIC
# MAGIC The next cell overwrites smoke-test tables. Do not point it at production
# MAGIC metadata tables.

# COMMAND ----------

DATABASE = "default"
TABLE_PREFIX = "rules_engine_guide"
AUTHOR = "developer_guide_author"
APPROVER = "developer_guide_approver"


def table_name(suffix: str) -> str:
    return f"{DATABASE}.{TABLE_PREFIX}_{suffix}"


table_names = RulesEngineTableNames(
    rulesets=table_name("rulesets"),
    rules=table_name("rules"),
    condition_groups=table_name("condition_groups"),
    conditions=table_name("conditions"),
    assignments=table_name("assignments"),
    function_registry=table_name("function_registry"),
    validation_results=table_name("validation_results"),
)

repository = SparkDeltaRulesetRepository(spark, table_names)
repository.create_base_tables(mode="overwrite")

table_names

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Save Draft And Publish
# MAGIC
# MAGIC `PublishService` orchestrates normalization, validation, validation-result
# MAGIC persistence, draft save, and publish status update.
# MAGIC
# MAGIC Published metadata is immutable by `(ruleset_id, version)`. If you rerun
# MAGIC this cell after publication, retire or overwrite the smoke tables first.

# COMMAND ----------

publish_service = PublishService(
    repository=repository,
    validator=SparkRulesetCompatibilityValidator(FunctionRegistry()),
    normalizer=RulesetNormalizer(),
)

draft_validation = publish_service.save_draft(
    ruleset,
    created_by=AUTHOR,
)
print(draft_validation.to_text())

if draft_validation.has_errors():
    raise ValueError(draft_validation.to_text())

publish_service.publish(
    ruleset,
    created_by=AUTHOR,
    published_by=APPROVER,
)

display(spark.table(table_names.rulesets))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Load Published Metadata And Evaluate A Spark DataFrame
# MAGIC
# MAGIC Runtime execution should load published metadata from the repository.
# MAGIC `fail_on_error=True` is the default and should remain enabled for regulated
# MAGIC workflows unless downstream controls explicitly inspect `rules_engine_error`.

# COMMAND ----------

published_ruleset = repository.load_published("Account Review Rules", version="1")

input_df = spark.createDataFrame(input_rows)
spark_runtime = SparkRulesEngineRuntime(repository, FunctionRegistry())
result_df = spark_runtime.evaluate_dataframe(
    input_df,
    published_ruleset,
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
# MAGIC - `rules_engine_error`
# MAGIC
# MAGIC Assignment and rule result payloads are JSON strings.

# COMMAND ----------

rows = result_df.orderBy("row_id").collect()
for row in rows:
    print(
        row["row_id"],
        row["rules_engine_matched"],
        row["rules_engine_matched_rule_ids"],
        row["rules_engine_assign"],
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
# MAGIC the validation table provides positive evidence that validation ran.

# COMMAND ----------

display(spark.table(table_names.rulesets))
display(spark.table(table_names.rules))
display(spark.table(table_names.condition_groups))
display(spark.table(table_names.conditions))
display(spark.table(table_names.assignments))
display(spark.table(table_names.validation_results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Retire Published Metadata
# MAGIC
# MAGIC Retired metadata should no longer load through `load_published`.

# COMMAND ----------

repository.retire("account_review_rules", "1")

try:
    repository.load_published("Account Review Rules", version="1")
except RepositoryError as exc:
    print(f"Expected load failure after retire: {exc}")
else:
    raise AssertionError("Retired ruleset was still loadable as published.")

display(spark.table(table_names.rulesets))

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
    ).translate(source_rows)

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
# MAGIC - run the smoke test against non-production tables,
# MAGIC - validate representative production-like rulesets,
# MAGIC - compare translated reconciliation output against known-good results,
# MAGIC - review metadata table permissions and retention,
# MAGIC - define package deployment and versioning controls,
# MAGIC - keep `fail_on_error=True` unless there is explicit downstream error
# MAGIC   handling.
