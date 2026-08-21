# Databricks notebook source
# MAGIC %md
# MAGIC # Rules Engine Quickstart
# MAGIC
# MAGIC This notebook is the short path for engineers who already understand the
# MAGIC package concepts and want to run the standard Databricks workflow:
# MAGIC
# MAGIC 1. Compile canonical YAML.
# MAGIC 2. Validate for Spark.
# MAGIC 3. Create metadata tables.
# MAGIC 4. Publish one ruleset.
# MAGIC 5. Load published metadata.
# MAGIC 6. Evaluate a Spark DataFrame.

# COMMAND ----------

from pyspark.sql import functions as F

import rules_engine
from rules_engine import (
    RulesEngineService,
)

print(f"rules_engine version: {rules_engine.__version__}")
print(f"rules_engine package: {rules_engine.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure Service
# MAGIC
# MAGIC Set `RULES_ENGINE_QUICKSTART_DATABASE` to a disposable `catalog.schema`.
# MAGIC The notebook creates missing metadata tables but never overwrites existing
# MAGIC tables. Published name/version pairs are immutable, so use a new version or
# MAGIC a fresh schema when rerunning the publication step.

# COMMAND ----------

DATABASE = globals().get(
    "RULES_ENGINE_QUICKSTART_DATABASE",
    "YOUR_CATALOG.YOUR_SCHEMA",
)

if not DATABASE or DATABASE == "YOUR_CATALOG.YOUR_SCHEMA":
    raise ValueError(
        "Set RULES_ENGINE_QUICKSTART_DATABASE to a disposable catalog.schema."
    )
database_parts = DATABASE.split(".")
if len(database_parts) != 2 or not all(
    part.replace("_", "a").isalnum() and not part[0].isdigit()
    for part in database_parts
):
    raise ValueError("RULES_ENGINE_QUICKSTART_DATABASE must be catalog.schema.")

quoted_database = ".".join(f"`{part}`" for part in database_parts)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_database}")
service = RulesEngineService.from_schema(spark, DATABASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compile And Validate

# COMMAND ----------

ruleset_yaml = """
ruleset_id: quickstart_account_review
ruleset_name: Quickstart Account Review
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: open_high_value
    rule_name: Open High Value
    rule_order: 1
    active_flag: true
    stop_on_match: true
    when:
      all:
        - left: {field: status}
          operator: eq
          right: {literal: OPEN, value_type: string}
        - left: {field: amount}
          operator: ge
          right: {literal: 100, value_type: number}
    assign:
      review_bucket: high_value_open
expect:
  - name: open high-value account
    given: {status: OPEN, amount: 150}
    then:
      matched: true
      matched_rule_ids: [open_high_value]
      review_bucket: high_value_open
  - name: open low-value account
    given: {status: OPEN, amount: 25}
    then:
      matched: false
      matched_rule_ids: []
"""

ruleset = service.compile_yaml_text(ruleset_yaml)
validation = service.validator.validate(ruleset)
print(validation.to_text())

if validation.has_errors():
    raise ValueError(validation.to_text())

expected_cases = service.test_ruleset(ruleset)
print(expected_cases.to_text())
if not expected_cases.passed:
    raise AssertionError(expected_cases.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Metadata Tables

# COMMAND ----------

service.create_tables(mode="ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Publish

# COMMAND ----------

service.publish(ruleset)
display(spark.table(service.table_names.ruleset_versions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load And Evaluate

# COMMAND ----------

input_df = spark.createDataFrame(
    [
        {"account": "A", "status": "OPEN", "amount": 150},
        {"account": "B", "status": "OPEN", "amount": 25},
        {"account": "C", "status": "CLOSED", "amount": 500},
    ]
)

result_df = service.evaluate_dataframe(
    input_df,
    ruleset_name="Quickstart Account Review",
    version="1",
    fail_on_error=True,
)

display(result_df.orderBy("account"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sanity Check

# COMMAND ----------

rows = {
    row["account"]: row.asDict(recursive=True)
    for row in result_df.orderBy("account").collect()
}

assert rows["A"]["rules_engine_matched"] is True
assert rows["A"]["rules_engine_matched_rule_ids"] == ["open_high_value"]
assert rows["A"]["rules_engine_assign"] == {"review_bucket": "high_value_open"}
assert rows["B"]["rules_engine_matched"] is False
assert rows["C"]["rules_engine_matched"] is False
assert all(row["rules_engine_error"] is None for row in rows.values())
assert all(row["rules_engine_ruleset"]["id"] == ruleset.ruleset_id for row in rows.values())
assert all(row["rules_engine_ruleset"]["version"] == ruleset.version for row in rows.values())
assert all(row["rules_engine_ruleset"]["content_hash"] for row in rows.values())
assert all(row["rules_engine_engine_version"] == rules_engine.__version__ for row in rows.values())
assert result_df.where(F.col("rules_engine_matched")).count() == 1

print("Quickstart passed.")
