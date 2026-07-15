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

from pathlib import Path
import sys

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from pyspark.sql import functions as F

from rules_engine import (
    RulesEngineService,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure Service
# MAGIC
# MAGIC Update `DATABASE` to a scratch schema where you can create or overwrite
# MAGIC rules engine metadata tables.

# COMMAND ----------

DATABASE = "YOUR_CATALOG.YOUR_SCHEMA"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")
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
          null_input_mode: propagate
          null_result_mode: "null"
        - left: {field: amount}
          operator: ge
          right: {literal: 100, value_type: number}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket: high_value_open
"""

ruleset = service.compile_yaml_text(ruleset_yaml)
validation = service.validator.validate(ruleset)
print(validation.to_text())

if validation.has_errors():
    raise ValueError(validation.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Metadata Tables

# COMMAND ----------

service.create_tables(mode="overwrite")

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

matched_count = result_df.where(F.col("rules_engine_matched")).count()
if matched_count != 1:
    raise AssertionError(f"Expected one matched row, got {matched_count}.")

print("Quickstart passed.")
