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
    FunctionRegistry,
    PublishService,
    RulesetNormalizer,
    SparkRulesEngineRuntime,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
)
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Compile And Validate

# COMMAND ----------

ruleset_yaml = """
ruleset_id: quickstart_account_review
ruleset_name: Quickstart Account Review
version: "1"
status: draft
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

ruleset = YamlRulesetCompiler().compile_text(ruleset_yaml)
registry = FunctionRegistry()
validator = SparkRulesetCompatibilityValidator(registry)
validation = validator.validate(ruleset)
print(validation.to_text())

if validation.has_errors():
    raise ValueError(validation.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Metadata Tables
# MAGIC
# MAGIC Update `DATABASE` to a scratch schema you are allowed to drop and recreate.

# COMMAND ----------

DATABASE = "YOUR_CATALOG.YOUR_SCHEMA"
TABLE_PREFIX = "quickstart"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")


def table_name(suffix: str) -> str:
    return f"{DATABASE}.{TABLE_PREFIX}_{suffix}"


table_names = RulesEngineTableNames(
    ruleset_versions=table_name("ruleset_versions"),
    function_registry=table_name("function_registry"),
)

repository = SparkDeltaRulesetRepository(spark, table_names)
repository.create_base_tables(mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Publish

# COMMAND ----------

publish_service = PublishService(
    repository=repository,
    validator=validator,
    normalizer=RulesetNormalizer(),
)

publish_service.publish(ruleset)
display(spark.table(table_names.ruleset_versions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load And Evaluate

# COMMAND ----------

published_ruleset = repository.load_published("Quickstart Account Review", version="1")

input_df = spark.createDataFrame(
    [
        {"account": "A", "status": "OPEN", "amount": 150},
        {"account": "B", "status": "OPEN", "amount": 25},
        {"account": "C", "status": "CLOSED", "amount": 500},
    ]
)

result_df = SparkRulesEngineRuntime(repository, registry).evaluate_dataframe(
    input_df,
    published_ruleset,
    fail_on_error=True,
)

display(result_df.orderBy("account"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sanity Check

# COMMAND ----------

matched_count = result_df.where(F.col("rules_engine_matched")).count()
if matched_count != 1:
    raise AssertionError(f"Expected one matched row, got {matched_count}.")

print("Quickstart passed.")
