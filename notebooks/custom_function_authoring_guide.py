# Databricks notebook source
# MAGIC %md
# MAGIC # Custom Function Authoring Guide
# MAGIC
# MAGIC Custom functions let a ruleset call controlled Python logic through an
# MAGIC explicit registry contract. Ruleset YAML stores only metadata references:
# MAGIC the function name and literal, field-backed, or nested-function arguments.
# MAGIC Runtime environments register the actual Python callable separately.
# MAGIC
# MAGIC Use custom functions when a rule needs logic that is too specific for the
# MAGIC built-in field, literal, and comparison operands.

# COMMAND ----------

from datetime import date

import rules_engine
from rules_engine import (  # noqa: E402
    CustomFunctionSpec,
    FunctionRegistry,
    RulesEngineService,
    RulesetValidator,
    SparkRulesEngineRuntime,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    register_standard_functions,
    standard_function_rows,
)

print(f"rules_engine version: {rules_engine.__version__}")
print(f"rules_engine package: {rules_engine.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Author The Callable
# MAGIC
# MAGIC A custom function receives keyword arguments and returns one value. Keep the
# MAGIC function deterministic: no hidden I/O, no random values, no clock reads, and
# MAGIC no dependence on mutable global state.

# COMMAND ----------

def score_account(risk_score, balance):
    if risk_score is None or balance is None:
        return None
    return risk_score + min(balance / 1000, 50)


def risk_bucket(risk_score):
    if risk_score is None:
        return "unknown"
    if risk_score >= 80:
        return "high"
    if risk_score >= 50:
        return "medium"
    return "low"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register The Function Contract
# MAGIC
# MAGIC `CustomFunctionSpec` is the metadata contract. The runtime callable is
# MAGIC optional at registration time for validation-only flows, but it is required
# MAGIC when evaluating rules.
# MAGIC
# MAGIC Important fields:
# MAGIC
# MAGIC - `function_name`: name referenced by YAML.
# MAGIC - `implementation_reference`: environment-specific metadata only; executable
# MAGIC   code is not loaded from this string.
# MAGIC - `arg_names`: exact keyword arguments allowed by the function.
# MAGIC - `allowed_in_condition_flag`: allow use in `when` conditions.
# MAGIC - `allowed_in_assignment_flag`: allow use in `assign` values.
# MAGIC - `active_flag`: inactive functions fail validation.

# COMMAND ----------

registry = register_standard_functions(FunctionRegistry())

registry.register(
    CustomFunctionSpec(
        function_name="score_account",
        implementation_reference="my_rules.functions.score_account",
        arg_names=("risk_score", "balance"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        active_flag=True,
        return_type_hint="number",
        description="Combines risk score and balance into a review score.",
        version="1.0.0",
    ),
    implementation=lambda **kwargs: score_account(
        kwargs["risk_score"],
        kwargs["balance"],
    ),
)

registry.register(
    CustomFunctionSpec(
        function_name="risk_bucket",
        implementation_reference="my_rules.functions.risk_bucket",
        arg_names=("risk_score",),
        allowed_in_condition_flag=False,
        allowed_in_assignment_flag=True,
        active_flag=True,
        return_type_hint="string",
        description="Maps a risk score to a bucket.",
        version="1.0.0",
    ),
    implementation=lambda **kwargs: risk_bucket(kwargs["risk_score"]),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reference Functions In YAML
# MAGIC
# MAGIC A custom function operand has this shape:
# MAGIC
# MAGIC ```yaml
# MAGIC custom_function:
# MAGIC   name: function_name
# MAGIC   args:
# MAGIC     arg_name: literal_value
# MAGIC ```
# MAGIC
# MAGIC Arguments may be literal metadata values or operand-shaped values:
# MAGIC
# MAGIC ```yaml
# MAGIC value: { field: account_code }
# MAGIC ```
# MAGIC
# MAGIC For shared transformations—including substring, trim, regex extraction,
# MAGIC numeric conversion, and calendar-safe date arithmetic—use
# MAGIC `register_standard_functions`. Date functions include `to_date`,
# MAGIC `date_add_days`, `date_add_months`, `date_add_years`, `date_diff_days`,
# MAGIC `month_start`, and `month_end`.

# COMMAND ----------

ruleset_yaml = """
ruleset_id: custom_function_example
ruleset_name: Custom Function Example
version: "1.0.0"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: high_custom_score
    rule_name: High Custom Score
    rule_order: 1
    when:
      all:
        - left:
            custom_function:
              name: score_account
              args:
                risk_score: { field: risk_score }
                balance: { field: balance }
          operator: ge
          right: { literal: 100, value_type: number }
          null_input_mode: propagate
          null_result_mode: "null"
        - left:
            custom_function:
              name: date_add_months
              args:
                value: { field: funded_date }
                months: 1
          operator: ge
          right: { literal: "2024-02-29", value_type: date }
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket:
        custom_function:
          name: risk_bucket
          args:
            risk_score: { field: risk_score }
      review_date:
        custom_function:
          name: date_add_years
          args:
            value: { field: funded_date }
            years: 1
      age_days:
        custom_function:
          name: date_diff_days
          args:
            start: { field: funded_date }
            end: { field: as_of_date }
"""

ruleset = YamlRulesetCompiler().compile_text(ruleset_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate With The Same Registry
# MAGIC
# MAGIC Validation checks that the function exists, is active, is allowed in the
# MAGIC current context, and receives exactly the registered argument names.

# COMMAND ----------

validation = RulesetValidator(registry).validate(ruleset)
if validation.has_errors():
    raise ValueError(validation.to_text())

print(validation.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evaluate With The Spark Runtime
# MAGIC
# MAGIC Runtime evaluation requires both the function spec and the callable
# MAGIC implementation. Missing implementations fail at runtime. Production
# MAGIC evaluation uses Spark DataFrames.

# COMMAND ----------

class NotebookRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("This guide passes the ruleset directly.")


runtime = SparkRulesEngineRuntime(NotebookRepository(), registry)
output_df = runtime.evaluate_dataframe(
    spark.createDataFrame(
        [
            {
                "account_id": "A",
                "risk_score": 82,
                "balance": 25000,
                "funded_date": "2024-01-31",
                "as_of_date": "2024-02-29",
            },
            {
                "account_id": "B",
                "risk_score": 55,
                "balance": 1000,
                "funded_date": "2024-01-01",
                "as_of_date": "2024-02-01",
            },
        ]
    ),
    ruleset,
)

display(output_df)

rows = {
    row["account_id"]: row.asDict(recursive=True)
    for row in output_df.orderBy("account_id").collect()
}
assert rows["A"]["rules_engine_matched_rule_ids"] == ["high_custom_score"]
assert rows["A"]["rules_engine_assign"] == {
    "review_bucket": "high",
    "review_date": date(2025, 1, 31),
    "age_days": 29,
}
assert rows["B"]["rules_engine_matched"] is False
assert rows["B"]["rules_engine_assign"] is None
assert all(row["rules_engine_error"] is None for row in rows.values())

authored_expressions = {
    event["target_field"]: event["authored_expression"]
    for event in rows["A"]["rules_engine_assignment_results"]
}
assert authored_expressions == {
    "review_bucket": "review_bucket = risk_bucket(risk_score=risk_score)",
    "review_date": "review_date = date_add_years(value=funded_date, years=1)",
    "age_days": "age_days = date_diff_days(start=funded_date, end=as_of_date)",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persist Function Registry Metadata
# MAGIC
# MAGIC Persist function metadata when you want the environment registry table to
# MAGIC document the allowed custom functions. This does not persist executable
# MAGIC Python code.
# MAGIC
# MAGIC In a Databricks environment:
# MAGIC
# MAGIC ```python
# MAGIC schema = "catalog.schema"
# MAGIC service = RulesEngineService.from_schema(spark, schema)
# MAGIC service.save_function_registry_rows(
# MAGIC     standard_function_rows()
# MAGIC     + [
# MAGIC         registry.get_spec("score_account").to_row(),
# MAGIC         registry.get_spec("risk_bucket").to_row(),
# MAGIC     ]
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Spark And Production Pipeline Notes
# MAGIC
# MAGIC The Spark validator uses the registry metadata to validate function
# MAGIC references:
# MAGIC
# MAGIC ```python
# MAGIC spark_validation = SparkRulesetCompatibilityValidator(registry).validate(ruleset)
# MAGIC ```
# MAGIC
# MAGIC The Spark runtime also needs callable implementations registered in the
# MAGIC runtime environment. In production, import and register the approved
# MAGIC function implementations before validating or evaluating rulesets that
# MAGIC reference custom functions.
# MAGIC
# MAGIC The production YAML publish pipeline registers the package-owned standard
# MAGIC functions. If production YAML uses environment-specific functions such as
# MAGIC `score_account` or `risk_bucket`, update the pipeline to register those
# MAGIC approved specs and implementations before validation and publication.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Common Validation Failures
# MAGIC
# MAGIC - `CUSTOM_FUNCTION_REGISTRY_REQUIRED`: a ruleset references custom functions
# MAGIC   but validation did not receive a registry.
# MAGIC - `UNKNOWN_CUSTOM_FUNCTION`: YAML references a function name not registered
# MAGIC   in `FunctionRegistry`.
# MAGIC - `INACTIVE_CUSTOM_FUNCTION`: the registered spec has `active_flag=False`.
# MAGIC - `CUSTOM_FUNCTION_NOT_ALLOWED_IN_CONDITION`: a function was used in `when`
# MAGIC   but `allowed_in_condition_flag=False`.
# MAGIC - `CUSTOM_FUNCTION_NOT_ALLOWED_IN_ASSIGNMENT`: a function was used in
# MAGIC   `assign` but `allowed_in_assignment_flag=False`.
# MAGIC - `CUSTOM_FUNCTION_ARGS_MISMATCH`: YAML argument names do not exactly match
# MAGIC   the registered `arg_names`.
