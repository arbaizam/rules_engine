# Databricks notebook source
# MAGIC %md
# MAGIC # Custom Function Authoring Guide
# MAGIC
# MAGIC Custom functions let a ruleset call controlled Python logic through an
# MAGIC explicit registry contract. Ruleset YAML stores only metadata references:
# MAGIC the function name and literal argument values. Runtime environments register
# MAGIC the actual Python callable separately.
# MAGIC
# MAGIC Use custom functions when a rule needs logic that is too specific for the
# MAGIC built-in field, literal, comparison, and aggregate operands.

# COMMAND ----------

from pathlib import Path
import sys

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from rules_engine import (  # noqa: E402
    CustomFunctionSpec,
    FunctionRegistry,
    RulesEngineRuntime,
    RulesEngineService,
    RulesetValidator,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    register_standard_functions,
    standard_function_rows,
)

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
    return risk_score * 10 + min(balance / 1000, 50)


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
# MAGIC For common transformations such as substring, trim, regex extraction, or
# MAGIC casing, use `rules_engine.standard_functions.register_standard_functions`.

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
                risk_score: 8
                balance: 25000
          operator: ge
          right: { literal: 100, value_type: number }
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket:
        custom_function:
          name: risk_bucket
          args:
            risk_score: 82
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
# MAGIC ## 5. Evaluate With The Callable Registered
# MAGIC
# MAGIC Runtime evaluation requires both the function spec and the callable
# MAGIC implementation. Missing implementations fail at runtime.

# COMMAND ----------

class NotebookRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("This guide passes the ruleset directly.")


runtime = RulesEngineRuntime(NotebookRepository(), registry)
output_rows, traces = runtime.evaluate([{"account_id": "A"}], ruleset)

output_rows

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
# MAGIC The production YAML publish pipeline currently creates an empty
# MAGIC `FunctionRegistry()`. If production YAML uses custom functions, update the
# MAGIC pipeline to register the environment's approved function specs before
# MAGIC validation.

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
