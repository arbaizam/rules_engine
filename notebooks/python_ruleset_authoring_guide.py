# Databricks notebook source
# MAGIC %md
# MAGIC # Python Ruleset Authoring Guide
# MAGIC
# MAGIC This notebook shows how to create a ruleset directly with the Python
# MAGIC dataclass API, validate it, export it to canonical YAML, and optionally run
# MAGIC it against a small Spark DataFrame.
# MAGIC
# MAGIC Use this path when a ruleset is easier to generate from Python code than to
# MAGIC maintain by hand in YAML. The exported YAML remains the reviewable
# MAGIC governance artifact.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports And Installed Package
# MAGIC
# MAGIC Install the release wheel on the Databricks compute before running this
# MAGIC notebook. Printing the imported path prevents an unnoticed workspace source
# MAGIC checkout from shadowing the installed artifact.

# COMMAND ----------

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from tempfile import gettempdir

import rules_engine
from rules_engine import FunctionRegistry, SparkRulesEngineRuntime, YamlRulesetExporter
from rules_engine.enums import ComparisonOperator, LogicalOperator, RulesetStatus
from rules_engine.models import AssignedOperand, Assignment, Condition, ConditionGroup, FieldOperand, LiteralOperand, Rule, Ruleset
from rules_engine.validator import RulesetValidator

print(f"rules_engine version: {rules_engine.__version__}")
print(f"rules_engine package: {rules_engine.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Small Helper Functions
# MAGIC
# MAGIC These helpers reduce constructor repetition. They do not introduce alternate
# MAGIC semantics; every helper still returns canonical rules engine dataclasses.

# COMMAND ----------

def field(
    name: str,
    *,
    default_if_null: LiteralOperand | None = None,
) -> FieldOperand:
    return FieldOperand(name, default_if_null=default_if_null)


def assigned(
    target_field: str,
    *,
    default_if_null: LiteralOperand | None = None,
) -> AssignedOperand:
    """Read a value committed by a matched rule with a lower rule_order."""
    return AssignedOperand(target_field, default_if_null=default_if_null)


def literal(value, value_type: str = "string") -> LiteralOperand:
    return LiteralOperand(value, value_type)


def row_condition(
    condition_id: str,
    left,
    operator: ComparisonOperator,
    right=None,
    *,
    tolerance_abs: str = "0",
    error_on_null: bool = False,
) -> Condition:
    return Condition(
        condition_id=condition_id,
        left=left,
        operator=operator,
        right=right,
        tolerance_abs=Decimal(tolerance_abs),
        error_on_null=error_on_null,
    )


def assign_literal(assignment_id: str, target_field: str, value, value_type: str = "string") -> Assignment:
    return Assignment(
        assignment_id=assignment_id,
        target_field=target_field,
        value=literal(value, value_type),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create A Ruleset In Python
# MAGIC
# MAGIC This example evaluates rows in rule order:
# MAGIC
# MAGIC - Rule 1 matches open accounts with amount greater than or equal to 100.
# MAGIC - Rule 2 matches closed accounts.
# MAGIC - `stop_on_match=True` means the first matching rule wins.
# MAGIC
# MAGIC A binary comparison with a remaining null operand does not match. The amount
# MAGIC operand below uses `default_if_null=0`, so substitution happens before the
# MAGIC comparison. Use `error_on_null=True` on `row_condition` instead when a
# MAGIC remaining null should fail the row.

# COMMAND ----------

ruleset = Ruleset(
    ruleset_id="python_account_review",
    ruleset_name="Python Account Review",
    version="1",
    status=RulesetStatus.PUBLISHED,
    description="Example ruleset authored with Python dataclasses.",
    owner="Rules Team",
    owner_department="ALM Engineering",
    rules=(
        Rule(
            rule_id="open_high_value",
            rule_name="Open High Value",
            rule_order=1,
            active_flag=True,
            stop_on_match=True,
            root_group=ConditionGroup(
                condition_group_id="cg:open_high_value:root",
                logical_operator=LogicalOperator.ALL,
                conditions=(
                    row_condition(
                        "open_high_value:c1",
                        field("status"),
                        ComparisonOperator.EQ,
                        literal("OPEN"),
                    ),
                    row_condition(
                        "open_high_value:c2",
                        field(
                            "amount",
                            default_if_null=literal(0, "number"),
                        ),
                        ComparisonOperator.GE,
                        literal(100, "number"),
                    ),
                ),
            ),
            assignments=(
                assign_literal("assignment:open_high_value:1", "review_bucket", "high_value_open"),
            ),
        ),
        Rule(
            rule_id="closed_account",
            rule_name="Closed Account",
            rule_order=2,
            active_flag=True,
            stop_on_match=True,
            root_group=ConditionGroup(
                condition_group_id="cg:closed_account:root",
                logical_operator=LogicalOperator.ALL,
                conditions=(
                    row_condition(
                        "closed_account:c1",
                        field("status"),
                        ComparisonOperator.EQ,
                        literal("CLOSED"),
                    ),
                ),
            ),
            assignments=(
                assign_literal("assignment:closed_account:1", "review_bucket", "closed"),
            ),
        ),
    ),
)

ruleset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate The Ruleset
# MAGIC
# MAGIC Python-authored rulesets go through the same validator as YAML-authored
# MAGIC rulesets. Do not publish or export for review until validation passes.

# COMMAND ----------

validation = RulesetValidator(FunctionRegistry()).validate(ruleset)
print(validation.to_text())

if validation.has_errors():
    raise ValueError(validation.to_text())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Export Canonical YAML
# MAGIC
# MAGIC The exporter writes the same canonical vocabulary accepted by the compiler.
# MAGIC The resulting file can be reviewed, checked into source control, and later
# MAGIC compiled/published through the standard workflow. Set `RULESET_EXPORT_PATH`
# MAGIC for a durable governed artifact; otherwise this guide writes under the
# MAGIC driver's temporary directory instead of changing the repository checkout.

# COMMAND ----------

configured_output = globals().get("RULESET_EXPORT_PATH")
output_path = (
    Path(configured_output).expanduser()
    if configured_output
    else Path(gettempdir()) / "rules_engine_guide" / "python_account_review.yaml"
)
output_path.parent.mkdir(parents=True, exist_ok=True)

YamlRulesetExporter().export_path(ruleset, output_path)

print(output_path)
print(output_path.read_text(encoding="utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evaluate With The Spark Runtime
# MAGIC
# MAGIC This is useful for quick checks before publishing the ruleset metadata.
# MAGIC Production Databricks workflows use the same Spark runtime against managed
# MAGIC DataFrames.

# COMMAND ----------

class NotebookRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("This example passes the ruleset directly.")


rows = [
    {"account": "A", "status": "OPEN", "amount": 150},
    {"account": "B", "status": "OPEN", "amount": 25},
    {"account": "C", "status": "CLOSED", "amount": 5},
]

runtime = SparkRulesEngineRuntime(NotebookRepository(), FunctionRegistry())
output_df = runtime.evaluate_dataframe(spark.createDataFrame(rows), ruleset)
display(output_df)

actual = {
    row["account"]: row.asDict(recursive=True)
    for row in output_df.orderBy("account").collect()
}
assert actual["A"]["rules_engine_matched_rule_ids"] == ["open_high_value"]
assert actual["A"]["rules_engine_assign"] == {"review_bucket": "high_value_open"}
assert actual["B"]["rules_engine_matched"] is False
assert actual["B"]["rules_engine_assign"] is None
assert actual["C"]["rules_engine_matched_rule_ids"] == ["closed_account"]
assert actual["C"]["rules_engine_assign"] == {"review_bucket": "closed"}
assert all(row["rules_engine_error"] is None for row in actual.values())
assert all(row["rules_engine_ruleset"]["id"] == ruleset.ruleset_id for row in actual.values())
assert all(row["rules_engine_ruleset"]["content_hash"] for row in actual.values())
assert all(
    row["rules_engine_engine_version"] == rules_engine.__version__
    for row in actual.values()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next Step
# MAGIC
# MAGIC For Databricks use, hand the exported YAML to `RulesEngineService` so the
# MAGIC standard facade compiles, validates, publishes, and evaluates through the
# MAGIC configured Delta metadata tables:
# MAGIC
# MAGIC ```python
# MAGIC from rules_engine import RulesEngineService
# MAGIC
# MAGIC service = RulesEngineService.from_schema(spark, "catalog.schema")
# MAGIC service.create_tables(mode="ignore")
# MAGIC published_ruleset = service.publish_yaml_path(output_path, published_by="rules-pipeline")
# MAGIC result_df = service.evaluate_dataframe(
# MAGIC     input_df,
# MAGIC     ruleset_name=published_ruleset.ruleset_name,
# MAGIC     version=published_ruleset.version,
# MAGIC )
# MAGIC ```
