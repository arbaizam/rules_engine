# Databricks notebook source
# MAGIC %md
# MAGIC # Python Ruleset Authoring Guide
# MAGIC
# MAGIC This notebook shows how to create a ruleset directly with the Python
# MAGIC dataclass API, validate it, export it to canonical YAML, and optionally run
# MAGIC it against a small in-memory row set.
# MAGIC
# MAGIC Use this path when a ruleset is easier to generate from Python code than to
# MAGIC maintain by hand in YAML. The exported YAML remains the reviewable
# MAGIC governance artifact.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports And Package Path
# MAGIC
# MAGIC In a packaged Databricks job, the wheel should already be installed and no
# MAGIC `sys.path` change is required. In a copied workspace folder, set `repo_root`
# MAGIC to the folder that contains `rules_engine/`.

# COMMAND ----------

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from rules_engine import FunctionRegistry, RulesEngineRuntime, YamlRulesetExporter
from rules_engine.enums import ComparisonOperator, LogicalOperator, NullInputMode, NullResultMode, RulesetStatus
from rules_engine.models import Assignment, Condition, ConditionGroup, FieldOperand, LiteralOperand, Rule, Ruleset
from rules_engine.validator import RulesetValidator

print("repo_root:", repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Small Helper Functions
# MAGIC
# MAGIC These helpers reduce constructor repetition. They do not introduce alternate
# MAGIC semantics; every helper still returns canonical rules engine dataclasses.

# COMMAND ----------

def field(name: str) -> FieldOperand:
    return FieldOperand(name)


def literal(value, value_type: str = "string") -> LiteralOperand:
    return LiteralOperand(value, value_type)


def row_condition(
    condition_id: str,
    left,
    operator: ComparisonOperator,
    right=None,
    *,
    tolerance_abs: str = "0",
    null_input_mode: NullInputMode = NullInputMode.PROPAGATE,
    null_result_mode: NullResultMode = NullResultMode.NULL,
) -> Condition:
    return Condition(
        condition_id=condition_id,
        left=left,
        operator=operator,
        right=right,
        tolerance_abs=Decimal(tolerance_abs),
        null_input_mode=null_input_mode,
        null_result_mode=null_result_mode,
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

# COMMAND ----------

ruleset = Ruleset(
    ruleset_id="python_account_review",
    ruleset_name="Python Account Review",
    version="1",
    status=RulesetStatus.DRAFT,
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
                        field("amount"),
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
# MAGIC compiled/published through the standard workflow.

# COMMAND ----------

output_dir = repo_root / "rule_sets"
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / "python_account_review.yaml"

YamlRulesetExporter().export_path(ruleset, output_path)

print(output_path)
print(output_path.read_text(encoding="utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evaluate With The Pure-Python Runtime
# MAGIC
# MAGIC This is useful for quick local checks. Production Databricks workflows should
# MAGIC publish the ruleset metadata and use the Spark runtime against a DataFrame.

# COMMAND ----------

class NotebookRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("This example passes the ruleset directly.")


runtime = RulesEngineRuntime(NotebookRepository(), FunctionRegistry())
rows = [
    {"account": "A", "status": "OPEN", "amount": 150},
    {"account": "B", "status": "OPEN", "amount": 25},
    {"account": "C", "status": "CLOSED", "amount": 5},
]

output_rows, traces = runtime.evaluate(rows, ruleset)
output_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next Step
# MAGIC
# MAGIC For production use, compile/validate the exported YAML, save it as draft,
# MAGIC publish it to the `ruleset_versions` Delta table, and run the Spark runtime
# MAGIC against the published metadata.
