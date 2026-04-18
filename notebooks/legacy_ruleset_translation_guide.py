# Databricks notebook source
# MAGIC %md
# MAGIC # Python-Authored Legacy Account Key Rulesets
# MAGIC
# MAGIC This notebook shows how to generate canonical rules engine YAML from the
# MAGIC Python authoring API.
# MAGIC
# MAGIC The old `rules_engine_old/rulesets` YAML files are used only as a source of
# MAGIC legacy rule facts. The notebook converts those facts into `Ruleset`, `Rule`,
# MAGIC `ConditionGroup`, `Condition`, and `Assignment` dataclasses, validates the
# MAGIC dataclasses, and exports reviewable YAML with `YamlRulesetExporter`.
# MAGIC
# MAGIC Use this notebook when you want to regenerate the account-key YAML artifacts
# MAGIC from Python-authored rules instead of writing canonical YAML by hand.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports And Package Path
# MAGIC
# MAGIC The rules engine supports two authoring paths:
# MAGIC
# MAGIC - YAML authoring: write canonical YAML and compile it.
# MAGIC - Python authoring: build immutable dataclasses and export or publish them.
# MAGIC
# MAGIC This notebook focuses on the second path. In Databricks, set `repo_root` to
# MAGIC the workspace folder that contains `rules_engine/`, `tools/`, and
# MAGIC `notebooks/` if `Path.cwd()` is not already that folder.

# COMMAND ----------

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any
import sys

import yaml

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from rules_engine import (
    FunctionRegistry,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
)
from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
    RulesetStatus,
)
from rules_engine.models import (
    Assignment,
    Condition,
    ConditionGroup,
    FieldOperand,
    LiteralOperand,
    Rule,
    Ruleset,
)
from rules_engine.validator import RulesetValidator

print("repo_root:", repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Source And Output Paths
# MAGIC
# MAGIC `SOURCE_DIR` contains the legacy account-key rulesets from the old engine.
# MAGIC `OUTPUT_DIR` is intentionally separate from the runtime package. The files
# MAGIC written there are authoring artifacts for review and manual refinement.
# MAGIC
# MAGIC The default output folder is `python_authored_legacy_rulesets/` so this
# MAGIC notebook does not overwrite the already-translated YAML files unless you
# MAGIC intentionally change the path.

# COMMAND ----------

SOURCE_DIR = Path(r"C:\Users\aarba\pydev\rules_engine_old\rulesets")
OUTPUT_DIR = repo_root / "python_authored_legacy_rulesets"

if not SOURCE_DIR.exists():
    raise FileNotFoundError(
        f"SOURCE_DIR does not exist: {SOURCE_DIR}. "
        "Update SOURCE_DIR to the folder containing the legacy YAML files."
    )

legacy_files = sorted(SOURCE_DIR.glob("*.yaml"))
if not legacy_files:
    raise FileNotFoundError(f"No legacy YAML files found in {SOURCE_DIR}")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("SOURCE_DIR:", SOURCE_DIR)
print("OUTPUT_DIR:", OUTPUT_DIR)
for source_path in legacy_files:
    print("-", source_path.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Minimal Python Authoring Example
# MAGIC
# MAGIC This small example is the core API shape.
# MAGIC
# MAGIC A Python-authored ruleset is not a loose dictionary. It is a set of immutable
# MAGIC dataclasses with canonical enum values. That means the same object can be:
# MAGIC
# MAGIC - validated before publish,
# MAGIC - exported to canonical YAML,
# MAGIC - serialized to Delta metadata rows,
# MAGIC - passed to the Python or Spark runtime.

# COMMAND ----------

example_ruleset = Ruleset(
    ruleset_id="example_account_key",
    ruleset_name="Example Account Key",
    version="1",
    status=RulesetStatus.DRAFT,
    description="Small Python-authored example.",
    rules=(
        Rule(
            rule_id="example_001",
            rule_name="Investment Account",
            rule_order=10,
            active_flag=True,
            stop_on_match=True,
            root_group=ConditionGroup(
                condition_group_id="cg:example_001:root",
                logical_operator=LogicalOperator.ALL,
                conditions=(
                    Condition(
                        condition_id="example_001:c1",
                        left=FieldOperand("BK_PositionID"),
                        operator=ComparisonOperator.STARTS_WITH,
                        right=LiteralOperand("INV", "string"),
                        tolerance_abs=Decimal("0"),
                        null_input_mode=NullInputMode.PROPAGATE,
                        null_result_mode=NullResultMode.NULL,
                    ),
                    Condition(
                        condition_id="example_001:c2",
                        left=FieldOperand("BK_AccountID"),
                        operator=ComparisonOperator.EQ,
                        right=LiteralOperand("LTFA", "string"),
                        tolerance_abs=Decimal("0"),
                        null_input_mode=NullInputMode.PROPAGATE,
                        null_result_mode=NullResultMode.NULL,
                    ),
                ),
            ),
            assignments=(
                Assignment(
                    assignment_id="assignment:example_001:1",
                    target_field="leaf_key",
                    value=LiteralOperand("10120", "string"),
                ),
            ),
        ),
    ),
)

print(YamlRulesetExporter().export_text(example_ruleset))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reusable Authoring Helpers
# MAGIC
# MAGIC The full account-key rulesets contain many repetitive row-level conditions.
# MAGIC These helpers keep the Python authoring concise while still returning the
# MAGIC canonical dataclasses used by the engine.
# MAGIC
# MAGIC The helpers deliberately do not create alternate semantics. They only reduce
# MAGIC boilerplate around repeated constructor arguments such as tolerance and null
# MAGIC handling.

# COMMAND ----------

OPERATOR_MAP = {
    "=": ComparisonOperator.EQ,
    "!=": ComparisonOperator.NE,
    "contains": ComparisonOperator.CONTAINS,
    "in": ComparisonOperator.IN,
}

TYPE_MAP = {
    "string": "string",
    "integer": "number",
    "number": "number",
    "float": "number",
    "decimal": "number",
    "boolean": "boolean",
}


class PythonAuthoringTranslationError(ValueError):
    """Raised when a legacy rule cannot be represented deterministically."""


def required(payload: dict[str, Any], key: str) -> Any:
    """Return a required key or raise a clear conversion error."""
    if not isinstance(payload, dict) or key not in payload:
        raise PythonAuthoringTranslationError(f"Missing required key {key}: {payload}")
    return payload[key]


def literal_from_legacy(payload: dict[str, Any]) -> LiteralOperand:
    """Convert a legacy literal operand to a canonical literal operand."""
    if "value" in payload:
        value_type = TYPE_MAP.get(str(payload.get("type", "string")).lower(), "string")
        return LiteralOperand(payload["value"], value_type)
    if "values" in payload:
        values = payload["values"]
        if not isinstance(values, list):
            raise PythonAuthoringTranslationError("Legacy values operand must be a list.")
        return LiteralOperand([required(item, "value") for item in values], "list")
    raise PythonAuthoringTranslationError(f"Legacy literal operand is unsupported: {payload}")


def operand_from_legacy(payload: dict[str, Any]) -> FieldOperand | LiteralOperand:
    """Convert a non-function legacy operand to a canonical operand."""
    if "field" in payload:
        return FieldOperand(str(payload["field"]))
    if "value" in payload or "values" in payload:
        return literal_from_legacy(payload)
    raise PythonAuthoringTranslationError(f"Legacy operand is unsupported: {payload}")


def is_group(payload: Any) -> bool:
    """Return whether a payload is a legacy logical group."""
    return isinstance(payload, dict) and any(key in payload for key in ("all", "any"))


def is_substring_operand(payload: Any) -> bool:
    """Return whether a legacy operand is substring(field, start, length)."""
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("func"), dict)
        and payload["func"].get("name") == "substring"
    )


def substring_parts(payload: dict[str, Any]) -> tuple[str, int, int]:
    """Extract substring(field, start, length) arguments from the legacy shape."""
    func = required(payload, "func")
    args = func.get("args")
    if func.get("name") != "substring":
        raise PythonAuthoringTranslationError(f"Unsupported legacy function: {func.get('name')}")
    if not isinstance(args, list) or len(args) != 3:
        raise PythonAuthoringTranslationError("substring requires exactly three args.")
    field_arg, start_arg, length_arg = args
    return (
        str(required(field_arg, "field")),
        int(required(start_arg, "value")),
        int(required(length_arg, "value")),
    )


def condition(
    condition_id: str,
    left: FieldOperand | LiteralOperand,
    operator: ComparisonOperator,
    right: FieldOperand | LiteralOperand | None,
) -> Condition:
    """Build a row-level condition with explicit v1 null/tolerance metadata."""
    return Condition(
        condition_id=condition_id,
        left=left,
        operator=operator,
        right=right,
        tolerance_abs=Decimal("0"),
        null_input_mode=NullInputMode.PROPAGATE,
        null_result_mode=NullResultMode.NULL,
        active_flag=True,
    )


def condition_from_legacy(payload: dict[str, Any], condition_id: str) -> Condition:
    """Convert one legacy condition into a canonical Python dataclass condition."""
    operator = required(payload, "op")
    if operator not in OPERATOR_MAP:
        raise PythonAuthoringTranslationError(f"Unsupported legacy operator: {operator}")

    left = required(payload, "left")
    right = required(payload, "right")

    if is_substring_operand(left):
        return substring_condition_from_legacy(left, operator, right, condition_id)

    return condition(
        condition_id=condition_id,
        left=operand_from_legacy(left),
        operator=OPERATOR_MAP[operator],
        right=operand_from_legacy(right),
    )


def substring_condition_from_legacy(
    left: dict[str, Any],
    operator: str,
    right: dict[str, Any],
    condition_id: str,
) -> Condition:
    """Translate supported legacy substring comparisons into canonical operators."""
    if operator not in {"=", "!="}:
        raise PythonAuthoringTranslationError(
            f"substring operands only support = or !=, found: {operator}"
        )

    literal = literal_from_legacy(right)
    if not isinstance(literal.value, str):
        raise PythonAuthoringTranslationError("substring comparison requires a string literal.")

    field_name, start, length = substring_parts(left)
    if start != 1:
        raise PythonAuthoringTranslationError("Only substring start position 1 is supported.")
    if length < len(literal.value):
        raise PythonAuthoringTranslationError(
            f"Cannot compare substring length {length} to longer literal {literal.value!r}."
        )

    if length > len(literal.value):
        # This preserves the existing legacy translator behavior for the small
        # number of legacy cases where the substring length exceeds the literal.
        return condition(
            condition_id=condition_id,
            left=FieldOperand(field_name),
            operator=ComparisonOperator.EQ if operator == "=" else ComparisonOperator.NOT_LIKE,
            right=literal,
        )

    return condition(
        condition_id=condition_id,
        left=FieldOperand(field_name),
        operator=ComparisonOperator.STARTS_WITH
        if operator == "="
        else ComparisonOperator.NOT_LIKE,
        right=literal if operator == "=" else LiteralOperand(f"{literal.value}%", "string"),
    )


def group_from_legacy(
    payload: dict[str, Any],
    rule_id: str,
    group_id: str,
    counters: dict[str, int],
) -> ConditionGroup:
    """Convert a legacy logical group into a canonical condition-group tree."""
    logical_keys = [key for key in ("all", "any") if key in payload]
    if len(logical_keys) != 1:
        raise PythonAuthoringTranslationError(
            f"Legacy condition group must define exactly one of all/any: {payload}"
        )

    logical_key = logical_keys[0]
    items = payload[logical_key]
    if not isinstance(items, list) or not items:
        raise PythonAuthoringTranslationError(
            f"Legacy condition group {logical_key} must contain a non-empty list."
        )

    conditions: list[Condition] = []
    groups: list[ConditionGroup] = []

    for item in items:
        if is_group(item):
            counters["group"] += 1
            groups.append(
                group_from_legacy(
                    item,
                    rule_id,
                    f"cg:{rule_id}:g{counters['group']}",
                    counters,
                )
            )
        else:
            counters["condition"] += 1
            conditions.append(
                condition_from_legacy(
                    item,
                    f"{rule_id}:c{counters['condition']}",
                )
            )

    return ConditionGroup(
        condition_group_id=group_id,
        logical_operator=LogicalOperator(logical_key),
        conditions=tuple(conditions),
        groups=tuple(groups),
    )


def rule_from_legacy(payload: dict[str, Any], index: int) -> Rule:
    """Convert one legacy rule into a canonical Python-authored rule."""
    rule_id = str(required(payload, "rule_id"))
    counters = {"condition": 0, "group": 0}
    assign_payload = required(required(payload, "then"), "assign")
    if not isinstance(assign_payload, dict) or "leaf_key" not in assign_payload:
        raise PythonAuthoringTranslationError(f"Legacy assign must contain leaf_key: {payload}")

    return Rule(
        rule_id=rule_id,
        rule_name=str(required(payload, "rule_name")),
        rule_order=int(payload.get("priority", index)),
        active_flag=bool(payload.get("enabled", True)),
        stop_on_match=True,
        root_group=group_from_legacy(
            required(payload, "when"),
            rule_id,
            f"cg:{rule_id}:root",
            counters,
        ),
        assignments=(
            Assignment(
                assignment_id=f"assignment:{rule_id}:1",
                target_field="leaf_key",
                value=LiteralOperand(str(assign_payload["leaf_key"]), "string"),
            ),
        ),
    )


def ruleset_from_legacy(payload: dict[str, Any]) -> Ruleset:
    """Convert one parsed legacy ruleset into a canonical Python-authored ruleset."""
    rules = payload.get("rules")
    if not isinstance(rules, list):
        raise PythonAuthoringTranslationError("Legacy ruleset must contain a rules list.")

    sorted_rules = sorted(rules, key=lambda item: int(item.get("priority", 0)))

    return Ruleset(
        ruleset_id=str(required(payload, "rule_set_id")),
        ruleset_name=str(required(payload, "rule_set_name")),
        version=str(payload.get("version", "1")),
        status=RulesetStatus.DRAFT,
        description=payload.get("description"),
        rules=tuple(
            rule_from_legacy(rule_payload, index)
            for index, rule_payload in enumerate(sorted_rules, start=1)
        ),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build Ruleset Dataclasses From The Legacy Source Files
# MAGIC
# MAGIC This cell is the conversion step.
# MAGIC
# MAGIC It reads each legacy YAML file, then creates a `Ruleset` dataclass. At this
# MAGIC point no output file has been written. The result is normal Python-authored
# MAGIC rules engine metadata.

# COMMAND ----------

python_authored_rulesets: list[Ruleset] = []

for source_path in legacy_files:
    source_payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    if not isinstance(source_payload, dict):
        raise PythonAuthoringTranslationError(f"Legacy YAML must be a mapping: {source_path}")

    ruleset = ruleset_from_legacy(source_payload)
    python_authored_rulesets.append(ruleset)

    print(
        {
            "source": source_path.name,
            "ruleset_id": ruleset.ruleset_id,
            "ruleset_name": ruleset.ruleset_name,
            "version": ruleset.version,
            "rule_count": len(ruleset.rules),
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate The Python-Authored Rulesets
# MAGIC
# MAGIC Python authoring does not bypass governance. The same validators used for
# MAGIC YAML-authored metadata are run here:
# MAGIC
# MAGIC - `RulesetValidator` checks the runtime-neutral semantic contract.
# MAGIC - `SparkRulesetCompatibilityValidator` checks whether the ruleset is safe for
# MAGIC   the Spark runtime subset.
# MAGIC
# MAGIC Any validation error stops the notebook before files are exported.

# COMMAND ----------

semantic_validator = RulesetValidator(FunctionRegistry())
spark_validator = SparkRulesetCompatibilityValidator(FunctionRegistry())

for ruleset in python_authored_rulesets:
    semantic_result = semantic_validator.validate(ruleset)
    spark_result = spark_validator.validate(ruleset)

    print("=" * 100)
    print(ruleset.ruleset_id)
    print("semantic:")
    print(semantic_result.to_text())
    print("spark:")
    print(spark_result.to_text())

    if semantic_result.has_errors():
        raise ValueError(f"Semantic validation failed for {ruleset.ruleset_id}")
    if spark_result.has_errors():
        raise ValueError(f"Spark compatibility validation failed for {ruleset.ruleset_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Export Canonical YAML From The Dataclasses
# MAGIC
# MAGIC `YamlRulesetExporter` writes the same canonical authoring vocabulary accepted
# MAGIC by `YamlRulesetCompiler`.
# MAGIC
# MAGIC The resulting YAML is intentionally explicit. It includes condition ids,
# MAGIC condition group ids, assignment ids, `tolerance_abs`, `null_input_mode`, and
# MAGIC `null_result_mode`. That makes the YAML suitable for code review and later
# MAGIC Delta persistence.

# COMMAND ----------

exporter = YamlRulesetExporter()
output_paths: list[Path] = []

for ruleset in python_authored_rulesets:
    output_path = OUTPUT_DIR / f"{ruleset.ruleset_id}.yaml"
    exporter.export_path(ruleset, output_path)
    output_paths.append(output_path)
    print(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Round-Trip Check The Exported YAML
# MAGIC
# MAGIC This cell compiles the exported YAML back into dataclasses and validates it
# MAGIC again. This proves the generated YAML is not just readable text; it is valid
# MAGIC engine authoring metadata.

# COMMAND ----------

compiler = YamlRulesetCompiler()

for output_path in output_paths:
    compiled_ruleset = compiler.compile_path(output_path)
    semantic_result = semantic_validator.validate(compiled_ruleset)
    spark_result = spark_validator.validate(compiled_ruleset)

    print("=" * 100)
    print(output_path.name)
    print("compiled rules:", len(compiled_ruleset.rules))
    print("semantic:", semantic_result.to_text())
    print("spark:", spark_result.to_text())

    if semantic_result.has_errors():
        raise ValueError(f"Round-trip semantic validation failed for {output_path}")
    if spark_result.has_errors():
        raise ValueError(f"Round-trip Spark validation failed for {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inspect A Generated Ruleset
# MAGIC
# MAGIC This prints the first part of each exported YAML file. In normal use, review
# MAGIC the full files in `OUTPUT_DIR`, refine them manually where the old account-key
# MAGIC rules did not capture the full business logic, then publish through the
# MAGIC standard repository workflow.

# COMMAND ----------

for output_path in output_paths:
    print("=" * 100)
    print(output_path.name)
    print("=" * 100)
    print(output_path.read_text(encoding="utf-8")[:5000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. What This Notebook Guarantees
# MAGIC
# MAGIC This notebook guarantees that:
# MAGIC
# MAGIC - legacy account-key rule facts can be represented with the Python authoring
# MAGIC   API,
# MAGIC - the generated objects satisfy the same validation gates as hand-written
# MAGIC   YAML,
# MAGIC - exported YAML round-trips through the compiler,
# MAGIC - generated files remain outside the runtime package and can be manually
# MAGIC   reviewed before publish.
# MAGIC
# MAGIC This notebook does not publish the rulesets. Publication should happen only
# MAGIC after review, manual refinement, and environment-specific approval.
