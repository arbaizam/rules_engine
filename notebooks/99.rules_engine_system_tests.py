# Databricks notebook source
# ruff: noqa: E402, F821, I001
# MAGIC %md
# MAGIC # Rules Engine System Tests
# MAGIC
# MAGIC ALM Engineering uses this notebook to test the boundaries that require a
# MAGIC real Databricks environment: Spark execution, Python workers, Delta
# MAGIC metadata tables, and the public service facade. Pure compiler and
# MAGIC validator permutations belong in the pytest suite.
# MAGIC
# MAGIC The notebook overwrites two package-owned test tables in the supplied
# MAGIC disposable schema. Every test covers current package behavior at a real
# MAGIC Spark, Python-worker, or Delta boundary.

# COMMAND ----------

import os
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

root = next((p for p in [Path.cwd(), *Path.cwd().parents] if (p / "databricks.yml").exists()), None)
if root:
    src_path = os.path.normpath(root / "src")
    if src_path not in sys.path:
        print(f"Adding to sys.path: {src_path}")
        sys.path.append(src_path)

from pyspark.sql import functions as F
from pyspark.sql import types as T

from rules_engine import (
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    DeltaRowSerializer,
    RulesEngineService,
    standard_function_rows,
)
from rules_engine.exceptions import RepositoryError
from rules_engine.models import FunctionRegistryRow


def _job_parameter(name):
    """Return a Databricks task parameter when one is available."""
    try:
        return str(dbutils.widgets.get(name)).strip()
    except Exception:  # noqa: BLE001 - widget lookup differs across Databricks contexts.
        return None


def _quoted_identifier(value):
    """Quote a validated one-, two-, or three-part Spark identifier."""
    return ".".join(f"`{part}`" for part in value.split("."))


def _expect_raises(exception_type, operation, *, contains=None):
    """Run one operation and return its expected exception."""
    try:
        operation()
    except exception_type as exc:
        if contains is not None:
            assert contains in str(exc), (
                f"Expected {exception_type.__name__} containing {contains!r}, found {exc!r}."
            )
        return exc
    raise AssertionError(f"Expected {exception_type.__name__} to be raised.")


def _start(test_id, name):
    print()
    print(f"{test_id}: {name}")
    print("-" * 80)


assert root is not None, "Could not locate the repository root containing databricks.yml."
REPO_ROOT = root
SCHEMA = globals().get("SCHEMA") or _job_parameter("SCHEMA")
assert SCHEMA, "Set SCHEMA to a disposable catalog.schema before running this notebook."

schema_parts = SCHEMA.split(".")
assert len(schema_parts) == 2 and all(
    re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in schema_parts
), "SCHEMA must be a safe catalog.schema identifier."
schema_leaf = schema_parts[-1].lower()
assert any(marker in schema_leaf for marker in ("test", "scratch", "tmp")), (
    "SCHEMA must visibly identify a disposable test, scratch, or tmp schema."
)

configured_ruleset_path = globals().get("RULESET_YAML_PATH") or _job_parameter("RULESET_YAML_PATH")
RULESET_YAML_PATH = (
    Path(configured_ruleset_path).expanduser()
    if configured_ruleset_path
    else REPO_ROOT / "examples" / "rulesets" / "rules_engine_system_testing_rules.yaml"
)
if not RULESET_YAML_PATH.is_absolute():
    RULESET_YAML_PATH = (REPO_ROOT / RULESET_YAML_PATH).resolve()
assert RULESET_YAML_PATH.is_file(), f"Ruleset fixture does not exist: {RULESET_YAML_PATH}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quoted_identifier(SCHEMA)}")

RULESET_VERSIONS_TABLE = f"{SCHEMA}.ruleset_versions_system_test"
FUNCTION_REGISTRY_TABLE = f"{SCHEMA}.function_registry_system_test"
service = RulesEngineService.from_schema(
    spark,
    SCHEMA,
    ruleset_versions_table=RULESET_VERSIONS_TABLE,
    function_registry_table=FUNCTION_REGISTRY_TABLE,
)

print(f"Repository root: {REPO_ROOT}")
print(f"Ruleset fixture: {RULESET_YAML_PATH}")
print(f"Disposable schema: {SCHEMA}")

# COMMAND ----------

_start("ST-001", "Create the current Delta metadata contract")

service.create_tables(mode="overwrite")

assert spark.catalog.tableExists(RULESET_VERSIONS_TABLE)
assert spark.catalog.tableExists(FUNCTION_REGISTRY_TABLE)

expected_ruleset_columns = [
    "ruleset_id",
    "ruleset_name",
    "version",
    "status",
    "description",
    "payload_json",
    "content_hash",
    "rule_count",
    "condition_count",
    "assignment_count",
    "custom_function_count",
    "owner",
    "owner_department",
    "published_by",
    "published_at",
    "retired_by",
    "retired_at",
]
expected_function_columns = [
    "function_name",
    "implementation_reference",
    "arg_contract_payload_json",
    "return_type_hint",
    "allowed_in_condition_flag",
    "allowed_in_assignment_flag",
    "active_flag",
    "description",
    "version",
]

assert spark.table(RULESET_VERSIONS_TABLE).columns == expected_ruleset_columns
assert spark.table(FUNCTION_REGISTRY_TABLE).columns == expected_function_columns

print("PASS: Delta tables match the current repository schema.")

# COMMAND ----------

_start("ST-002", "Persist standard and custom function metadata")

service.save_standard_function_registry()
service.save_standard_function_registry()

standard_names = {row.function_name for row in standard_function_rows()}
assert len(standard_names) == 58
assert {"length", "contains_any", "to_number"}.isdisjoint(standard_names)
persisted_standard_names = {
    row["function_name"]
    for row in spark.table(FUNCTION_REGISTRY_TABLE)
    .where(F.col("function_name").isin(sorted(standard_names)))
    .select("function_name")
    .collect()
}
assert persisted_standard_names == standard_names

metadata_only_row = FunctionRegistryRow(
    function_name="system_metadata_only",
    implementation_reference="alm.system_tests.system_metadata_only",
    arg_contract_payload={
        "arguments": [
            {
                "name": "value",
                "required": True,
                "type_hint": "integer",
                "literal_only": False,
            }
        ]
    },
    return_type_hint="string",
    allowed_in_condition_flag=True,
    allowed_in_assignment_flag=False,
    active_flag=True,
    description="System-test metadata row without executable code.",
    version="1",
)
service.save_function_registry_rows([metadata_only_row])

custom_rows = (
    spark.table(FUNCTION_REGISTRY_TABLE)
    .where(F.col("function_name") == "system_metadata_only")
    .collect()
)
assert len(custom_rows) == 1
assert custom_rows[0]["allowed_in_condition_flag"] is True
assert custom_rows[0]["allowed_in_assignment_flag"] is False

print("PASS: Registry metadata is rerunnable and preserves the current contract.")

# COMMAND ----------

_start("ST-003", "Compile, validate, publish, load, and hash the shipped fixture")

fixture_ruleset = service.compile_yaml_path(RULESET_YAML_PATH)
fixture_validation = service.validator.validate(fixture_ruleset)
assert fixture_validation.passed, fixture_validation.to_text()

service.publish(fixture_ruleset, published_by="alm-system-tests")
loaded_fixture = service.load_published(
    fixture_ruleset.ruleset_name,
    fixture_ruleset.version,
)
assert loaded_fixture == fixture_ruleset

fixture_rows = (
    spark.table(RULESET_VERSIONS_TABLE)
    .where(
        (F.col("ruleset_id") == fixture_ruleset.ruleset_id)
        & (F.col("version") == fixture_ruleset.version)
    )
    .collect()
)
assert len(fixture_rows) == 1
fixture_row = fixture_rows[0].asDict(recursive=True)
assert fixture_row["status"] == "published"
assert fixture_row["published_by"] == "alm-system-tests"
assert fixture_row["content_hash"] == DeltaRowSerializer().content_hash(fixture_ruleset)

print("PASS: The shipped YAML completed the full governed publication path.")

# COMMAND ----------

_start("ST-004", "Reject both immutable identity collisions")

identity_base = service.compile_yaml_text(
    """
ruleset_id: system_identity
ruleset_name: System Identity Rules
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: identity_match
    rule_name: Identity match
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      identity_result: base
"""
)
service.publish(identity_base, published_by="alm-system-tests")

same_id_and_version = service.compile_yaml_text(
    """
ruleset_id: system_identity
ruleset_name: Different System Identity Name
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_name: Same ID collision
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign: {identity_result: same_id}
"""
)
same_name_and_version = service.compile_yaml_text(
    """
ruleset_id: different_system_identity
ruleset_name: System Identity Rules
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_name: Same name collision
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign: {identity_result: same_name}
"""
)

_expect_raises(
    RepositoryError,
    lambda: service.publish(same_id_and_version),
    contains="ruleset_id=system_identity",
)
_expect_raises(
    RepositoryError,
    lambda: service.publish(same_name_and_version),
    contains="ruleset_name=System Identity Rules",
)

assert (
    spark.table(RULESET_VERSIONS_TABLE)
    .where(F.col("version") == "1")
    .where(
        (F.col("ruleset_id") == "system_identity")
        | (F.col("ruleset_name") == "System Identity Rules")
    )
    .count()
    == 1
)

print("PASS: Neither stable identity can be overwritten.")

# COMMAND ----------

_start("ST-005", "Require an explicit version when published versions are ambiguous")

identity_v2 = service.compile_yaml_text(
    """
ruleset_id: system_identity
ruleset_name: System Identity Rules
version: "2"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: identity_match_v2
    rule_name: Identity match version two
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      identity_result: version_two
"""
)
service.publish(identity_v2, published_by="alm-system-tests")

_expect_raises(
    RepositoryError,
    lambda: service.load_published("System Identity Rules"),
    contains="specify version",
)
assert service.load_published("System Identity Rules", "1") == identity_base
assert service.load_published("System Identity Rules", "2") == identity_v2

print("PASS: Name-only loading fails safely when more than one version is published.")

# COMMAND ----------

_start("ST-006", "Retire by stable ID and exact version")

service.retire("system_identity", "1", retired_by="alm-system-tests")

_expect_raises(
    RepositoryError,
    lambda: service.load_published("System Identity Rules", "1"),
    contains="not found",
)
assert service.load_published("System Identity Rules", "2") == identity_v2

retired_rows = (
    spark.table(RULESET_VERSIONS_TABLE)
    .where((F.col("ruleset_id") == "system_identity") & (F.col("version") == "1"))
    .collect()
)
assert len(retired_rows) == 1
retired_row = retired_rows[0].asDict(recursive=True)
assert retired_row["status"] == "retired"
assert retired_row["retired_by"] == "alm-system-tests"
assert retired_row["retired_at"]

_expect_raises(
    RepositoryError,
    lambda: service.retire("system_identity", "1"),
    contains="already retired",
)

print("PASS: Retirement changes lifecycle only and cannot be repeated silently.")

# COMMAND ----------

_start("ST-007", "Keep compact and full-audit outcomes identical")

parity_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_output_parity
ruleset_name: System Output Parity
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: positive_amount
    rule_name: Positive amount
    when:
      condition_group_id: parity_any
      any:
        - condition_id: inactive_branch
          left: {field: row_id}
          operator: eq
          right: {literal: never}
          active_flag: false
        - condition_id: positive_branch
          left: {field: amount}
          operator: gt
          right: {literal: 0}
          error_on_null: true
    assign:
      outcome: positive
"""
)

parity_input = spark.createDataFrame(
    [
        ("success", 10),
        ("no_match", -1),
        ("error", None),
    ],
    T.StructType(
        [
            T.StructField("row_id", T.StringType(), False),
            T.StructField("amount", T.IntegerType(), True),
        ]
    ),
)

compact_evaluation = service.evaluate_dataframe(
    parity_input,
    ruleset=parity_ruleset,
    key_columns=["row_id"],
    fail_on_error=False,
    full_audit=False,
)
full_evaluation = service.evaluate_dataframe(
    parity_input,
    ruleset=parity_ruleset,
    key_columns=["row_id"],
    fail_on_error=False,
    full_audit=True,
)
compact = compact_evaluation.results_df
full = full_evaluation.results_df

assert compact.columns == [
    "row_id",
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
]
assert full.columns == [
    "row_id",
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_matched_rules",
    "rules_engine_assignment_results",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
]

compact_rows = {
    row["row_id"]: row.asDict(recursive=True) for row in compact.orderBy("row_id").collect()
}
full_rows = {row["row_id"]: row.asDict(recursive=True) for row in full.orderBy("row_id").collect()}

for row_id in compact_rows:
    for field_name in (
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_ruleset",
    ):
        assert compact_rows[row_id][field_name] == full_rows[row_id][field_name]

assert compact_rows["success"]["rules_engine_matched"] is True
assert compact_rows["success"]["rules_engine_matched_rule_ids"] == ["positive_amount"]
assert compact_rows["success"]["rules_engine_assign"] == {
    "outcome": {"applied": True, "value": "positive"}
}
assert compact_rows["no_match"]["rules_engine_assign"] == {
    "outcome": {"applied": False, "value": None}
}
assert compact_rows["no_match"]["rules_engine_matched"] is False
assert compact_rows["no_match"]["rules_engine_matched_rule_ids"] == []
assert compact_rows["no_match"]["rules_engine_error"] is None
assert compact_rows["error"]["rules_engine_matched"] is False
assert compact_rows["error"]["rules_engine_matched_rule_ids"] == []
assert "error_on_null=true" in compact_rows["error"]["rules_engine_error"]
assert full_rows["no_match"]["rules_engine_matched_rules"] == []
assert full_rows["error"]["rules_engine_assignment_results"] == []

print("PASS: Success, no-match, and error outcomes are identical across audit modes.")

# COMMAND ----------

_start("ST-008", "Emit complete condition identity in full audit")

matched_trace = full_rows["success"]["rules_engine_matched_rules"]
assert len(matched_trace) == 1
assert matched_trace[0]["rule_id"] == "positive_amount"
assert [condition["condition_id"] for condition in matched_trace[0]["conditions"]] == [
    "inactive_branch",
    "positive_branch",
]

inactive_trace, active_trace = matched_trace[0]["conditions"]
assert inactive_trace["condition_group_id"] == "parity_any"
assert inactive_trace["condition_group_operator"] == "any"
assert inactive_trace["active_flag"] is False
assert inactive_trace["passed"] is False
assert active_trace["condition_group_id"] == "parity_any"
assert active_trace["condition_group_operator"] == "any"
assert active_trace["active_flag"] is True
assert active_trace["passed"] is True
assert active_trace["columns"] == ["amount"]

print("PASS: Full audit distinguishes active and inactive condition branches.")

# COMMAND ----------

_start("ST-009", "Apply the stopping rule and skip every later rule")

stop_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_stop_on_match
ruleset_name: System Stop On Match
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: stop_first
    rule_name: Stop first
    rule_order: 1
    stop_on_match: true
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      stage: first
  - rule_id: skipped_second
    rule_name: Skipped second
    rule_order: 2
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      stage: second
      later_rule_ran: true
"""
)

stop_row = (
    service.evaluate_dataframe(
        spark.createDataFrame([("one",)], ["row_id"]),
        ruleset=stop_ruleset,
        key_columns=["row_id"],
        full_audit=True,
    )
    .results_df.collect()[0]
    .asDict(recursive=True)
)

assert stop_row["rules_engine_matched_rule_ids"] == ["stop_first"]
assert stop_row["rules_engine_assign"]["stage"] == {
    "applied": True,
    "value": "first",
}
assert stop_row["rules_engine_assign"]["later_rule_ran"] == {
    "applied": False,
    "value": None,
}
assert [item["rule_id"] for item in stop_row["rules_engine_matched_rules"]] == ["stop_first"]

print("PASS: The matching stopping rule commits before later evaluation stops.")

# COMMAND ----------

_start("ST-010", "Read committed earlier assignments through an atomic rule snapshot")

chain_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_assignment_chain
ruleset_name: System Assignment Chain
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: chain_producer
    rule_name: Chain producer
    rule_order: 1
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      stage: produced
      score: 10
  - rule_id: chain_consumer
    rule_name: Chain consumer
    rule_order: 2
    when:
      all:
        - condition_id: reads_produced_stage
          left: {assigned: stage}
          operator: eq
          right: {literal: produced}
    assign:
      stage: consumed
      copied_stage: {assigned: stage}
      copied_score: {assigned: score}
  - rule_id: chain_final
    rule_name: Chain final
    rule_order: 3
    when:
      all:
        - left: {assigned: stage}
          operator: eq
          right: {literal: consumed}
    assign:
      final_rule_ran: true
"""
)

chain_row = (
    service.evaluate_dataframe(
        spark.createDataFrame([("one", "original")], ["row_id", "stage"]),
        ruleset=chain_ruleset,
        key_columns=["row_id"],
        full_audit=True,
    )
    .results_df.collect()[0]
    .asDict(recursive=True)
)

assert chain_row["rules_engine_matched_rule_ids"] == [
    "chain_producer",
    "chain_consumer",
    "chain_final",
]
assert chain_row["rules_engine_assign"]["stage"]["value"] == "consumed"
assert chain_row["rules_engine_assign"]["copied_stage"]["value"] == "produced"
assert chain_row["rules_engine_assign"]["copied_score"]["value"] == 10
assert chain_row["rules_engine_assign"]["final_rule_ran"]["value"] is True

consumer_left = chain_row["rules_engine_matched_rules"][1]["conditions"][0]["left"]
assert consumer_left["value"] == "produced"
assert consumer_left["produced_by_rule_id"] == "chain_producer"
assert consumer_left["produced_by_assignment_id"] == "assignment:chain_producer:stage"

print("PASS: Later rules see commits while sibling assignments share one pre-rule snapshot.")

# COMMAND ----------

_start("ST-011", "Substitute operand nulls before comparison and preserve explicit errors")

null_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_null_defaults
ruleset_name: System Null Defaults
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: numeric_default
    rule_name: Numeric default
    rule_order: 1
    when:
      all:
        - condition_id: amount_defaults_to_zero
          left: {field: amount, default_if_null: 0}
          operator: eq
          right: {literal: 0}
    assign:
      numeric_defaulted: true
  - rule_id: text_default
    rule_name: Text default
    rule_order: 2
    when:
      all:
        - condition_id: code_defaults_to_missing
          left: {field: code, default_if_null: MISSING}
          operator: eq
          right: {literal: MISSING}
    assign:
      text_defaulted: true
"""
)

null_input = spark.createDataFrame(
    [("nulls", None, None)],
    T.StructType(
        [
            T.StructField("row_id", T.StringType(), False),
            T.StructField("amount", T.IntegerType(), True),
            T.StructField("code", T.StringType(), True),
        ]
    ),
)
null_row = (
    service.evaluate_dataframe(
        null_input,
        ruleset=null_ruleset,
        key_columns=["row_id"],
        full_audit=True,
    )
    .results_df.collect()[0]
    .asDict(recursive=True)
)

assert null_row["rules_engine_matched_rule_ids"] == ["numeric_default", "text_default"]
assert null_row["rules_engine_assign"] == {
    "numeric_defaulted": {"applied": True, "value": True},
    "text_defaulted": {"applied": True, "value": True},
}
numeric_left = null_row["rules_engine_matched_rules"][0]["conditions"][0]["left"]
text_left = null_row["rules_engine_matched_rules"][1]["conditions"][0]["left"]
assert numeric_left["original_value"] is None
assert numeric_left["value"] == "0"
assert numeric_left["default_if_null"] == "0"
assert numeric_left["default_applied"] is True
assert text_left["original_value"] is None
assert text_left["value"] == "MISSING"
assert text_left["default_applied"] is True
assert "error_on_null=true" in full_rows["error"]["rules_engine_error"]

print("PASS: Numeric and text fallbacks run before comparison; explicit null errors remain errors.")

# COMMAND ----------

_start("ST-012", "Record assignment override history against the effective prior value")

history_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_assignment_history
ruleset_name: System Assignment History
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: history_first
    rule_name: History first
    rule_order: 1
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      target: first
  - rule_id: history_second
    rule_name: History second
    rule_order: 2
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      target: second
"""
)

history_row = (
    service.evaluate_dataframe(
        spark.createDataFrame([("one", "original")], ["row_id", "target"]),
        ruleset=history_ruleset,
        key_columns=["row_id"],
        full_audit=True,
    )
    .results_df.collect()[0]
    .asDict(recursive=True)
)

events = history_row["rules_engine_assignment_results"]
assert len(events) == 2
first_event, second_event = events
assert first_event["old_value"] == "original"
assert first_event["proposed_value"] == "first"
assert first_event["changed"] is True
assert first_event["effective"] is False
assert first_event["overridden_by_rule_id"] == "history_second"
assert first_event["overridden_by_assignment_id"] == "assignment:history_second:target"
assert second_event["old_value"] == "first"
assert second_event["proposed_value"] == "second"
assert second_event["changed"] is True
assert second_event["effective"] is True
assert second_event["overridden_by_rule_id"] is None
assert history_row["rules_engine_assign"]["target"] == {
    "applied": True,
    "value": "second",
}

print("PASS: Assignment history follows original value, prior commit, and final winner.")

# COMMAND ----------

_start("ST-013", "Preserve decimal, date, and nested struct assignments through Spark")

typed_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_typed_assignments
ruleset_name: System Typed Assignments
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: typed_rule
    rule_name: Typed rule
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      exact_amount: {literal: 12.34, value_type: decimal}
      business_date: {literal: 2026-08-21, value_type: date}
      review_flags:
        literal:
          material: true
          manual: false
"""
)

typed_evaluation = service.evaluate_dataframe(
    spark.createDataFrame([("one",)], ["row_id"]),
    ruleset=typed_ruleset,
    key_columns=["row_id"],
)
typed_result = typed_evaluation.results_df
typed_assign_schema = typed_result.schema["rules_engine_assign"].dataType
typed_row = typed_result.collect()[0].asDict(recursive=True)

assert isinstance(
    typed_assign_schema["exact_amount"].dataType["value"].dataType,
    T.DecimalType,
)
assert typed_assign_schema["business_date"].dataType["value"].dataType == T.DateType()
assert isinstance(
    typed_assign_schema["review_flags"].dataType["value"].dataType,
    T.StructType,
)
assert typed_row["rules_engine_assign"]["exact_amount"]["value"] == Decimal("12.34")
assert typed_row["rules_engine_assign"]["business_date"]["value"] == date(2026, 8, 21)
assert typed_row["rules_engine_assign"]["review_flags"]["value"] == {
    "material": True,
    "manual": False,
}

print("PASS: Financial, temporal, and nested values cross the real worker boundary exactly.")

# COMMAND ----------

_start("ST-014", "Execute a registered custom function in a real Spark worker")


def system_double(**kwargs):
    value = kwargs["value"]
    return None if value is None else int(value) * 2


if not service.registry.has_spec("system_double"):
    service.registry.register(
        CustomFunctionSpec(
            function_name="system_double",
            implementation_reference="alm.system_tests.system_double",
            arguments=(CustomFunctionArgSpec("value", type_hint="integer"),),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=True,
            return_type_hint="integer",
            description="Double an integer for the system worker test.",
            version="1",
        ),
        system_double,
    )

function_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_custom_function
ruleset_name: System Custom Function
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: custom_function_rule
    rule_name: Custom function rule
    when:
      all:
        - left:
            custom_function:
              name: system_double
              args:
                value: {field: amount}
          operator: eq
          right: {literal: 10}
    assign:
      doubled:
        custom_function:
          name: system_double
          args:
            value: {field: amount}
"""
)

function_row = (
    service.evaluate_dataframe(
        spark.createDataFrame([("one", 5)], ["row_id", "amount"]),
        ruleset=function_ruleset,
        key_columns=["row_id"],
    )
    .results_df.collect()[0]
    .asDict(recursive=True)
)

assert function_row["rules_engine_matched_rule_ids"] == ["custom_function_rule"]
assert function_row["rules_engine_assign"]["doubled"] == {
    "applied": True,
    "value": 10,
}

print("PASS: Registered metadata and executable code work across the Spark worker boundary.")

# COMMAND ----------

_start("ST-015", "Preserve ordinary result-named inputs and reject reserved output collisions")

collision_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_column_collisions
ruleset_name: System Column Collisions
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: collision_rule
    rule_name: Collision rule
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      collision_result: matched
"""
)

ordinary_result_input = spark.createDataFrame(
    [("one", "keep-me")],
    ["row_id", "rules_engine_result"],
)
ordinary_evaluation = service.evaluate_dataframe(
    ordinary_result_input,
    ruleset=collision_ruleset,
    key_columns=["row_id"],
)
ordinary_row = ordinary_evaluation.apply_assignments().collect()[0].asDict(recursive=True)
assert ordinary_row["rules_engine_result"] == "keep-me"

custom_prefix = service.evaluate_dataframe(
    ordinary_result_input,
    ruleset=collision_ruleset,
    key_columns=["row_id"],
    column_prefix="decision",
).results_df
assert custom_prefix.columns == [
    "row_id",
    "decision_error",
    "decision_matched",
    "decision_matched_rule_ids",
    "decision_assign",
    "decision_ruleset",
    "decision_engine_version",
]

full_only_conflict = spark.createDataFrame(
    [("one", "reserved")],
    ["row_id", "decision_matched_rules"],
)
_expect_raises(
    ValueError,
    lambda: service.evaluate_dataframe(
        full_only_conflict,
        ruleset=collision_ruleset,
        key_columns=["row_id"],
        column_prefix="decision",
        full_audit=False,
    ),
    contains="decision_matched_rules",
)

print("PASS: Internal temp names are private and all public names remain reserved.")

# COMMAND ----------

_start("ST-016", "Report rule coverage and clean no-match rows")

coverage_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_coverage
ruleset_name: System Coverage
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: broad_a
    rule_name: Broad A
    rule_order: 1
    when:
      all:
        - left: {field: segment}
          operator: eq
          right: {literal: A}
    assign:
      coverage_result: a
  - rule_id: dead_z
    rule_name: Dead Z
    rule_order: 2
    when:
      all:
        - left: {field: segment}
          operator: eq
          right: {literal: Z}
    assign:
      coverage_result: z
"""
)

coverage_input = spark.createDataFrame(
    [("1", "A"), ("2", "A"), ("3", "A"), ("4", "B")],
    ["row_id", "segment"],
)
coverage = service.coverage_report(
    coverage_input,
    ruleset=coverage_ruleset,
    broad_match_threshold=0.50,
)

assert coverage.total_row_count == 4
assert coverage.no_match_count == 1
assert coverage.error_count == 0
assert coverage.first_match_distribution == {"broad_a": 3, "dead_z": 0}
assert coverage.dead_rule_ids == ("dead_z",)
assert coverage.suspiciously_broad_rule_ids == ("broad_a",)
assert coverage.no_match_rows.select("row_id").collect()[0]["row_id"] == "4"

print("PASS: Coverage reports counts, dead rules, broad rules, and clean no-match rows only.")

# COMMAND ----------

_start("ST-017", "Separate keyed results and apply scalar, null, and struct assignments")

apply_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_apply_assignments
ruleset_name: System Apply Assignments
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: clear_values
    rule_name: Clear values
    rule_order: 1
    when:
      all:
        - left: {field: action}
          operator: eq
          right: {literal: clear}
    assign:
      status: {literal: null, value_type: string}
      details: {literal: null}
  - rule_id: replace_values
    rule_name: Replace values
    rule_order: 2
    when:
      all:
        - left: {field: action}
          operator: eq
          right: {literal: replace}
    assign:
      status: updated
      details:
        literal:
          material: true
          manual: false
      new_note: created
"""
)

details_schema = T.StructType(
    [
        T.StructField("material", T.BooleanType(), True),
        T.StructField("manual", T.BooleanType(), True),
    ]
)
apply_input = spark.createDataFrame(
    [
        ("clear", "clear", "original", {"material": False, "manual": True}),
        ("replace", "replace", "original", {"material": False, "manual": True}),
        ("keep", "none", "original", {"material": False, "manual": True}),
    ],
    T.StructType(
        [
            T.StructField("row_id", T.StringType(), False),
            T.StructField("action", T.StringType(), False),
            T.StructField("status", T.StringType(), True),
            T.StructField("details", details_schema, True),
        ]
    ),
)

apply_evaluation = service.evaluate_dataframe(
    apply_input,
    ruleset=apply_ruleset,
    key_columns=["row_id"],
    full_audit=True,
).persist()

assert apply_evaluation.results_df.columns == [
    "row_id",
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_matched_rules",
    "rules_engine_assignment_results",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
]
result_rows = {
    row["row_id"]: row.asDict(recursive=True) for row in apply_evaluation.results_df.collect()
}
assert result_rows["clear"]["rules_engine_assign"]["status"] == {
    "applied": True,
    "value": None,
}
assert result_rows["keep"]["rules_engine_assign"]["status"] == {
    "applied": False,
    "value": None,
}
assert result_rows["replace"]["rules_engine_assign"]["details"] == {
    "applied": True,
    "value": {"material": True, "manual": False},
}

applied_df = apply_evaluation.apply_assignments()
assert applied_df.columns == ["row_id", "action", "status", "details", "new_note"]
applied_rows = {row["row_id"]: row.asDict(recursive=True) for row in applied_df.collect()}
assert applied_rows["clear"]["status"] is None
assert applied_rows["clear"]["details"] is None
assert applied_rows["replace"]["status"] == "updated"
assert applied_rows["replace"]["details"] == {
    "material": True,
    "manual": False,
}
assert applied_rows["replace"]["new_note"] == "created"
assert applied_rows["keep"]["status"] == "original"
assert applied_rows["keep"]["details"] == {
    "material": False,
    "manual": True,
}
assert applied_rows["keep"]["new_note"] is None
apply_evaluation.unpersist()

key_assignment_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_immutable_keys
ruleset_name: System Immutable Keys
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: change_key
    rule_name: Change key
    when:
      all:
        - left: {field: row_id}
          operator: is_not_null
    assign:
      row_id: changed
"""
)
_expect_raises(
    ValueError,
    lambda: service.evaluate_dataframe(
        apply_input,
        ruleset=key_assignment_ruleset,
        key_columns=["row_id"],
    ),
    contains="key columns",
)

print("PASS: Keyed results stay separate and assignments replace, clear, or append atomically.")

# COMMAND ----------

_start("ST-018", "Execute composition, array, decimal, and business-calendar functions")

function_ruleset = service.compile_yaml_text(
    """
ruleset_id: system_standard_functions
ruleset_name: System Standard Functions
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: derive_values
    rule_name: Derive values
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
    assign:
      selected_code:
        custom_function:
          name: coalesce
          args:
            values:
              - {field: primary_code}
              - {field: secondary_code}
      code_suffix:
        custom_function:
          name: substring
          args:
            value: {field: secondary_code}
            start: 2
      has_required_tags:
        custom_function:
          name: array_contains_all
          args:
            values: {field: tags}
            candidates: [review, active]
      tags_text:
        custom_function:
          name: array_join
          args:
            values: {field: tags}
            separator: "|"
      quarter_amount:
        custom_function:
          name: decimal_safe_divide
          args:
            numerator:
              custom_function:
                name: to_decimal
                args:
                  value: {field: annual_amount}
            denominator: 4
            scale: 2
      first_business_date:
        custom_function:
          name: first_business_day_of_month
          args:
            value: {field: business_date}
            holidays: ["2024-06-03"]
      parsed_count:
        custom_function:
          name: to_integer
          args:
            value: {field: raw_count}
            on_error: "null"
"""
)

function_input = spark.createDataFrame(
    [
        (
            "1",
            None,
            "ABC",
            ["review", "active"],
            "10.00",
            "2024-06-15",
            "not-an-integer",
        )
    ],
    "row_id string, primary_code string, secondary_code string, "
    "tags array<string>, annual_amount string, business_date string, raw_count string",
)
function_evaluation = service.evaluate_dataframe(
    function_input,
    ruleset=function_ruleset,
    key_columns=["row_id"],
)
function_result = function_evaluation.apply_assignments().collect()[0].asDict(recursive=True)

assert function_result["selected_code"] == "ABC"
assert function_result["code_suffix"] == "BC"
assert function_result["has_required_tags"] is True
assert function_result["tags_text"] == "review|active"
assert function_result["quarter_amount"] == Decimal("2.500000000000000000")
assert function_result["first_business_date"] == date(2024, 6, 4)
assert function_result["parsed_count"] is None

print("PASS: Standard functions retain optional, nested, array, decimal, and date semantics.")

# COMMAND ----------

print()
print("=" * 80)
print("PASS: All 18 current-contract rules engine system tests completed.")
print("=" * 80)
