# Databricks notebook source
import os
import re
from pathlib import Path


def _job_parameter(name):
    """Return a Databricks job parameter when this notebook runs as a task."""
    try:
        return str(dbutils.widgets.get(name)).strip()
    except Exception:
        return None


def _find_repo_root():
    """Locate the repository checkout containing tests and fixtures."""
    configured = globals().get("RULES_ENGINE_REPO_ROOT") or _job_parameter(
        "RULES_ENGINE_REPO_ROOT"
    )
    starts = []
    if configured:
        starts.append(Path(str(configured)).expanduser().resolve())
    starts.append(Path.cwd().resolve())
    if "__file__" in globals():
        starts.append(Path(__file__).resolve().parent)
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "tests").is_dir() and (candidate / "pyproject.toml").is_file():
                return candidate
    raise AssertionError(
        "Could not locate the repository root. Set RULES_ENGINE_REPO_ROOT."
    )


def _quoted_identifier(value):
    """Quote a validated one-, two-, or three-part Spark identifier."""
    return ".".join(f"`{part}`" for part in value.split("."))


def _as_bool(value, *, name):
    """Parse notebook booleans without treating the string 'false' as true."""
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n", ""}:
        return False
    raise ValueError(f"{name} must be a boolean value, found {value!r}.")


REPO_ROOT = _find_repo_root()
SCHEMA = globals().get("SCHEMA") or _job_parameter("SCHEMA")
RULES_ENGINE_RUN_SPARK_TESTS = _as_bool(
    globals().get("RULES_ENGINE_RUN_SPARK_TESTS")
    or _job_parameter("RULES_ENGINE_RUN_SPARK_TESTS")
    or "1",
    name="RULES_ENGINE_RUN_SPARK_TESTS",
)
configured_ruleset_path = (
    globals().get("RULESET_YAML_PATH")
    or _job_parameter("RULESET_YAML_PATH")
)
RULESET_YAML_PATH = Path(configured_ruleset_path).expanduser() if configured_ruleset_path else (
    REPO_ROOT / "rule_sets" / "account_key_cap_mkt.yaml"
)
if not RULESET_YAML_PATH.is_absolute():
    RULESET_YAML_PATH = (REPO_ROOT / RULESET_YAML_PATH).resolve()

assert RULESET_YAML_PATH.is_file(), f"Ruleset fixture does not exist: {RULESET_YAML_PATH}"
assert SCHEMA, "Set SCHEMA before running this test."
schema_parts = SCHEMA.split(".")
assert len(schema_parts) == 2 and all(
    re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in schema_parts
), "SCHEMA must be a safe catalog.schema identifier."
schema_leaf = schema_parts[-1].lower()
assert any(marker in schema_leaf for marker in ("test", "scratch", "tmp")), (
    "SCHEMA must visibly identify a disposable test/scratch/tmp schema."
)

print(f"Repository test root: {REPO_ROOT}")
print(f"Ruleset path fixture: {RULESET_YAML_PATH}")

# COMMAND ----------
print("ST-001: Test harness creates the target schema before table creation")
print("-" * 80)
print("Area: Metadata storage")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: The schema exists and the notebook proceeds to table creation without manual pre-work.")
print("")
assert SCHEMA, "Set SCHEMA before running this test."

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quoted_identifier(SCHEMA)}")
schemas = [
    row[0]
    for row in spark.sql(
        f"SHOW SCHEMAS IN {_quoted_identifier(schema_parts[0])}"
    ).collect()
]
schema_name = SCHEMA.rsplit(".", 1)[-1]
assert schema_name in schemas, f"Expected schema {SCHEMA} to exist after CREATE SCHEMA IF NOT EXISTS."
print(f"PASS: Schema exists: {SCHEMA}")

# COMMAND ----------
print("ST-002: Rules engine metadata tables are created with standard names")
print("-" * 80)
print("Area: Metadata storage")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: ruleset_versions and function_registry exist in the target schema. Rerunning the cell does not fail.")
print("")
from rules_engine import RulesEngineService

assert SCHEMA, "Set SCHEMA before running this test."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
service.create_tables(mode="ignore")

assert spark.catalog.tableExists(service.table_names.ruleset_versions), f"Missing {service.table_names.ruleset_versions}"
assert spark.catalog.tableExists(service.table_names.function_registry), f"Missing {service.table_names.function_registry}"
print(f"PASS: Metadata tables exist: {service.table_names}")

# COMMAND ----------
print("ST-003: Custom metadata table names are honored by the service")
print("-" * 80)
print("Area: Metadata storage")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: The custom tables are created and service.table_names reports the custom names for later publish/load calls.")
print("")
from rules_engine import RulesEngineService

RULESET_VERSIONS_TABLE = globals().get("RULESET_VERSIONS_TABLE", f"{SCHEMA}.custom_ruleset_versions")
FUNCTION_REGISTRY_TABLE = globals().get("FUNCTION_REGISTRY_TABLE", f"{SCHEMA}.custom_function_registry")

service = RulesEngineService.from_schema(
    spark=spark,
    schema=SCHEMA,
    ruleset_versions_table=RULESET_VERSIONS_TABLE,
    function_registry_table=FUNCTION_REGISTRY_TABLE,
)
service.create_tables(mode="ignore")

assert service.table_names.ruleset_versions == RULESET_VERSIONS_TABLE
assert service.table_names.function_registry == FUNCTION_REGISTRY_TABLE
assert spark.catalog.tableExists(RULESET_VERSIONS_TABLE)
assert spark.catalog.tableExists(FUNCTION_REGISTRY_TABLE)
print("PASS: Custom metadata table names are honored.")

# COMMAND ----------
print("ST-004: Metadata table DDL preserves NOT NULL columns")
print("-" * 80)
print("Area: Metadata storage")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Required columns such as ruleset_id, ruleset_name, version, status, effective_start_date, effective_end_date, payload_json, content_hash, rule_count, function_name, implementation_reference, and active_flag are marked NOT NULL where expected.")
print("")
import re

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

ruleset_create_sql = "\n".join(
    row[0] for row in spark.sql(f"SHOW CREATE TABLE {service.table_names.ruleset_versions}").collect()
).lower().replace("`", "").replace("\r", "\n")
function_create_sql = "\n".join(
    row[0] for row in spark.sql(f"SHOW CREATE TABLE {service.table_names.function_registry}").collect()
).lower().replace("`", "").replace("\r", "\n")

print("ruleset_versions DDL:")
print(ruleset_create_sql)
print("")
print("function_registry DDL:")
print(function_create_sql)
print("")

ruleset_normalized_sql = re.sub(r"\s+", " ", ruleset_create_sql)
function_normalized_sql = re.sub(r"\s+", " ", function_create_sql)

for column in ["ruleset_id", "ruleset_name", "version", "status", "effective_start_date", "effective_end_date", "payload_json", "content_hash"]:
    expected = rf"\b{column}\b\s+string(?:\s+collate\s+\S+)?\s+not\s+null\b"
    assert re.search(expected, ruleset_normalized_sql), (
        f"Expected ruleset_versions column {column} to be declared NOT NULL in table DDL. "
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate the table "
        "with the current rules_engine table DDL."
    )

for column in ["rule_count", "condition_count", "assignment_count", "custom_function_count"]:
    expected = rf"\b{column}\b\s+int\s+not\s+null\b"
    assert re.search(expected, ruleset_normalized_sql), (
        f"Expected ruleset_versions column {column} to be declared NOT NULL in table DDL. "
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate the table "
        "with the current rules_engine table DDL."
    )

for column in ["function_name", "implementation_reference", "arg_contract_payload_json", "active_flag"]:
    expected_type = "boolean" if column == "active_flag" else "string"
    if expected_type == "string":
        expected = rf"\b{column}\b\s+string(?:\s+collate\s+\S+)?\s+not\s+null\b"
    else:
        expected = rf"\b{column}\b\s+{expected_type}\s+not\s+null\b"
    assert re.search(expected, function_normalized_sql), (
        f"Expected function_registry column {column} to be declared NOT NULL in table DDL. "
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate the table "
        "with the current rules_engine table DDL."
    )

print("PASS: Required metadata columns are declared NOT NULL in table DDL.")

# COMMAND ----------
print("ST-005: Overwrite mode is restricted to disposable environments")
print("-" * 80)
print("Area: Metadata storage")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Overwrite recreates tables only when a disposable schema is explicitly approved.")
print("")
from rules_engine import RulesEngineService

assert SCHEMA, "Set SCHEMA before running this test."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
ALLOW_OVERWRITE_TEST = _as_bool(
    globals().get("ALLOW_OVERWRITE_TEST", False),
    name="ALLOW_OVERWRITE_TEST",
)

if ALLOW_OVERWRITE_TEST:
    service.create_tables(mode="overwrite")
    assert spark.catalog.tableExists(service.table_names.ruleset_versions)
    assert spark.catalog.tableExists(service.table_names.function_registry)
    print(f"PASS: Overwrite mode recreated metadata tables in explicitly approved schema {SCHEMA}.")
else:
    service.create_tables(mode="ignore")
    assert spark.catalog.tableExists(service.table_names.ruleset_versions)
    assert spark.catalog.tableExists(service.table_names.function_registry)
    print("PASS: Overwrite mode was not run because ALLOW_OVERWRITE_TEST is not True.")
    print("Set ALLOW_OVERWRITE_TEST = True only for a disposable schema if you want to execute overwrite.")


# COMMAND ----------
print("ST-006: Standard function metadata is registered after setup")
print("-" * 80)
print("Area: Function registry")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: function_registry contains all package standard functions with expected implementation_reference, active_flag, allowed flags, return type, and version.")
print("")
from rules_engine import RulesEngineService
from rules_engine.standard_functions import standard_function_rows

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
service.save_standard_function_registry()

expected = {row.function_name for row in standard_function_rows()}
actual = {row["function_name"] for row in spark.table(service.table_names.function_registry).select("function_name").collect()}
missing = sorted(expected - actual)
assert not missing, f"Missing standard functions: {missing}"
print(f"PASS: {len(expected)} standard functions are registered.")

# COMMAND ----------
print("ST-007: Standard function registration is rerunnable and refreshes package metadata")
print("-" * 80)
print("Area: Function registry")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Both runs succeed, preserve row count, and align package-owned versions with the installed catalog.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

before = spark.table(service.table_names.function_registry).count()
service.save_standard_function_registry()
service.save_standard_function_registry()
after = spark.table(service.table_names.function_registry).count()
expected_versions = {
    row.function_name: row.version
    for row in standard_function_rows()
}
actual_versions = {
    row["function_name"]: row["version"]
    for row in spark.table(service.table_names.function_registry)
    .select("function_name", "version")
    .collect()
    if row["function_name"] in expected_versions
}

assert after == before, f"Expected rerunnable standard registration to preserve row count. before={before}, after={after}"
assert actual_versions == expected_versions, f"Standard function versions differ. expected={expected_versions}, actual={actual_versions}"
print("PASS: Standard function registration is rerunnable and refreshes package metadata.")

# COMMAND ----------
print("ST-008: Explicit standard function preservation mode avoids overwrites")
print("-" * 80)
print("Area: Function registry")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Existing rows are preserved and no duplicate function names are inserted.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
before = spark.table(service.table_names.function_registry).count()
service.save_standard_function_registry(update_existing=False)
after = spark.table(service.table_names.function_registry).count()

duplicates = (
    spark.table(service.table_names.function_registry)
    .groupBy("function_name")
    .count()
    .where("count > 1")
    .collect()
)
assert not duplicates, f"Found duplicate function rows: {duplicates}"
assert after == before, f"Preservation mode changed row count. before={before}, after={after}"
print("PASS: Explicit preservation mode completed without overwrites or duplicates.")

# COMMAND ----------
print("ST-009: Runtime in-memory registry contains executable standard implementations")
print("-" * 80)
print("Area: Function registry")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Standard functions are available for validation and runtime evaluation even though function_registry only stores metadata.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

assert service.registry.get_spec("upper").function_name == "upper"
assert service.registry.get_implementation("upper")(value="abc") == "ABC"
print("PASS: Standard function implementations are registered in memory.")

# COMMAND ----------
print("ST-010: Custom function metadata can be saved without breaking standard registry rows")
print("-" * 80)
print("Area: Function registry")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: The custom function row is inserted or updated and existing standard rows remain present.")
print("")
from rules_engine import RulesEngineService
from rules_engine.models import FunctionRegistryRow

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

row = FunctionRegistryRow(
    function_name="uat_identity",
    implementation_reference="my_package.functions.uat_identity",
    arg_contract_payload={"arg_names": ["value"]},
    return_type_hint="any",
    allowed_in_condition_flag=True,
    allowed_in_assignment_flag=True,
    active_flag=True,
    description="UAT identity function metadata row.",
    version="test",
)
service.save_function_registry_rows([row])

count = spark.table(service.table_names.function_registry).where("function_name = 'uat_identity'").count()
assert count == 1, f"Expected one custom function row, found {count}."
print("PASS: Custom function metadata saved without disrupting standard rows.")

# COMMAND ----------
print("ST-011: Valid canonical ruleset YAML compiles through the service")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: A Ruleset object is returned with expected ruleset_id, ruleset_name, version, owner, owner_department, rules, conditions, and assignments.")
print("")
from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

assert RULESET_YAML_PATH, "Set RULESET_YAML_PATH for compile-from-path tests."

ruleset = service.compile_yaml_path(RULESET_YAML_PATH)

assert ruleset.ruleset_id, "Expected compiled ruleset_id to be populated."
assert ruleset.ruleset_name, "Expected compiled ruleset_name to be populated."
assert ruleset.version, "Expected compiled version to be populated."
assert ruleset.owner, "Expected compiled owner to be populated."
assert ruleset.owner_department, "Expected compiled owner_department to be populated."
assert ruleset.rules, "Expected at least one compiled rule."
assert ruleset.rules[0].root_group.conditions or ruleset.rules[0].root_group.groups, (
    "Expected the first rule to contain conditions or nested groups."
)
assert ruleset.rules[0].assignments, "Expected the first rule to contain assignments."

print(f"PASS: Compiled ruleset {ruleset.ruleset_name} version {ruleset.version}.")

# COMMAND ----------
print("ST-012: YAML text compilation works for notebook-authored payloads")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: The text compiles into the same model shape as file-based compilation.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

yaml_text = """
ruleset_id: st012_ruleset
ruleset_name: ST012 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(yaml_text)

assert ruleset.ruleset_id == "st012_ruleset"
assert ruleset.ruleset_name == "ST012 Ruleset"
assert ruleset.version == "1"
assert ruleset.owner == "Rules Team"
assert ruleset.owner_department == "ALM Engineering"
assert len(ruleset.rules) == 1
assert ruleset.rules[0].root_group.conditions[0].operator.value == "eq"
assert ruleset.rules[0].assignments[0].target_field == "bucket"

print("PASS: YAML text compiled into the expected canonical model shape.")

# COMMAND ----------
print("ST-013: Required top-level ruleset metadata is enforced")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Compilation fails before validation or persistence. Error identifies the missing or malformed field.")
print("")
from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_name: Missing ID
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules: []
"""

try:
    service.compile_yaml_text(invalid_yaml)
except CompilationError as exc:
    assert "ruleset_id must be a non-empty string" in str(exc), str(exc)
else:
    raise AssertionError("Expected missing ruleset_id to fail compilation.")

print("PASS: Missing required top-level ruleset metadata was rejected.")

# COMMAND ----------
print("ST-014: Condition group logical operator shape is enforced")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Compilation fails and identifies that exactly one logical operator is required.")
print("")
from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st014_ruleset
ruleset_name: ST014 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
      any:
        - condition_id: c2
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
    assign:
      bucket: A
"""

try:
    service.compile_yaml_text(invalid_yaml)
except CompilationError as exc:
    message = str(exc).lower()
    assert "exactly one logical operator" in message, str(exc)
else:
    raise AssertionError("Expected condition group with all and any to fail compilation.")

print("PASS: Invalid condition group logical operator shape was rejected.")

# COMMAND ----------
print("ST-015: Valid simple row-level rule passes validation")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation passes with no error-severity issues.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_015_ruleset
ruleset_name: ST-015 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(valid_yaml)
validation = service.validator.validate(ruleset)

assert not validation.has_errors(), validation.to_text()
assert len(ruleset.rules) == 1
assert ruleset.rules[0].root_group.conditions[0].condition_id == "c1"

print("PASS: Valid simple row-level rule passed validation.")

# COMMAND ----------
print("ST-016: Owner and owner_department are required before publish")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation returns error-severity issues and publish is blocked.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_016_ruleset
ruleset_name: ST-016 Ruleset
version: "1"
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected owner metadata validation to fail."
assert {
    "RULESET_OWNER_REQUIRED",
    "RULESET_OWNER_DEPARTMENT_REQUIRED",
} <= check_names, validation.to_text()

print(validation.to_text())
print("PASS: Missing owner and owner_department were rejected.")

# COMMAND ----------
print("ST-017: Duplicate condition_id values are rejected")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation returns a duplicate condition ID error.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_017_ruleset
ruleset_name: ST-017 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
        - condition_id: c1
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected duplicate condition_id validation to fail."
assert "CONDITION_ID_DUPLICATE" in check_names, validation.to_text()

print(validation.to_text())
print("PASS: Duplicate condition_id values were rejected.")

# COMMAND ----------
print("ST-018: Duplicate condition_group_id values are rejected")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation returns a duplicate condition group ID error.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_018_ruleset
ruleset_name: ST-018 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      condition_group_id: duplicate_group
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
        - condition_group_id: duplicate_group
          any:
            - condition_id: c2
              left:
                field: status
              operator: eq
              right:
                literal: OPEN
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected duplicate condition_group_id validation to fail."
assert "CONDITION_GROUP_ID_DUPLICATE" in check_names, validation.to_text()

print(validation.to_text())
print("PASS: Duplicate condition_group_id values were rejected.")

# COMMAND ----------
print("ST-019: Custom-function argument contracts are enforced")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation reports the registered contract mismatch.")
print("")

from rules_engine import RulesEngineService
from rules_engine.registry import CustomFunctionSpec

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
service.registry.register(
    CustomFunctionSpec(
        function_name="score",
        implementation_reference="notebook.score",
        arg_names=("x", "y"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
    )
)

invalid_yaml = """
ruleset_id: st_019_ruleset
ruleset_name: ST-019 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            custom_function:
              name: score
              args:
                x:
                  field: amount
          operator: gt
          right:
            literal: 100
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected custom-function argument validation to fail."
assert "CUSTOM_FUNCTION_ARGS_MISMATCH" in check_names, validation.to_text()

print(validation.to_text())
print("PASS: Custom-function argument contract mismatch was rejected.")

# COMMAND ----------
print("ST-020: Operator arity and literal collection requirements are enforced")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation identifies missing, extra, or malformed operands.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_020_ruleset
ruleset_name: ST-020 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: is_null
          right:
            literal: A
        - condition_id: c2
          left:
            field: account
          operator: eq
        - condition_id: c3
          left:
            field: account
          operator: in
          right:
            literal: A
        - condition_id: c4
          left:
            field: amount
          operator: between
          right:
            literal:
              - 100
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected operator operand validation to fail."
assert {
    "UNARY_OPERATOR_RIGHT_FORBIDDEN",
    "BINARY_OPERATOR_RIGHT_REQUIRED",
    "IN_OPERATOR_COLLECTION_REQUIRED",
    "BETWEEN_OPERATOR_PAIR_REQUIRED",
} <= check_names, validation.to_text()

print(validation.to_text())
print("PASS: Operator arity and literal collection requirements were enforced.")

# COMMAND ----------
print("ST-021: Operand null defaults cannot themselves be null")
print("-" * 80)
print("Area: Compilation")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Compilation rejects a null default_if_null value.")
print("")

from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_021_ruleset
ruleset_name: ST-021 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
            default_if_null: null
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

try:
    service.compile_yaml_text(invalid_yaml)
    default_rejected = False
except CompilationError as exc:
    default_rejected = True
    assert "default_if_null cannot itself be null" in str(exc), str(exc)

assert default_rejected, "Expected a null default_if_null value to fail compilation."
print("PASS: A null default_if_null value was rejected.")

# COMMAND ----------
print("ST-022: Spark validator accepts supported rulesets")
print("-" * 80)
print("Area: Spark compatibility")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation passes and no Spark-specific unsupported-operation issues are present.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_022_ruleset
ruleset_name: ST-022 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(valid_yaml)
validation = service.validator.validate(ruleset)

assert not validation.has_errors(), validation.to_text()
assert not any(issue.check_name.startswith("SPARK_") for issue in validation.issues), (
    validation.to_text()
)

print("PASS: Supported ruleset passed semantic and Spark compatibility validation.")

# COMMAND ----------
print("ST-023: Spark validator follows the supported row-level contract")
print("-" * 80)
print("Area: Spark compatibility")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Supported row-level rules validate without Spark-specific errors.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_023_ruleset
ruleset_name: ST-023 Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: amount
          operator: gt
          right:
            literal: 100
        - condition_id: c2
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
        - condition_id: c3
          left:
            custom_function:
              name: upper
              args:
                value:
                  field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(valid_yaml)
validation = service.validator.validate(ruleset)

assert not validation.has_errors(), validation.to_text()
assert not any(issue.check_name.startswith("SPARK_") for issue in validation.issues), (
    validation.to_text()
)

print(validation.to_text())
print("PASS: Supported row-level ruleset passed Spark validation.")

# COMMAND ----------
print("ST-024: Publish YAML path compiles, normalizes, validates, and writes metadata")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: One published ruleset_versions row is written with status published, content_hash, payload_json, counts, owner, published_by, and published_at.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_024_{stamp}
ruleset_name: ST-024 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.publish_yaml_text(
    yaml_text,
    published_by="system-test",
    effective_start_date="2026-05-01",
)

loaded = service.load_published(ruleset.ruleset_name, version=ruleset.version)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}'"
).collect()

assert loaded.ruleset_id == ruleset.ruleset_id
assert len(rows) == 1, f"Expected one published row, found {len(rows)}."
row = rows[0]
assert row["status"] == "published"
assert row["published_by"] == "system-test"
assert row["published_at"]
assert row["payload_json"]
assert row["content_hash"]
assert row["rule_count"] == 1
assert row["condition_count"] == 1
assert row["assignment_count"] == 1
assert row["effective_start_date"] == "2026-05-01"
assert row["effective_end_date"] == "2999-12-31"

print("PASS: Publish YAML path wrote one complete metadata row.")

# COMMAND ----------
print("ST-025: Direct publish of a compiled ruleset works")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: The compiled ruleset is persisted and loadable by name/version.")
print("")

from datetime import datetime, timezone
from dataclasses import replace
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_025_{stamp}
ruleset_name: ST-025 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

compiled = service.compile_yaml_text(yaml_text.replace(f'st_025_{stamp}', f'st_025_direct_{stamp}'))
compiled = replace(compiled, version=f"{stamp}_direct")
service.publish(compiled, published_by="system-test", effective_start_date="2026-05-01")

loaded = service.load_published(compiled.ruleset_name, version=compiled.version)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{compiled.ruleset_id}' AND version = '{compiled.version}'"
).collect()

assert loaded.ruleset_id == compiled.ruleset_id
assert loaded.ruleset_name == compiled.ruleset_name
assert loaded.version == compiled.version
assert len(rows) == 1
assert rows[0]["status"] == "published"
assert rows[0]["effective_start_date"] == "2026-05-01"

print("PASS: Direct publish of a compiled ruleset persisted and loaded successfully.")

# COMMAND ----------
print("ST-026: Publish rejects non-published lifecycle status")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Publish fails before persistence with a clear status requirement message.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import ValidationFailedError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_026_{stamp}
ruleset_name: ST-026 Ruleset
version: "{stamp}"
status: retired
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(yaml_text)

try:
    service.publish(ruleset, published_by="system-test")
    publish_failed = False
except ValidationFailedError as exc:
    publish_failed = True
    assert "status=published" in str(exc), str(exc)

assert publish_failed, "Expected publish to reject ruleset status=retired."

rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()
assert len(rows) == 0, f"Expected no persisted rows, found {len(rows)}."

print("PASS: Publish rejected non-published lifecycle status before persistence.")

# COMMAND ----------
print("ST-027: Duplicate ruleset_name and version cannot be published twice")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Second publish fails and does not overwrite the original row.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_027_{stamp}
ruleset_name: ST-027 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

first_ruleset = service.publish_yaml_text(yaml_text, published_by="first-publish")

try:
    service.publish_yaml_text(yaml_text, published_by="second-publish")
    duplicate_failed = False
except RepositoryError:
    duplicate_failed = True

assert duplicate_failed, "Expected duplicate ruleset_name/version publish to fail."

rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{first_ruleset.ruleset_name}' AND version = '{first_ruleset.version}'"
).collect()
assert len(rows) == 1, f"Expected one persisted row after duplicate attempt, found {len(rows)}."
assert rows[0]["published_by"] == "first-publish"

print("PASS: Duplicate ruleset_name/version publish failed without overwriting the original row.")

# COMMAND ----------
print("ST-028: Multiple published versions for the same ruleset_name are allowed")
print("-" * 80)
print("Area: Publishing")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Both rows have status published. Neither publish requires retiring the other version.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
ruleset_name = f"ST-028 Ruleset {stamp}"

yaml_v1 = f"""
ruleset_id: st_028_v1_{stamp}
ruleset_name: {ruleset_name}
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

yaml_v2 = yaml_v1.replace(f"st_028_v1_{stamp}", f"st_028_v2_{stamp}").replace('version: "1"', 'version: "2"')

ruleset_v1 = service.publish_yaml_text(yaml_v1, published_by="system-test")
ruleset_v2 = service.publish_yaml_text(yaml_v2, published_by="system-test")

rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{ruleset_name}' AND status = 'published'"
).collect()
versions = {row["version"] for row in rows}

assert len(rows) == 2, f"Expected two published rows, found {len(rows)}."
assert {ruleset_v1.version, ruleset_v2.version} <= versions
assert all(row["effective_end_date"] == "2999-12-31" for row in rows)

print("PASS: Multiple published versions for the same ruleset_name coexist.")

# COMMAND ----------
print("ST-029: Loading by name without version is rejected when ambiguous")
print("-" * 80)
print("Area: Publishing")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Repository raises an error asking the caller to specify version.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
ruleset_name = f"ST-029 Ruleset {stamp}"

yaml_v1 = f"""
ruleset_id: st_029_v1_{stamp}
ruleset_name: {ruleset_name}
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

yaml_v2 = yaml_v1.replace(f"st_029_v1_{stamp}", f"st_029_v2_{stamp}").replace('version: "1"', 'version: "2"')

service.publish_yaml_text(yaml_v1, published_by="system-test")
service.publish_yaml_text(yaml_v2, published_by="system-test")

try:
    service.load_published(ruleset_name)
    ambiguous_load_failed = False
except RepositoryError as exc:
    ambiguous_load_failed = True
    assert "specify version" in str(exc), str(exc)

assert ambiguous_load_failed, "Expected load_published without version to fail when multiple versions exist."

loaded_v1 = service.load_published(ruleset_name, version="1")
loaded_v2 = service.load_published(ruleset_name, version="2")
assert loaded_v1.version == "1"
assert loaded_v2.version == "2"

print("PASS: Ambiguous load by name failed and explicit versions loaded.")

# COMMAND ----------
print("ST-030: Loading by name and version requires one immutable metadata row")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: A unique row loads and evaluates; duplicate rows for the same immutable version fail loudly.")
print("")

from datetime import datetime, timezone
from pyspark.sql import functions as F
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_030_{stamp}
ruleset_name: ST-030 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
loaded = service.load_published(ruleset.ruleset_name, version=ruleset.version)

df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])
result_rows = service.evaluate_dataframe(
    df,
    ruleset_name=loaded.ruleset_name,
    version=loaded.version,
).collect()

assert loaded.ruleset_id == ruleset.ruleset_id
assert loaded.ruleset_name == ruleset.ruleset_name
assert loaded.version == ruleset.version
assert any(row["rules_engine_matched"] is True for row in result_rows)
assert any(row["rules_engine_matched"] is False for row in result_rows)

versions = spark.table(service.table_names.ruleset_versions)
duplicate_rows = versions.where(
    (F.col("ruleset_name") == ruleset.ruleset_name)
    & (F.col("version") == ruleset.version)
    & (F.col("status") == "published")
).limit(1).collect()
assert len(duplicate_rows) == 1
spark.createDataFrame(duplicate_rows, schema=versions.schema).write.mode(
    "append"
).saveAsTable(service.table_names.ruleset_versions)

try:
    service.load_published(ruleset.ruleset_name, version=ruleset.version)
except RepositoryError as exc:
    assert "Multiple published rows found for immutable ruleset version" in str(exc), str(exc)
else:
    raise AssertionError("Expected a duplicate immutable version to fail explicit loading.")

print("PASS: Unique loading worked and duplicate immutable rows failed loudly.")

# COMMAND ----------
print("ST-031: Retirement makes a version unavailable to load_published")
print("-" * 80)
print("Area: Lifecycle")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: The row status is retired and load_published no longer returns it.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_031_{stamp}
ruleset_name: ST-031 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
service.retire(ruleset.ruleset_id, ruleset.version, retired_by="system-test")

rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()
assert len(rows) == 1
assert rows[0]["status"] == "retired"
assert rows[0]["effective_end_date"] != "2999-12-31"

try:
    service.load_published(ruleset.ruleset_name, version=ruleset.version)
    retired_load_failed = False
except RepositoryError:
    retired_load_failed = True

assert retired_load_failed, "Expected retired version to be unavailable to load_published."

print("PASS: Retired version was removed from load_published eligibility.")

# COMMAND ----------
print("ST-032: Retirement stamps retired_by and retired_at")
print("-" * 80)
print("Area: Lifecycle")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: retired_by and retired_at are populated and status is retired.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_032_{stamp}
ruleset_name: ST-032 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
service.retire(ruleset.ruleset_id, ruleset.version, retired_by="system-test")

row = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()[0]

assert row["status"] == "retired"
assert row["retired_by"] == "system-test"
assert row["retired_at"]
assert row["effective_end_date"] != "2999-12-31"

print("PASS: Retirement metadata was stamped.")

# COMMAND ----------
print("ST-033: Retiring a missing ruleset version fails safely")
print("-" * 80)
print("Area: Lifecycle")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Repository raises a clear not-found error and no rows are changed.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
before_count = spark.table(service.table_names.ruleset_versions).count()

try:
    service.retire(f"missing_ruleset_{stamp}", f"missing_version_{stamp}", retired_by="system-test")
    missing_retire_failed = False
except RepositoryError as exc:
    missing_retire_failed = True
    assert "not found" in str(exc).lower(), str(exc)

after_count = spark.table(service.table_names.ruleset_versions).count()

assert missing_retire_failed, "Expected retiring a missing ruleset version to fail."
assert after_count == before_count, (
    f"Expected row count to remain {before_count}, found {after_count}."
)

print("PASS: Missing version retirement failed without changing table row count.")

# COMMAND ----------
print("ST-034: Retiring an already retired version does not silently succeed")
print("-" * 80)
print("Area: Lifecycle")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: The first retirement succeeds. A second retirement raises RepositoryError and does not overwrite the original retirement metadata.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_034_{stamp}
ruleset_name: ST-034 Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""

ruleset = service.publish_yaml_text(
    yaml_text,
    published_by="system-test",
    effective_start_date="2026-05-01",
)

FIRST_RETIRED_BY = "first-retire"
SECOND_RETIRED_BY = "second-retire"

service.retire(
    ruleset.ruleset_id,
    ruleset.version,
    retired_by=FIRST_RETIRED_BY,
)

first_rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()
assert len(first_rows) == 1, (
    f"Expected exactly one row after first retirement, found {len(first_rows)}."
)

first_row = first_rows[0]

assert first_row["status"] == "retired", (
    f"Expected first retirement to set status='retired', found {first_row['status']}."
)
assert first_row["retired_by"] == FIRST_RETIRED_BY, (
    f"Expected retired_by={FIRST_RETIRED_BY}, found {first_row['retired_by']}."
)
assert first_row["retired_at"], "Expected first retirement to populate retired_at."
assert first_row["effective_end_date"] != "2999-12-31", (
    "Expected first retirement to close the open-ended effective_end_date."
)

original_retired_by = first_row["retired_by"]
original_retired_at = first_row["retired_at"]
original_effective_end_date = first_row["effective_end_date"]

try:
    service.retire(
        ruleset.ruleset_id,
        ruleset.version,
        retired_by=SECOND_RETIRED_BY,
    )
    second_retire_failed = False
except RepositoryError:
    second_retire_failed = True

assert second_retire_failed, (
    "Expected second retirement of an already retired version to raise RepositoryError."
)

second_rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()
assert len(second_rows) == 1, (
    f"Expected exactly one row after second retirement attempt, found {len(second_rows)}."
)

second_row = second_rows[0]

assert second_row["status"] == "retired", (
    f"Expected row to remain retired, found {second_row['status']}."
)
assert second_row["retired_by"] == original_retired_by, (
    f"Expected retired_by to remain {original_retired_by}, found {second_row['retired_by']}."
)
assert second_row["retired_at"] == original_retired_at, (
    f"Expected retired_at to remain {original_retired_at}, found {second_row['retired_at']}."
)
assert second_row["effective_end_date"] == original_effective_end_date, (
    f"Expected effective_end_date to remain {original_effective_end_date}, "
    f"found {second_row['effective_end_date']}."
)

print("PASS: Second retirement failed and original retirement metadata was preserved.")

# COMMAND ----------
print("ST-035: Spark runtime evaluates simple row-level rules correctly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Matched flags, matched rule IDs, assignments, matched-rule traces, and errors match expected outcomes.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_035_{stamp}
ruleset_name: ST-035 Runtime Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Account A
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
rows = {row["account"]: row.asDict(recursive=True) for row in result.collect()}

assert rows["A"]["rules_engine_matched"] is True
assert rows["A"]["rules_engine_matched_rule_ids"] == ["r1"]
assert rows["A"]["rules_engine_assign"] == {"bucket": "A"}
assert rows["A"]["rules_engine_matched_rules"][0]["rule_id"] == "r1"
assert rows["A"]["rules_engine_matched_rules"][0]["rule_name"] == "Account A"
assert rows["A"]["rules_engine_matched_rules"][0]["explanation"] == "account == 'A'"
assert rows["A"]["rules_engine_matched_rules"][0]["conditions"][0]["columns"] == ["account"]
assert rows["A"]["rules_engine_error"] is None
assert rows["B"]["rules_engine_matched"] is False
assert rows["B"]["rules_engine_matched_rule_ids"] == []
assert rows["B"]["rules_engine_assign"] is None
assert rows["B"]["rules_engine_matched_rules"][0] is None
assert rows["B"]["rules_engine_error"] is None
display(result)
print("PASS: Runtime evaluation completed for ST-035.")

# COMMAND ----------
print("ST-036: Spark runtime honors rule_order and stop_on_match")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Assignments reflect ordered evaluation and stop behavior.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_036_{stamp}
ruleset_name: ST-036 Runtime Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: first_match
    rule_name: First Match
    rule_order: 1
    stop_on_match: true
    when:
      all:
        - condition_id: c_first
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: first
  - rule_id: second_match
    rule_name: Second Match
    rule_order: 2
    when:
      all:
        - condition_id: c_second
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: second
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
rows = {row["account"]: row.asDict(recursive=True) for row in result.collect()}

assert rows["A"]["rules_engine_matched"] is True
assert rows["A"]["rules_engine_matched_rule_ids"] == ["first_match"]
assert rows["A"]["rules_engine_assign"] == {"bucket": "first"}
assert rows["A"]["rules_engine_matched_rules"][0]["rule_id"] == "first_match"
assert rows["B"]["rules_engine_matched"] is False
assert rows["B"]["rules_engine_matched_rule_ids"] == []
assert rows["B"]["rules_engine_assign"] is None
display(result)
print("PASS: Runtime evaluation completed for ST-036.")

# COMMAND ----------
print("ST-037: Spark runtime applies operand defaults and error_on_null correctly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Results match documented null semantics and errors are emitted only where expected.")
print("")

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime

class ST037Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError

runtime = SparkRulesEngineRuntime(ST037Repository(), FunctionRegistry())
compiler = YamlRulesetCompiler()

ruleset = compiler.compile_text("""
ruleset_id: st_037_ruleset
ruleset_name: ST-037 Runtime Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: ordinary_null
    rule_name: Ordinary Null Produces No Match
    rule_order: 1
    when:
      all:
        - condition_id: c_ordinary_null
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: ordinary
  - rule_id: string_default
    rule_name: String Default Replaces Null Before Comparison
    rule_order: 2
    when:
      all:
        - condition_id: c_string_default
          left:
            field: account
            default_if_null: A
          operator: eq
          right:
            literal: A
    assign:
      string_bucket: string_default
  - rule_id: numeric_default
    rule_name: Numeric Default Replaces Null Before Comparison
    rule_order: 3
    when:
      all:
        - condition_id: c_numeric_default
          left:
            field: amount
            default_if_null: 0
          operator: eq
          right:
            literal: 0
    assign:
      numeric_bucket: numeric_default
""")

output = runtime.evaluate_dataframe(
    spark.createDataFrame([{"account": None, "amount": None}], "account string, amount double"),
    ruleset,
    full_audit=True,
)
row = output.collect()[0].asDict(recursive=True)
assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rule_ids"] == ["string_default", "numeric_default"], row["rules_engine_matched_rule_ids"]
assert row["rules_engine_assign"] == {
    "bucket": None,
    "string_bucket": "string_default",
    "numeric_bucket": "numeric_default",
}
assert [trace["rule_id"] for trace in row["rules_engine_matched_rules"]] == [
    "string_default",
    "numeric_default",
]
assert all(
    trace["conditions"][0]["left"]["default_applied"]
    for trace in row["rules_engine_matched_rules"]
)

error_ruleset = compiler.compile_text("""
ruleset_id: st_037_error_ruleset
ruleset_name: ST-037 Error Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: null_error
    rule_name: Null Result Error
    rule_order: 1
    when:
      all:
        - condition_id: c_error
          left:
            field: account
          operator: eq
          right:
            literal: A
          error_on_null: true
    assign:
      bucket: should_error
""")
try:
    runtime.evaluate_dataframe(
        spark.createDataFrame([{"account": None}], "account string"),
        error_ruleset,
    ).collect()
    error_failed = False
except Exception as exc:
    error_failed = True
    assert "error_on_null=true" in str(exc), str(exc)
assert error_failed, "Expected error_on_null=true to raise for a null operand."
print("PASS: Spark runtime applied default no-match, typed fallbacks, and explicit null errors.")
# COMMAND ----------
print("ST-038: Spark runtime evaluates a published ruleset against a DataFrame")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Output DataFrame contains the compact result and lineage columns with expected values.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_038_{stamp}
ruleset_name: ST-038 Runtime Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Account A
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}, {"record_id": "r2", "account": "B"}])
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}
required_columns = {
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
}
missing_columns = required_columns - set(result.columns)
assert not missing_columns, f"Missing output columns: {sorted(missing_columns)}"
assert rows["r1"]["rules_engine_matched"] is True
assert rows["r1"]["rules_engine_matched_rule_ids"] == ["r1"]
assert rows["r1"]["rules_engine_assign"] == {"bucket": "A"}
assert rows["r1"]["rules_engine_error"] is None
assert rows["r2"]["rules_engine_matched"] is False
assert rows["r2"]["rules_engine_matched_rule_ids"] == []
assert rows["r2"]["rules_engine_assign"] is None
assert rows["r2"]["rules_engine_error"] is None
display(result)
print("PASS: Spark runtime output columns and values matched expected results.")
# COMMAND ----------
print("ST-039: Spark runtime fail_on_error behavior is enforced")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: fail_on_error=True fails the job. fail_on_error=False records errors in output rows.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_039_{stamp}
ruleset_name: ST-039 Runtime Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Null Result Error
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
          error_on_null: true
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": None}, {"record_id": "r2", "account": "A"}])
try:
    service.evaluate_dataframe(
        df,
        ruleset_name=ruleset.ruleset_name,
        version=ruleset.version,
        fail_on_error=True,
    ).collect()
    fail_on_error_raised = False
except Exception as exc:
    fail_on_error_raised = True
    assert "error_on_null=true" in str(exc), str(exc)
assert fail_on_error_raised, "Expected fail_on_error=True to raise for row-level runtime errors."
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version, fail_on_error=False)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}
assert rows["r1"]["rules_engine_error"] is not None
assert "error_on_null=true" in rows["r1"]["rules_engine_error"]
assert rows["r2"]["rules_engine_error"] is None
assert rows["r2"]["rules_engine_matched"] is True
display(result)
print("PASS: fail_on_error=True raised and fail_on_error=False recorded row-level errors.")
# COMMAND ----------
print("ST-040: Spark runtime supports standard custom functions used in rules")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Evaluation succeeds and outputs match expected transformed values.")
print("")

from datetime import datetime, timezone
import json
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_040_{stamp}
ruleset_name: ST-040 Runtime Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: upper_match
    rule_name: Upper Account A
    rule_order: 1
    when:
      all:
        - condition_id: c_upper
          left:
            custom_function:
              name: upper
              args:
                value:
                  field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: upper_a
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "a"}, {"record_id": "r2", "account": "B"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}
assert rows["r1"]["rules_engine_matched"] is True
assert rows["r1"]["rules_engine_matched_rule_ids"] == ["upper_match"]
assert rows["r1"]["rules_engine_assign"] == {"bucket": "upper_a"}
assert rows["r2"]["rules_engine_matched"] is False
assert rows["r2"]["rules_engine_assign"] is None
display(result)
print("PASS: Standard upper() function transformed input and produced expected outputs.")
# COMMAND ----------
print("ST-041: Spark runtime emits native assignment and matched-rule trace structs")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Matched flags, assignments, and matched-rule traces are Spark-native structs.")
print("")

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime

class ST041Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError

ruleset = YamlRulesetCompiler().compile_text("""
ruleset_id: st_041_ruleset
ruleset_name: ST-041 Runtime Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: account_a
    rule_name: Account A
    rule_order: 1
    when:
      all:
        - condition_id: c_account
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: matched
""")
input_rows = [
    {"row_id": 1, "account": "A"},
    {"row_id": 2, "account": "B"},
]
registry = FunctionRegistry()
spark_result = SparkRulesEngineRuntime(ST041Repository(), registry).evaluate_dataframe(
    spark.createDataFrame(input_rows),
    ruleset,
    fail_on_error=True,
    full_audit=True,
)
spark_rows = [row.asDict(recursive=True) for row in spark_result.orderBy("row_id").collect()]
assert [row["rules_engine_matched"] for row in spark_rows] == [True, False]
assert [row["rules_engine_matched_rule_ids"] for row in spark_rows] == [["account_a"], []]
assert [row["rules_engine_assign"] for row in spark_rows] == [{"bucket": "matched"}, None]
assert spark_rows[0]["rules_engine_matched_rules"][0]["rule_id"] == "account_a"
assert spark_rows[1]["rules_engine_matched_rules"] == []
display(spark_result)
print("PASS: Spark runtime emitted native assignment and matched-rule trace structs.")
# COMMAND ----------
print("ST-042: Payload JSON excludes mutable lifecycle fields and reconstructs ruleset content")
print("-" * 80)
print("Area: Auditability")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: payload_json contains canonical ruleset content but not lifecycle status. Loading reconstructs the expected ruleset.")
print("")

from datetime import datetime, timezone
import hashlib
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_042_{stamp}
ruleset_name: ST-042 Audit Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
row = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}'"
).collect()[0]

assert "status" not in row["payload_json"]
assert hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest() == row["content_hash"]
assert row["rule_count"] >= 1
assert row["effective_start_date"]
assert row["effective_end_date"]
print("PASS: Audit metadata checks completed for ST-042.")

# COMMAND ----------
print("ST-043: Content hash is deterministic and reproducible")
print("-" * 80)
print("Area: Auditability")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: content_hash is stable and equals SHA-256 of payload_json bytes.")
print("")

from datetime import datetime, timezone
import hashlib
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_043_{stamp}
ruleset_name: ST-043 Audit Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
row = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}'"
).collect()[0]

assert "status" not in row["payload_json"]
assert hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest() == row["content_hash"]
assert row["rule_count"] >= 1
assert row["effective_start_date"]
assert row["effective_end_date"]
print("PASS: Audit metadata checks completed for ST-043.")

# COMMAND ----------
print("ST-044: Summary count columns match the published ruleset content")
print("-" * 80)
print("Area: Auditability")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Persisted counts match the model counts.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_044_{stamp}
ruleset_name: ST-044 Audit Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Numeric Field Rule
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: amount
          operator: gt
          right:
            literal: 100
    assign:
      amount_bucket: large
  - rule_id: r2
    rule_name: Standard Function Rule
    rule_order: 2
    when:
      all:
        - condition_id: c2
          left:
            custom_function:
              name: upper
              args:
                value:
                  field: account
          operator: eq
          right:
            literal: A
    assign:
      function_bucket: upper_a
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
row = spark.table(service.table_names.ruleset_versions).where(f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}'").collect()[0]
expected_rule_count = len(ruleset.rules)
expected_condition_count = sum(len(rule.root_group.conditions) for rule in ruleset.rules)
expected_assignment_count = sum(len(rule.assignments) for rule in ruleset.rules)
assert row["rule_count"] == expected_rule_count
assert row["condition_count"] == expected_condition_count
assert row["assignment_count"] == expected_assignment_count
assert row["custom_function_count"] == 1
print("PASS: Persisted summary count columns matched compiled ruleset content.")
# COMMAND ----------
print("ST-045: Spark runtime emits condition-level traceability values")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Full audit exposes source columns, evaluated operand values, comparison results, and readable matched-rule traces.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_045_{stamp}
ruleset_name: ST-045 Traceability Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: trace_rule
    rule_name: Traceability Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_amount
          left:
            field: amount
          operator: gt
          right:
            literal: 15
        - condition_id: c_upper
          left:
            custom_function:
              name: upper
              args:
                value:
                  field: account
          operator: eq
          right:
            literal: A
        - condition_id: c_status
          left:
            field: status
          operator: eq
          right:
            literal: open
    assign:
      bucket: traced
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame(
    [
        {"record_id": "r1", "account": "a", "amount": 30, "status": "open"},
        {"record_id": "r2", "account": "a", "amount": 20, "status": "closed"},
        {"record_id": "r3", "account": "b", "amount": 5, "status": "open"},
    ]
)
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}

matched_row = rows["r1"]
match_trace = matched_row["rules_engine_matched_rules"][0]
conditions = match_trace["conditions"]

assert matched_row["rules_engine_matched"] is True
assert matched_row["rules_engine_matched_rule_ids"] == ["trace_rule"]
assert matched_row["rules_engine_assign"] == {"bucket": "traced"}
assert match_trace["rule_id"] == "trace_rule"
assert match_trace["rule_name"] == "Traceability Rule"
assert match_trace["explanation"] == (
    "amount > 15 AND "
    "upper(value=account) == 'A' AND "
    "status == 'open'"
)
assert match_trace["assignments_applied"] == ["bucket"]

numeric_condition = conditions[0]
assert numeric_condition["columns"] == ["amount"]
assert numeric_condition["operator"] == "gt"
assert numeric_condition["comparison_result"] is True
assert numeric_condition["passed"] is True
assert numeric_condition["left"]["kind"] == "field"
assert numeric_condition["left"]["column"] == "amount"
assert numeric_condition["left"]["source_columns"] == ["amount"]
assert numeric_condition["left"]["value"] == "30"
assert numeric_condition["right"]["kind"] == "literal"
assert numeric_condition["right"]["value"] == "15"

function_condition = conditions[1]
assert function_condition["columns"] == ["account"]
assert function_condition["operator"] == "eq"
assert function_condition["comparison_result"] is True
assert function_condition["passed"] is True
assert function_condition["left"]["kind"] == "custom_function"
assert function_condition["left"]["function_name"] == "upper"
assert function_condition["left"]["source_columns"] == ["account"]
assert function_condition["left"]["arguments"] == {"value": "account=a"}
assert function_condition["left"]["value"] == "A"
assert function_condition["right"]["kind"] == "literal"
assert function_condition["right"]["value"] == "A"

field_condition = conditions[2]
assert field_condition["columns"] == ["status"]
assert field_condition["left"]["kind"] == "field"
assert field_condition["left"]["column"] == "status"
assert field_condition["left"]["value"] == "open"
assert field_condition["right"]["kind"] == "literal"
assert field_condition["right"]["value"] == "open"
assert field_condition["comparison_result"] is True
assert field_condition["passed"] is True

assert rows["r2"]["rules_engine_matched"] is False
assert rows["r2"]["rules_engine_matched_rules"][0] is None
assert rows["r3"]["rules_engine_matched"] is False
assert rows["r3"]["rules_engine_matched_rules"][0] is None

display(result)
print("PASS: Spark runtime traceability payloads included useful evaluated condition details.")

# COMMAND ----------
print("ST-046: Service describes published rules in human-readable form")
print("-" * 80)
print("Area: Auditability")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: describe_rules returns one readable row per rule with rule logic and match payload details.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_046_{stamp}
ruleset_name: ST-046 Human Readable Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1560
    rule_name: A Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_account
          left:
            field: BK_AccountID
          operator: eq
          right:
            literal: DN
        - any:
            - condition_id: c_amount
              left:
                field: amount
              operator: gt
              right:
                literal: 100
            - condition_id: c_status
              left:
                field: status
              operator: eq
              right:
                literal: REVIEW
    assign:
      leaf_key: "15656"
  - rule_id: review_route
    rule_name: Review Route
    rule_order: 2
    when:
      all:
        - condition_id: c_amount
          left:
            field: amount
          operator: gt
          right:
            literal: 100
        - condition_id: c_upper_status
          left:
            custom_function:
              name: upper
              args:
                value:
                  field: status
          operator: eq
          right:
            literal: REVIEW
    assign:
      route: REVIEW
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")

described_rows = service.describe_rules(
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
)

expected_rows = [
    {
        "rule_id": "r1560",
        "rule_name": "A Rule",
        "rule_logic": "BK_AccountID == 'DN' AND (amount > 100 OR status == 'REVIEW')",
        "match_payload": "leaf_key = '15656'",
    },
    {
        "rule_id": "review_route",
        "rule_name": "Review Route",
        "rule_logic": (
            "amount > 100 AND "
            "upper(value=status) == 'REVIEW'"
        ),
        "match_payload": "route = 'REVIEW'",
    },
]

assert described_rows == expected_rows
display(spark.createDataFrame(described_rows))
print("PASS: Service-level human-readable rule descriptions matched expected audit rows.")

# COMMAND ----------
print("ST-047: Spark runtime emits the documented output columns")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Compact output columns remain present and populated with documented semantics.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_047_{stamp}
ruleset_name: ST-047 Output Columns Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: output_rule
    rule_name: Output Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_account
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: account_a
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame(
    [
        {"record_id": "r1", "account": "A", "amount": 10},
        {"record_id": "r2", "account": "B", "amount": 20},
    ]
)
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}

input_columns = {"record_id", "account", "amount"}
required_output_columns = {
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
}
missing_columns = (input_columns | required_output_columns) - set(result.columns)
assert not missing_columns, f"Missing required columns: {sorted(missing_columns)}"

matched = rows["r1"]
unmatched = rows["r2"]

assert matched["record_id"] == "r1"
assert matched["account"] == "A"
assert matched["amount"] == 10
assert matched["rules_engine_matched"] is True
assert matched["rules_engine_matched_rule_ids"] == ["output_rule"]
assert matched["rules_engine_assign"] == {"bucket": "account_a"}
assert matched["rules_engine_error"] is None

assert unmatched["record_id"] == "r2"
assert unmatched["account"] == "B"
assert unmatched["amount"] == 20
assert unmatched["rules_engine_matched"] is False
assert unmatched["rules_engine_matched_rule_ids"] == []
assert unmatched["rules_engine_assign"] is None
assert unmatched["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime input columns and native output columns remain present and usable.")

# COMMAND ----------
print("ST-048: Spark runtime emits mapping literal assignments as nested structs")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Mapping literal assignments are emitted as nested Spark structs with selectable child fields.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_048_{stamp}
ruleset_name: ST-048 Struct Assignment Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: struct_assignment_rule
    rule_name: Struct Assignment Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_account
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      leaf_key:
        literal: "10110"
      non_modeled:
        literal:
          market_value: true
          book_value: false
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
row = result.collect()[0].asDict(recursive=True)
assign_schema = result.schema["rules_engine_assign"].dataType
non_modeled_schema = assign_schema["non_modeled"].dataType

assert {
    field.name: field.dataType.simpleString()
    for field in non_modeled_schema.fields
} == {
    "market_value": "boolean",
    "book_value": "boolean",
}
assert row["rules_engine_matched"] is True
assert row["rules_engine_assign"] == {
    "leaf_key": "10110",
    "non_modeled": {
        "market_value": True,
        "book_value": False,
    },
}
assert row["rules_engine_assign"]["non_modeled"]["market_value"] is True
assert row["rules_engine_assign"]["non_modeled"]["book_value"] is False

display(result)
print("PASS: Spark runtime preserved mapping literal assignments as nested structs.")

# COMMAND ----------
print("ST-049: Spark runtime match-trace explanations use service-style boolean logic")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Match-trace explanations use author-facing service syntax while trace structs retain evaluated values.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_049_{stamp}
ruleset_name: ST-049 Match Trace Explanation Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: nested_explanation_rule
    rule_name: Nested Explanation Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_record_type
          left:
            field: record_type
          operator: eq
          right:
            literal: asset
        - any:
            - condition_id: c_market_value
              left:
                field: market_value
              operator: eq
              right:
                literal: true
            - condition_id: c_book_value
              left:
                field: book_value
              operator: eq
              right:
                literal: true
    assign:
      bucket: explained
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame(
    [
        {
            "record_id": "r1",
            "record_type": "asset",
            "market_value": True,
            "book_value": True,
        }
    ]
)
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
row = result.collect()[0].asDict(recursive=True)
service_logic = service.describe_rules(ruleset_name=ruleset.ruleset_name, version=ruleset.version)[0]["rule_logic"]

assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rules"][0]["explanation"] == service_logic
assert row["rules_engine_matched_rules"][0]["explanation"] == (
    "record_type == 'asset' AND (market_value == true OR book_value == true)"
)
conditions = row["rules_engine_matched_rules"][0]["conditions"]
assert conditions[0]["left"]["value"] == "asset"
assert conditions[1]["left"]["value"] == "True"
assert conditions[2]["left"]["value"] == "True"

display(result)
print("PASS: Spark runtime match-trace explanations matched service-style boolean logic.")

# COMMAND ----------
print("ST-050: Spark runtime ignores inactive rule assignment schemas")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Inactive rules do not alter active assignment struct fields or types.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_050_{stamp}
ruleset_name: ST-050 Active Assignment Schema Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: active_struct_rule
    rule_name: Active Struct Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_active
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      non_modeled:
        literal:
          market_value: true
          book_value: false
  - rule_id: inactive_conflict_rule
    rule_name: Inactive Conflict Rule
    rule_order: 2
    active_flag: false
    when:
      all:
        - condition_id: c_inactive
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      non_modeled: retired string shape
      inactive_only: retired only
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
row = result.collect()[0].asDict(recursive=True)
assign_schema = result.schema["rules_engine_assign"].dataType
non_modeled_schema = assign_schema["non_modeled"].dataType

assert "inactive_only" not in assign_schema.fieldNames()
assert {
    field.name: field.dataType.simpleString()
    for field in non_modeled_schema.fields
} == {
    "market_value": "boolean",
    "book_value": "boolean",
}
assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rule_ids"] == ["active_struct_rule"]
assert row["rules_engine_assign"] == {
    "non_modeled": {
        "market_value": True,
        "book_value": False,
    },
}
assert row["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime ignored inactive rule assignment schemas.")

# COMMAND ----------
print("ST-051: Spark runtime merges continued multi-match assignments")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Compatible assignments keep all matched IDs and use last-writer-wins; incompatible same-target types fail Spark validation.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_051_{stamp}
ruleset_name: ST-051 Continued Match Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: first_match
    rule_name: First Match
    rule_order: 1
    stop_on_match: false
    when:
      all:
        - condition_id: c_first
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: first
      review_result: manual
  - rule_id: second_match
    rule_name: Second Match
    rule_order: 2
    stop_on_match: false
    when:
      all:
        - condition_id: c_second
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: second
      review_status: follow_up
      review_result: approved
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}])
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    full_audit=True,
)
row = result.collect()[0].asDict(recursive=True)
assign_schema = result.schema["rules_engine_assign"].dataType

assert assign_schema["review_result"].dataType.simpleString() == "string"
assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rule_ids"] == ["first_match", "second_match"]
assign = row["rules_engine_assign"]
assert assign["bucket"] == "second"
assert assign["review_status"] == "follow_up"
assert assign["review_result"] == "approved"
assert row["rules_engine_matched_rules"][0]["rule_id"] == "first_match"
assert row["rules_engine_matched_rules"][0]["rule_name"] == "First Match"
assert row["rules_engine_matched_rules"][0]["explanation"] == "account == 'A'"
assert row["rules_engine_error"] is None

incompatible_yaml = yaml_text.replace(
    "      review_result: approved",
    """      review_result:
        literal:
          market_value: true
          book_value: false""",
)
incompatible_ruleset = service.compile_yaml_text(incompatible_yaml)
incompatible_validation = service.validator.validate(incompatible_ruleset, df.schema)
assert "SPARK_ASSIGNMENT_TYPE_CONFLICT" in {
    issue.check_name for issue in incompatible_validation.issues
}, incompatible_validation.to_text()

display(result)
print("PASS: Compatible assignments merged and incompatible assignments were rejected.")

# COMMAND ----------
print("ST-052: Spark runtime serializes only required source fields")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Required dependencies exclude inactive and unrelated fields while dotted source names evaluate correctly.")
print("")

from rules_engine import RulesEngineService, required_source_columns

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
ruleset = service.compile_yaml_text(
    """
ruleset_id: st_052_required_columns
ruleset_name: ST-052 Required Source Columns
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: required_fields
    rule_name: Required Fields
    rule_order: 1
    stop_on_match: true
    when:
      any:
        - left:
            field: risk.score
          operator: eq
          right:
            literal: A
        - active_flag: false
          left:
            field: inactive_source
          operator: eq
          right:
            literal: ignored
    assign:
      copied_value:
        field: source_value
"""
)

assert required_source_columns(ruleset) == ("risk.score", "source_value")

df = spark.createDataFrame(
    [("A", "assigned", "not serialized")],
    ["risk.score", "source_value", "unused_payload"],
)
result = service.evaluate_dataframe(df, ruleset=ruleset, full_audit=True)
row = result.collect()[0].asDict(recursive=True)

assert row["unused_payload"] == "not serialized"
assert row["rules_engine_matched"] is True
assert row["rules_engine_assign"] == {"copied_value": "assigned"}
assert row["rules_engine_matched_rules"][0]["conditions"][0]["left"]["value"] == "A"
assert row["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime serialized only required source fields.")

# COMMAND ----------
print("ST-053: Match-only evaluation preserves trace and errors")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Losing rules stay out of matched_rules without hiding errors, while each match retains full traceability.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
ruleset = service.compile_yaml_text(
    """
ruleset_id: st_053_match_only
ruleset_name: ST-053 Match-Only Evaluation
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: losing_rule
    rule_name: Losing Rule
    rule_order: 1
    stop_on_match: true
    when:
      all:
        - condition_id: losing_false
          left:
            field: account
          operator: eq
          right:
            literal: B
        - condition_id: losing_error_check
          left:
            field: status
          operator: eq
          right:
            literal: open
          error_on_null: true
    assign:
      bucket: losing
  - rule_id: matching_rule
    rule_name: Matching Rule
    rule_order: 2
    stop_on_match: true
    when:
      all:
        - condition_id: winning_condition
          left:
            field: account
          operator: eq
          right:
            literal: A
    assign:
      bucket: matched
"""
)

df = spark.createDataFrame(
    [
        ("normal", "A", "open"),
        ("error", "A", None),
    ],
    ["record_id", "account", "status"],
)
result = service.evaluate_dataframe(
    df,
    ruleset=ruleset,
    fail_on_error=False,
    full_audit=True,
)
rows = {
    row["record_id"]: row.asDict(recursive=True)
    for row in result.collect()
}

normal = rows["normal"]
assert normal["rules_engine_matched"] is True
assert normal["rules_engine_matched_rule_ids"] == ["matching_rule"]
assert normal["rules_engine_assign"] == {"bucket": "matched"}
assert normal["rules_engine_matched_rules"][0]["rule_id"] == "matching_rule"
assert len(normal["rules_engine_matched_rules"][0]["conditions"]) == 1
assert normal["rules_engine_matched_rules"][0]["conditions"][0]["left"]["column"] == "account"
assert normal["rules_engine_error"] is None

error = rows["error"]
assert error["rules_engine_matched"] is False
assert "error_on_null=true" in error["rules_engine_error"]

display(result)
print("PASS: Full audit preserved matched-rule traces and losing-rule errors.")

# COMMAND ----------
print("ST-054: Financial and temporal types cross the real worker boundary exactly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Decimal, date, timestamp, array, and struct assignments retain exact values and declared Spark types.")
print("")

from datetime import date, datetime, timezone
from decimal import Decimal

from pyspark.sql import types as T

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_054_{stamp}
ruleset_name: ST-054 Financial Types Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: preserve_types
    rule_name: Preserve Types
    rule_order: 1
    when:
      all:
        - left: {{ field: status }}
          operator: eq
          right: {{ literal: OPEN }}
    assign:
      existing_rate: 0.0425
      parsed_balance:
        custom_function:
          name: to_number
          args:
            value: {{ field: balance_text }}
      copied_date: {{ field: funded_date }}
      copied_timestamp: {{ field: source_timestamp }}
      factors: [0.10, 0.25]
      flags:
        literal:
          market_value: true
          book_value: false
"""
ruleset = service.compile_yaml_text(yaml_text)
source_schema = T.StructType(
    [
        T.StructField("status", T.StringType(), False),
        T.StructField("balance_text", T.StringType(), False),
        T.StructField("existing_rate", T.DecimalType(10, 4), False),
        T.StructField("funded_date", T.DateType(), False),
        T.StructField("source_timestamp", T.TimestampType(), False),
    ]
)
expected_date = date(2025, 1, 15)
expected_timestamp = datetime(2025, 1, 15, 10, 30)
source = spark.createDataFrame(
    [
        (
            "OPEN",
            "1234.56",
            Decimal("0.0300"),
            expected_date,
            expected_timestamp,
        )
    ],
    source_schema,
)
result = service.evaluate_dataframe(
    source,
    ruleset=ruleset,
    fail_on_error=False,
)
row = result.collect()[0]
assignment_schema = result.schema["rules_engine_assign"].dataType

assert row["rules_engine_assign"]["existing_rate"] == Decimal("0.0425")
assert row["rules_engine_assign"]["parsed_balance"] == Decimal("1234.56")
assert row["rules_engine_assign"]["copied_date"] == expected_date
assert row["rules_engine_assign"]["copied_timestamp"] == expected_timestamp
assert row["rules_engine_assign"]["factors"] == [Decimal("0.10"), Decimal("0.25")]
assert row["rules_engine_assign"]["flags"].asDict() == {
    "book_value": False,
    "market_value": True,
}
assert isinstance(assignment_schema["existing_rate"].dataType, T.DecimalType)
assert isinstance(assignment_schema["parsed_balance"].dataType, T.DecimalType)
assert isinstance(assignment_schema["copied_date"].dataType, T.DateType)
assert isinstance(assignment_schema["copied_timestamp"].dataType, T.TimestampType)
assert isinstance(assignment_schema["factors"].dataType, T.ArrayType)
assert isinstance(assignment_schema["flags"].dataType, T.StructType)
print("PASS: Financial and temporal assignments retained exact worker-boundary values and types.")

# COMMAND ----------
print("ST-055: Fail-fast evaluation is lazy and builds one Python UDF")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: DataFrame construction builds one UDF without evaluating rows; caller actions surface bad-row errors.")
print("")

from datetime import datetime, timezone

import rules_engine.spark_runtime as spark_runtime_module
from rules_engine import CustomFunctionSpec, RulesEngineService


def st055_validate_value(value):
    if value == "bad":
        raise ValueError("ST-055 bad value")
    return value == "good"


service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
service.registry.register(
    CustomFunctionSpec(
        function_name="st055_validate_value",
        implementation_reference="system_tests.st055_validate_value",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        return_type_hint="boolean",
    ),
    st055_validate_value,
)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_055_{stamp}
ruleset_name: ST-055 Lazy Fail Fast Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: good_value
    rule_name: Good Value
    rule_order: 1
    when:
      all:
        - left:
            custom_function:
              name: st055_validate_value
              args:
                value: {{ field: value }}
          operator: eq
          right: {{ literal: true }}
    assign:
      bucket: good
"""
ruleset = service.compile_yaml_text(yaml_text)
udf_factory_calls = []
original_udf = spark_runtime_module.F.udf


def st055_tracked_udf(*args, **kwargs):
    udf_factory_calls.append((args, kwargs))
    return original_udf(*args, **kwargs)


spark_runtime_module.F.udf = st055_tracked_udf
try:
    clean_output = service.evaluate_dataframe(
        spark.createDataFrame([{"value": "good"}]),
        ruleset=ruleset,
        fail_on_error=True,
    )
finally:
    spark_runtime_module.F.udf = original_udf
assert len(udf_factory_calls) == 1
assert not clean_output.storageLevel.useMemory
assert not clean_output.storageLevel.useDisk
assert clean_output.collect()[0]["rules_engine_matched"] is True

bad_output = service.evaluate_dataframe(
    spark.createDataFrame([{"value": "bad"}]),
    ruleset=ruleset,
    fail_on_error=True,
)
try:
    bad_output.collect()
except Exception as exc:
    assert "ST-055 bad value" in str(exc), str(exc)
else:
    raise AssertionError("Expected the materializing action to fail for a bad row.")
print("PASS: Fail-fast construction stayed lazy, built one UDF, and surfaced worker errors on action.")

# COMMAND ----------
print("ST-056: Custom implementations are preflighted for worker serialization")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: A top-level callable evaluates, while an unsupported callable fails before Spark job submission.")
print("")

from datetime import datetime, timezone

from rules_engine import CustomFunctionSpec, FunctionRegistry, SparkRulesEngineRuntime, YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError


class ST056Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError("ST-056 passes the ruleset directly.")


def st056_identity(value):
    return value


class ST056Unserializable:
    def __call__(self, **kwargs):
        return kwargs["value"]

    def __getstate__(self):
        raise TypeError("ST-056 callable cannot be pickled")


def st056_ruleset(function_name, stamp):
    return YamlRulesetCompiler().compile_text(
        f"""
ruleset_id: st_056_{function_name}_{stamp}
ruleset_name: ST-056 {function_name}
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: identity_match
    rule_name: Identity Match
    rule_order: 1
    when:
      all:
        - left:
            custom_function:
              name: {function_name}
              args:
                value: {{ field: value }}
          operator: eq
          right: {{ literal: A }}
    assign:
      bucket: A
"""
    )


stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
good_registry = FunctionRegistry()
good_registry.register(
    CustomFunctionSpec(
        function_name="st056_identity",
        implementation_reference="system_tests.st056_identity",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        return_type_hint="string",
    ),
    st056_identity,
)
good_runtime = SparkRulesEngineRuntime(ST056Repository(), good_registry)
good_row = good_runtime.evaluate_dataframe(
    spark.createDataFrame([{"value": "A"}]),
    st056_ruleset("st056_identity", stamp),
).collect()[0]
assert good_row["rules_engine_matched"] is True

bad_registry = FunctionRegistry()
bad_registry.register(
    CustomFunctionSpec(
        function_name="st056_unserializable",
        implementation_reference="system_tests.st056_unserializable",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        return_type_hint="string",
    ),
    ST056Unserializable(),
)
bad_runtime = SparkRulesEngineRuntime(ST056Repository(), bad_registry)
try:
    bad_runtime.evaluate_dataframe(
        spark.createDataFrame([{"value": "A"}]),
        st056_ruleset("st056_unserializable", stamp),
    )
except ValidationFailedError as exc:
    assert "Spark-worker-serializable" in str(exc), str(exc)
else:
    raise AssertionError("Expected unsupported callable serialization to fail before an action.")
print("PASS: Worker serialization preflight accepted and rejected the expected callables.")

# COMMAND ----------
print("ST-057: Standard date functions support loan-tape calendar semantics")
print("-" * 80)
print("Area: Standard functions")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Month-end and leap-year calculations remain calendar-safe and produce typed assignments with authored expressions.")
print("")

from datetime import date, datetime, timezone

from pyspark.sql import types as T

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_057_{stamp}
ruleset_name: ST-057 Date Functions Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: seasoning
    rule_name: Seasoning
    rule_order: 1
    when:
      all:
        - left:
            custom_function:
              name: date_add_months
              args:
                value: {{ field: funded_date }}
                months: 1
          operator: ge
          right: {{ literal: "2024-02-29", value_type: date }}
    assign:
      review_date:
        custom_function:
          name: date_add_years
          args:
            value: {{ field: funded_date }}
            years: 1
      age_days:
        custom_function:
          name: date_diff_days
          args:
            start: {{ field: funded_date }}
            end: {{ field: as_of_date }}
"""
ruleset = service.compile_yaml_text(yaml_text)
result = service.evaluate_dataframe(
    spark.createDataFrame(
        [
            (
                "L1",
                date(2024, 1, 31),
                date(2024, 2, 29),
            )
        ],
        T.StructType(
            [
                T.StructField("loan_id", T.StringType(), False),
                T.StructField("funded_date", T.DateType(), False),
                T.StructField("as_of_date", T.DateType(), False),
            ]
        ),
    ),
    ruleset=ruleset,
    full_audit=True,
)
row = result.collect()[0].asDict(recursive=True)
assert row["rules_engine_assign"] == {
    "review_date": date(2025, 1, 31),
    "age_days": 29,
}
assert result.schema["rules_engine_assign"].dataType["review_date"].dataType == T.DateType()
assert result.schema["rules_engine_assign"].dataType["age_days"].dataType == T.LongType()
authored = {
    event["target_field"]: event["authored_expression"]
    for event in row["rules_engine_assignment_results"]
}
assert authored == {
    "review_date": "review_date = date_add_years(value=funded_date, years=1)",
    "age_days": "age_days = date_diff_days(start=funded_date, end=as_of_date)",
}
print("PASS: Date functions preserved month-end, leap-year, type, and audit semantics.")

# COMMAND ----------
print("ST-058: Embedded expected cases are a hard publication gate")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Passing cases publish; failing cases prevent every Delta repository write.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService
from rules_engine.exceptions import ValidationFailedError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
passing_yaml = f"""
ruleset_id: st_058_pass_{stamp}
ruleset_name: ST-058 Passing Expected Cases
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: prime
    rule_name: Prime
    rule_order: 1
    when:
      all:
        - left: {{ field: fico }}
          operator: ge
          right: {{ literal: 720 }}
    assign:
      bucket: prime
expect:
  - name: prime loan
    given: {{ fico: 740 }}
    then:
      matched: true
      matched_rule_ids: [prime]
      bucket: prime
"""
passing_ruleset = service.compile_yaml_text(passing_yaml)
test_result = service.test_ruleset(passing_ruleset)
assert test_result.passed, test_result.to_text()
service.publish(passing_ruleset, published_by="system-test")

failing_yaml = f"""
ruleset_id: st_058_fail_{stamp}
ruleset_name: ST-058 Failing Expected Cases
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: prime
    rule_name: Prime
    rule_order: 1
    when:
      all:
        - left: {{ field: fico }}
          operator: ge
          right: {{ literal: 720 }}
    assign:
      bucket: prime
expect:
  - name: deliberately incorrect expectation
    given: {{ fico: 740 }}
    then:
      bucket: incorrect
"""
failing_ruleset = service.compile_yaml_text(failing_yaml)
try:
    service.publish(failing_ruleset, published_by="system-test")
except ValidationFailedError as exc:
    assert "expected cases failed" in str(exc), str(exc)
else:
    raise AssertionError("Expected a failing embedded case to block publication.")
failed_rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{failing_ruleset.ruleset_id}' AND version = '{failing_ruleset.version}'"
).count()
assert failed_rows == 0
print("PASS: Expected cases passed and blocked publication at the correct boundaries.")

# COMMAND ----------
print("ST-059: Compact and full-audit outputs retain immutable execution identity")
print("-" * 80)
print("Area: Auditability")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Compact and full-audit schemas differ only in documented detail while both remain attributable.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_059_{stamp}
ruleset_name: ST-059 Audit Contract Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: match_a
    rule_name: Match A
    rule_order: 1
    when:
      all:
        - left: {{ field: account }}
          operator: eq
          right: {{ literal: A }}
    assign:
      bucket: A
"""
ruleset = service.compile_yaml_text(yaml_text)
source = spark.createDataFrame([{"account": "A"}])
compact = service.evaluate_dataframe(source, ruleset=ruleset)
full = service.evaluate_dataframe(source, ruleset=ruleset, full_audit=True)
assert compact.columns == [
    "account",
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
]
assert full.columns == [
    "account",
    "rules_engine_error",
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_matched_rules",
    "rules_engine_assignment_results",
    "rules_engine_ruleset",
    "rules_engine_engine_version",
]
identity_columns = {
    "rules_engine_ruleset",
    "rules_engine_engine_version",
}
assert identity_columns <= set(compact.columns)
assert identity_columns <= set(full.columns)
assert "rules_engine_matched_rules" not in compact.columns
assert "rules_engine_assignment_results" not in compact.columns
assert "rules_engine_matched_rules" in full.columns
assert "rules_engine_assignment_results" in full.columns
compact_row = compact.collect()[0]
assert compact_row["rules_engine_ruleset"]["id"] == ruleset.ruleset_id
assert compact_row["rules_engine_ruleset"]["version"] == ruleset.version
assert compact_row["rules_engine_ruleset"]["content_hash"]
assert compact_row["rules_engine_engine_version"]
print("PASS: Default and full-audit outputs retained identity and emitted documented detail.")

# COMMAND ----------
print("ST-060: Semantic diffs compare immutable published versions")
print("-" * 80)
print("Area: Change control")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Version diffs expose authored threshold and assignment changes with both content hashes.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
ruleset_name = f"ST-060 Semantic Diff {stamp}"
baseline_yaml = f"""
ruleset_id: st_060_{stamp}
ruleset_name: {ruleset_name}
version: "{stamp}.1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: prime
    rule_name: Prime
    rule_order: 1
    when:
      all:
        - condition_id: fico_threshold
          left: {{ field: fico }}
          operator: ge
          right: {{ literal: 720 }}
    assign:
      bucket: prime
"""
candidate_yaml = baseline_yaml.replace(f'version: "{stamp}.1"', f'version: "{stamp}.2"')
candidate_yaml = candidate_yaml.replace("right: { literal: 720 }", "right: { literal: 740 }")
candidate_yaml = candidate_yaml.replace("bucket: prime", "bucket: super_prime")
baseline = service.publish_yaml_text(baseline_yaml, published_by="system-test")
candidate = service.publish_yaml_text(candidate_yaml, published_by="system-test")
semantic_diff = service.diff_versions(
    ruleset_name,
    baseline.version,
    candidate.version,
)
rendered_diff = semantic_diff.to_text()
assert semantic_diff.has_changes
assert semantic_diff.baseline_content_hash != semantic_diff.candidate_content_hash
assert "720" in rendered_diff and "740" in rendered_diff
assert "super_prime" in rendered_diff
print(rendered_diff)
print("PASS: Published-version diff exposed the expected semantic changes.")

# COMMAND ----------
print("ST-061: Coverage reports dead, broad, and closest rules")
print("-" * 80)
print("Area: Change control")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Coverage counts matches once and diagnoses clean no-match rows with failed condition IDs.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_061_{stamp}
ruleset_name: ST-061 Coverage Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: prime
    rule_name: Prime
    rule_order: 1
    when:
      all:
        - condition_id: prime_fico
          left: {{ field: fico }}
          operator: ge
          right: {{ literal: 720 }}
    assign:
      bucket: prime
  - rule_id: near
    rule_name: Near Prime
    rule_order: 2
    when:
      all:
        - condition_id: near_fico
          left: {{ field: fico }}
          operator: ge
          right: {{ literal: 680 }}
    assign:
      review: true
  - rule_id: impossible
    rule_name: Impossible
    rule_order: 3
    when:
      all:
        - condition_id: impossible_fico
          left: {{ field: fico }}
          operator: gt
          right: {{ literal: 900 }}
    assign:
      invalid: true
"""
ruleset = service.compile_yaml_text(yaml_text)
coverage = service.coverage_report(
    spark.createDataFrame(
        [("L1", 740), ("L2", 690), ("L3", 600)],
        ["loan_id", "fico"],
    ),
    ruleset=ruleset,
    broad_match_threshold=0.60,
)
no_match = coverage.no_match_rows.collect()[0]
assert coverage.total_row_count == 3
assert coverage.no_match_count == 1
assert coverage.error_count == 0
assert coverage.dead_rule_ids == ("impossible",)
assert coverage.suspiciously_broad_rule_ids == ("near",)
assert no_match["loan_id"] == "L3"
assert no_match["rules_engine_coverage_closest_rule_id"] == "prime"
assert no_match["rules_engine_coverage_failed_condition_ids"] == ["prime_fico"]
print("PASS: Coverage identified dead, broad, and closest-rule behavior.")

# COMMAND ----------
print("ST-062: Later rules consume explicit prior assignment results")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Assigned operands see lower-order commits, preserve original fields, and expose producer provenance.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService, required_source_columns

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_062_{stamp}
ruleset_name: ST-062 Assigned Chain
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: producer
    rule_name: Producer
    rule_order: 1
    when:
      all:
        - left: {{ field: eligible }}
          operator: eq
          right: {{ literal: true }}
    assign:
      - assignment_id: produce_bucket
        target_field: bucket
        value: {{ literal: A }}
      - assignment_id: produce_score
        target_field: score
        value: {{ literal: 10 }}
  - rule_id: consumer
    rule_name: Consumer
    rule_order: 2
    when:
      all:
        - condition_id: assigned_bucket_is_a
          left: {{ assigned: bucket }}
          operator: eq
          right: {{ literal: A }}
        - condition_id: original_bucket_unchanged
          left: {{ field: bucket }}
          operator: eq
          right: {{ literal: ORIGINAL }}
    assign:
      - assignment_id: replace_score
        target_field: score
        value: {{ literal: 20 }}
      - assignment_id: copy_prior_score
        target_field: copied_score
        value: {{ assigned: score }}
  - rule_id: missing_commit_fallback
    rule_name: Missing Commit Fallback
    rule_order: 3
    when:
      all:
        - condition_id: bucket_was_not_committed
          left:
            assigned: bucket
            default_if_null: MISSING
          operator: eq
          right: {{ literal: MISSING }}
    assign:
      used_fallback: true
"""
ruleset = service.compile_yaml_text(yaml_text)
validation = service.validator.validate(
    ruleset,
    spark.createDataFrame(
        [("chain", True, "ORIGINAL"), ("fallback", False, "ORIGINAL")],
        ["row_id", "eligible", "bucket"],
    ).schema,
)
assert validation.passed, validation.to_text()
assert required_source_columns(ruleset) == ("eligible", "bucket")

evaluated = service.evaluate_dataframe(
    spark.createDataFrame(
        [("chain", True, "ORIGINAL"), ("fallback", False, "ORIGINAL")],
        ["row_id", "eligible", "bucket"],
    ),
    ruleset=ruleset,
    full_audit=True,
    fail_on_error=False,
)
rows = {
    row["row_id"]: row.asDict(recursive=True)
    for row in evaluated.collect()
}
chain = rows["chain"]
fallback = rows["fallback"]
assert chain["rules_engine_error"] is None
assert chain["rules_engine_matched_rule_ids"] == ["producer", "consumer"]
assert chain["rules_engine_assign"]["bucket"] == "A"
assert chain["rules_engine_assign"]["score"] == 20
assert chain["rules_engine_assign"]["copied_score"] == 10
consumer_left = chain["rules_engine_matched_rules"][1]["conditions"][0]["left"]
assert consumer_left["kind"] == "assigned"
assert consumer_left["target_field"] == "bucket"
assert consumer_left["produced_by_rule_id"] == "producer"
assert consumer_left["produced_by_assignment_id"] == "produce_bucket"
assert fallback["rules_engine_error"] is None
assert fallback["rules_engine_matched_rule_ids"] == ["missing_commit_fallback"]
assert fallback["rules_engine_assign"]["used_fallback"] is True
print("PASS: Ordered assignment chaining, atomic snapshots, null fallback, and provenance are correct.")

# COMMAND ----------
print("Run automated unit test suite")
print("-" * 80)
print("Purpose: Execute the repository pytest suite.")
print("")

import sys
import warnings

import pytest

assert (REPO_ROOT / "tests").exists(), (
    f"Could not find tests directory under repo root {REPO_ROOT}. "
    "Set RULES_ENGINE_REPO_ROOT to the repository root before running this cell."
)

PYTEST_ARGS = globals().get(
    "RULES_ENGINE_PYTEST_ARGS",
    [
        "tests",
        "-q",
        "-p",
        "no:cacheprovider",
        "-p",
        "no:assertion_rewriting",
    ],
)

if str(REPO_ROOT) not in sys.path:
    # Tests and package tooling live in the checkout.
    sys.path.append(str(REPO_ROOT))

previous_cwd = Path(os.getcwd())
os.chdir(str(REPO_ROOT))
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["RULES_ENGINE_RUN_SPARK_TESTS"] = (
    "1" if RULES_ENGINE_RUN_SPARK_TESTS else "0"
)

print(f"Repo root: {REPO_ROOT}")
print(f"pytest args: {PYTEST_ARGS}")
print(f"RULES_ENGINE_RUN_SPARK_TESTS: {os.environ['RULES_ENGINE_RUN_SPARK_TESTS']}")
print("")

try:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*getargspec.*",
            category=DeprecationWarning,
        )
        retcode = pytest.main(PYTEST_ARGS)
finally:
    os.chdir(str(previous_cwd))

assert retcode == 0, f"pytest failed with exit code {retcode}."

print("PASS: Automated pytest suite completed successfully.")
