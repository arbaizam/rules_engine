# Databricks notebook source
print("ST-001: Setup notebook creates the target schema before table creation")
print("-" * 80)
print("Area: Deployment setup")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: The schema exists and the notebook proceeds to table creation without manual pre-work.")
print("")
assert SCHEMA, "Set SCHEMA before running this test."

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
schemas = [row[0] for row in spark.sql(f"SHOW SCHEMAS IN {SCHEMA.rsplit('.', 1)[0]}" if "." in SCHEMA else "SHOW SCHEMAS").collect()]
schema_name = SCHEMA.rsplit(".", 1)[-1]
assert schema_name in schemas, f"Expected schema {SCHEMA} to exist after CREATE SCHEMA IF NOT EXISTS."
print(f"PASS: Schema exists: {SCHEMA}")

# COMMAND ----------
print("ST-002: Rules engine metadata tables are created with standard names")
print("-" * 80)
print("Area: Deployment setup")
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
print("Area: Deployment setup")
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
print("Area: Deployment setup")
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
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate or migrate the table "
        "with the current rules_engine table DDL."
    )

for column in ["rule_count", "condition_count", "assignment_count", "custom_function_count"]:
    expected = rf"\b{column}\b\s+int\s+not\s+null\b"
    assert re.search(expected, ruleset_normalized_sql), (
        f"Expected ruleset_versions column {column} to be declared NOT NULL in table DDL. "
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate or migrate the table "
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
        "If SHOW CREATE TABLE shows the column without NOT NULL, recreate or migrate the table "
        "with the current rules_engine table DDL."
    )

print("PASS: Required metadata columns are declared NOT NULL in table DDL.")

# COMMAND ----------
print("ST-005: Overwrite mode is restricted to disposable environments")
print("-" * 80)
print("Area: Deployment setup")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Overwrite recreates tables only in disposable schemas. Production setup uses mode='ignore' or 'error'.")
print("")
from rules_engine import RulesEngineService

assert SCHEMA, "Set SCHEMA before running this test."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
ALLOW_OVERWRITE_TEST = bool(globals().get("ALLOW_OVERWRITE_TEST", False))

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
print("ST-007: Standard function registration is rerunnable and skips existing functions")
print("-" * 80)
print("Area: Function registry")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Second run succeeds and does not overwrite existing rows. Row count remains stable and existing metadata is preserved.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

before = spark.table(service.table_names.function_registry).count()
service.save_standard_function_registry()
service.save_standard_function_registry()
after = spark.table(service.table_names.function_registry).count()

assert after == before, f"Expected rerunnable standard registration to preserve row count. before={before}, after={after}"
print("PASS: Standard function registration is rerunnable and skips existing functions.")

# COMMAND ----------
print("ST-008: Explicit standard function metadata refresh can update existing rows")
print("-" * 80)
print("Area: Function registry")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Existing standard function rows are upserted without duplicates. Metadata remains valid after refresh.")
print("")
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
service.save_standard_function_registry(update_existing=True)

duplicates = (
    spark.table(service.table_names.function_registry)
    .groupBy("function_name")
    .count()
    .where("count > 1")
    .collect()
)
assert not duplicates, f"Found duplicate function rows: {duplicates}"
print("PASS: Explicit standard function metadata refresh completed without duplicates.")

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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-013: Unsupported legacy aliases are rejected clearly")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Compilation fails with a clear message identifying the unsupported key and canonical replacement.")
print("")
from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st013_ruleset
ruleset_name: ST013 Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Alias Rule
    rule_order: 1
    when:
      all:
        - left:
            field: account
          operator: eq
          right:
            value: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

try:
    service.compile_yaml_text(invalid_yaml)
except CompilationError as exc:
    assert "Unsupported operand key: value" in str(exc), str(exc)
else:
    raise AssertionError("Expected unsupported operand alias to fail compilation.")

print("PASS: Unsupported legacy operand alias was rejected clearly.")

# COMMAND ----------
print("ST-014: Required top-level ruleset metadata is enforced")
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
print("ST-015: Condition group logical operator shape is enforced")
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
ruleset_id: st015_ruleset
ruleset_name: ST015 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
      any:
        - condition_id: c2
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-016: Valid simple row-level rule passes validation")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation passes with no error-severity issues.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_016_ruleset
ruleset_name: ST-016 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-017: Owner and owner_department are required before publish")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation returns error-severity issues and publish is blocked.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_017_ruleset
ruleset_name: ST-017 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-018: Duplicate condition_id values are rejected")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation returns a duplicate condition ID error.")
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
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c1
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-019: Duplicate condition_group_id values are rejected")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation returns a duplicate condition group ID error.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

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
      condition_group_id: duplicate_group
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_group_id: duplicate_group
          any:
            - condition_id: c2
              left:
                field: status
              operator: eq
              right:
                literal: OPEN
              null_input_mode: propagate
              null_result_mode: "null"
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
print("ST-020: Aggregate operands are rejected")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Compilation tells authors to precompute aggregate fields upstream.")
print("")

from rules_engine import RulesEngineService
from rules_engine.exceptions import CompilationError

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
            aggregate:
              function: sum
              field: amount
              scope: group
              null_input_mode: ignore
              null_result_mode: "null"
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c2
          left:
            aggregate:
              function: sum
              field: amount
              scope: dataset
              by:
                - account
              null_input_mode: ignore
              null_result_mode: "null"
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

try:
    service.compile_yaml_text(invalid_yaml)
    aggregate_rejected = False
except CompilationError as exc:
    aggregate_rejected = True
    assert "Unsupported operand key: aggregate" in str(exc), str(exc)

assert aggregate_rejected, "Expected aggregate operand compilation to fail."

print("PASS: Aggregate operands were rejected at compile time.")

# COMMAND ----------
print("ST-021: Precomputed aggregate facts compile as fields")
print("-" * 80)
print("Area: YAML compilation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: The field operand compiles and evaluates like any other row-level field.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
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
            field: account_amount_sum
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(valid_yaml)
validation = service.validator.validate(ruleset)

assert not validation.has_errors(), validation.to_text()
assert ruleset.rules[0].root_group.conditions[0].left.field_name == "account_amount_sum"

print(validation.to_text())
print("PASS: Precomputed aggregate field compiled and validated.")

# COMMAND ----------
print("ST-022: Custom-function argument contracts are enforced")
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
            custom_function:
              name: score
              args:
                x:
                  field: amount
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-023: Operator arity and literal collection requirements are enforced")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Validation identifies missing, extra, or malformed operands.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
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
            field: account
          operator: is_null
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c2
          left:
            field: account
          operator: eq
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c3
          left:
            field: account
          operator: in
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c4
          left:
            field: amount
          operator: between
          right:
            literal:
              - 100
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-024: Null handling modes require default values when configured")
print("-" * 80)
print("Area: Semantic validation")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Validation returns a default-value requirement error.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: st_024_ruleset
ruleset_name: ST-024 Ruleset
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
          null_input_mode: propagate
          null_result_mode: default
    assign:
      bucket: A
"""

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
check_names = {issue.check_name for issue in validation.issues}

assert validation.has_errors(), "Expected null default validation to fail."
assert "NULL_DEFAULT_REQUIRED" in check_names, validation.to_text()

print(validation.to_text())
print("PASS: null_result_mode=default without null_default_value was rejected.")

# COMMAND ----------
print("ST-025: Spark validator accepts supported rulesets")
print("-" * 80)
print("Area: Spark compatibility")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Validation passes and no Spark-specific unsupported-operation issues are present.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_025_ruleset
ruleset_name: ST-025 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-026: Spark validator follows the supported row-level contract")
print("-" * 80)
print("Area: Spark compatibility")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Supported row-level rules validate without Spark-specific errors.")
print("")

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

valid_yaml = """
ruleset_id: st_026_ruleset
ruleset_name: ST-026 Ruleset
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
            field: account_amount_sum
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c2
          left:
            field: status
          operator: eq
          right:
            literal: OPEN
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-027: Publish YAML path compiles, normalizes, validates, and writes metadata")
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-028: Direct publish of a compiled ruleset works")
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
ruleset_id: st_028_{stamp}
ruleset_name: ST-028 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

compiled = service.compile_yaml_text(yaml_text.replace(f'st_028_{stamp}', f'st_028_direct_{stamp}'))
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
print("ST-029: Publish rejects non-published lifecycle status")
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
ruleset_id: st_029_{stamp}
ruleset_name: ST-029 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-030: Duplicate ruleset_name and version cannot be published twice")
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-031: Multiple published versions for the same ruleset_name are allowed")
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
ruleset_name = f"ST-031 Ruleset {stamp}"

yaml_v1 = f"""
ruleset_id: st_031_v1_{stamp}
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

yaml_v2 = yaml_v1.replace(f"st_031_v1_{stamp}", f"st_031_v2_{stamp}").replace('version: "1"', 'version: "2"')

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
print("ST-032: Loading by name without version is rejected when ambiguous")
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
ruleset_name = f"ST-032 Ruleset {stamp}"

yaml_v1 = f"""
ruleset_id: st_032_v1_{stamp}
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""

yaml_v2 = yaml_v1.replace(f"st_032_v1_{stamp}", f"st_032_v2_{stamp}").replace('version: "1"', 'version: "2"')

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
print("ST-033: Loading by name and version reconstructs the canonical ruleset")
print("-" * 80)
print("Area: Publishing")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: The returned Ruleset matches the published metadata and can be evaluated.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: st_033_{stamp}
ruleset_name: ST-033 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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

print("PASS: Loaded ruleset matched published metadata and evaluated successfully.")

# COMMAND ----------
print("ST-034: Retirement makes a version unavailable to load_published")
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-035: Retirement stamps retired_by and retired_at")
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
ruleset_id: st_035_{stamp}
ruleset_name: ST-035 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-036: Retiring a missing ruleset version fails safely")
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
print("ST-037: Retiring an already retired version does not silently succeed")
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
ruleset_id: st_037_{stamp}
ruleset_name: ST-037 Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-038: Spark runtime evaluates simple row-level rules correctly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Matched flags, matched rule IDs, assignments, winning-rule trace, and errors match expected outcomes.")
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
        - left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["account"]: row.asDict(recursive=True) for row in result.collect()}

assert rows["A"]["rules_engine_matched"] is True
assert rows["A"]["rules_engine_matched_rule_ids"] == ["r1"]
assert rows["A"]["rules_engine_assign"] == {"bucket": "A"}
assert rows["A"]["rules_engine_winning_rule_id"] == "r1"
assert rows["A"]["rules_engine_winning_rule_name"] == "Account A"
assert rows["A"]["rules_engine_winning_rule_explanation"] == "account == 'A'"
assert rows["A"]["rules_engine_winning_rule"]["rule_id"] == "r1"
assert rows["A"]["rules_engine_winning_rule"]["matched"] is True
assert rows["A"]["rules_engine_winning_rule"]["conditions"][0]["columns"] == ["account"]
assert rows["A"]["rules_engine_error"] is None
assert rows["B"]["rules_engine_matched"] is False
assert rows["B"]["rules_engine_matched_rule_ids"] == []
assert rows["B"]["rules_engine_assign"] is None
assert rows["B"]["rules_engine_winning_rule"] is None
assert rows["B"]["rules_engine_winning_rule_explanation"] is None
assert rows["B"]["rules_engine_error"] is None
display(result)
print("PASS: Runtime evaluation completed for ST-038.")

# COMMAND ----------
print("ST-039: Spark runtime honors rule_order and stop_on_match")
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
ruleset_id: st_039_{stamp}
ruleset_name: ST-039 Runtime Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: second
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["account"]: row.asDict(recursive=True) for row in result.collect()}

assert rows["A"]["rules_engine_matched"] is True
assert rows["A"]["rules_engine_matched_rule_ids"] == ["first_match"]
assert rows["A"]["rules_engine_assign"] == {"bucket": "first"}
assert rows["A"]["rules_engine_winning_rule_id"] == "first_match"
assert rows["B"]["rules_engine_matched"] is False
assert rows["B"]["rules_engine_matched_rule_ids"] == []
assert rows["B"]["rules_engine_assign"] is None
display(result)
print("PASS: Runtime evaluation completed for ST-039.")

# COMMAND ----------
print("ST-040: Spark runtime applies null_input_mode and null_result_mode correctly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Results match documented null semantics and errors are emitted only where expected.")
print("")

from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime

class ST040Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError

runtime = SparkRulesEngineRuntime(ST040Repository(), FunctionRegistry())
compiler = YamlRulesetCompiler()

ruleset = compiler.compile_text("""
ruleset_id: st_040_ruleset
ruleset_name: ST-040 Runtime Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: propagate_null
    rule_name: Propagate Null Produces No Match
    rule_order: 1
    when:
      all:
        - condition_id: c_propagate
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: propagate
  - rule_id: default_true
    rule_name: Default True Converts Null Result To Match
    rule_order: 2
    when:
      all:
        - condition_id: c_default_true
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: default
          null_default_value: true
    assign:
      default_bucket: default_true
  - rule_id: zero_mode
    rule_name: Zero Mode Replaces Null Numeric Inputs
    rule_order: 3
    when:
      all:
        - condition_id: c_zero
          left:
            field: amount
          operator: eq
          right:
            literal: 0
          null_input_mode: zero
          null_result_mode: "null"
    assign:
      zero_bucket: zero_match
""")

output = runtime.evaluate_dataframe(
    spark.createDataFrame([{"account": None, "amount": None}], "account string, amount double"),
    ruleset,
)
row = output.collect()[0].asDict(recursive=True)
assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rule_ids"] == ["default_true", "zero_mode"], row["rules_engine_matched_rule_ids"]
assert row["rules_engine_assign"] == {
    "bucket": None,
    "default_bucket": "default_true",
    "zero_bucket": "zero_match",
}
assert row["rules_engine_winning_rule_id"] == "default_true"

error_ruleset = compiler.compile_text("""
ruleset_id: st_040_error_ruleset
ruleset_name: ST-040 Error Ruleset
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
          null_input_mode: propagate
          null_result_mode: error
    assign:
      bucket: should_error
""")
try:
    runtime.evaluate_dataframe(
        spark.createDataFrame([{"account": None}], "account string"),
        error_ruleset,
    )
    error_failed = False
except RuntimeError as exc:
    error_failed = True
    assert "null_result_mode=error" in str(exc), str(exc)
assert error_failed, "Expected null_result_mode=error to raise on null comparison result."
print("PASS: Spark runtime null semantics matched documented propagate/default/zero/error behavior.")
# COMMAND ----------
print("ST-041: Spark runtime evaluates precomputed aggregate fields correctly")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Precomputed aggregate comparisons and assignments match manually calculated expected values.")
print("")

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime

class ST041Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError

runtime = SparkRulesEngineRuntime(ST041Repository(), FunctionRegistry())
ruleset = YamlRulesetCompiler().compile_text("""
ruleset_id: st_041_ruleset
ruleset_name: ST-041 Precomputed Aggregate Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: dataset_total
    rule_name: Dataset Total Equals 35
    rule_order: 1
    when:
      all:
        - condition_id: c_dataset
          left:
            field: dataset_amount_sum
          operator: eq
          right:
            literal: 35
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      dataset_total_check: pass
  - rule_id: group_total
    rule_name: Group Total Greater Than 15
    rule_order: 2
    when:
      all:
        - condition_id: c_group
          left:
            field: account_amount_sum
          operator: gt
          right:
            literal: 15
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      group_total_check: pass
""")
input_rows = [
    {"record_id": "r1", "account": "A", "amount": 10, "dataset_amount_sum": 35, "account_amount_sum": 30},
    {"record_id": "r2", "account": "A", "amount": 20, "dataset_amount_sum": 35, "account_amount_sum": 30},
    {"record_id": "r3", "account": "B", "amount": 5, "dataset_amount_sum": 35, "account_amount_sum": 5},
]
manual_dataset_total = sum(row["amount"] for row in input_rows)
manual_group_totals = {}
for item in input_rows:
    manual_group_totals[item["account"]] = manual_group_totals.get(item["account"], 0) + item["amount"]
output = runtime.evaluate_dataframe(spark.createDataFrame(input_rows), ruleset)
actual_by_id = {row["record_id"]: row.asDict(recursive=True) for row in output.collect()}
assert manual_dataset_total == 35
assert manual_group_totals == {"A": 30, "B": 5}
assert actual_by_id["r1"]["rules_engine_matched_rule_ids"] == ["dataset_total", "group_total"]
assert actual_by_id["r2"]["rules_engine_matched_rule_ids"] == ["dataset_total", "group_total"]
assert actual_by_id["r3"]["rules_engine_matched_rule_ids"] == ["dataset_total"]
assert actual_by_id["r3"]["rules_engine_assign"] == {"dataset_total_check": "pass", "group_total_check": None}
print("PASS: Precomputed dataset and group aggregate fields matched manual calculations.")
# COMMAND ----------
print("ST-042: Spark runtime evaluates a published ruleset against a DataFrame")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Output DataFrame contains matched, matched_rule_ids, assign, winning_rule, and error columns with expected values.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_042_{stamp}
ruleset_name: ST-042 Runtime Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}, {"record_id": "r2", "account": "B"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}
required_columns = {
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_winning_rule",
    "rules_engine_winning_rule_id",
    "rules_engine_winning_rule_name",
    "rules_engine_winning_rule_explanation",
    "rules_engine_error",
}
missing_columns = required_columns - set(result.columns)
assert not missing_columns, f"Missing output columns: {sorted(missing_columns)}"
assert rows["r1"]["rules_engine_matched"] is True
assert rows["r1"]["rules_engine_matched_rule_ids"] == ["r1"]
assert rows["r1"]["rules_engine_assign"] == {"bucket": "A"}
assert rows["r1"]["rules_engine_winning_rule_id"] == "r1"
assert rows["r1"]["rules_engine_winning_rule_explanation"] == "account == 'A'"
assert rows["r1"]["rules_engine_error"] is None
assert rows["r2"]["rules_engine_matched"] is False
assert rows["r2"]["rules_engine_matched_rule_ids"] == []
assert rows["r2"]["rules_engine_assign"] is None
assert rows["r2"]["rules_engine_winning_rule"] is None
assert rows["r2"]["rules_engine_winning_rule_explanation"] is None
assert rows["r2"]["rules_engine_error"] is None
assert rows["r1"]["rules_engine_winning_rule"]["rule_id"] == "r1"
display(result)
print("PASS: Spark runtime output columns and values matched expected results.")
# COMMAND ----------
print("ST-043: Spark runtime fail_on_error behavior is enforced")
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
ruleset_id: st_043_{stamp}
ruleset_name: ST-043 Runtime Ruleset
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
          null_input_mode: propagate
          null_result_mode: error
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": None}, {"record_id": "r2", "account": "A"}])
try:
    service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version, fail_on_error=True)
    fail_on_error_raised = False
except RuntimeError as exc:
    fail_on_error_raised = True
    assert "row-level errors" in str(exc).lower(), str(exc)
assert fail_on_error_raised, "Expected fail_on_error=True to raise for row-level runtime errors."
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version, fail_on_error=False)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}
assert rows["r1"]["rules_engine_error"] is not None
assert "null_result_mode=error" in rows["r1"]["rules_engine_error"]
assert rows["r2"]["rules_engine_error"] is None
assert rows["r2"]["rules_engine_matched"] is True
display(result)
print("PASS: fail_on_error=True raised and fail_on_error=False recorded row-level errors.")
# COMMAND ----------
print("ST-044: Spark runtime supports standard custom functions used in rules")
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
ruleset_id: st_044_{stamp}
ruleset_name: ST-044 Runtime Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-045: Spark runtime emits native assignment and winning-rule structs")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Medium")
print("Owner Role: Engineering")
print("Expected Result: Matched flags, assignments, and winning-rule trace are Spark-native structs.")
print("")

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime

class ST045Repository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError

ruleset = YamlRulesetCompiler().compile_text("""
ruleset_id: st_045_ruleset
ruleset_name: ST-045 Runtime Ruleset
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: dataset_total
    rule_name: Dataset Total Equals 30
    rule_order: 1
    when:
      all:
        - condition_id: c_dataset
          left:
            field: dataset_amount_sum
          operator: eq
          right:
            literal: 30
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: dataset_match
""")
input_rows = [
    {"row_id": 1, "amount": 10, "dataset_amount_sum": 30},
    {"row_id": 2, "amount": 20, "dataset_amount_sum": 30},
]
registry = FunctionRegistry()
spark_result = SparkRulesEngineRuntime(ST045Repository(), registry).evaluate_dataframe(spark.createDataFrame(input_rows), ruleset, fail_on_error=True)
spark_rows = [row.asDict(recursive=True) for row in spark_result.orderBy("row_id").collect()]
assert [row["rules_engine_matched"] for row in spark_rows] == [True, True]
assert [row["rules_engine_matched_rule_ids"] for row in spark_rows] == [["dataset_total"], ["dataset_total"]]
assert [row["rules_engine_assign"] for row in spark_rows] == [{"bucket": "dataset_match"}, {"bucket": "dataset_match"}]
assert [row["rules_engine_winning_rule"]["rule_id"] for row in spark_rows] == ["dataset_total", "dataset_total"]
display(spark_result)
print("PASS: Spark runtime emitted native assignment and winning-rule structs.")
# COMMAND ----------
print("ST-046: Payload JSON excludes mutable lifecycle fields and reconstructs ruleset content")
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
ruleset_id: st_046_{stamp}
ruleset_name: ST-046 Audit Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("PASS: Audit metadata checks completed for ST-046.")

# COMMAND ----------
print("ST-047: Content hash is deterministic and reproducible")
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
ruleset_id: st_047_{stamp}
ruleset_name: ST-047 Audit Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("PASS: Audit metadata checks completed for ST-047.")

# COMMAND ----------
print("ST-048: Summary count columns match the published ruleset content")
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
ruleset_id: st_048_{stamp}
ruleset_name: ST-048 Audit Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Precomputed Aggregate Rule
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: dataset_amount_sum
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      precomputed_bucket: large
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-049: Publish notebook re-instantiates service against existing setup tables")
print("-" * 80)
print("Area: Pipeline notebooks")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Notebook 2 compiles, validates, publishes, and verifies the ruleset without recreating setup objects.")
print("")
from datetime import datetime, timezone
from dataclasses import replace
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

assert spark.catalog.tableExists(service.table_names.ruleset_versions), f"Missing {service.table_names.ruleset_versions}"
assert spark.catalog.tableExists(service.table_names.function_registry), f"Missing {service.table_names.function_registry}"
assert RULESET_YAML_PATH, "Set RULESET_YAML_PATH before running the publish notebook verification test."

ruleset = service.compile_yaml_path(RULESET_YAML_PATH)
ruleset = replace(ruleset, version=f"ST049_{stamp}")
validation = service.validator.validate(ruleset)
assert not validation.has_errors(), validation.to_text()

service.publish(ruleset, published_by="system-test")
loaded = service.load_published(ruleset.ruleset_name, version=ruleset.version)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}'"
).collect()

assert loaded.ruleset_id == ruleset.ruleset_id
assert len(rows) == 1, f"Expected exactly one published row, found {len(rows)}."
assert rows[0]["status"] == "published"

print(f"PASS: Re-instantiated service published and loaded {ruleset.ruleset_name} version {ruleset.version}.")

# COMMAND ----------
print("ST-050: Notebook verification checks are present after setup and publish")
print("-" * 80)
print("Area: Pipeline notebooks")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: Both notebooks fail fast when expected deployment or publish verification conditions are not met.")
print("")
from datetime import datetime, timezone
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
assert spark.catalog.tableExists(service.table_names.ruleset_versions), f"Missing {service.table_names.ruleset_versions}"
assert spark.catalog.tableExists(service.table_names.function_registry), f"Missing {service.table_names.function_registry}"
required_functions = {"upper", "lower", "substring"}
function_rows = spark.table(service.table_names.function_registry).where("active_flag = true").select("function_name").collect()
active_function_names = {row["function_name"] for row in function_rows}
missing_functions = required_functions - active_function_names
assert not missing_functions, f"Missing expected active standard functions: {sorted(missing_functions)}"
yaml_text = f"""
ruleset_id: st_050_{stamp}
ruleset_name: ST-050 Verification Ruleset
version: "{stamp}"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Verification Rule
    rule_order: 1
    when:
      all:
        - condition_id: c1
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
loaded = service.load_published(ruleset.ruleset_name, version=ruleset.version)
published_rows = spark.table(service.table_names.ruleset_versions).where(f"ruleset_name = '{ruleset.ruleset_name}' AND version = '{ruleset.version}' AND status = 'published'").collect()
assert loaded.ruleset_id == ruleset.ruleset_id
assert len(published_rows) == 1, f"Expected exactly one published verification row, found {len(published_rows)}."
assert published_rows[0]["content_hash"]
assert published_rows[0]["payload_json"]
assert published_rows[0]["rule_count"] == 1
assert published_rows[0]["condition_count"] == 1
assert published_rows[0]["assignment_count"] == 1
print("PASS: Setup and publish verification checks fail fast and prove metadata is usable.")

# COMMAND ----------
print("ST-051: Spark runtime emits condition-level traceability values")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: winning_rule exposes source columns, evaluated operand values, comparison results, and readable winning-rule output.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_051_{stamp}
ruleset_name: ST-051 Traceability Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: trace_rule
    rule_name: Traceability Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_group_sum
          left:
            field: account_amount_sum
          operator: gt
          right:
            literal: 15
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
        - condition_id: c_status
          left:
            field: status
          operator: eq
          right:
            literal: open
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: traced
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame(
    [
        {"record_id": "r1", "account": "a", "amount": 10, "account_amount_sum": 30, "status": "open"},
        {"record_id": "r2", "account": "a", "amount": 20, "account_amount_sum": 30, "status": "closed"},
        {"record_id": "r3", "account": "b", "amount": 5, "account_amount_sum": 5, "status": "open"},
    ]
)
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
rows = {row["record_id"]: row.asDict(recursive=True) for row in result.collect()}

matched_row = rows["r1"]
winning_rule = matched_row["rules_engine_winning_rule"]
conditions = winning_rule["conditions"]

assert matched_row["rules_engine_matched"] is True
assert matched_row["rules_engine_matched_rule_ids"] == ["trace_rule"]
assert matched_row["rules_engine_assign"] == {"bucket": "traced"}
assert matched_row["rules_engine_winning_rule_id"] == "trace_rule"
assert matched_row["rules_engine_winning_rule_name"] == "Traceability Rule"
assert matched_row["rules_engine_winning_rule_explanation"] == (
    "account_amount_sum > 15 AND "
    "upper(value=account) == 'A' AND "
    "status == 'open'"
)
assert winning_rule["rule_id"] == "trace_rule"
assert winning_rule["rule_name"] == "Traceability Rule"
assert winning_rule["matched"] is True
assert winning_rule["assignments_applied"] == ["bucket"]

precomputed_condition = conditions[0]
assert precomputed_condition["columns"] == ["account_amount_sum"]
assert precomputed_condition["operator"] == "gt"
assert precomputed_condition["comparison_result"] is True
assert precomputed_condition["passed"] is True
assert precomputed_condition["left"]["kind"] == "field"
assert precomputed_condition["left"]["column"] == "account_amount_sum"
assert precomputed_condition["left"]["source_columns"] == ["account_amount_sum"]
assert precomputed_condition["left"]["value"] == "30"
assert precomputed_condition["right"]["kind"] == "literal"
assert precomputed_condition["right"]["value"] == "15"

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
assert rows["r2"]["rules_engine_winning_rule"] is None
assert rows["r2"]["rules_engine_winning_rule_explanation"] is None
assert rows["r3"]["rules_engine_matched"] is False
assert rows["r3"]["rules_engine_winning_rule"] is None
assert rows["r3"]["rules_engine_winning_rule_explanation"] is None

display(result)
print("PASS: Spark runtime traceability payloads included useful evaluated condition details.")

# COMMAND ----------
print("ST-052: Service describes published rules in human-readable form")
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
ruleset_id: st_052_{stamp}
ruleset_name: ST-052 Human Readable Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
        - any:
            - condition_id: c_amount
              left:
                field: amount
              operator: gt
              right:
                literal: 100
              null_input_mode: propagate
              null_result_mode: "null"
            - condition_id: c_status
              left:
                field: status
              operator: eq
              right:
                literal: REVIEW
              null_input_mode: propagate
              null_result_mode: "null"
    assign:
      leaf_key: "15656"
  - rule_id: precomputed_review
    rule_name: Precomputed Review
    rule_order: 2
    when:
      all:
        - condition_id: c_sum
          left:
            field: account_amount_sum
          operator: gt
          right:
            literal: 100
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
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
        "rule_id": "precomputed_review",
        "rule_name": "Precomputed Review",
        "rule_logic": (
            "account_amount_sum > 100 AND "
            "upper(value=status) == 'REVIEW'"
        ),
        "match_payload": "route = 'REVIEW'",
    },
]

assert described_rows == expected_rows
display(spark.createDataFrame(described_rows))
print("PASS: Service-level human-readable rule descriptions matched expected audit rows.")

# COMMAND ----------
print("ST-053: Spark runtime preserves legacy output columns")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Output columns that existed before winning-rule traceability remain present and populated with the same semantics.")
print("")

from datetime import datetime, timezone
import json

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_053_{stamp}
ruleset_name: ST-053 Legacy Output Columns Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: legacy_rule
    rule_name: Legacy Output Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_account
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: legacy_a
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

previous_input_columns = {"record_id", "account", "amount"}
previous_output_columns = {
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_winning_rule",
    "rules_engine_winning_rule_id",
    "rules_engine_error",
}
missing_columns = (previous_input_columns | previous_output_columns) - set(result.columns)
assert not missing_columns, f"Missing previously available columns: {sorted(missing_columns)}"

matched = rows["r1"]
unmatched = rows["r2"]

assert matched["record_id"] == "r1"
assert matched["account"] == "A"
assert matched["amount"] == 10
assert matched["rules_engine_matched"] is True
assert matched["rules_engine_matched_rule_ids"] == ["legacy_rule"]
assert matched["rules_engine_assign"] == {"bucket": "legacy_a"}
assert matched["rules_engine_winning_rule"]["rule_id"] == "legacy_rule"
assert matched["rules_engine_winning_rule"]["matched"] is True
assert matched["rules_engine_error"] is None

assert unmatched["record_id"] == "r2"
assert unmatched["account"] == "B"
assert unmatched["amount"] == 20
assert unmatched["rules_engine_matched"] is False
assert unmatched["rules_engine_matched_rule_ids"] == []
assert unmatched["rules_engine_assign"] is None
assert unmatched["rules_engine_winning_rule"] is None
assert unmatched["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime input columns and native output columns remain present and usable.")

# COMMAND ----------
print("ST-054: Spark runtime emits mapping literal assignments as nested structs")
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
ruleset_id: st_054_{stamp}
ruleset_name: ST-054 Struct Assignment Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
print("ST-055: Spark runtime winning-rule explanations use service-style boolean logic")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: Winning-rule explanations use author-facing service syntax while trace structs retain evaluated values.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_055_{stamp}
ruleset_name: ST-055 Winning Explanation Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
        - any:
            - condition_id: c_market_value
              left:
                field: market_value
              operator: eq
              right:
                literal: true
              null_input_mode: propagate
              null_result_mode: "null"
            - condition_id: c_book_value
              left:
                field: book_value
              operator: eq
              right:
                literal: true
              null_input_mode: propagate
              null_result_mode: "null"
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
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
row = result.collect()[0].asDict(recursive=True)
service_logic = service.describe_rules(ruleset_name=ruleset.ruleset_name, version=ruleset.version)[0]["rule_logic"]

assert row["rules_engine_matched"] is True
assert row["rules_engine_winning_rule_explanation"] == service_logic
assert row["rules_engine_winning_rule_explanation"] == (
    "record_type == 'asset' AND (market_value == true OR book_value == true)"
)
conditions = row["rules_engine_winning_rule"]["conditions"]
assert conditions[0]["left"]["value"] == "asset"
assert conditions[1]["left"]["value"] == "True"
assert conditions[2]["left"]["value"] == "True"

display(result)
print("PASS: Spark runtime winning-rule explanations matched service-style boolean logic.")

# COMMAND ----------
print("ST-056: Spark runtime ignores inactive rule assignment schemas")
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
ruleset_id: st_056_{stamp}
ruleset_name: ST-056 Active Assignment Schema Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
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
assert row["rules_engine_winning_rule_id"] == "active_struct_rule"
assert row["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime ignored inactive rule assignment schemas.")

# COMMAND ----------
print("ST-057: Spark runtime merges continued multi-match assignments")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: High")
print("Owner Role: Engineering")
print("Expected Result: stop_on_match=false keeps all matched IDs, uses last-writer-wins assignment values, and falls back to string for incompatible same-target assignment types.")
print("")

from datetime import datetime, timezone

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_057_{stamp}
ruleset_name: ST-057 Continued Match Ruleset
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
          null_input_mode: propagate
          null_result_mode: "null"
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
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: second
      review_status: follow_up
      review_result:
        literal:
          market_value: true
          book_value: false
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame([{"record_id": "r1", "account": "A"}])
result = service.evaluate_dataframe(df, ruleset_name=ruleset.ruleset_name, version=ruleset.version)
row = result.collect()[0].asDict(recursive=True)
assign_schema = result.schema["rules_engine_assign"].dataType

assert assign_schema["review_result"].dataType.simpleString() == "string"
assert row["rules_engine_matched"] is True
assert row["rules_engine_matched_rule_ids"] == ["first_match", "second_match"]
assign = row["rules_engine_assign"]
assert assign["bucket"] == "second"
assert assign["review_status"] == "follow_up"
assert "market_value=True" in assign["review_result"]
assert "book_value=False" in assign["review_result"]
assert row["rules_engine_winning_rule_id"] == "first_match"
assert row["rules_engine_winning_rule_name"] == "First Match"
assert row["rules_engine_winning_rule_explanation"] == "account == 'A'"
assert row["rules_engine_error"] is None

display(result)
print("PASS: Spark runtime merged continued multi-match assignments.")

# COMMAND ----------
print("ST-058: Field/literal rules execute without a Python UDF")
print("-" * 80)
print("Area: Runtime Spark")
print("Priority: Critical")
print("Owner Role: Engineering")
print("Expected Result: require_native produces no Python evaluation node, assignment-only projection prunes winning-trace expressions, and native outputs remain correct.")
print("")

from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO

from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
yaml_text = f"""
ruleset_id: st_058_{stamp}
ruleset_name: ST-058 Native Spark Execution Ruleset
version: "{stamp}"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: native_rule
    rule_name: Native Rule
    rule_order: 1
    when:
      all:
        - condition_id: c_native
          left:
            field: account
          operator: eq
          right:
            literal: A
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      leaf_key: "10110"
"""
ruleset = service.publish_yaml_text(yaml_text, published_by="system-test")
df = spark.createDataFrame(
    [
        {"record_id": "matched", "account": "A"},
        {"record_id": "unmatched", "account": "B"},
    ]
)
result = service.evaluate_dataframe(
    df,
    ruleset_name=ruleset.ruleset_name,
    version=ruleset.version,
    require_native=True,
)
plan_output = StringIO()
with redirect_stdout(plan_output):
    result.explain(mode="extended")
plan_text = plan_output.getvalue().lower()
pruned_plan_output = StringIO()
with redirect_stdout(pruned_plan_output):
    result.select("rules_engine_assign.leaf_key").explain(mode="formatted")
pruned_plan_text = pruned_plan_output.getvalue().lower()
rows = {
    row["record_id"]: row.asDict(recursive=True)
    for row in result.collect()
}

assert "pythonudf" not in plan_text
assert "batchevalpython" not in plan_text
assert "arrowevalpython" not in plan_text
assert "winning_rule" not in pruned_plan_text
assert rows["matched"]["rules_engine_matched"] is True
assert rows["matched"]["rules_engine_assign"]["leaf_key"] == "10110"
assert rows["matched"]["rules_engine_winning_rule_id"] == "native_rule"
assert rows["matched"]["rules_engine_winning_rule"]["conditions"][0]["left"]["value"] == "A"
assert rows["unmatched"]["rules_engine_matched"] is False
assert rows["unmatched"]["rules_engine_assign"] is None
assert rows["unmatched"]["rules_engine_winning_rule"] is None

display(result)
print("PASS: Field/literal ruleset executed through a native Spark plan.")

# COMMAND ----------
print("Run automated unit test suite")
print("-" * 80)
print("Purpose: Execute the repository pytest suite.")
print("")

import os
from pathlib import Path
import sys
import warnings

import pytest

configured_repo_root = globals().get("RULES_ENGINE_REPO_ROOT")
if configured_repo_root:
    REPO_ROOT = Path(configured_repo_root).expanduser().resolve()
else:
    search_start = Path(os.getcwd()).resolve()
    candidates = [search_start, *search_start.parents]
    REPO_ROOT = next(
        (
            candidate
            for candidate in candidates
            if (candidate / "tests").exists() and (candidate / "pyproject.toml").exists()
        ),
        search_start,
    )

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

sys.path.insert(0, str(REPO_ROOT))
src_path = REPO_ROOT / "src"
bundle_src_path = REPO_ROOT / "rules_engine_bundle" / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))
if bundle_src_path.exists():
    sys.path.insert(0, str(bundle_src_path))

previous_cwd = Path(os.getcwd())
os.chdir(str(REPO_ROOT))
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["RULES_ENGINE_RUN_SPARK_TESTS"] = globals().get(
    "RULES_ENGINE_RUN_SPARK_TESTS",
    os.environ.get("RULES_ENGINE_RUN_SPARK_TESTS", "1"),
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
