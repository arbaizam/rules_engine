# Databricks notebook source
print("UAT-001: Business owner can identify the ruleset from YAML metadata")
print("-" * 80)
print("Area: Business authoring")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Metadata matches the intended business rule set and release candidate.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

print(f"Ruleset Name:       {row['ruleset_name']}")
print(f"Version:            {row['version']}")
print(f"Status:             {row['status']}")
print(f"Owner:              {row['owner']}")
print(f"Owner Department:   {row['owner_department']}")
print(f"Effective Start:    {row['effective_start_date']}")
print(f"Effective End:      {row['effective_end_date']}")

assert row["status"] == "published", f"Expected published status, found {row['status']}."
assert row["owner"], "Expected owner to be visible in metadata."
assert row["owner_department"], "Expected owner_department to be visible in metadata."
assert loaded.ruleset_name == RULESET_NAME
assert loaded.version == RULESET_VERSION

print("PASS: Published metadata identifies the UAT candidate.")
print("Business review prompt: Confirm name, version, owner, department, and dates are the intended candidate.")

# COMMAND ----------
print("UAT-002: Rules listed in YAML match approved business scenarios")
print("-" * 80)
print("Area: Business authoring")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Rules correspond to approved business logic and no expected rule is missing.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

EXPECTED_RULE_NAMES = globals().get("UAT_EXPECTED_RULE_NAMES")
actual_rule_names = [rule.rule_name for rule in loaded.rules]

print("Published Rule Names")
print("--------------------")
for name in actual_rule_names:
    print(f"- {name}")

assert actual_rule_names, "Expected at least one rule in the UAT ruleset."
assert row["rule_count"] == len(loaded.rules), (
    f"Expected persisted rule_count {row['rule_count']} to equal loaded rule count {len(loaded.rules)}."
)
if EXPECTED_RULE_NAMES is not None:
    missing = set(EXPECTED_RULE_NAMES) - set(actual_rule_names)
    assert not missing, f"Missing expected business rules: {sorted(missing)}"

print("PASS: Published rules are visible for business review.")
print("Business review prompt: Confirm these rule names match approved scenarios.")

# COMMAND ----------
print("UAT-003: Assignments produced by matching rules are business-meaningful")
print("-" * 80)
print("Area: Business authoring")
print("Priority: High")
print("Owner Role: Business Owner")
print("Expected Result: Assignment fields and values match business expectations and downstream consumers understand them.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

EXPECTED_ASSIGNMENT_FIELDS = globals().get("UAT_EXPECTED_ASSIGNMENT_FIELDS")
assignment_fields = sorted({assignment.target_field for rule in loaded.rules for assignment in rule.assignments})

print("Assignment Fields")
print("-----------------")
for field_name in assignment_fields:
    print(f"- {field_name}")

assert assignment_fields, "Expected at least one assignment field in the UAT ruleset."
if EXPECTED_ASSIGNMENT_FIELDS is not None:
    missing = set(EXPECTED_ASSIGNMENT_FIELDS) - set(assignment_fields)
    assert not missing, f"Missing expected assignment fields: {sorted(missing)}"

print("PASS: Assignment fields are present for downstream/business review.")
print("Business review prompt: Confirm assignment fields and values are meaningful to consumers.")

# COMMAND ----------
print("UAT-004: Rule order and stop-on-match behavior match business priority")
print("-" * 80)
print("Area: Business authoring")
print("Priority: High")
print("Owner Role: Business Owner")
print("Expected Result: The chosen rule order and stop behavior produce the intended winning assignment.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

rule_summary = [(rule.rule_order, rule.rule_name, rule.stop_on_match) for rule in loaded.rules]
print("Rule Priority Order")
print("-------------------")
for rule_order, rule_name, stop_on_match in rule_summary:
    print(f"{rule_order}: {rule_name} | stop_on_match={stop_on_match}")

assert rule_summary, "Expected rules to review for priority order."
assert [item[0] for item in rule_summary] == sorted(item[0] for item in rule_summary), (
    "Expected loaded rules to be ordered by rule_order."
)

print("PASS: Rule order and stop_on_match settings are visible and ordered.")
print("Business review prompt: Confirm this priority order matches business intent.")

# COMMAND ----------
print("UAT-005: Validation errors are understandable to a non-engineering rule author")
print("-" * 80)
print("Area: Validation messaging")
print("Priority: High")
print("Owner Role: UAT Tester")
print("Expected Result: The message identifies what is wrong and where the author should fix the YAML.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

invalid_yaml = """
ruleset_id: uat_invalid
ruleset_name: UAT Invalid Ruleset
version: "1"
rules:
  - rule_id: r1
    rule_name: Missing Owner Rule
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

ruleset = service.compile_yaml_text(invalid_yaml)
validation = service.validator.validate(ruleset)
message = validation.to_text()
print(message)

assert validation.has_errors(), "Expected validation errors for intentionally invalid UAT ruleset."
assert "RULESET_OWNER_REQUIRED" in message
assert "RULESET_OWNER_DEPARTMENT_REQUIRED" in message

print("PASS: Validation message identifies missing business ownership metadata.")
print("Business review prompt: Confirm this message is understandable to rule authors.")

# COMMAND ----------
print("UAT-006: Validated release candidate can be approved for publishing")
print("-" * 80)
print("Area: Publish readiness")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: No blocking errors are present. Any warnings are reviewed and accepted before publish.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

validation = service.validator.validate(loaded)
print(validation.to_text())

assert not validation.has_errors(), validation.to_text()
assert row["status"] == "published"
assert row["content_hash"], "Expected content_hash for approved candidate."

print("PASS: Release candidate has no blocking validation errors.")
print("Business review prompt: Confirm this candidate is ready for approval.")

# COMMAND ----------
print("UAT-007: Published version is visible and identifiable in metadata")
print("-" * 80)
print("Area: Publish readiness")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Business owner can see the expected ruleset_name, version, status, owner, published_by, and published_at.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

for field_name in ["ruleset_name", "version", "status", "owner", "published_by", "published_at"]:
    print(f"{field_name}: {row[field_name]}")
    assert row[field_name], f"Expected {field_name} to be populated."
assert row["status"] == "published"

print("PASS: Published version metadata is complete for review.")
print("Business review prompt: Confirm published_by and published_at are acceptable release evidence.")

# COMMAND ----------
print("UAT-008: Multiple published versions can coexist for comparison testing")
print("-" * 80)
print("Area: Version testing")
print("Priority: High")
print("Owner Role: Business Owner")
print("Expected Result: Both versions are visible and testers can choose the intended version explicitly.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

version_rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND status = 'published'"
).select("version", "effective_start_date", "effective_end_date").collect()

print("Published Versions")
print("------------------")
for version_row in version_rows:
    print(f"{version_row['version']} | {version_row['effective_start_date']} to {version_row['effective_end_date']}")

assert len(version_rows) >= 1, "Expected at least one published version for UAT target."
assert RULESET_VERSION in {version_row["version"] for version_row in version_rows}

print("PASS: Published versions are visible for comparison testing.")
print("Business review prompt: Confirm testers know which version to evaluate.")

# COMMAND ----------
print("UAT-009: Testers understand that version is required when multiple published versions exist")
print("-" * 80)
print("Area: Version testing")
print("Priority: High")
print("Owner Role: UAT Tester")
print("Expected Result: Name-only load reports ambiguity when multiple versions exist. Explicit version load succeeds.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

version_count = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND status = 'published'"
).count()

explicit = service.load_published(RULESET_NAME, version=RULESET_VERSION)
assert explicit.version == RULESET_VERSION

print(f"Published version count for {RULESET_NAME}: {version_count}")
if version_count > 1:
    from rules_engine.exceptions import RepositoryError
    try:
        service.load_published(RULESET_NAME)
        ambiguous_failed = False
    except RepositoryError:
        ambiguous_failed = True
    assert ambiguous_failed, "Expected name-only load to fail when multiple versions are published."
    print("Name-only load correctly failed because version is required.")
else:
    print("Only one published version exists; explicit version load was verified.")

print("PASS: Explicit version loading is proven for UAT target.")
print("Business review prompt: Confirm test instructions require explicit version when multiple versions exist.")

# COMMAND ----------
print("UAT-010: Representative business records receive expected rule outcomes")
print("-" * 80)
print("Area: Runtime results")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Each sampled record has expected matched status, matched rule, and assignment output.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

UAT_INPUT_ROWS = globals().get("UAT_INPUT_ROWS")
UAT_INPUT_TABLE = globals().get("UAT_INPUT_TABLE")
if not UAT_INPUT_ROWS and UAT_INPUT_TABLE:
    UAT_INPUT_ROWS = [
        row.asDict(recursive=True)
        for row in spark.table(UAT_INPUT_TABLE).collect()
    ]

assert UAT_INPUT_ROWS, (
    "Set UAT_INPUT_ROWS to a non-empty list of dictionaries with a record_id field, "
    "or set UAT_INPUT_TABLE to a Spark table containing the UAT records."
)
assert all("record_id" in item for item in UAT_INPUT_ROWS), "Every UAT_INPUT_ROWS item must include record_id."

result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}
assert len(results_by_id) == len(UAT_INPUT_ROWS), "Expected one output row per UAT input row."

UAT_EXPECTED_MATCHES = globals().get("UAT_EXPECTED_MATCHES")
assert UAT_EXPECTED_MATCHES, (
    "Set UAT_EXPECTED_MATCHES as {record_id: expected_boolean}. Example: "
    "UAT_EXPECTED_MATCHES = {'case_001': True, 'case_002': False}"
)

for record_id, expected_matched in UAT_EXPECTED_MATCHES.items():
    assert record_id in results_by_id, f"Missing result for record_id={record_id}."
    actual_matched = results_by_id[record_id]["rules_engine_matched"]
    print(f"{record_id}: expected_matched={expected_matched}, actual_matched={actual_matched}")
    assert actual_matched == expected_matched, (
        f"Expected matched={expected_matched} for record_id={record_id}, found {actual_matched}."
    )

print("PASS: Representative business records matched expected outcomes.")
print("Business review prompt: Confirm the sampled outcomes are acceptable evidence.")

# COMMAND ----------
print("UAT-011: Non-matching records remain unassigned or assigned as expected")
print("-" * 80)
print("Area: Runtime results")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Outputs match the intended non-match behavior without unexpected assignments.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

UAT_INPUT_ROWS = globals().get("UAT_INPUT_ROWS")
UAT_INPUT_TABLE = globals().get("UAT_INPUT_TABLE")
if not UAT_INPUT_ROWS and UAT_INPUT_TABLE:
    UAT_INPUT_ROWS = [
        row.asDict(recursive=True)
        for row in spark.table(UAT_INPUT_TABLE).collect()
    ]

assert UAT_INPUT_ROWS, (
    "Set UAT_INPUT_ROWS to a non-empty list of dictionaries with a record_id field, "
    "or set UAT_INPUT_TABLE to a Spark table containing the UAT records."
)
assert all("record_id" in item for item in UAT_INPUT_ROWS), "Every UAT_INPUT_ROWS item must include record_id."

result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}
assert len(results_by_id) == len(UAT_INPUT_ROWS), "Expected one output row per UAT input row."

UAT_EXPECTED_NON_MATCH_IDS = globals().get("UAT_EXPECTED_NON_MATCH_IDS")
assert UAT_EXPECTED_NON_MATCH_IDS, (
    "Set UAT_EXPECTED_NON_MATCH_IDS as a list of record_id values expected not to match. "
    "Example: UAT_EXPECTED_NON_MATCH_IDS = ['case_002', 'case_004']"
)

for record_id in UAT_EXPECTED_NON_MATCH_IDS:
    assert record_id in results_by_id, f"Missing result for record_id={record_id}."
    result = results_by_id[record_id]
    print(f"{record_id}: matched={result['rules_engine_matched']}, assign={result['rules_engine_assign']}")
    assert result["rules_engine_matched"] is False, f"Expected record_id={record_id} not to match."
    assert result["rules_engine_assign"] in (None, "{}"), (
        f"Expected no assignment for record_id={record_id}, found {result['rules_engine_assign']}."
    )

print("PASS: Non-matching records remained unassigned.")
print("Business review prompt: Confirm non-match behavior is business-approved.")

# COMMAND ----------
print("UAT-012: Boundary values produce expected outcomes")
print("-" * 80)
print("Area: Runtime results")
print("Priority: High")
print("Owner Role: Business Owner")
print("Expected Result: Boundary records follow approved business semantics.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")
UAT_BOUNDARY_ROWS = globals().get("UAT_BOUNDARY_ROWS")
UAT_BOUNDARY_EXPECTED_MATCHES = globals().get("UAT_BOUNDARY_EXPECTED_MATCHES")

assert SCHEMA and RULESET_NAME and RULESET_VERSION, "Set RULES_ENGINE_SCHEMA, UAT_RULESET_NAME, and UAT_RULESET_VERSION."
assert UAT_BOUNDARY_ROWS, "Set UAT_BOUNDARY_ROWS to boundary input records with record_id."
assert UAT_BOUNDARY_EXPECTED_MATCHES, "Set UAT_BOUNDARY_EXPECTED_MATCHES as {record_id: expected_boolean}."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_BOUNDARY_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}

for record_id, expected_matched in UAT_BOUNDARY_EXPECTED_MATCHES.items():
    actual_matched = results_by_id[record_id]["rules_engine_matched"]
    print(f"{record_id}: expected={expected_matched}, actual={actual_matched}")
    assert actual_matched == expected_matched

print("PASS: Boundary values produced expected outcomes.")
print("Business review prompt: Confirm threshold/null/text edge cases are approved.")

# COMMAND ----------
print("UAT-013: Aggregate-based business rules match manual calculations")
print("-" * 80)
print("Area: Runtime results")
print("Priority: High")
print("Owner Role: Business Owner")
print("Expected Result: Engine results agree with manual calculations for sampled groups or datasets.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")
UAT_AGGREGATE_ROWS = globals().get("UAT_AGGREGATE_ROWS")
UAT_AGGREGATE_EXPECTED_MATCHES = globals().get("UAT_AGGREGATE_EXPECTED_MATCHES")

assert SCHEMA and RULESET_NAME and RULESET_VERSION, "Set RULES_ENGINE_SCHEMA, UAT_RULESET_NAME, and UAT_RULESET_VERSION."
assert UAT_AGGREGATE_ROWS, "Set UAT_AGGREGATE_ROWS to aggregate sample records with record_id."
assert UAT_AGGREGATE_EXPECTED_MATCHES, "Set UAT_AGGREGATE_EXPECTED_MATCHES as {record_id: expected_boolean}."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_AGGREGATE_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}

for record_id, expected_matched in UAT_AGGREGATE_EXPECTED_MATCHES.items():
    actual_matched = results_by_id[record_id]["rules_engine_matched"]
    print(f"{record_id}: expected={expected_matched}, actual={actual_matched}")
    assert actual_matched == expected_matched

print("PASS: Aggregate-driven outcomes matched manual expectations.")
print("Business review prompt: Confirm manual aggregate calculations support these outcomes.")

# COMMAND ----------
print("UAT-014: Standard text/number transformations produce recognizable business outcomes")
print("-" * 80)
print("Area: Custom functions")
print("Priority: Medium")
print("Owner Role: Business Owner")
print("Expected Result: Function-driven outcomes match what business users expect from the source data.")
print("")

import json
from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")
UAT_FUNCTION_ROWS = globals().get("UAT_FUNCTION_ROWS")
UAT_FUNCTION_EXPECTED_ASSIGNMENTS = globals().get("UAT_FUNCTION_EXPECTED_ASSIGNMENTS")

assert SCHEMA and RULESET_NAME and RULESET_VERSION, "Set RULES_ENGINE_SCHEMA, UAT_RULESET_NAME, and UAT_RULESET_VERSION."
assert UAT_FUNCTION_ROWS, "Set UAT_FUNCTION_ROWS to function-focused sample records with record_id."
assert UAT_FUNCTION_EXPECTED_ASSIGNMENTS, "Set UAT_FUNCTION_EXPECTED_ASSIGNMENTS as {record_id: expected_assign_dict}."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_FUNCTION_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}

for record_id, expected_assign in UAT_FUNCTION_EXPECTED_ASSIGNMENTS.items():
    actual_assign = json.loads(results_by_id[record_id]["rules_engine_assign"] or "{}")
    print(f"{record_id}: expected={expected_assign}, actual={actual_assign}")
    assert actual_assign == expected_assign

print("PASS: Function-driven assignment outcomes matched expectations.")
print("Business review prompt: Confirm function behavior matches source-data expectations.")

# COMMAND ----------
print("UAT-015: Setup notebook can be rerun safely before publishing")
print("-" * 80)
print("Area: Operational workflow")
print("Priority: High")
print("Owner Role: UAT Tester")
print("Expected Result: Notebook succeeds and does not duplicate or overwrite standard function metadata unexpectedly.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

service.create_tables(mode="ignore")
before = spark.table(service.table_names.function_registry).where("active_flag = true").count()
service.save_standard_function_registry(update_existing=False)
after = spark.table(service.table_names.function_registry).where("active_flag = true").count()

duplicates = spark.sql(f"""
    SELECT function_name, COUNT(*) AS row_count
    FROM {service.table_names.function_registry}
    GROUP BY function_name
    HAVING COUNT(*) > 1
""").collect()

assert after >= before
assert len(duplicates) == 0, f"Expected no duplicate function registry rows, found {duplicates}."

print("PASS: Setup rerun completed without duplicate standard function metadata.")
print("Business review prompt: Confirm setup rerun behavior is acceptable for operators.")

# COMMAND ----------
print("UAT-016: Publish notebook can be rerun safely for same candidate and handles duplicate publish clearly")
print("-" * 80)
print("Area: Operational workflow")
print("Priority: High")
print("Owner Role: UAT Tester")
print("Expected Result: Notebook fails or stops with a clear duplicate version message and does not overwrite the original published row.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

from rules_engine.exceptions import RepositoryError

original_published_by = row["published_by"]
try:
    service.publish(loaded, published_by="uat-duplicate-attempt")
    duplicate_failed = False
except RepositoryError:
    duplicate_failed = True

rerun_rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert duplicate_failed, "Expected duplicate publish rerun to fail clearly."
assert len(rerun_rows) == 1
assert rerun_rows[0]["published_by"] == original_published_by

print("PASS: Duplicate publish was rejected and original row was preserved.")
print("Business review prompt: Confirm duplicate publish behavior is clear enough for UAT operators.")

# COMMAND ----------
print("UAT-017: Retirement workflow removes a version from runtime eligibility")
print("-" * 80)
print("Area: Operational workflow")
print("Priority: High")
print("Owner Role: Release Manager")
print("Expected Result: The version status changes to retired and cannot be loaded through published-only load calls.")
print("")

from datetime import datetime, timezone
from rules_engine import RulesEngineService
from rules_engine.exceptions import RepositoryError

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

yaml_text = f"""
ruleset_id: uat_retire_{stamp}
ruleset_name: UAT Retirement Test {stamp}
version: "1"
status: published
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Retirement Test Rule
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

ruleset = service.publish_yaml_text(yaml_text, published_by="uat-test")
service.retire(ruleset.ruleset_id, ruleset.version, retired_by="uat-test")
row = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_id = '{ruleset.ruleset_id}' AND version = '{ruleset.version}'"
).collect()[0]

try:
    service.load_published(ruleset.ruleset_name, version=ruleset.version)
    load_failed = False
except RepositoryError:
    load_failed = True

assert row["status"] == "retired"
assert load_failed, "Expected retired version not to load as published."

print("PASS: Retirement workflow removed test version from published eligibility.")
print("Business review prompt: Confirm retirement behavior meets release-management expectations.")

# COMMAND ----------
print("UAT-018: Release evidence is sufficient for audit review")
print("-" * 80)
print("Area: Operational workflow")
print("Priority: Medium")
print("Owner Role: Release Manager")
print("Expected Result: Release evidence is complete enough to support approval and later audit.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

for field_name in ["payload_json", "content_hash", "rule_count", "condition_count", "assignment_count", "published_by", "published_at"]:
    print(f"{field_name}: {row[field_name]}")
    assert row[field_name] is not None, f"Expected {field_name} to be populated."
assert row["rule_count"] >= 1
assert row["condition_count"] >= 1
assert row["assignment_count"] >= 1

print("PASS: Release evidence fields are populated for audit review.")
print("Business review prompt: Confirm notebook run links and approvals are stored with release evidence.")

# COMMAND ----------
print("UAT-019: Output DataFrame schema is usable by downstream jobs")
print("-" * 80)
print("Area: Downstream readiness")
print("Priority: High")
print("Owner Role: Data Consumer")
print("Expected Result: Column names, JSON fields, matched flags, and error field are understood and usable.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

UAT_INPUT_ROWS = globals().get("UAT_INPUT_ROWS")
assert UAT_INPUT_ROWS, "Set UAT_INPUT_ROWS to a non-empty list of dictionaries with a record_id field."
assert all("record_id" in item for item in UAT_INPUT_ROWS), "Every UAT_INPUT_ROWS item must include record_id."

result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()
results_by_id = {row["record_id"]: row.asDict(recursive=True) for row in result_rows}
assert len(results_by_id) == len(UAT_INPUT_ROWS), "Expected one output row per UAT input row."

required_columns = {
    "rules_engine_matched",
    "rules_engine_matched_rule_ids",
    "rules_engine_assign",
    "rules_engine_rule_results",
    "rules_engine_winning_rule",
    "rules_engine_winning_rule_id",
    "rules_engine_winning_rule_name",
    "rules_engine_winning_rule_explanation",
    "rules_engine_error",
}
actual_columns = set(service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).columns)
missing = required_columns - actual_columns
assert not missing, f"Missing expected runtime output columns: {sorted(missing)}"

print("PASS: Runtime output schema contains expected downstream columns.")
print("Business review prompt: Confirm downstream consumers can use these output columns.")

# COMMAND ----------
print("UAT-020: Assignment JSON can be parsed by downstream Spark consumers")
print("-" * 80)
print("Area: Downstream readiness")
print("Priority: High")
print("Owner Role: Data Consumer")
print("Expected Result: Assignments can be parsed and consumed without manual cleanup.")
print("")

import json
from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")
UAT_INPUT_ROWS = globals().get("UAT_INPUT_ROWS")

assert SCHEMA and RULESET_NAME and RULESET_VERSION, "Set RULES_ENGINE_SCHEMA, UAT_RULESET_NAME, and UAT_RULESET_VERSION."
assert UAT_INPUT_ROWS, "Set UAT_INPUT_ROWS to a non-empty list of dictionaries with record_id."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
result_rows = service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
).collect()

parsed_count = 0
for row in result_rows:
    if row["rules_engine_assign"]:
        parsed = json.loads(row["rules_engine_assign"])
        print(f"record_id={row['record_id']}: {parsed}")
        assert isinstance(parsed, dict)
        parsed_count += 1

assert parsed_count > 0, "Expected at least one assigned row to parse."

print("PASS: Assignment JSON parsed into dictionaries for downstream use.")
print("Business review prompt: Confirm parsed assignment structure is acceptable to consumers.")

# COMMAND ----------
print("UAT-021: Runtime error handling is acceptable for business operations")
print("-" * 80)
print("Area: Exception handling")
print("Priority: Medium")
print("Owner Role: UAT Tester")
print("Expected Result: The team agrees which mode is appropriate for production and how errors will be monitored.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")
UAT_INPUT_ROWS = globals().get("UAT_INPUT_ROWS")

assert SCHEMA and RULESET_NAME and RULESET_VERSION, "Set RULES_ENGINE_SCHEMA, UAT_RULESET_NAME, and UAT_RULESET_VERSION."
assert UAT_INPUT_ROWS, "Set UAT_INPUT_ROWS to evaluate runtime error handling."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
result = service.evaluate_dataframe(
    spark.createDataFrame(UAT_INPUT_ROWS),
    ruleset_name=RULESET_NAME,
    version=RULESET_VERSION,
    fail_on_error=False,
)
error_count = result.where("rules_engine_error IS NOT NULL").count()
print(f"Runtime error row count with fail_on_error=False: {error_count}")
assert "rules_engine_error" in result.columns

print("PASS: Runtime error column is available for operational review.")
print("Business review prompt: Confirm whether fail_on_error=True or False is appropriate for production.")

# COMMAND ----------
print("UAT-022: Notebook instructions are clear enough for a new operator")
print("-" * 80)
print("Area: Documentation")
print("Priority: Medium")
print("Owner Role: UAT Tester")
print("Expected Result: Tester can complete the process using notebook instructions and parameters without engineering intervention.")
print("")

UAT_OPERATOR_CONFIRMED = globals().get("UAT_OPERATOR_CONFIRMED")
UAT_OPERATOR_NOTES = globals().get("UAT_OPERATOR_NOTES", "")

print(f"Operator confirmed: {UAT_OPERATOR_CONFIRMED}")
print(f"Operator notes: {UAT_OPERATOR_NOTES}")

assert UAT_OPERATOR_CONFIRMED is True, (
    "Set UAT_OPERATOR_CONFIRMED = True after an operator completes setup/publish without engineering intervention."
)

print("PASS: Operator confirmed notebook instructions are clear enough.")

# COMMAND ----------
print("UAT-023: Business owner approves the published candidate version")
print("-" * 80)
print("Area: Sign-off")
print("Priority: Critical")
print("Owner Role: Business Owner")
print("Expected Result: Business owner records approve/defer/reject decision with comments.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

UAT_BUSINESS_APPROVAL_STATUS = globals().get("UAT_BUSINESS_APPROVAL_STATUS")
UAT_BUSINESS_APPROVAL_COMMENTS = globals().get("UAT_BUSINESS_APPROVAL_COMMENTS")

print(f"Business approval status: {UAT_BUSINESS_APPROVAL_STATUS}")
print(f"Business approval comments: {UAT_BUSINESS_APPROVAL_COMMENTS}")

assert UAT_BUSINESS_APPROVAL_STATUS in {"approve", "defer", "reject"}, (
    "Set UAT_BUSINESS_APPROVAL_STATUS to approve, defer, or reject."
)
assert UAT_BUSINESS_APPROVAL_COMMENTS, "Set UAT_BUSINESS_APPROVAL_COMMENTS with business owner rationale."

print("PASS: Business owner decision was recorded for the UAT candidate.")

# COMMAND ----------
print("UAT-024: Release manager confirms deployment readiness")
print("-" * 80)
print("Area: Sign-off")
print("Priority: Critical")
print("Owner Role: Release Manager")
print("Expected Result: Release manager signs off or records conditions for release.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

assert SCHEMA, "Set RULES_ENGINE_SCHEMA before running UAT tests."
assert RULESET_NAME, "Set UAT_RULESET_NAME before running UAT tests."
assert RULESET_VERSION, "Set UAT_RULESET_VERSION before running UAT tests."

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)
rows = spark.table(service.table_names.ruleset_versions).where(
    f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
).collect()
assert len(rows) == 1, (
    f"Expected exactly one UAT metadata row for {RULESET_NAME} version {RULESET_VERSION}, "
    f"found {len(rows)}."
)
row = rows[0]
loaded = service.load_published(RULESET_NAME, version=RULESET_VERSION)

UAT_RELEASE_MANAGER_DECISION = globals().get("UAT_RELEASE_MANAGER_DECISION")
UAT_RELEASE_MANAGER_COMMENTS = globals().get("UAT_RELEASE_MANAGER_COMMENTS")

print(f"Release manager decision: {UAT_RELEASE_MANAGER_DECISION}")
print(f"Release manager comments: {UAT_RELEASE_MANAGER_COMMENTS}")

assert row["status"] == "published"
assert UAT_RELEASE_MANAGER_DECISION in {"ready", "not_ready", "conditional"}, (
    "Set UAT_RELEASE_MANAGER_DECISION to ready, not_ready, or conditional."
)
assert UAT_RELEASE_MANAGER_COMMENTS, "Set UAT_RELEASE_MANAGER_COMMENTS with release rationale."

print("PASS: Release manager decision was recorded for deployment readiness.")
