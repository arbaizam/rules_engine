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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Open the ruleset YAML or notebook summary and review ruleset name, version, owner, department, and description.')
print("")
print("Expected result:")
print('Metadata matches the intended business rule set and release candidate.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review the rule names, descriptions, order, and plain-English conditions for the release candidate.')
print("")
print("Expected result:")
print('Rules correspond to approved business logic and no expected rule is missing.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review target fields and assigned values for each rule.')
print("")
print("Expected result:")
print('Assignment fields and values match business expectations and downstream consumers understand them.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review scenarios where multiple rules could match the same record.')
print("")
print("Expected result:")
print('The chosen rule order and stop behavior produce the intended winning assignment.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Run validation against an intentionally invalid ruleset and review the output.')
print("")
print("Expected result:")
print('The message identifies what is wrong and where the author should fix the YAML.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Run compile and validation on the candidate ruleset.')
print("")
print("Expected result:")
print('No blocking errors are present. Any warnings are reviewed and accepted before publish.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('After publish, review the ruleset_versions row for the candidate.')
print("")
print("Expected result:")
print('Business owner can see the expected ruleset_name, version, status, owner, published_by, and published_at.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Publish current and candidate versions for the same ruleset_name in UAT.')
print("")
print("Expected result:")
print('Both versions are visible and testers can choose the intended version explicitly.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

# COMMAND ----------
print("UAT-009: Testers understand that version is required when multiple published versions exist")
print("-" * 80)
print("Area: Version testing")
print("Priority: High")
print("Owner Role: UAT Tester")
print("Expected Result: Name-only load reports ambiguity. Explicit version load succeeds.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Attempt to load by name only when multiple versions are published, then load by explicit version.')
print("")
print("Expected result:")
print('Name-only load reports ambiguity. Explicit version load succeeds.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Run the published ruleset against a curated UAT input dataset.')
print("")
print("Expected result:")
print('Each sampled record has expected matched status, matched rule, and assignment output.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Include records that should not match any rule or should follow default behavior.')
print("")
print("Expected result:")
print('Outputs match the intended non-match behavior without unexpected assignments.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Test thresholds, equality edges, nulls, blanks, and representative text/case variations.')
print("")
print("Expected result:")
print('Boundary records follow approved business semantics.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('For rules using group or dataset aggregates, compare engine outputs to manually reviewed aggregate calculations.')
print("")
print("Expected result:")
print('Engine results agree with manual calculations for sampled groups or datasets.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

# COMMAND ----------
print("UAT-014: Standard text/number transformations produce recognizable business outcomes")
print("-" * 80)
print("Area: Custom functions")
print("Priority: Medium")
print("Owner Role: Business Owner")
print("Expected Result: Function-driven outcomes match what business users expect from the source data.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review rules using functions such as trim, upper, regex_extract, or to_number on representative records.')
print("")
print("Expected result:")
print('Function-driven outcomes match what business users expect from the source data.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Rerun setup notebook after tables and function metadata already exist.')
print("")
print("Expected result:")
print('Notebook succeeds and does not duplicate or overwrite standard function metadata unexpectedly.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Rerun publish notebook for an already-published ruleset_name/version.')
print("")
print("Expected result:")
print('Notebook fails or stops with a clear duplicate version message and does not overwrite the original published row.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

# COMMAND ----------
print("UAT-017: Retirement workflow removes a version from runtime eligibility")
print("-" * 80)
print("Area: Operational workflow")
print("Priority: High")
print("Owner Role: Release Manager")
print("Expected Result: The version status changes to retired and cannot be loaded through published-only load calls.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Retire a test version that should no longer be used.')
print("")
print("Expected result:")
print('The version status changes to retired and cannot be loaded through published-only load calls.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review publish metadata, content hash, payload_json availability, validation output, and notebook run links.')
print("")
print("Expected result:")
print('Release evidence is complete enough to support approval and later audit.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review Spark runtime output columns with a downstream consumer or integration owner.')
print("")
print("Expected result:")
print('Column names, JSON fields, matched flags, and error field are understood and usable.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

# COMMAND ----------
print("UAT-020: Assignment JSON can be parsed by downstream Spark consumers")
print("-" * 80)
print("Area: Downstream readiness")
print("Priority: High")
print("Owner Role: Data Consumer")
print("Expected Result: Assignments can be parsed and consumed without manual cleanup.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Parse the assign output using downstream expected schema or from_json logic.')
print("")
print("Expected result:")
print('Assignments can be parsed and consumed without manual cleanup.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review behavior when fail_on_error is true and false using a controlled bad input scenario.')
print("")
print("Expected result:")
print('The team agrees which mode is appropriate for production and how errors will be monitored.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

# COMMAND ----------
print("UAT-022: Notebook instructions are clear enough for a new operator")
print("-" * 80)
print("Area: Documentation")
print("Priority: Medium")
print("Owner Role: UAT Tester")
print("Expected Result: Tester can complete the process using notebook instructions and parameters without engineering intervention.")
print("")

from rules_engine import RulesEngineService

SCHEMA = globals().get("RULES_ENGINE_SCHEMA")
RULESET_NAME = globals().get("UAT_RULESET_NAME")
RULESET_VERSION = globals().get("UAT_RULESET_VERSION")

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Ask a tester who did not write the notebooks to run setup and publish in UAT.')
print("")
print("Expected result:")
print('Tester can complete the process using notebook instructions and parameters without engineering intervention.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review validation output, UAT outputs, and open defects.')
print("")
print("Expected result:")
print('Business owner records approve/defer/reject decision with comments.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")

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

service = RulesEngineService.from_schema(spark=spark, schema=SCHEMA)

print("Execution steps:")
print('Review system test status, UAT status, defect disposition, and rollback/retirement plan.')
print("")
print("Expected result:")
print('Release manager signs off or records conditions for release.')
print("")

if RULESET_NAME and RULESET_VERSION:
    rows = spark.table(service.table_names.ruleset_versions).where(
        f"ruleset_name = '{RULESET_NAME}' AND version = '{RULESET_VERSION}'"
    ).collect()
    print(f"Published metadata rows for UAT target: {len(rows)}")
    if rows:
        row = rows[0]
        print(f"Status: {row['status']}")
        print(f"Effective Start Date: {row['effective_start_date']}")
        print(f"Effective End Date: {row['effective_end_date']}")
else:
    print("Set UAT_RULESET_NAME and UAT_RULESET_VERSION to display target published metadata.")

print("")
print("Business review prompt: confirm the expected result and record evidence in the Results Log.")
