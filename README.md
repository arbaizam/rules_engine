# Rules Engine

`rules_engine` is a strict, metadata-first Python rules engine designed for
Databricks workflows that require deterministic behavior, explicit metadata,
and audit-ready persistence.

The package compiles canonical YAML or code-authored dataclasses into immutable
ruleset models, validates them, normalizes them into publish-ready metadata,
persists them to Spark/Delta tables, and evaluates them against Python row
sets or Spark DataFrames.

The design intentionally avoids aliases, expression DSLs, raw persisted
lambdas, hidden runtime defaults, implicit aggregate behavior, and silent
semantic weakening.

## Contents

- [Who This Is For](#who-this-is-for)
- [Core Concepts](#core-concepts)
- [Package Layout](#package-layout)
- [Semantic Contract](#semantic-contract)
- [Authoring YAML](#authoring-yaml)
- [Compile, Validate, Normalize](#compile-validate-normalize)
- [YAML Export And Round Trip](#yaml-export-and-round-trip)
- [Custom Function Registry](#custom-function-registry)
- [Pure-Python Runtime](#pure-python-runtime)
- [Spark Runtime](#spark-runtime)
- [Spark Compatibility Validation](#spark-compatibility-validation)
- [Spark/Delta Repository](#sparkdelta-repository)
- [Publish Lifecycle](#publish-lifecycle)
- [Auditability Model](#auditability-model)
- [Reconciliation CSV Translation Utility](#reconciliation-csv-translation-utility)
- [Databricks Smoke Test](#databricks-smoke-test)
- [Testing](#testing)
- [Developer Workflow](#developer-workflow)
- [Troubleshooting](#troubleshooting)
- [Known Limitations](#known-limitations)

## Who This Is For

This package is intended for engineers building governed rule execution
workflows where rules must be:

- authored in a readable external format,
- validated before execution,
- persisted as queryable metadata,
- executed deterministically,
- auditable after the fact,
- promoted through controlled lifecycle states.

The primary target runtime is Databricks Spark. The pure-Python runtime exists
as a reference/runtime utility for small row sets, unit tests, and semantic
parity checks.

## Core Concepts

### Ruleset

A `Ruleset` is the top-level metadata object. It has:

- `ruleset_id`
- `ruleset_name`
- `version`
- `status`
- optional `description`
- ordered `rules`

Lifecycle statuses are exactly:

```text
draft
published
retired
```

Runtime execution should use published metadata.

### Rule

A `Rule` contains:

- `rule_id`
- `rule_name`
- `rule_order`
- `root_group`
- `assignments`
- `active_flag`
- `stop_on_match`
- optional `description`

Rules are evaluated in `rule_order`. If a rule matches and `stop_on_match` is
`true`, later rules are not evaluated for that row.

### Condition Group

A condition group is a logical tree node with exactly one logical operator:

```text
all
any
```

Groups may contain conditions and nested groups. Empty groups are invalid.

### Condition

A condition compares a left operand to an optional right operand using a
canonical operator. Null behavior and tolerance are explicit metadata fields.

### Operand

Operands are one of:

```text
field
literal
aggregate
custom_function
```

No operand aliases are accepted.

### Assignment

Assignments are emitted when a rule matches. Assignment values may be literals,
fields, or custom functions. Aggregate assignments are forbidden in v1.

## Package Layout

```text
rules_engine/
  __init__.py
  compiler_yaml.py        # YAML -> dataclasses
  enums.py                # canonical vocabulary
  exceptions.py           # package exceptions
  exporter_yaml.py        # dataclasses -> canonical YAML
  models.py               # domain and Delta row dataclasses
  normalizer.py           # publish-ready explicit metadata
  publish.py              # lifecycle orchestration
  registry.py             # custom function registry
  repository.py           # Spark/Delta metadata repository
  runtime.py              # pure-Python evaluator
  serializer.py           # dataclasses <-> Delta rows
  spark_runtime.py        # Spark DataFrame runtime
  spark_validator.py      # Spark compatibility validator
  validator.py            # semantic validator

databricks/
  smoke_test_rules_engine.py

notebooks/
  rules_engine_developer_guide.py

tools/
  recon_spec_translation/
    audit.py
    models.py
    normalizer.py
    reader_csv.py
    translator.py
    writer_yaml.py

tests/
```

## Semantic Contract

The following rules are intentional and must not be relaxed without a design
review.

- Canonical vocabulary only. No aliases.
- YAML authoring is supported.
- Code-based authoring is supported through dataclasses.
- Published metadata must be fully resolved and explicit.
- Delta metadata is shaped for queryability.
- Runtime reads published metadata.
- Aggregate conditions are first-class operands.
- Aggregate scope is required and explicit.
- Aggregate scopes are exactly `group` and `dataset`.
- `scope=group` requires non-empty `by`.
- `scope=dataset` forbids `by`.
- Aggregates operate on the incoming row set exactly as provided.
- There is no implicit deduplication, reshaping, filtering, ordering, or
  cross-run state.
- Filtered aggregates are supported.
- Nested aggregates are forbidden.
- Collection-returning aggregates are not supported.
- Window/analytic aggregates are not part of v1.
- Null handling is explicit per condition and aggregate.
- Tolerance is absolute only.
- Omitted tolerance is normalized and persisted as `0`.
- Custom logic is allowed only through `FunctionRegistry`.
- Raw Python lambda persistence is not supported.
- Pre-publish and publish-time validation are mandatory.

## Supported Operators

Comparison operators:

```text
eq
ne
gt
ge
lt
le
in
not_in
between
not_between
like
not_like
contains
not_contains
starts_with
ends_with
is_null
is_not_null
```

String operators use canonical underscore-form values:

```text
contains
not_contains
starts_with
ends_with
```

`like` and `not_like` use SQL wildcard semantics in both the Python and Spark
runtimes.

Unary operators:

```text
is_null
is_not_null
```

Unary operators must not define a right operand. All other operators require a
right operand.

## Supported Aggregates

Aggregate functions:

```text
sum
mean
min
max
count
count_distinct
quantile
median
stddev
variance
first
last
```

Important aggregate rules:

- `quantile` requires `args.q` within `[0, 1]`.
- `first` and `last` require explicit `order_by`.
- `scope=group` requires `by`.
- `scope=dataset` forbids `by`.
- Aggregate filters may contain only row-level predicates.
- Nested aggregates are invalid.
- Aggregate assignments are invalid.
- Spark currently fails fast for `median` and `quantile` because Spark's
  available approximation functions do not match the exact Python semantics.

## Authoring YAML

### Minimal Row Rule

```yaml
ruleset_id: account_rules
ruleset_name: Account Rules
version: "1"
status: draft
rules:
  - rule_id: trad_account
    rule_name: TRAD Account
    rule_order: 1
    active_flag: true
    stop_on_match: false
    when:
      all:
        - left: { field: account_type }
          operator: eq
          right: { literal: TRAD, value_type: string }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket: trad
```

### Group Aggregate Rule

```yaml
ruleset_id: account_rules
ruleset_name: Account Rules
version: "1"
status: draft
rules:
  - rule_id: high_value_account
    rule_name: High Value Account
    rule_order: 1
    when:
      all:
        - left:
            aggregate:
              function: sum
              field: amount
              scope: group
              by: [account_id]
              args: {}
              order_by: []
              null_input_mode: ignore
              null_result_mode: "null"
          operator: gt
          right: { literal: 1000000, value_type: number }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      review_bucket: high_value
```

### Filtered Aggregate Rule

```yaml
left:
  aggregate:
    function: sum
    field: amount
    scope: dataset
    args: {}
    order_by: []
    filter:
      all:
        - left: { field: status }
          operator: eq
          right: { literal: OPEN, value_type: string }
          tolerance_abs: "0"
          null_input_mode: propagate
          null_result_mode: "null"
    null_input_mode: ignore
    null_result_mode: "null"
operator: gt
right: { literal: 0, value_type: number }
tolerance_abs: "0"
null_input_mode: propagate
null_result_mode: "null"
```

### Custom Function Operand

```yaml
left:
  custom_function:
    name: score
    args:
      x: 2
      y: 3
operator: eq
right: { literal: 5, value_type: number }
tolerance_abs: "0"
null_input_mode: propagate
null_result_mode: "null"
```

## Compile, Validate, Normalize

Use `YamlRulesetCompiler` to compile YAML into dataclasses.

```python
from rules_engine import YamlRulesetCompiler, RulesetValidator

compiler = YamlRulesetCompiler()
ruleset = compiler.compile_path("rules.yaml")

validation = RulesetValidator().validate(ruleset)
if validation.has_errors():
    raise ValueError(validation.to_text())
```

Use `RulesetNormalizer` to materialize runtime/persistence-ready defaults.

```python
from rules_engine import RulesetNormalizer

normalized = RulesetNormalizer().normalize_ruleset(ruleset)
```

Normalization makes implicit authoring omissions explicit, including
`tolerance_abs = 0`, aggregate `args`, `by`, and `order_by` payloads.

## YAML Export And Round Trip

Rulesets can be exported back to canonical YAML for review, source control, or
governance workflows.

```python
from rules_engine import YamlRulesetCompiler, YamlRulesetExporter

ruleset = YamlRulesetCompiler().compile_path("rules.yaml")
yaml_text = YamlRulesetExporter().export_text(ruleset)
YamlRulesetExporter().export_path(ruleset, "rules_exported.yaml")
```

The exporter writes explicit rule, condition-group, condition, and assignment
identifiers so exported YAML can compile back into the same canonical
dataclasses.

## Custom Function Registry

Custom functions are executable code referenced by metadata. The repository
can persist the function specification, but it does not persist executable
Python callables.

```python
from rules_engine import CustomFunctionSpec, FunctionRegistry

registry = FunctionRegistry()
registry.register(
    CustomFunctionSpec(
        function_name="score",
        implementation_reference="my_package.scoring.score",
        arg_names=("x", "y"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        active_flag=True,
    ),
    implementation=lambda **kwargs: kwargs["x"] + kwargs["y"],
)
```

Validation checks:

- referenced function exists,
- function is active,
- function is allowed in condition or assignment context,
- supplied argument names exactly match the registered contract.

## Pure-Python Runtime

Use `RulesEngineRuntime` for iterable row sets.

```python
from rules_engine import FunctionRegistry, RulesEngineRuntime

runtime = RulesEngineRuntime(repository, FunctionRegistry())
output_rows, traces = runtime.evaluate(
    [
        {"account_type": "TRAD", "account_id": "A", "amount": 10},
        {"account_type": "AFS", "account_id": "B", "amount": 20},
    ],
    ruleset,
)
```

Output rows have this shape:

```python
{
    "row": {...},
    "matched": True,
    "matched_rule_ids": ["high_value_account"],
    "assign": {"review_bucket": "high_value"},
    "rule_results": [{"rule_id": "high_value_account", "matched": True}],
}
```

The Python runtime is useful for:

- unit tests,
- local semantic validation,
- cross-runtime parity checks,
- small metadata examples.

## Spark Runtime

Use `SparkRulesEngineRuntime` for Databricks Spark DataFrames.

```python
from rules_engine import FunctionRegistry, SparkRulesEngineRuntime

spark_runtime = SparkRulesEngineRuntime(repository, FunctionRegistry())
result_df = spark_runtime.evaluate_dataframe(
    input_df,
    ruleset,
    fail_on_error=True,
)
```

The Spark runtime appends:

```text
rules_engine_matched
rules_engine_matched_rule_ids
rules_engine_assign
rules_engine_rule_results
rules_engine_error
```

`rules_engine_assign` and `rules_engine_rule_results` are JSON strings.

Spark evaluation strategy:

1. Discover aggregate operands in the ruleset.
2. Precompute dataset and group aggregates with Spark DataFrame operations.
3. Join aggregate values back to the original input DataFrame.
4. Evaluate final rule logic in a Python UDF using row semantics aligned with
   the pure-Python runtime.
5. Fail fast if any row-level evaluator error is produced and `fail_on_error`
   is true.

The default is `fail_on_error=True`. This prevents row-level evaluator
exceptions from silently becoming false non-matches.

## Spark Compatibility Validation

Use `SparkRulesetCompatibilityValidator` before Databricks execution.

```python
from rules_engine import SparkRulesetCompatibilityValidator

spark_validation = SparkRulesetCompatibilityValidator(registry).validate(ruleset)
if spark_validation.has_errors():
    raise ValueError(spark_validation.to_text())
```

The Spark compatibility validator adds runtime-specific checks for metadata
that is valid in the abstract but unsupported by the current Spark execution
path:

- exact `median`,
- exact `quantile`,
- aggregate `null_input_mode=error`,
- aggregate `null_result_mode=error`,
- aggregate-filter `null_input_mode=error`,
- aggregate-filter `null_result_mode=error`,
- ordered `first` / `last` with aggregate `null_input_mode=propagate`.

These are errors by design. The runtime should not approximate or silently
weaken explicit authoring semantics.

## Spark/Delta Repository

`SparkDeltaRulesetRepository` writes metadata using explicit Spark schemas.

```python
from rules_engine.repository import SparkDeltaRulesetRepository, RulesEngineTableNames

tables = RulesEngineTableNames(
    rulesets="catalog.schema.rulesets",
    rules="catalog.schema.rules",
    condition_groups="catalog.schema.condition_groups",
    conditions="catalog.schema.conditions",
    assignments="catalog.schema.assignments",
    function_registry="catalog.schema.function_registry",
    validation_results="catalog.schema.validation_results",
)

repository = SparkDeltaRulesetRepository(spark, tables)
repository.create_base_tables()
```

Published metadata is stored across:

- rulesets
- rules
- condition groups
- conditions
- assignments
- function registry
- validation results

Stable/queryable fields are first-class columns. Variable operand and registry
contracts are stored as JSON payload columns.

## Publish Lifecycle

Publishing is coordinated through `PublishService`.

```python
from rules_engine import (
    PublishService,
    RulesetNormalizer,
    SparkRulesetCompatibilityValidator,
)

publish_service = PublishService(
    repository=repository,
    validator=SparkRulesetCompatibilityValidator(registry),
    normalizer=RulesetNormalizer(),
)

validation = publish_service.save_draft(ruleset)
if validation.has_errors():
    raise ValueError(validation.to_text())

publish_service.publish(ruleset)
```

Lifecycle rules:

- `save_draft` requires `ruleset.status == draft`.
- `publish` requires `ruleset.status == draft`.
- publish validates before status change.
- published rows are immutable by `(ruleset_id, version)`.
- `save_draft` cannot overwrite published or retired metadata.
- only one version of a given `ruleset_name` may be published at a time.
- `retire` changes a persisted ruleset version to `retired`.
- `load_published` loads only `published` metadata.

## Auditability Model

Ruleset metadata rows include:

```text
created_by
created_at
published_by
published_at
content_hash
```

`created_by` and `published_by` are optional. When omitted, persisted metadata
uses `system`, which is appropriate for locked-down production jobs that run
through a dedicated cluster or service principal.

Child metadata rows include:

```text
created_by
created_at
```

Validation result rows include:

```text
run_at
```

Clean validation runs write an explicit `INFO / VALIDATION_PASSED` row with
`details_payload = {"issue_count": 0}`. Validation-result persistence therefore
records positive evidence that validation ran, rather than relying on an empty
table to imply success.

`content_hash` is a deterministic SHA-256 hash of canonical ruleset content.
Lifecycle status and provenance are excluded from the hash, so the hash
represents rule content rather than who saved or published it.

The audit model supports these operational questions:

- Who created this metadata version?
- When was it saved?
- Who published it?
- When was it published?
- Did the content change between environments?
- Which validation issues existed at save or publish time?

## Reconciliation CSV Translation Utility

The reconciliation translator is outside the runtime package. It is a
one-time migration utility that converts external reconciliation CSV specs
into canonical YAML authoring payloads.

It does not participate in runtime execution.

The intended workflow is:

1. translate the source CSV into YAML,
2. write the translation audit artifact,
3. manually refine the YAML for rules or semantics not captured by the source
   reconciliation spec,
4. compile, validate, publish, and execute the refined YAML through the rules
   engine.

Source CSV columns:

```text
MatchRuleName
GroupSequence
GroupJoinOperator
CriteriaSequence
FieldName
ValueOperator
Value
JoinType
```

Supported source operators:

```text
TextEquals          -> eq
TextNotEquals       -> ne
TextContains        -> contains
TextNotContains     -> not_contains
TextInList          -> in
NumericLessThan     -> lt
NumericGreaterThan  -> gt
```

Example:

```python
from tools.recon_spec_translation.reader_csv import read_reconciliation_csv
from tools.recon_spec_translation.translator import ReconciliationSpecTranslator
from tools.recon_spec_translation.writer_yaml import write_yaml
from tools.recon_spec_translation.audit import write_audit

rows = read_reconciliation_csv("source_spec.csv")
result = ReconciliationSpecTranslator(
    assignment_target_field="translated_match_rule_name",
).translate(rows)

if any(record.failures for record in result.audit_records):
    write_audit(result.audit_records, "translation_audit.json")
    raise ValueError("Translation failed. Review translation_audit.json.")

write_yaml(result.payload, "translated_rules.yaml")
write_audit(result.audit_records, "translation_audit.json")
```

Join semantics:

- `JoinType` connects the current source row to the next source row.
- `GroupJoinOperator` connects the current source group to the next source
  group.
- `JoinType = null` terminates the current condition group and is only valid
  on the final condition row for that `GroupSequence`.
- `GroupJoinOperator = null` terminates the rule group chain and is only valid
  on the final `GroupSequence`.
- condition and group chains are folded left-to-right. For example,
  `A And B And C Or D` becomes `(((A And B) And C) Or D)`.
- translated rules default to `stop_on_match: true`, so the first matching
  rule by `rule_order` wins. Pass `stop_on_match=False` to
  `ReconciliationSpecTranslator` only when multi-match behavior is explicitly
  intended.
- malformed chains fail translation and are reported in the audit.

## Databricks Smoke Test

After copying or installing the package into Databricks, run:

```text
databricks/smoke_test_rules_engine.py
```

The smoke test:

1. Creates smoke-test Delta tables.
2. Compiles a small Spark-compatible ruleset.
3. Runs Spark compatibility validation.
4. Publishes the ruleset with explicit provenance.
5. Loads the published ruleset.
6. Evaluates a Spark DataFrame with `fail_on_error=True`.
7. Verifies expected matches.
8. Retires the ruleset.
9. Verifies the retired version is no longer loadable as published.

## Supplemental Notebook

The notebook source file is:

```text
notebooks/rules_engine_developer_guide.py
```

It is a Databricks-style Python notebook source file. Import it into
Databricks or copy cells into a Databricks notebook. It walks through:

- YAML authoring,
- compile/validate/normalize,
- YAML export,
- Python runtime execution,
- Spark compatibility validation,
- Delta repository setup,
- publish/load/retire workflow,
- Spark DataFrame evaluation,
- reconciliation CSV translation.

## Testing

Run the default local suite:

```powershell
& 'C:\Users\aarba\.conda\envs\GeneralEnv\python.exe' -m pytest -q tests -p no:cacheprovider --basetemp pytest-cache-files-local
```

Spark tests are skipped by default because local Spark startup can be
environment-sensitive. To run them:

```powershell
$env:RULES_ENGINE_RUN_SPARK_TESTS = "1"
& 'C:\Users\aarba\.conda\envs\GeneralEnv\python.exe' -m pytest -q tests
```

Run Spark tests in Databricks before relying on Spark execution in production.

## Developer Workflow

Recommended local workflow:

1. Edit YAML, dataclasses, or runtime code.
2. Run compile/validator tests.
3. Run serializer/exporter tests.
4. Run pure-Python runtime tests.
5. Run default test suite.
6. Run Spark tests in Databricks or Spark-enabled CI.
7. Run `databricks/smoke_test_rules_engine.py`.
8. Review generated Delta metadata rows.
9. Promote package artifact or source to the target environment.

Recommended Databricks workflow:

1. Install or copy the package.
2. Configure table names in the target catalog/schema.
3. Run the smoke test against non-production tables.
4. Publish a representative draft ruleset.
5. Load published metadata.
6. Evaluate a representative DataFrame.
7. Assert `rules_engine_error` is empty.
8. Validate output counts and assignments against expected results.
9. Retire test metadata.

## Troubleshooting

### YAML Compile Fails

Common causes:

- unsupported key such as `value` instead of `literal`,
- unsupported key such as `assignments` instead of `assign`,
- unsupported aggregate key such as `field_name` instead of `field`,
- missing `null_input_mode`,
- missing `null_result_mode`,
- malformed condition group with more than one logical operator.

### Validation Fails

Use:

```python
print(validation.to_text())
```

Validation issues have stable `check_name` values for programmatic filtering.

### Spark Compatibility Fails

Use `SparkRulesetCompatibilityValidator` output. The current Spark runtime
intentionally rejects exact percentile aggregates and unsupported explicit
error null modes.

### Spark Evaluation Raises Row-Level Errors

This is expected when `fail_on_error=True`. Inspect the first surfaced error,
fix the ruleset or input schema, then rerun. Do not disable fail-fast behavior
for regulated production workflows unless downstream monitoring explicitly
handles `rules_engine_error`.

### Published Ruleset Cannot Be Loaded

Check:

- the ruleset was published, not only saved as draft,
- the name and version match,
- another version was not left published,
- the version was not retired.

### Save Draft Fails

`save_draft` cannot overwrite a published or retired version. Increment the
version or retire/publish through the intended lifecycle.

## Known Limitations

- Spark runtime uses a Python UDF for final rule evaluation.
- Spark runtime does not yet compile every row predicate into native Spark
  expressions.
- Spark aggregate precompute fails fast for explicit modes listed in
  [Spark Compatibility Validation](#spark-compatibility-validation).
- Spark `median` and `quantile` are not enabled until exact Spark semantics are
  implemented.
- Runtime traces are compact rule/condition pass-fail structures, not full
  resolved-value audit records.
- Packaging and deployment strategy should be finalized before broader
  Databricks rollout.
