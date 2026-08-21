# Rules Engine

`rules_engine` is a strict, YAML-authored rules engine for applying ordered
row-level rules to PySpark DataFrames. It compiles YAML into immutable
dataclasses, validates the contract, runs embedded examples, evaluates rows,
and can persist published versions and custom-function metadata in Delta.

The supported authoring surface is canonical YAML. The dataclasses are the
compiled in-memory model, not a second authoring format.

## Core contract

A ruleset contains ordered rules. Each active rule:

1. evaluates its `when` condition tree;
2. applies every assignment when it matches;
3. makes those assignments available to later rules through `assigned`;
4. stops evaluation only when its own `stop_on_match` is `true`.

Rules read original input columns with `field`. A later rule reads a value
assigned by an earlier matched rule with `assigned`. Multiple matching rules
contribute to `rules_engine_assign`; if more than one rule assigns the same
target, the last applied assignment is the final value.

```yaml
ruleset_id: account_review
ruleset_name: Account Review
version: "1"
owner: Rules Team
owner_department: Engineering
rules:
  - rule_id: classify_open
    rule_name: Classify open accounts
    rule_order: 1
    when:
      all:
        - condition_id: is_open
          left: {field: status}
          operator: eq
          right: {literal: OPEN}
    assign:
      review_bucket: open

  - rule_id: escalate_open
    rule_name: Escalate classified accounts
    rule_order: 2
    when:
      all:
        - condition_id: was_classified_open
          left: {assigned: review_bucket}
          operator: eq
          right: {literal: open}
        - condition_id: amount_is_material
          left: {field: amount, default_if_null: 0}
          operator: gt
          right: {literal: 100}
    assign:
      requires_review: true

expect:
  - name: material open account
    given: {status: OPEN, amount: 150}
    then:
      matched: true
      matched_rule_ids: [classify_open, escalate_open]
      review_bucket: open
      requires_review: true
```

Unknown keys, duplicate YAML keys, unsupported aliases, and invalid shapes are
rejected. Ruleset lifecycle status is not authored in YAML.

## YAML reference

### Ruleset fields

| Field | Required | Meaning |
|---|---:|---|
| `ruleset_id` | Yes | Stable ruleset identifier. |
| `ruleset_name` | Yes | Name used to load a published ruleset. |
| `version` | Yes | Immutable version identifier. |
| `rules` | Yes | Ordered rule definitions. |
| `description` | No | Human-readable description. |
| `owner` | No | Owning person or team. |
| `owner_department` | No | Owning department. |
| `expect` | No | Executable examples run before publication. |

### Rule fields

| Field | Required | Default | Meaning |
|---|---:|---|---|
| `rule_name` | Yes | — | Human-readable rule name. |
| `when` | Yes | — | One `all` or `any` condition group. Groups may nest. |
| `assign` | Yes | — | Assignment mapping or explicit assignment list. |
| `rule_id` | No | Generated | Stable rule identifier. Explicit IDs are recommended. |
| `rule_order` | No | YAML order | Evaluation order. Values must be unique. |
| `active_flag` | No | `true` | Whether the rule is evaluated. |
| `stop_on_match` | No | `false` | Stop after this rule matches and applies assignments. |
| `description` | No | `null` | Human-readable description. |

### Condition fields

| Field | Required | Default | Meaning |
|---|---:|---|---|
| `left` | Yes | — | Left operand. |
| `operator` | Yes | — | Canonical comparison operator. |
| `right` | Usually | — | Right operand; omitted for `is_null` and `is_not_null`. |
| `condition_id` | No | Generated | Stable identifier. Explicit IDs are recommended. |
| `tolerance_abs` | No | `0` | Absolute numeric tolerance for supported comparisons. |
| `error_on_null` | No | `false` | Raise a row error if an operand is still null. |
| `active_flag` | No | `true` | Inactive conditions evaluate as not passed. |

Condition groups contain exactly one logical key:

```yaml
when:
  all:
    - left: {field: country}
      operator: eq
      right: {literal: US}
    - any:
        - left: {field: amount}
          operator: gt
          right: {literal: 100}
        - left: {field: priority}
          operator: eq
          right: {literal: HIGH}
```

### Operands

Exactly one operand kind is allowed per operand.

| Form | Meaning |
|---|---|
| `{field: amount}` | Read an original DataFrame column. |
| `{assigned: review_bucket}` | Read the latest value committed by an earlier matched rule. |
| `{literal: 100}` | Use a literal value. |
| `{custom_function: {name: trim, args: {value: {field: raw_name}}}}` | Call a registered function. |

Any operand may define `default_if_null`. The fallback must be a non-null
literal and is applied before the comparison or assignment:

```yaml
left: {field: amount, default_if_null: 0}
```

Typed fallback form:

```yaml
left:
  field: business_date
  default_if_null: {literal: 2026-01-01, value_type: date}
```

### Null semantics

Null handling has two steps:

1. Resolve each operand. If its result is null and it has `default_if_null`,
   replace the null with that literal.
2. If an operand is still null, `error_on_null: false` makes the condition not
   match; `error_on_null: true` produces a row evaluation error.

`is_null` and `is_not_null` inspect the resolved value. Therefore a fallback
can intentionally make an originally null value non-null before the unary
comparison.

### Operators

| Category | Operators |
|---|---|
| Equality and order | `eq`, `ne`, `gt`, `ge`, `lt`, `le` |
| Membership and ranges | `in`, `not_in`, `between`, `not_between` |
| Text | `like`, `not_like`, `contains`, `not_contains`, `starts_with`, `ends_with` |
| Null | `is_null`, `is_not_null` |

### Assignments

Mapping form is the usual shorthand:

```yaml
assign:
  review_bucket: high
  normalized_name:
    custom_function:
      name: trim
      args:
        value: {field: raw_name}
```

Explicit form is available when stable assignment IDs are needed:

```yaml
assign:
  - assignment_id: set_review_bucket
    target_field: review_bucket
    value: {literal: high}
```

Assignments from one matched rule are committed together after all of that
rule's assignment expressions are resolved. Consequently, `assigned` refers
only to values committed by earlier matched rules, never another assignment in
the same rule.

### Executable expected cases

`expect` examples run without Spark through `RulesetTester`. Publication is
blocked when any case fails.

`then` may assert `matched`, `matched_rule_ids`, an `assign` mapping, or
assignment target names directly:

```yaml
expect:
  - name: no match
    given: {status: CLOSED, amount: 10}
    then:
      matched: false
      matched_rule_ids: []
```

## Compile, validate, and export

```python
from rules_engine import (
    FunctionRegistry,
    RulesetTester,
    RulesetValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
)

registry = FunctionRegistry()
compiler = YamlRulesetCompiler()
ruleset = compiler.compile_path("account_review.yaml")

validation = RulesetValidator(registry).validate(ruleset)
if validation.has_errors():
    raise ValueError(validation.to_text())

tests = RulesetTester(registry).test(ruleset)
if not tests.passed:
    raise ValueError(tests.to_text())

canonical_yaml = YamlRulesetExporter().export_text(ruleset)
```

`compile_text` and `compile_path` build the immutable dataclasses. Validation
checks rule identities, ordering, operator/operand compatibility, assignment
dependencies, function contracts, and expected-case shape. Exported YAML can
be compiled back into the same model.

## Spark evaluation

```python
from rules_engine import FunctionRegistry, SparkRulesEngineRuntime

runtime = SparkRulesEngineRuntime(repository, FunctionRegistry())
result_df = runtime.evaluate_dataframe(
    input_df,
    ruleset,
    fail_on_error=False,
    full_audit=False,
)
```

The returned DataFrame keeps all original columns first and appends the rules
engine columns in the order below. Replace `rules_engine` with the requested
`column_prefix` when a custom prefix is used.

### Compact output columns

| Order | Column | Type | Definition |
|---:|---|---|---|
| 1 | Original DataFrame columns | Existing | Preserved in their original order. |
| 2 | `rules_engine_error` | string, nullable | Row error text when `fail_on_error=false`; otherwise null. |
| 3 | `rules_engine_matched` | boolean | Whether at least one rule matched. |
| 4 | `rules_engine_matched_rule_ids` | array<string> | Matching rule IDs in evaluation order. |
| 5 | `rules_engine_assign` | struct, nullable | Final assignment values from all applied rules. Its fields are derived from the ruleset. |
| 6 | `rules_engine_ruleset` | struct | Immutable ruleset identity. |
| 7 | `rules_engine_engine_version` | string | Installed package version used for evaluation. |

### Full-audit additions

With `full_audit=true`, two columns are inserted after
`rules_engine_assign` and before `rules_engine_ruleset`:

| Order after assign | Column | Type | Definition |
|---:|---|---|---|
| 1 | `rules_engine_matched_rules` | array<struct> | Detailed trace for every matched rule. |
| 2 | `rules_engine_assignment_results` | array<struct> | Every applied assignment with override provenance. |

Full audit is optional because it resolves and serializes substantially more
row-level detail. There is no separate first-match trace: the first element of
`rules_engine_matched_rules` is the first match, and all later matches use the
same schema.

### `rules_engine_assign` fields

The struct contains one field per assignment target across the ruleset.

| Value | Meaning |
|---|---|
| Struct with values | At least one rule matched; each assigned target holds its final value. |
| Null field in a struct | A different rule matched, but that target was not assigned, or its assignment resolved to null. |
| Null struct | No rule matched, or the row failed before assignments were returned. |

### `rules_engine_ruleset` fields

| Nested field | Type | Definition |
|---|---|---|
| `id` | string | `ruleset_id` used for evaluation. |
| `version` | string | Ruleset version used for evaluation. |
| `content_hash` | string | SHA-256 hash of the canonical persisted payload. |

### `rules_engine_matched_rules` element fields

| Nested field | Type | Definition |
|---|---|---|
| `rule_id` | string | Matched rule identifier. |
| `rule_name` | string | Matched rule name. |
| `rule_order` | long | Evaluation order. |
| `explanation` | string | Human-readable explanation of the matched condition logic. |
| `assignments_applied` | array<string> | Assignment target fields applied by the rule. |
| `conditions` | array<struct> | Resolved condition details for the rule. |

Each `conditions` element contains:

| Nested field | Type | Definition |
|---|---|---|
| `columns` | array<string> | Original source columns referenced by the condition. |
| `left` | struct | Resolved left operand. |
| `right` | struct, nullable | Resolved right operand; null for unary operators. |
| `operator` | string | Canonical comparison operator. |
| `comparison_result` | boolean, nullable | Direct comparison result. |
| `passed` | boolean, nullable | Whether the condition passed. |
| `tolerance_abs` | string, nullable | Non-default absolute tolerance. |

Each operand struct (`left` or `right`) contains:

| Nested field | Type | Definition |
|---|---|---|
| `kind` | string | `field`, `assigned`, `literal`, or `custom_function`. |
| `column` | string, nullable | Original source column for a field operand. |
| `target_field` | string, nullable | Referenced assignment target for an assigned operand. |
| `original_value` | string, nullable | Value before `default_if_null`. |
| `value` | string, nullable | Resolved value used by the comparison. |
| `value_type` | string, nullable | Declared literal type when present. |
| `default_if_null` | string, nullable | Configured fallback value. |
| `default_applied` | boolean | Whether the fallback replaced a null. |
| `function_name` | string, nullable | Registered custom-function name. |
| `produced_by_rule_id` | string, nullable | Rule that produced an assigned value. |
| `produced_by_assignment_id` | string, nullable | Assignment that produced an assigned value. |
| `source_columns` | array<string>, nullable | Source columns used by this operand. |
| `arguments` | map<string,string>, nullable | Compact resolved custom-function arguments. |

### `rules_engine_assignment_results` element fields

| Nested field | Type | Definition |
|---|---|---|
| `assignment_id` | string | Stable assignment identifier. |
| `rule_id` | string | Rule that applied the assignment. |
| `rule_name` | string | Rule name. |
| `rule_order` | long | Rule evaluation order. |
| `target_field` | string | Assigned target. |
| `authored_expression` | string | Readable YAML assignment expression. |
| `old_value` | string, nullable | Original input value of the target column. |
| `proposed_value` | string, nullable | Value proposed by this assignment. |
| `changed` | boolean | Whether proposed and original input values differ. |
| `effective` | boolean | Whether this is the final assignment to the target. |
| `overridden_by_rule_id` | string, nullable | Later rule that supplied the final value. |
| `overridden_by_assignment_id` | string, nullable | Later assignment that supplied the final value. |

### Parameter behavior

| Setting | Returned behavior |
|---|---|
| All matching rules use `stop_on_match: false` | Every matching rule is applied. IDs and traces follow rule order; `rules_engine_assign` contains their combined final assignments. |
| A matching rule uses `stop_on_match: true` | That rule's assignments are applied, then later rules are not evaluated. |
| `full_audit=false` | Returns the compact columns only. |
| `full_audit=true` | Adds matched-rule traces and assignment provenance. Business results remain the same. |
| `fail_on_error=true` | A row evaluation error raises during the first materializing Spark action. |
| `fail_on_error=false` | The row returns `rules_engine_error`, `matched=false`, an empty ID array, and null assignments. Other rows continue. |
| `include_error_traceback=true` | Adds the Python traceback to captured row errors. Use only for debugging. |
| Custom `column_prefix="decision"` | Emits `decision_error`, `decision_matched`, and the equivalent prefixed columns. |

All compact and full-audit names for the selected prefix are reserved. An
input DataFrame containing any of them is rejected before evaluation, even
when `full_audit=false`.

## Coverage

```python
report = service.coverage_report(
    input_df,
    ruleset=ruleset,
    broad_match_threshold=0.40,
)
```

Coverage uses the production evaluator and returns total/no-match/error counts,
per-rule match and first-match counts, dead-rule IDs, broad-rule IDs, and a
filtered DataFrame of clean no-match rows. It does not score a “closest” rule.

## Custom function registry

YAML may call only functions registered in `FunctionRegistry`. Metadata and
the executable callable are registered separately so persisted rulesets never
contain executable code.

```python
from rules_engine import CustomFunctionSpec, FunctionRegistry

def normalize_code(*, value):
    return None if value is None else str(value).strip().upper()

registry = FunctionRegistry()
registry.register(
    CustomFunctionSpec(
        function_name="normalize_code",
        implementation_reference="my_package.rules.normalize_code",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
    ),
    normalize_code,
)
```

Callables must be top-level and serializable for Spark workers. The package
also provides `register_standard_functions(registry)` and
`standard_function_rows()` for its built-in function set.

## Repository and service

The repository owns two Delta tables:

- `ruleset_versions`: immutable canonical payloads, hashes, summary counts,
  ownership, publication/retirement audit fields, and row lifecycle status;
- `function_registry`: function references, argument contracts, permissions,
  and activation metadata.

`status` belongs only to a persisted ruleset-version row (`published` or
`retired`). It is not part of the authored `Ruleset` or canonical payload.

```python
from rules_engine import RulesEngineService

service = RulesEngineService.from_schema(
    spark,
    "catalog.rules_metadata",
)

service.create_tables(mode="ignore")
service.save_standard_function_registry()

ruleset = service.publish_yaml_path(
    "account_review.yaml",
    published_by="rules-team",
)

loaded = service.load_published("Account Review", version="1")
result_df = service.evaluate_dataframe(
    input_df,
    ruleset=loaded,
    fail_on_error=False,
)

service.retire(
    loaded.ruleset_id,
    loaded.version,
    retired_by="rules-team",
)
```

`create_tables` is explicit and never runs as a side effect of evaluation or
publication. Published `(ruleset_name, version)` identities cannot be
overwritten. Loading without a version requires exactly one published version;
otherwise callers must pin the version.

## Repository layout

```text
rules_engine/                         Package source
tests/                                Unit and Spark tests
rule_sets/                            Example source rulesets
outputs/                              Additional example YAML
notebooks/
  rules_engine_quickstart.py          Short Databricks walkthrough
  rules_engine_developer_guide.py     Detailed YAML-first workflow
  custom_function_authoring_guide.py  Function registry walkthrough
  rules_engine_system_tests.py        Databricks system-test notebook
docs/
  rules_engine_unit_test_summary.md   Test-suite inventory
```

## Development checks

```powershell
python -m ruff check rules_engine tests
python -m pytest tests
```

Spark tests are skipped unless `RULES_ENGINE_RUN_SPARK_TESTS=1` is set.

## Known boundaries

- Evaluation is row-level. Cross-row facts must already be DataFrame columns.
- YAML is the supported rule-authoring format.
- Custom functions execute as registered Python callables in the row UDF.
- Full audit is intended for targeted explainability, not as the default
  production payload.
- Repository creation and permissions are environment responsibilities; the
  package creates tables only when explicitly asked.
