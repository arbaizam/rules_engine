# Rules Engine

ALM Engineering designed the `rules_engine` to apply clear, reviewable,
**row-level**, business rules to PySpark DataFrames. Rules are authored
in strict YAML, compiled into immutable Python dataclasses, and validated.
The rules engine supports one authoring language: canonical YAML. The
dataclasses are the compiled in-memory model.

## Why we designed it this way

We elected to keep the core contract narrow and explicit to:

- evaluate one row at a time. Cross-row facts must already be present as
  DataFrame columns;
- reject unknown YAML keys, duplicate YAML keys, aliases, and ambiguous
  shapes instead of guessing what an author meant;
- use `field` when a rule needs the original input row and `assigned` when
  a later rule needs a value committed by an earlier matching rule;
- resolve null substitutions on the operand before we compare values;
- evaluate rules in explicit `rule_order` order;
- let every matching rule contribute assignments unless a matching rule
  has `stop_on_match: true`;
- return keyed engine results separately from the business DataFrame, then let
  callers explicitly apply final assignments when they want business rows;
- distinguish "no assignment" from "assign null" for every target;
- keep the normal DataFrame output compact. Detailed traces are returned
  only when `full_audit=true`;
- never create metadata tables as a side effect of compiling, publishing,
  loading, or evaluating rules;
- treat both `(ruleset_id, version)` and `(ruleset_name, version)` as
  immutable published identities.

## How one row is evaluated

For each active rule, in ascending `rule_order`, the engine will:

1. evaluate every active condition in its `when` tree;
2. resolve all assignment expressions against one pre-rule snapshot;
3. commit every assignment together when the rule matches;
4. expose those committed values to later rules through `assigned`; and
5. stop only when that matching rule has `stop_on_match: true`.

Multiple rules may match. Their assignments are merged into
`rules_engine_assign`. Each target reports whether an assignment was applied
and its typed final value. When multiple rules assign the same target, the
last applied assignment is the final value. Full audit preserves every
assignment event so we can see what was replaced and which assignment won.

```yaml
ruleset_id: account_review
ruleset_name: Account Review
version: "1"
owner: ALM Rules Team
owner_department: ALM Engineering
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
```

## YAML contract reference

### Document rules

| Contract rule | Allowed values | Definition |
|---|---|---|
| Root document | One YAML mapping | The document must contain exactly one ruleset mapping. |
| Keys | Only keys listed in the tables below | Unknown and duplicate keys fail compilation. |
| Names and IDs | Non-empty strings | IDs are case-sensitive. We recommend stable, explicit IDs for audit work. |
| Booleans | YAML `true` or `false` | Strings such as `"true"` are not accepted as booleans. |
| Integers | YAML integers, excluding booleans | We use integers for `rule_order`. |
| Numbers | Finite YAML integers, decimals, or floats | `NaN` and infinite values are rejected. Untyped fractional YAML values are preserved as `Decimal`. |
| Dates | YAML date values or ISO `YYYY-MM-DD` with `value_type: date` | Invalid calendar dates fail compilation. |
| Collections | YAML lists and mappings; tuples may appear in canonical exported YAML | The operator or Spark target type may impose tighter rules. |

### Ruleset fields

| Field | Required | Type and allowed values | Definition |
|---|---:|---|---|
| `ruleset_id` | Yes | Non-empty string | Stable technical identity for the ruleset across versions. |
| `ruleset_name` | Yes | Non-empty string | Human-facing name used by `load_published`. The same name may have multiple versions. |
| `version` | Yes | Non-empty string | Immutable caller-defined version. We do not parse or order the value as semantic versioning. |
| `rules` | Yes | YAML list containing at least one active rule before validation can pass | Rules are evaluated by `rule_order`, not by list position once explicit orders are supplied. An all-inactive ruleset is rejected because it has no writable assignment contract. |
| `description` | No | String or `null`; default `null` | Plain-English purpose of the ruleset. |
| `owner` | Required for validation | Non-empty string; compile default `null` | Person or team accountable for the rules. YAML can compile without it, but validation and publication fail. |
| `owner_department` | Required for validation | Non-empty string; compile default `null` | Department accountable for the rules. YAML can compile without it, but validation and publication fail. |

### Rule fields

| Field | Required | Type and allowed values | Default | Definition |
|---|---:|---|---|---|
| `rule_name` | Yes | Non-empty string | — | Human-readable rule name. |
| `when` | Yes | One condition-group mapping containing exactly one of `all` or `any` | — | Boolean condition tree for the rule. |
| `assign` | Yes | Non-empty mapping or explicit assignment list | — | Values committed when the rule matches. Validation rejects a rule with no assignments. |
| `rule_id` | No | Non-empty string, unique within the ruleset | `rule:<YAML position>` | Stable technical rule identity. We recommend authoring it explicitly. |
| `rule_order` | No | Integer, unique within the ruleset | One-based YAML position | Evaluation order. Lower values run first; values do not need to be consecutive. |
| `active_flag` | No | `true` or `false` | `true` | An inactive rule is not evaluated and does not produce assignments. At least one rule in the ruleset must remain active. |
| `stop_on_match` | No | `true` or `false` | `false` | When this rule matches, we apply its assignments and skip all later rules. A non-matching rule never stops evaluation. |
| `description` | No | String or `null` | `null` | Plain-English purpose or business explanation. |

`rule_id` and `rule_order` must each be unique. Assignment dependencies are
validated against `rule_order`, so an `assigned` reference must have an active
producer with a lower order.

### Condition-group fields

A group contains conditions, nested groups, or both.

| Field | Required | Type and allowed values | Default | Definition |
|---|---:|---|---|---|
| `all` | Exactly one of `all` or `any` | YAML list with at least one condition or nested group before validation can pass | — | The group passes only when every item passes. |
| `any` | Exactly one of `all` or `any` | YAML list with at least one condition or nested group before validation can pass | — | The group passes when at least one item passes. |
| `condition_group_id` | No | Non-empty string, unique across the ruleset | Generated from its location | Stable group identity included in full-audit traces. |

We evaluate every active condition in both `all` and `any` groups. We elected
not to short-circuit because errors must not disappear merely because an
earlier branch already determined the Boolean result. An inactive condition
evaluates as not passed. Therefore it blocks an `all` group but does not block
another passing branch in an `any` group.

```yaml
when:
  all:
    - condition_id: is_us
      left: {field: country}
      operator: eq
      right: {literal: US}
    - condition_group_id: material_or_priority
      any:
        - condition_id: material_amount
          left: {field: amount}
          operator: gt
          right: {literal: 100}
        - condition_id: high_priority
          left: {field: priority}
          operator: eq
          right: {literal: HIGH}
```

### Condition fields

| Field | Required | Type and allowed values | Default | Definition |
|---|---:|---|---|---|
| `left` | Yes | Exactly one operand mapping | — | Value on the left side of the comparison. |
| `operator` | Yes | One canonical operator from the operator table below | — | Comparison to perform. Aliases and SQL symbols are not accepted. |
| `right` | Required for binary operators | Exactly one operand mapping; forbidden for `is_null` and `is_not_null` | — | Value on the right side of the comparison. |
| `condition_id` | No | Non-empty string, unique across the ruleset | Generated from its location | Stable identity included in validation and audit output. |
| `tolerance_abs` | No | Finite number greater than or equal to `0` | `0` | Absolute numeric tolerance. It must be `0` for date/timestamp comparisons and for `between`/`not_between`. |
| `error_on_null` | No | `true` or `false`; forbidden for unary null operators | `false` | When a binary operand remains null after fallback, `false` means no match and `true` produces a row error. |
| `active_flag` | No | `true` or `false` | `true` | An inactive condition is recorded as inactive in full audit and evaluates as not passed. |

### Operand forms

Every operand defines exactly one of `field`, `assigned`, `literal`, or
`custom_function`. The allowed keys depend on that choice.

| Operand | Allowed keys | Allowed values | Definition |
|---|---|---|---|
| Field | `field`, optional `default_if_null` | `field` is a non-empty original DataFrame column name | Reads the input row. It never reads a value created by a rule. Dotted names are treated as literal column names and escaped for Spark. |
| Assigned | `assigned`, optional `default_if_null` | `assigned` is a non-empty assignment target produced by an active lower-order rule | Reads the latest value committed to that target. It does not fall back to an input column with the same name. |
| Literal | `literal`, optional `value_type`, optional `default_if_null` | Any supported finite scalar or supported collection | Uses authored data directly. A typed null is allowed, but a new Spark assignment target needs a supported `value_type`. |
| Custom function | `custom_function`, optional `default_if_null` | Function mapping with exactly `name` and optional `args` | Calls a registered function. The argument names must exactly match its registry contract. |

Examples:

```yaml
left: {field: amount}
right: {literal: 100}
```

```yaml
left: {assigned: review_bucket}
right: {literal: high}
```

```yaml
left:
  custom_function:
    name: trim
    args:
      value: {field: raw_name}
right: {literal: ABC}
```

Custom-function arguments may be literals, operands, or lists/mappings that
contain operands. Nested custom functions are supported at any of those
levels. Required arguments must be present, optional arguments use their
registered defaults when omitted, and unknown argument names fail validation.

### Literal type hints

We use type hints when Spark cannot safely infer an assignment type, most
notably for null literals and custom-function results assigned to a new field.

| Canonical Spark meaning | Allowed `value_type` or `return_type_hint` values | Notes |
|---|---|---|
| String | `string`, `str` | The value must be compatible with a Spark string target. |
| Integer | `integer`, `int`, `long` | Mapped to Spark `LongType`; values must fit without loss. |
| Floating number | `number`, `float`, `double` | Mapped to Spark `DoubleType`. Explicit use elects floating-point behavior. |
| Exact decimal | `decimal` | Mapped to `DecimalType(38,18)` when a concrete target type is not already known. Values must be finite and fit. |
| Boolean | `boolean`, `bool` | Values must be actual booleans; strings are not coerced. |
| Date | `date` | ISO `YYYY-MM-DD` strings are converted to Python dates during compilation. |
| Timestamp | `timestamp` | ISO strings must include a UTC offset and compile to a UTC-normalized Python `datetime`. |
| Timestamp without timezone | `timestamp_ntz` | ISO strings must omit a UTC offset and compile to a naive Python `datetime`. Available only when the installed PySpark runtime provides `TimestampNTZType`. |
| Polymorphic custom return | `any` | Allowed for a custom function only when Spark already knows the assignment target type. It cannot define a new target field. |
| Same as one argument | `same_as:<argument_name>` | The result takes the Spark type of the named argument. We use this for `null_if`. |
| Common type of array items | `common_type:<argument_name>` | The result takes the safe common Spark type of items in the named argument. We use this for `coalesce`. |

The compiler retains an unrecognized non-empty `value_type` as metadata, but
Spark compatibility validation rejects it when it is needed for an
assignment. We recommend using only the values in this table.

### Null behavior

Any operand may define `default_if_null`. Allowed fallback forms are:

- a non-null scalar or list literal, such as `default_if_null: 0`; or
- a mapping containing `literal` and optional `value_type` only.

```yaml
left:
  field: business_date
  default_if_null: {literal: 2026-01-01, value_type: date}
```

We handle nulls in this order:

1. Resolve the operand.
2. If the result is null and `default_if_null` exists, replace it with the
   fallback.
3. Run the comparison against the effective value.
4. For a binary comparison that still has a null, return no match when
   `error_on_null=false` or a row error when `error_on_null=true`.

`is_null` and `is_not_null` inspect the effective value. A fallback can
therefore intentionally make an originally null value non-null before the
unary comparison. The fallback itself cannot be null and cannot contain a
nested `default_if_null`.

### Operator reference

| Operator | Right operand | Allowed values or shapes | Definition |
|---|---|---|---|
| `eq` | Required | Comparable scalar values | Equality. Numeric equality uses `tolerance_abs` when at least one operand has a numeric runtime type; two strings use exact string equality. |
| `ne` | Required | Comparable scalar values | Negation of `eq`. Numeric inequality uses the same typed-numeric rule. |
| `gt` | Required | Numeric pair or matching temporal pair | Left is greater than right. Numeric tolerance moves the boundary outward. Temporal tolerance must be `0`. |
| `ge` | Required | Numeric pair or matching temporal pair | Left is greater than or equal to right. Numeric tolerance is supported. |
| `lt` | Required | Numeric pair or matching temporal pair | Left is less than right. Numeric tolerance is supported. |
| `le` | Required | Numeric pair or matching temporal pair | Left is less than or equal to right. Numeric tolerance is supported. |
| `in` | Required | Right side must resolve to a list, tuple, or set | True when left equals any right-side item. Numeric membership uses `tolerance_abs` when either compared item has a numeric runtime type; string codes remain exact. |
| `not_in` | Required | Right side must resolve to a list, tuple, or set | Negation of `in`. Strings and mappings are not treated as collections. |
| `between` | Required | Right side must be a two-item list or tuple | Inclusive lower and upper bounds. `tolerance_abs` must be `0`. |
| `not_between` | Required | Right side must be a two-item list or tuple | Negation of inclusive `between`. `tolerance_abs` must be `0`. |
| `like` | Required | Values that can be rendered as strings | SQL-style whole-value pattern match. `%` matches any number of characters and `_` matches one character. |
| `not_like` | Required | Values that can be rendered as strings | Negation of `like`. |
| `contains` | Required | Values that can be rendered as strings | True when the rendered right value is a substring of the rendered left value. |
| `not_contains` | Required | Values that can be rendered as strings | Negation of `contains`. |
| `starts_with` | Required | Values that can be rendered as strings | True when rendered left starts with rendered right. |
| `ends_with` | Required | Values that can be rendered as strings | True when rendered left ends with rendered right. |
| `is_null` | Forbidden | Left may resolve to any value | True when the effective left value is null. `error_on_null` is not allowed. |
| `is_not_null` | Forbidden | Left may resolve to any value | True when the effective left value is not null. `error_on_null` is not allowed. |

### Assignments

The normal mapping form uses the key as `target_field`. A scalar or list value
is shorthand for a literal operand. A mapping value must be an explicit
operand, so a mapping literal must be wrapped in `literal`.

```yaml
assign:
  review_bucket: high
  review_flags:
    literal:
      material: true
      manual: false
  normalized_name:
    custom_function:
      name: trim
      args:
        value: {field: raw_name}
```

The explicit list form gives us stable assignment IDs:

| Field | Required | Type and allowed values | Default | Definition |
|---|---:|---|---|---|
| `assignment_id` | No | Non-empty string, unique across the ruleset | `assignment:<rule_id>:<target_field>` | Identity used in full-audit provenance. |
| `target_field` | Yes | Non-empty top-level column name; unique within one rule; cannot be a declared evaluation key | — | Target written into `rules_engine_assign`. `apply_assignments()` replaces an existing target column or appends a new one. Struct targets are whole values, not nested merge paths. |
| `value` | Yes | Exactly one operand mapping | — | Expression resolved when the rule matches. |

```yaml
assign:
  - assignment_id: set_review_bucket
    target_field: review_bucket
    value: {literal: high}
```

Assignments within one rule read the same pre-rule state and are committed
together. An assignment cannot read another assignment from the same rule.
Only a later active rule may read the committed value through `assigned`.

## Compile, validate, and export

```python
from rules_engine import (
    FunctionRegistry,
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

canonical_yaml = YamlRulesetExporter().export_text(ruleset)
```

Compilation checks YAML syntax, keys, primitive types, and structural shapes.
Semantic validation checks ownership, unique identities, group contents,
operator arity, assignment dependencies, and function contracts.
`SparkRulesetCompatibilityValidator` adds DataFrame field and exact type
checks when we pass a DataFrame or `StructType`.

The exporter emits canonical YAML that compiles back into the same dataclass
model.

## Authoring manifest

Authoring applications should obtain validation-relevant choices from the
installed engine instead of copying its enums and function contracts.

```python
from rules_engine import (
    FunctionRegistry,
    build_authoring_manifest,
    register_standard_functions,
)

registry = register_standard_functions(FunctionRegistry())
manifest = build_authoring_manifest(registry)
```

The returned payload is deterministic and JSON-compatible. It includes the
manifest format and engine versions, comparison operators and operand shape,
tolerance support, logical operators, operand kinds, canonical literal type
hints and aliases, and registered function argument contracts. Function
implementation references are intentionally excluded.

`right_operand_shape` has four possible values:

| Value | Authoring requirement |
|---|---|
| `none` | Unary operator with no right operand. |
| `any` | Scalar or expression-valued right operand. |
| `collection` | Collection-valued right operand for membership. |
| `pair` | Exactly two bounds for `between` or `not_between`. |

Applications continue to own labels, help text, layout, mutable draft state,
and other presentation concerns. The caller supplies a `FunctionRegistry` so
the manifest reflects the functions available in that environment, including
registered custom functions.

## Spark evaluation contract

```python
from rules_engine import FunctionRegistry, SparkRulesEngineRuntime

runtime = SparkRulesEngineRuntime(repository, FunctionRegistry())
evaluation = runtime.evaluate_dataframe(
    input_df,
    ruleset,
    key_columns=["account_id"],
    fail_on_error=False,
    full_audit=False,
)
result_df = evaluation.results_df
applied_df = evaluation.apply_assignments()
```

`evaluate_dataframe()` returns a lazy `DataFrameEvaluation`, not a DataFrame.
We require at least one `key_columns` entry so every result row carries an
explicit business identity. The named columns must exist and be unambiguous,
and rules cannot assign to them. The caller owns the data guarantee that the
combined key is non-null and unique; we intentionally do not start a hidden
Spark job to verify values.

The object exposes two projections of one shared evaluated Spark plan:

| Public entry point | Allowed values or return | Purpose |
|---|---|---|
| `evaluation.key_columns` | Tuple of the exact declared key names | Inspect immutable row identity in caller-supplied order. |
| `evaluation.result_columns` | Tuple of the exact engine result names | Inspect ordered result names for the selected prefix and audit mode. Keys are not included. |
| `evaluation.results_df` | DataFrame with declared keys, then rules-engine result columns | Store, inspect, or report evaluation evidence without copying the business payload. |
| `evaluation.apply_assignments()` | DataFrame with original business columns and final assignments applied | Continue business processing with overwritten and newly appended values, without engine result columns. |
| `evaluation.persist(storage_level=None)` | The same `DataFrameEvaluation` | Cache the shared evaluated plan with Spark's default or a supplied `StorageLevel`. |
| `evaluation.unpersist(blocking=False)` | The same `DataFrameEvaluation` | Remove the shared plan from cache, optionally waiting for removal. |

Neither projection joins rows by key. Both select from the same lazy plan, so
there is no ambiguity if duplicate key values accidentally reach evaluation.
The key contract is still required for downstream storage and reporting.
Replace `rules_engine` below with the selected `column_prefix` when a custom
prefix is used.

### Compact output columns

| Order | Column | Type | Allowed returned values | Definition |
|---:|---|---|---|---|
| 1 | Declared key columns | Existing types | Original key values | Keys appear in the exact order supplied to `key_columns`. Other business columns are not copied into `results_df`. |
| 2 | `rules_engine_error` | Nullable string | `null` or error text | Row error captured when `fail_on_error=false`. It remains null for a clean match or no-match. |
| 3 | `rules_engine_matched` | Boolean | `true` or `false` | True when at least one active rule matched and the row completed successfully. |
| 4 | `rules_engine_matched_rule_ids` | `array<string>` | Ordered IDs or `[]` | Every matching rule ID in evaluation order. |
| 5 | `rules_engine_assign` | Non-null struct | One outcome per active assignment target | Final assignment state from all matching rules. Every target contains `applied` and typed `value` fields. |
| 6 | `rules_engine_ruleset` | Struct | Non-null identity struct | Immutable identity of the evaluated ruleset. |
| 7 | `rules_engine_engine_version` | String | Non-empty package version | Installed package version used for evaluation. |

### Full-audit additions

When `full_audit=true`, we insert two columns after `rules_engine_assign` and
before `rules_engine_ruleset`:

| Order after assign | Column | Type | Allowed returned values | Definition |
|---:|---|---|---|---|
| 1 | `rules_engine_matched_rules` | `array<struct>` | One element per match or `[]` | Complete trace for every matching rule. Losing rules do not emit match traces. |
| 2 | `rules_engine_assignment_results` | `array<struct>` | One element per applied assignment or `[]` | Assignment history, including replaced values and final-winner provenance. |

Full audit resolves and serializes substantially more data. We elected to make
it optional for targeted explainability instead of paying that cost on every
production row. There is no special first-match trace. The first element of
`rules_engine_matched_rules` is the first match, and every later match uses the
same schema.

### `rules_engine_assign`

The struct contains one non-null outcome struct per active assignment target
across the ruleset. This explicit state is how we distinguish assigning null
from not assigning the target.

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `<target>.applied` | Boolean | `true` or `false` | True when a matching rule assigned this target. On errors and clean no-matches, it is false. |
| `<target>.value` | Target Spark type, nullable | Any compatible typed value or `null` | Final value from the last matching assignment. It may be null even when `applied=true`. It is null when `applied=false`. |

For example, these states have different meanings:

| Outcome | Meaning for an existing business column |
|---|---|
| `{applied: false, value: null}` | No rule assigned the target; keep its current value. |
| `{applied: true, value: "review"}` | Replace its current value with `"review"`. |
| `{applied: true, value: null}` | The rule explicitly assigned null; clear its current value. |

### Applying assignments

`evaluation.apply_assignments()` applies those outcomes without a join and
returns no rules-engine result columns.

| Target shape | Applied behavior |
|---|---|
| Existing top-level column | The column stays in its original position. We replace it only when `applied=true`; otherwise we retain the input value. |
| New top-level column | We append it after all original columns in ruleset assignment order. It is null on rows where `applied=false`. |
| Struct column | We use the same atomic behavior as any other top-level value: retain the whole struct, replace the whole struct, or clear the whole struct. We do not merge nested fields. |
| Declared key column | Rejected before evaluation because keys are immutable row identity. |

The method returns a new lazy DataFrame and does not mutate the input
DataFrame. If a caller materializes both projections, explicit persistence
prevents the shared evaluation from running twice:

```python
evaluation.persist()
try:
    evaluation.results_df.write.mode("append").saveAsTable("audit.rule_results")
    evaluation.apply_assignments().write.mode("overwrite").saveAsTable("business.accounts")
finally:
    evaluation.unpersist()
```

`persist(storage_level=None)` uses Spark's default storage level or accepts an
explicit `pyspark.StorageLevel`. `unpersist(blocking=False)` forwards the
blocking choice to Spark. Both methods return the same evaluation object for
chaining.

### `rules_engine_ruleset`

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `id` | String | Authored `ruleset_id` | Technical ruleset identity used for evaluation. |
| `version` | String | Authored version | Exact ruleset version used for evaluation. |
| `content_hash` | String | SHA-256 hexadecimal hash | Hash of the canonical immutable payload. |

### `rules_engine_matched_rules` elements

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `rule_id` | String | Authored or generated rule ID | Identity of the matching rule. |
| `rule_name` | String | Authored rule name | Human-readable rule name. |
| `rule_order` | Long | Authored or generated integer order | Order in which we evaluated the rule. |
| `explanation` | String | Human-readable Boolean expression | Explanation built from authored logic and resolved values. |
| `assignments_applied` | `array<string>` | Ordered target names or `[]` | Targets applied by this matching rule. |
| `conditions` | `array<struct>` | One element per evaluated condition | Detailed condition results for this rule. |

Each `conditions` element contains:

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `condition_id` | String | Authored or generated ID | Stable condition identity. |
| `condition_group_id` | String | Authored or generated group ID | Group that directly contains the condition. |
| `condition_group_operator` | String | `all` or `any` | Logical operator of the containing group. |
| `active_flag` | Boolean | `true` or `false` | Whether we treated the condition as active. |
| `columns` | `array<string>` | Source column names or `[]` | Original input columns used directly or through function arguments. |
| `left` | Struct | Resolved operand trace | Left operand before and after fallback. |
| `right` | Nullable struct | Resolved operand trace or `null` | Right operand; null only for unary operators. |
| `operator` | String | One canonical comparison operator | Comparison that was evaluated. |
| `comparison_result` | Nullable Boolean | `true`, `false`, or `null` | Direct comparison result. Null means a binary operand remained null. |
| `passed` | Nullable Boolean | `true`, `false`, or `null` | Condition pass result. |
| `tolerance_abs` | Nullable string | Non-default tolerance text or `null` | Authored absolute tolerance when it is not zero. |

Each operand trace (`left` or `right`) contains:

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `kind` | String | `field`, `assigned`, `literal`, or `custom_function` | Operand kind. |
| `column` | Nullable string | Source field name or `null` | Direct field source. |
| `target_field` | Nullable string | Assignment target name or `null` | Target read by an `assigned` operand. |
| `original_value` | Nullable string | Rendered value or `null` | Value before `default_if_null`. |
| `value` | Nullable string | Rendered effective value or `null` | Value used by the comparison. |
| `value_type` | Nullable string | Authored hint or `null` | Literal type hint when present. |
| `default_if_null` | Nullable string | Rendered fallback or `null` | Configured fallback. |
| `default_applied` | Boolean | `true` or `false` | Whether fallback replaced a null. |
| `function_name` | Nullable string | Registered name or `null` | Function used by a custom-function operand. |
| `produced_by_rule_id` | Nullable string | Earlier rule ID or `null` | Rule that produced an assigned value. |
| `produced_by_assignment_id` | Nullable string | Earlier assignment ID or `null` | Assignment that produced an assigned value. |
| `source_columns` | Nullable `array<string>` | Column names, `[]`, or `null` | Transitive input columns used by the operand. |
| `arguments` | Nullable `map<string,string>` | Resolved argument text or `null` | Compact custom-function argument values. |

### `rules_engine_assignment_results` elements

| Nested field | Type | Allowed returned values | Definition |
|---|---|---|---|
| `assignment_id` | String | Authored or generated assignment ID | Stable assignment identity. |
| `rule_id` | String | Matching rule ID | Rule that applied the assignment. |
| `rule_name` | String | Matching rule name | Human-readable rule name. |
| `rule_order` | Long | Rule order integer | Evaluation order of the applying rule. |
| `target_field` | String | Declared target name | Field assigned by this event. |
| `authored_expression` | String | Readable operand expression | Human-readable assignment expression. |
| `old_value` | Nullable string | Rendered value or `null` | Latest earlier committed assignment for this target, otherwise the original input value. |
| `proposed_value` | Nullable string | Rendered value or `null` | Value proposed by this assignment. |
| `changed` | Boolean | `true` or `false` | Whether proposed value differs from `old_value`. |
| `effective` | Boolean | `true` or `false` | True only when this event supplies the final target value. |
| `overridden_by_rule_id` | Nullable string | Later rule ID or `null` | Rule that later replaced this value. |
| `overridden_by_assignment_id` | Nullable string | Later assignment ID or `null` | Assignment that later replaced this value. |

### Evaluation parameters and returned behavior

| Setting | Allowed values and default | Returned behavior |
|---|---|---|
| `key_columns` | Required non-empty sequence of distinct, non-empty input column names | Places those columns first in `results_df` and protects them from assignment. The caller guarantees their combined values are non-null and unique. |
| `column_prefix` | Non-empty string; default `rules_engine` | Renames every result column. For example, `decision` produces `decision_error` and `decision_matched`. |
| `fail_on_error` | `true` or `false`; default `true` | `true` raises from the worker during the caller's first Spark action. `false` returns an error row with `matched=false`, an empty ID array, and `applied=false` for every assignment target. |
| `include_error_traceback` | `true` or `false`; default `false` | Adds a Python traceback to captured row errors. We use it only for debugging because it makes rows much larger. |
| `full_audit` | `true` or `false`; default `false` | `true` adds matched-rule and assignment-history columns. It does not change matching or assignment results. |
| Rule `stop_on_match` | `true` or `false`; default `false` | `false` lets later rules run. `true` applies the matching rule and then stops. |

All compact and full-audit output names for the selected prefix are reserved.
We reject an input DataFrame containing any reserved name before evaluation,
even when `full_audit=false`.

## Coverage reports

```python
report = service.coverage_report(
    input_df,
    ruleset=ruleset,
    broad_match_threshold=0.40,
)
```

Coverage uses the production evaluator with `fail_on_error=false` and starts
one Spark aggregation action. It returns:

| Field | Allowed values | Definition |
|---|---|---|
| `total_row_count` | Integer greater than or equal to `0` | Rows evaluated. |
| `no_match_count` | Integer greater than or equal to `0` | Clean rows that matched no rule. |
| `error_count` | Integer greater than or equal to `0` | Rows with evaluation errors. |
| `rules` | Tuple of `RuleCoverage` | Match count, first-match count, match rate, dead flag, and broad flag for every active rule. |
| `first_match_distribution` | Mapping of rule ID to integer count | Derived first-match counts. |
| `dead_rule_ids` | Tuple of rule IDs | Active rules with zero matches. |
| `suspiciously_broad_rule_ids` | Tuple of rule IDs | Rules whose match rate is at or above `broad_match_threshold`. |
| `no_match_rows` | Spark DataFrame | Lazy diagnostic view retaining every original input column plus the coverage-prefixed result columns, filtered to clean no-match rows. |

`broad_match_threshold` must be between `0` and `1`, inclusive. Coverage is a
diagnostic summary; it does not change or publish the ruleset.

## Custom-function registry

YAML may call only functions registered in `FunctionRegistry`. We keep
metadata and executable callables separate so persisted rulesets never contain
executable code.

```python
from rules_engine import CustomFunctionArgSpec, CustomFunctionSpec, FunctionRegistry

def normalize_code(*, value):
    return None if value is None else str(value).strip().upper()

registry = FunctionRegistry()
registry.register(
    CustomFunctionSpec(
        function_name="normalize_code",
        implementation_reference="my_package.rules.normalize_code",
        arguments=(CustomFunctionArgSpec("value", type_hint="string"),),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
    ),
    normalize_code,
)
```

### `CustomFunctionSpec` contract

| Field | Required | Allowed values | Definition |
|---|---:|---|---|
| `function_name` | Yes | Unique non-empty string | Name used in YAML. Duplicate registration fails. |
| `implementation_reference` | Yes | Non-empty environment-defined string | Audit metadata only. We never import executable code from this string. |
| `arguments` | Yes | Tuple of unique `CustomFunctionArgSpec` values; an empty tuple is allowed | Declares argument order, required and optional names, defaults, types, and literal constraints. |
| `allowed_in_condition_flag` | Yes | `true` or `false` | Whether the function may appear in a condition operand. |
| `allowed_in_assignment_flag` | Yes | `true` or `false` | Whether the function may appear in an assignment operand. |
| `active_flag` | No | `true` or `false`; default `true` | Inactive functions fail ruleset validation. |
| `return_type_hint` | No | Fixed type hint, `any`, `same_as:<argument>`, `common_type:<argument>`, or `null` | Used for Spark assignment type resolution. A new target needs a type that can be resolved from the contract and authored arguments. |
| `description` | No | String or `null` | Human-readable purpose. |
| `version` | No | String or `null` | Implementation contract version stored as metadata. |

The callable receives keyword arguments. We require top-level, deterministic,
worker-serializable implementations for Spark evaluation. A spec without an
implementation can support metadata validation, but runtime evaluation fails
if a referenced implementation is missing.

### `CustomFunctionArgSpec` contract

| Field | Required | Allowed values | Definition |
|---|---:|---|---|
| `name` | Yes | Unique non-empty string within the function | Keyword passed to the callable and authored under `args`. |
| `required` | No | `true` or `false`; default `true` | When `false`, YAML may omit the argument. |
| `default` | No | JSON-compatible literal or collection; default `null` | Value bound by the runtime when an optional argument is omitted. |
| `type_hint` | No | `any`, `string`, `integer`, `number`, `boolean`, `date`, `timestamp`, `mapping`, `sequence`, `string_sequence`, `integer_sequence`, or `date_sequence`; default `any` | Validates literal arguments immediately and field-backed arguments when Spark schema metadata is available. Null is allowed for every type. |
| `allowed_values` | No | Non-empty tuple of JSON-compatible literal values or `null` | Restricts a configuration argument to an explicit set, such as `error` or `null`. It requires `literal_only=true`; an optional default must be in the set. |
| `literal_only` | No | `true` or `false`; default `false` | When `true`, the argument cannot read a field, assignment, or nested function. We use this for modes and other plan-level configuration. |

### Standard functions

`register_standard_functions(registry)` registers all 58 package functions and
their implementations. `standard_function_rows()` returns their persistence
rows. We allow every standard function in both conditions and assignments so a
ruleset can also materialize flags, cleaned values, dates, and reporting fields.
Optional arguments are shown with their defaults.

#### Text and pattern functions

| Function | YAML arguments | Return hint | Definition |
|---|---|---|---|
| `substring` | `value`, `start`; optional `length=null` | `string` | Returns a SQL-style one-based substring. Null length means through the end. |
| `left` | `value`, `length` | `string` | Returns the leftmost characters. Negative length behaves as zero. |
| `right` | `value`, `length` | `string` | Returns the rightmost characters. Negative length behaves as zero. |
| `trim` | `value` | `string` | Removes leading and trailing whitespace. |
| `ltrim` | `value` | `string` | Removes leading whitespace. |
| `rtrim` | `value` | `string` | Removes trailing whitespace. |
| `upper` | `value` | `string` | Converts text to uppercase. |
| `lower` | `value` | `string` | Converts text to lowercase. |
| `normalize_whitespace` | `value` | `string` | Trims text and collapses repeated whitespace to one space. |
| `text_length` | `value` | `integer` | Returns length after text conversion. |
| `replace` | `value`, `old`, `new` | `string` | Replaces literal text; it does not interpret a regex. |
| `split_part` | `value`, `delimiter`, `part` | `string` | Returns a positive, one-based delimited part or null when missing. |
| `pad_left` | `value`, `length`; optional `pad=" "` | `string` | Pads or truncates on the left to an exact width. |
| `pad_right` | `value`, `length`; optional `pad=" "` | `string` | Pads or truncates on the right to an exact width. |
| `concat_ws` | `values`, `separator`; optional `skip_nulls=true` | `string` | Joins array items. With `skip_nulls=false`, any null item returns null. |
| `regex_extract` | `value`, `pattern`; optional `group=1` | `string` | Returns a capture group or null when no match exists. |
| `regex_replace` | `value`, `pattern`, `replacement` | `string` | Replaces regular-expression matches. |
| `regex_match` | `value`, `pattern` | `boolean` | Tests whether a regex matches anywhere in the text. |
| `text_contains_any` | `value`, string `candidates` | `boolean` | Tests whether text contains any non-null candidate string. |
| `is_blank` | `value` | `boolean` | Returns true for null or whitespace-only text. |

#### Null and conversion functions

| Function | YAML arguments | Return hint | Definition |
|---|---|---|---|
| `null_if` | `value`, `compare_to` | `same_as:value` | Returns null when both values are equal; otherwise preserves `value`. |
| `coalesce` | `values` | `common_type:values` | Returns the first non-null item from an ordered array. Items may be field, assigned, literal, or nested-function operands. |
| `to_string` | `value`; optional `on_error=error` | `string` | Converts a scalar to deterministic text. Collections are rejected. |
| `to_decimal` | `value`; optional `on_error=error` | `decimal` | Converts a finite scalar to exact `Decimal`. |
| `to_integer` | `value`; optional `on_error=error` | `integer` | Converts only lossless whole values; it never rounds. |
| `to_boolean` | `value`; optional `on_error=error` | `boolean` | Accepts booleans plus case-insensitive `true/false`, `t/f`, `yes/no`, `y/n`, and `1/0`. |
| `to_date` | `value`; optional `on_error=error` | `date` | Converts an ISO `YYYY-MM-DD` string, date, or datetime. |
| `to_timestamp` | `value`; optional `on_error=error` | `timestamp` | Requires an ISO timestamp with an offset and normalizes it to UTC. |
| `to_timestamp_ntz` | `value`; optional `on_error=error` | `timestamp_ntz` | Requires an ISO wall-clock timestamp without an offset. |

Every converter allows only `on_error: error` or `on_error: "null"`. `error`
raises a row evaluation error for invalid nonblank input. `null` returns null,
which may then be handled by the operand's `default_if_null`.

#### Exact-decimal functions

| Function | YAML arguments | Return hint | Definition |
|---|---|---|---|
| `decimal_abs` | `value` | `decimal` | Returns the absolute decimal value. |
| `decimal_add` | `left`, `right` | `decimal` | Adds exact decimal values. |
| `decimal_subtract` | `left`, `right` | `decimal` | Subtracts `right` from `left`. |
| `decimal_multiply` | `left`, `right` | `decimal` | Multiplies exact decimal values. |
| `decimal_divide` | `numerator`, `denominator`; optional `scale=18`, `rounding_mode=half_up` | `decimal` | Divides and rounds; zero denominator is an error. |
| `decimal_safe_divide` | `numerator`, `denominator`; optional `scale=18`, `rounding_mode=half_up` | `decimal` | Divides and rounds; zero denominator returns null. |
| `decimal_round` | `value`, `scale`; optional `rounding_mode=half_up` | `decimal` | Rounds at scale `-38` through `18`. |
| `decimal_clamp` | `value`, `minimum`, `maximum` | `decimal` | Constrains a value to inclusive bounds. Minimum above maximum is an error. |
| `decimal_min` | `left`, `right` | `decimal` | Returns the smaller decimal. |
| `decimal_max` | `left`, `right` | `decimal` | Returns the larger decimal. |

Allowed `rounding_mode` values are `half_up`, `half_even`, `half_down`, `up`,
`down`, `ceiling`, and `floor`. Decimal functions propagate null when any
required value operand is null.

#### Calendar functions

| Function | YAML arguments | Return hint | Definition |
|---|---|---|---|
| `date_add_days` | `value`, `days` | `date` | Adds integral calendar days. |
| `date_add_months` | `value`, `months` | `date` | Adds calendar months with month-end clamping. |
| `date_add_years` | `value`, `years` | `date` | Adds calendar years with leap-day clamping. |
| `date_diff_days` | `start`, `end` | `integer` | Returns `end - start` in calendar days. |
| `date_diff_months` | `start`, `end` | `integer` | Returns signed completed whole calendar months. |
| `date_diff_years` | `start`, `end` | `integer` | Returns signed completed whole calendar years. |
| `date_part` | `value`, `part` | `integer` | Returns `year`, `quarter`, `month`, `day`, ISO `day_of_week` (Monday=1), or `day_of_year`. |
| `month_start` | `value` | `date` | Returns the first calendar day of the month. |
| `month_end` | `value` | `date` | Returns the final calendar day of the month. |
| `quarter_start` | `value` | `date` | Returns the first calendar day of the quarter. |
| `quarter_end` | `value` | `date` | Returns the final calendar day of the quarter. |
| `year_start` | `value` | `date` | Returns the first calendar day of the year. |
| `year_end` | `value` | `date` | Returns the final calendar day of the year. |
| `first_business_day_of_month` | `value`, `holidays`; optional `weekend_days=[6,7]` | `date` | Returns the month's first day not listed as a holiday or ISO weekend day. Pass `holidays: []` when no holiday calendar applies. |
| `last_business_day_of_month` | `value`, `holidays`; optional `weekend_days=[6,7]` | `date` | Returns the month's last day not listed as a holiday or ISO weekend day. Pass `holidays: []` when no holiday calendar applies. |

#### Array functions

| Function | YAML arguments | Return hint | Definition |
|---|---|---|---|
| `array_size` | `values` | `integer` | Returns the item count. Null returns null; an empty array returns zero. |
| `array_contains_any` | `values`, `candidates` | `boolean` | Returns true when at least one candidate is present. Empty candidates return false. |
| `array_contains_all` | `values`, `candidates` | `boolean` | Returns true when every candidate is present. Empty candidates return true. |
| `array_join` | `values`, `separator`; optional `skip_nulls=true` | `string` | Alias of `concat_ws`; joins array items as text. With `skip_nulls=false`, any null item returns null. |

Array functions require an actual array-like value. We intentionally reject a
scalar string instead of treating it as a character array or silently wrapping
it in a one-item array. A null required array returns null. Empty arrays remain
distinct: size is zero, `contains_any` is false, `contains_all` is true for an
empty candidate array, and joining an empty array returns an empty string.

This assignment example composes field operands inside an argument array,
uses an optional array default, and turns a bad conversion into an explicit
null result:

```yaml
assign:
  selected_code:
    custom_function:
      name: coalesce
      args:
        values:
          - {field: primary_code}
          - {field: secondary_code}
  tags_text:
    custom_function:
      name: array_join
      args:
        values: {field: tags}
        separator: "|"       # skip_nulls defaults to true
  parsed_count:
    custom_function:
      name: to_integer
      args:
        value: {field: raw_count}
        on_error: "null"     # quotes distinguish the mode from YAML null
```

## Delta repository contract

The repository owns two Delta tables. We create them only through an explicit
`create_tables` call.

### `ruleset_versions`

| Field group | Allowed values | Definition |
|---|---|---|
| Identity | Non-null `ruleset_id`, `ruleset_name`, and `version` | Both ID/version and name/version pairs are immutable. |
| `status` | `published` or `retired` | Repository lifecycle only; not part of canonical ruleset content. |
| Canonical content | Non-null `payload_json` and SHA-256 `content_hash` | Deterministic ruleset content used for reconstruction and audit. |
| Counts | Non-negative integer rule, condition, assignment, and custom-function counts | Queryable summary of the canonical payload. |
| Ownership | Nullable description, owner, and owner department | Copied from authored metadata. |
| Publication audit | Nullable actor and UTC timestamp | Stored when publication succeeds. Missing actor becomes `system`. |
| Retirement audit | Nullable actor and UTC timestamp | Populated once when a published row is retired. |

### `function_registry`

| Field group | Allowed values | Definition |
|---|---|---|
| Identity | Non-null `function_name` | Merge key for registry metadata. |
| Implementation metadata | Non-null reference and argument-contract JSON | Describes where the environment implementation comes from and its exact arguments. |
| Permissions | Non-null condition, assignment, and active booleans | Controls valid ruleset references. |
| Type and description | Nullable return hint, description, and version | Helps Spark infer assignment types and supports audit review. |

Publishing appends a new immutable ruleset-version row. It never overwrites an
existing ID/version or name/version pair, even if the existing row is retired.
Multiple different versions of the same ruleset may remain published at once.

## `RulesEngineService` API

`RulesEngineService` is our normal public facade. It wires the compiler,
registry, validators, repository, runtime, formatter, and coverage analyzer
into one object. It does not manage cluster libraries,
permissions, external logs, bundle deployment, or business approval.

### Public component attributes

The service keeps its wired components available when an advanced caller
needs to use a lower-level API directly.

| Attribute | Allowed value | Definition |
|---|---|---|
| `repository` | Configured repository object | Delta persistence and published-ruleset loading backend. |
| `registry` | `FunctionRegistry` | In-memory function specs and executable callables. |
| `validator` | `SparkRulesetCompatibilityValidator` | Shared semantic and Spark-schema validator. |
| `publish_service` | `PublishService` | Validation and persistence coordinator. |
| `runtime` | `SparkRulesEngineRuntime` | Typed Spark DataFrame evaluator. |
| `compiler` | `YamlRulesetCompiler` | Strict YAML compiler. |
| `rule_formatter` | `HumanReadableRulesetFormatter` | Readable rule and assignment formatter. |
| `coverage_analyzer` | `RulesetCoverageAnalyzer` | Spark match-coverage analyzer. |

### `RulesEngineService(...)`

```python
RulesEngineService(
    *,
    repository,
    registry,
    validator=None,
)
```

We use the constructor when an application already owns its repository and
registry objects.

| Parameter | Allowed values | Definition |
|---|---|---|
| `repository` | Object implementing the ruleset repository protocol | Required persistence and loading backend. |
| `registry` | `FunctionRegistry` | In-memory metadata and callables available to validation and evaluation. |
| `validator` | `SparkRulesetCompatibilityValidator` or `None` | Optional shared validator. When omitted, the service creates one from `registry`. |

The constructor creates the publish service, Spark runtime, compiler,
human-readable formatter, and coverage analyzer. It performs no Spark action
and writes no data.

### `from_schema`

```python
RulesEngineService.from_schema(
    spark,
    schema,
    *,
    ruleset_versions_table=None,
    function_registry_table=None,
    register_standard=True,
)
```

This is the easiest way to build a service for Databricks.

| Parameter | Allowed values and default | Definition |
|---|---|---|
| `spark` | Active `SparkSession` | Session used for Delta reads, writes, and Spark evaluation. |
| `schema` | Safe one- or two-part identifier such as `schema` or `catalog.schema` | Base namespace used to derive default table names. Each part must match `[A-Za-z_][A-Za-z0-9_]*`. The service does not create the schema. |
| `ruleset_versions_table` | Safe one-, two-, or three-part table name or `None` | Overrides `<schema>.ruleset_versions`. |
| `function_registry_table` | Safe one-, two-, or three-part table name or `None` | Overrides `<schema>.function_registry`. |
| `register_standard` | `true` or `false`; default `true` | Registers standard specs and executable implementations in memory. It does not persist them. |

Returns a configured service without creating tables or starting a Spark job.

### `table_names`

```python
service.table_names
```

Read-only property returning `RulesEngineTableNames` with `ruleset_versions`
and `function_registry`. It performs no I/O.

### `create_tables`

```python
service.create_tables(mode="error")
```

Creates both metadata tables with explicit Delta DDL.

| `mode` | Allowed behavior |
|---|---|
| `error` or `errorifexists` | Fail if either target table already exists. `error` is the default alias. |
| `ignore` | Create missing tables and leave existing tables unchanged. |
| `overwrite` | Drop and recreate both tables. This deletes their existing metadata and must be limited to disposable environments. |

Returns `None`. It does not create the parent catalog or schema.

### `save_standard_function_registry`

```python
service.save_standard_function_registry(update_existing=True)
```

Persists the package's standard function specs to `function_registry`.
`update_existing=true` refreshes package-owned rows; `false` inserts only
missing names. This method does not register executable callables—the service
already does that in memory when `register_standard=true`. Returns `None`.

### `save_function_registry_rows`

```python
service.save_function_registry_rows(rows, update_existing=True)
```

Persists caller-supplied `FunctionRegistryRow` objects. `rows` must be a list;
an empty list is a no-op. Existing rows are updated by `function_name` when
`update_existing=true` and preserved when false. Executable Python callables
are never stored in Delta. Returns `None`.

### `compile_yaml_text`

```python
ruleset = service.compile_yaml_text(yaml_text)
```

Compiles one YAML string into an immutable `Ruleset`. It rejects invalid YAML,
duplicate keys, unknown keys, wrong primitive types, and invalid shapes by
raising `CompilationError`. It does not perform semantic validation, access
Spark, or persist anything.

### `compile_yaml_path`

```python
ruleset = service.compile_yaml_path(path)
```

Reads one UTF-8 YAML file and delegates to `compile_yaml_text`. `path` may be a
string or `Path`. A missing file or invalid document raises
`CompilationError`. It returns the compiled `Ruleset` and writes nothing.

### `publish`

```python
service.publish(ruleset, published_by=None)
```

Runs semantic validation and then writes one immutable published row.
`published_by` may be a string or `None`; the repository records `system`
when no usable actor is supplied.

Returns `None`. `ValidationFailedError` is raised before any write when
validation fails. `RepositoryError` is raised for an identity collision or
repository failure. Because this entry point has no input DataFrame, it cannot
prove compatibility with a future DataFrame schema.

### `publish_yaml_text`

```python
ruleset = service.publish_yaml_text(yaml_text, published_by=None)
```

Compiles YAML text, delegates to `publish`, and returns the compiled `Ruleset`
after a successful write. It can raise either `CompilationError`,
`ValidationFailedError`, or `RepositoryError`. It does not create tables.

### `publish_yaml_path`

```python
ruleset = service.publish_yaml_path(path, published_by=None)
```

Reads and compiles a YAML file, delegates to `publish`, and returns the
compiled `Ruleset`. Failure behavior is the same as `publish_yaml_text` plus a
missing-file `CompilationError`.

### `load_published`

```python
ruleset = service.load_published(ruleset_name, version=None)
```

Loads and reconstructs one row whose status is `published`.

| Parameter | Allowed values | Definition |
|---|---|---|
| `ruleset_name` | Exact non-empty name string | Name stored in repository metadata. |
| `version` | Exact string or `None` | When supplied, selects that immutable version. When omitted, exactly one published version must exist. |

Returns a `Ruleset`. It raises `RepositoryError` when no row exists, when a
name-only lookup is ambiguous, or when duplicate immutable rows exist.

### `describe_rules`

```python
rows = service.describe_rules(
    ruleset=None,
    ruleset_name=None,
    version=None,
)
```

Returns one plain dictionary per rule with readable condition logic and
assignment text. Supply either a compiled `ruleset` or `ruleset_name` with an
optional version. A supplied `ruleset` takes precedence and causes the name
and version arguments to be ignored. Supplying neither raises `ValueError`.
Repository lookup errors pass through when loading by name. No Spark action is
started after a ruleset is available.

### `evaluate_dataframe`

```python
evaluation = service.evaluate_dataframe(
    df,
    *,
    ruleset=None,
    ruleset_name=None,
    version=None,
    key_columns=["row_id"],
    column_prefix="rules_engine",
    fail_on_error=True,
    include_error_traceback=False,
    full_audit=False,
)
```

Evaluates a supplied ruleset or loads one by name. A supplied `ruleset` takes
precedence. Supplying neither a ruleset nor a name raises `ValueError`.
`key_columns` is required and follows the identity contract in the Spark
evaluation section.

Before building the shared lazy plan, we validate semantics, source fields,
assignment types, null fallbacks, temporal compatibility, custom-function
contracts, key metadata, reserved output names, and worker serialization.
Building the object and either projection is lazy; row evaluation begins when
the caller starts a Spark action. Parameter values and exact output behavior
are defined in the Spark evaluation section above.

Returns `DataFrameEvaluation`. Its `results_df` property returns keys plus
engine evidence; `apply_assignments()` returns the business DataFrame with
final assignments applied; `persist()` and `unpersist()` manage the one shared
evaluated plan. The object is bound to the exact source DataFrame, ruleset,
prefix, and audit mode used in this call, so there is no separate ruleset or
result argument that can be mismatched later. It never mutates the input
DataFrame or publishes metadata.

### `coverage_report`

```python
report = service.coverage_report(
    df,
    *,
    ruleset=None,
    ruleset_name=None,
    version=None,
    broad_match_threshold=0.40,
    column_prefix="rules_engine_coverage",
)
```

Evaluates a supplied or loaded ruleset with captured row errors and starts one
Spark aggregation action. `broad_match_threshold` must be between `0` and `1`.
`column_prefix` must be non-empty and must not conflict with an existing input
column beginning with `<prefix>_`. Returns `CoverageReport` as described in
the coverage section. Supplying neither a ruleset nor a name raises
`ValueError`.

### `retire`

```python
service.retire(ruleset_id, version, retired_by=None)
```

Changes one persisted row from `published` to `retired` using the stable
`ruleset_id` and exact version. It records the actor and UTC retirement time,
then verifies the update. The canonical payload and content hash do not
change. Missing rows, duplicate stable identities, already-retired rows, and
failed verification raise `RepositoryError`. Returns `None`.

### End-to-end service example

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
    published_by="alm-rules-team",
)

loaded = service.load_published("Account Review", version="1")
evaluation = service.evaluate_dataframe(
    input_df,
    ruleset=loaded,
    key_columns=["account_id"],
    fail_on_error=False,
)
result_df = evaluation.results_df
applied_df = evaluation.apply_assignments()

service.retire(
    loaded.ruleset_id,
    loaded.version,
    retired_by="alm-rules-team",
)
```

## Package module guide

The package is intentionally split by responsibility. We keep Spark-specific
imports and behavior out of compile-only paths where practical.

| Module | What it does | Important public pieces |
|---|---|---|
| `rules_engine.__init__` | Defines the supported top-level package surface. It imports compile-only objects directly and lazily imports Spark-backed objects so YAML tooling can load without paying the full Spark import cost. | Top-level exports such as `RulesEngineService`, `YamlRulesetCompiler`, `RulesetValidator`, `SparkRulesEngineRuntime`, and `__version__`. |
| `rules_engine.analytics` | Runs the production Spark evaluator and aggregates rule match behavior. It calculates total, no-match, error, match, first-match, dead-rule, and broad-rule measures and retains a lazy DataFrame of clean no-match rows. | `RuleCoverage`, `CoverageReport`, `RulesetCoverageAnalyzer`. |
| `rules_engine.authoring` | Builds the deterministic, JSON-compatible contract consumed by authoring applications. It exposes engine-owned operator behavior, enums, literal type hints, function contracts, and build identity without presentation metadata. | `build_authoring_manifest`, `AUTHORING_MANIFEST_VERSION`. |
| `rules_engine.compiler_yaml` | Parses strict YAML with duplicate-key protection and converts canonical mappings into immutable dataclasses. It applies only structural defaults, preserves fractional values exactly, parses supported typed dates and decimals, and rejects unknown keys and ambiguous operand shapes. | `YamlRulesetCompiler`. |
| `rules_engine.dataframe_evaluation` | Owns the one lazy source-plus-results Spark plan created by DataFrame evaluation. It exposes the key-only result projection, applies explicit assignment outcomes to business columns without a join, preserves column order, handles atomic struct values, and manages optional shared persistence. | `DataFrameEvaluation`. |
| `rules_engine.enums` | Holds the only accepted lifecycle, logical, operand, comparison, and diagnostic object values. Centralizing these strings prevents aliases from drifting between compilation, validation, runtime behavior, and persistence. | `RulesetStatus`, `LogicalOperator`, `OperandKind`, `ComparisonOperator`, `ObjectType`. |
| `rules_engine.exceptions` | Defines the package exception hierarchy so callers can distinguish compilation, validation, registry, and repository failures from ordinary Python errors. | `RulesEngineError`, `CompilationError`, `ValidationFailedError`, `RegistryError`, `RepositoryError`. |
| `rules_engine.exporter_yaml` | Converts compiled dataclasses back to canonical YAML. It preserves explicit identities, nested groups, exact decimals and dates, operand forms, default values, and mappings so export and recompile produce the same model. | `YamlRulesetExporter`. |
| `rules_engine.human_readable` | Renders rules, groups, operands, operators, and assignments as readable text for reviewers and full-audit explanations. It formats authored logic; it does not evaluate or persist rules. | `HumanReadableRulesetFormatter`. |
| `rules_engine.models` | Defines the canonical immutable rule tree, operands, assignments, persistence rows, validation results, and runtime traces. These dataclasses are the shared language used by every other module. | `Ruleset`, `Rule`, `ConditionGroup`, `Condition`, operand classes, `Assignment`, row and trace models. |
| `rules_engine.publish` | Coordinates publication. It runs semantic validation and calls the repository only when validation passes. | `PublishService`. |
| `rules_engine.registry` | Keeps custom-function metadata and executable implementations in memory under one exact function name. It validates unique argument contracts, binds optional defaults in declared order, persists rich argument metadata, enforces unique registration, and provides focused errors for unknown specs or missing implementations. | `CustomFunctionArgSpec`, `CustomFunctionSpec`, `FunctionRegistry`, `CustomFunction`. |
| `rules_engine.repository` | Owns the Delta table names, schemas, DDL, immutable publication, explicit-version loading, retirement, and registry metadata merge behavior. It detects duplicate identities instead of selecting an arbitrary row. | `RulesEngineTableNames`, `RulesetRepository`, `SparkDeltaRulesetRepository`. |
| `rules_engine.runtime` | Implements the deterministic pure-Python row evaluator used inside the Spark UDF. It evaluates Boolean trees, resolves operands and null defaults, calls registered functions, performs comparisons, commits assignments atomically by rule, and returns explicit `{applied, value}` assignment outcomes. | `SparkRowEvaluator` and runtime result/trace behavior used by higher-level APIs. |
| `rules_engine.serializer` | Creates deterministic canonical JSON, SHA-256 content hashes, queryable summary counts, and `RulesetVersionRow` objects. It also reconstructs a `Ruleset` while preserving supported Python literal types. | `DeltaRowSerializer`. |
| `rules_engine.service` | Provides the public facade documented above. It wires the package components into the normal compile, publish, load, describe, evaluate, cover, and retire workflows. | `RulesEngineService`. |
| `rules_engine.spark_runtime` | Adapts the pure row evaluator to a typed Spark Python UDF. It validates key metadata and the incoming schema, infers typed `{applied, value}` assignment outcomes, sends only required source columns to workers, checks callable serialization, builds ordered compact/full-audit fields, and returns one lazy `DataFrameEvaluation`. | `SparkRulesEngineRuntime`, `required_source_columns`. |
| `rules_engine.spark_types` | Provides shared exact-fit helpers for Spark integer, decimal, date, and timestamp handling. We use it to prevent silent overflow, precision loss, and incompatible temporal assignments. | `decimal_literal_type`, `decimal_value_fits`, shared type constants. |
| `rules_engine.spark_validator` | Extends semantic validation with actual Spark schema checks. It verifies field existence, default compatibility, function return hints, collection and temporal comparisons, assignment target consistency, and lossless type coercion, then builds the assignment `StructType`. | `SparkRulesetCompatibilityValidator`. |
| `rules_engine.standard_functions` | Implements and declares 58 deterministic text, regex, conversion, exact-decimal, null-composition, calendar, business-day, and array functions. It keeps optional defaults, argument types, allowed configuration values, permissions, return hints, and implementation versions beside each callable. | Standard callables, `register_standard_functions`, `standard_function_rows`. |
| `rules_engine.validator` | Applies semantic rules that are independent of a DataFrame schema. It checks ownership, non-empty content, unique IDs/orders, operand arity, null options, assignment dependencies, and custom-function permissions and argument contracts. | `RulesetValidator`. |
| `rules_engine.version` | Stores the installed package version used by the top-level package and the `rules_engine_engine_version` output column. | `__version__`. |

## Repository layout

```text
docs/
  rules_engine_system_test_summary.md
  rules_engine_unit_test_summary.md
examples/
  rulesets/
    rules_engine_system_testing_rules.yaml
  rules_engine_custom_function_authoring_guide.py
  rules_engine_developer_guide.py
  rules_engine_quickstart_guide.py
notebooks/
  99.rules_engine_system_tests.py
src/
  rules_engine/                        Package source
tests/                                 Unit and Spark tests
outputs/                               Additional generated YAML artifacts
```

## Development checks

```powershell
python -m ruff check .
python -m pytest tests
```

Spark tests are skipped unless `RULES_ENGINE_RUN_SPARK_TESTS=1` is set.

## Known boundaries

- Row evaluation, not aggregates or windows. Cross-row facts belong in the
  input DataFrame.
- Support for YAML authoring only.
- Custom functions run as registered Python callables inside the row UDF.
- `like` and `not_like` support `%` and `_` wildcards but do not provide an
  escape character for matching those characters literally.
- Ruleset publication checks identities before append, but it does not provide
  an atomic cross-writer uniqueness guarantee. Serialize concurrent publishers
  or enforce uniqueness in the surrounding deployment workflow.
