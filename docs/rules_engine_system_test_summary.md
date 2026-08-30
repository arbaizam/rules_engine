# Rules Engine System Test Summary

Source: `notebooks/99.rules_engine_system_tests.py`.

ALM Engineering uses this Databricks notebook for behavior that needs a real
Spark, Python-worker, or Delta boundary. Compiler and validator permutations
remain in the pytest suite. The system notebook contains 18 focused tests and
does not run pytest itself.

## Test Inventory

| Test ID | Boundary | What we prove |
|---|---|---|
| ST-001 | Delta DDL | We can explicitly create the two metadata tables with the current column contract. |
| ST-002 | Function registry table | Standard metadata is rerunnable, and caller-supplied function metadata persists with its permissions. |
| ST-003 | YAML, publication, and Delta | The shipped fixture compiles, validates, publishes, loads unchanged, and retains its deterministic content hash. |
| ST-004 | Repository identity | Both `(ruleset_id, version)` and `(ruleset_name, version)` reject overwrite attempts. |
| ST-005 | Published-version loading | Continuing ST-004, name-only loading fails when more than one version is published, while explicit versions load correctly. |
| ST-006 | Repository lifecycle | Continuing ST-004/ST-005, retirement uses stable ID plus exact version, records its actor and time, removes that row from published loading, and cannot be repeated silently. |
| ST-007 | Spark output contract | Compact and full-audit success, no-match, and error rows have identical business results and exact ordered columns. |
| ST-008 | Full-audit trace | Continuing ST-007, every condition reports its condition ID, containing group ID, group operator, active state, pass state, and source columns. |
| ST-009 | Rule control flow | A matching `stop_on_match: true` rule commits its assignments and skips later rules. |
| ST-010 | Prior assignments | Later rules read committed values through `assigned`, while assignments within one rule share a pre-rule snapshot. |
| ST-011 | Null substitution | Numeric and text `default_if_null` values are applied before comparison. |
| ST-012 | Assignment history | Full audit records the original value, each proposed value, the immediate next override, and the final winning assignment. |
| ST-013 | Typed worker values | Decimal, date, and nested struct assignments retain their Spark types and values across the Python-worker boundary. |
| ST-014 | Custom-function worker | A registered custom function works in both a condition and an assignment inside a real Spark worker. |
| ST-015 | Output-name isolation | An ordinary `rules_engine_result` input is preserved, custom prefixes work, and full-audit names remain reserved in compact mode. |
| ST-016 | Coverage aggregation | Coverage returns total, no-match, error, per-rule first-match, dead-rule, broad-rule, and clean no-match results. |
| ST-017 | Assignment application | Keyed result rows remain separate from business rows; explicit nulls clear values, non-null values replace or append columns, structs replace atomically, and keys stay immutable. On compute supporting DataFrame cache APIs, both projections also share explicit persistence; serverless logs that cache-only check as skipped. |
| ST-018 | Standard-function worker contract | Optional defaults, operands nested in argument arrays, array predicates and joins, nested conversion plus decimal arithmetic, business-month boundaries, and `on_error: "null"` retain their types and values across a real Python worker. |

## Execution Contract

Run the notebook from a Databricks checkout containing `databricks.yml` at the
repository root. The import cell locates that file and appends
`<repository>/src` to `sys.path`.

Provide these inputs before execution:

| Input | Required | Allowed values | Definition |
|---|---:|---|---|
| `SCHEMA` | Yes | Safe two-part `catalog.schema` whose schema name contains `test`, `scratch`, or `tmp` | Disposable namespace used by the system-test metadata tables. |
| `RULESET_YAML_PATH` | No | Absolute path or path relative to the repository root | Fixture override. The default is `examples/rulesets/rules_engine_system_testing_rules.yaml`. |

The setup cell creates the supplied schema when it does not exist. ST-001 then
overwrites only these two tables inside that verified disposable schema:

- `ruleset_versions_system_test`
- `function_registry_system_test`

The notebook leaves both tables available after the run so ALM Engineering
can inspect the persisted rows. It does not create, alter, or delete any other
table.

## Pass Condition

The run is successful only when execution reaches the final message:

```text
PASS: All 18 current-contract rules engine system tests completed.
```

Any failed assertion or unexpected exception stops the notebook at the
responsible test ID.
