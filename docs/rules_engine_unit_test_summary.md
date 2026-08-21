# Rules Engine Unit Test Summary

Source: the current behavioral pytest suite under `tests/`.

The suite contains **221 explicit test functions** and collects **259 pytest cases** after parameter expansion. **19 cases** exercise the live Spark worker boundary and require `RULES_ENGINE_RUN_SPARK_TESTS=1` plus a compatible Spark and Java runtime.

Build, README, notebook execution, and workspace layout checks are outside the unit suite. The system and UAT notebooks cover persisted metadata tables and end-to-end Databricks behavior.

## Coverage Matrix

| Area | Explicit Tests | Current Contract Covered |
| --- | ---: | --- |
| YAML compilation | 19 | Strict mapping shapes and scalar types, exact numerics, operand null defaults, assigned operands, and duplicate-key protection. |
| YAML export | 4 | Stable round trips and the exact authoring vocabulary. |
| Governance and change control | 15 | Embedded expectations, compact/full-audit behavior, semantic diffs, and coverage diagnostics. |
| Publish workflow | 4 | Validation and governance gates before persistence. |
| Repository persistence | 9 | Immutable ruleset identity, lifecycle operations, effective dates, and registry behavior. |
| Repository schema | 5 | Metadata DDL, table naming, and nullability. |
| In-memory runtime | 55 | Ordered evaluation, assignment chaining, atomic same-rule assignments, null defaults, errors, output schemas, audit provenance, and worker safety. |
| Version serialization | 13 | Deterministic payloads and hashes, exact values, lifecycle separation, and deserialization. |
| Service orchestration | 14 | Public facade behavior across compile, publish, load, describe, evaluate, and retire operations. |
| Spark runtime | 19 | Native typed results, column ordering and prefixes, assignment chaining, null behavior, semantic preflight, audit output, and lazy fail-fast execution through a real worker. |
| Spark validation | 34 | Source-field validation and lossless type compatibility for conditions and assignments. |
| Standard functions | 12 | Text, numeric, null, and calendar behavior. |
| Ruleset validation | 18 | Authoring invariants, including assignment-producer ordering and function contracts. |
| **Total** | **221** | |

## Execution

Run the non-Spark suite:

```bash
python -m pytest tests -q
```

Run the complete suite with Spark enabled:

```bash
RULES_ENGINE_RUN_SPARK_TESTS=1 python -m pytest tests -q
```

The live-Spark cases are skipped unless explicitly enabled. They verify the actual Python-worker serialization and Spark schema boundary instead of mocking it.
