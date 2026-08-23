# Rules Engine Unit Test Summary

Source: the current behavioral pytest suite under `tests/`.

The suite contains **218 explicit test functions** and collects **263 pytest cases** after parameter expansion. **23 cases** exercise the live Spark worker boundary and require `RULES_ENGINE_RUN_SPARK_TESTS=1` plus a compatible Spark and Java runtime.

README, notebook execution, and workspace layout checks are outside the unit suite. The Databricks system-test notebook is separate from these counts and is documented in `rules_engine_system_test_summary.md`.

## Coverage Matrix

| Area | Explicit Tests | Current Contract Covered |
| --- | ---: | --- |
| YAML compilation | 19 | Strict mapping shapes and scalar types, exact numerics, operand null defaults, assigned operands, and duplicate-key protection. |
| YAML export | 5 | Stable round trips, shipped-artifact hashes, and the exact authoring vocabulary. |
| Governance and audit contract | 11 | Embedded expectations, explicit-null versus unapplied assertions, compact/full-audit behavior, assignment provenance, and coverage counts. |
| Publish workflow | 2 | Validation and executable-example gates before persistence. |
| Repository persistence | 9 | Dual immutable identity checks, duplicate-safe lifecycle operations, and registry behavior. |
| Repository schema | 5 | Metadata DDL, table naming, and nullability. |
| In-memory runtime | 59 | Ordered evaluation, explicit assignment outcomes, assignment chaining, atomic same-rule assignments, null defaults, errors, condition identity, audit provenance, worker safety, key metadata, ambiguous-key rejection, and immutable assignment keys. |
| Version serialization | 11 | Deterministic payloads and hashes, exact values, lifecycle separation, and deserialization. |
| Service orchestration | 14 | Public facade behavior across compile, publish, load, describe, evaluate, and retire operations. |
| Spark runtime | 23 | Keyed result separation, scalar and atomic-struct assignment application, explicit null clearing, retained values on captured errors, evaluate-once shared persistence, native typed results, column ordering and prefixes, assignment chaining, active-rule preflight, audit output, and proven lazy fail-fast execution through a real worker. |
| Spark validation | 34 | Source-field validation and lossless type compatibility for conditions and assignments. |
| Standard functions | 12 | Text, numeric, null, and calendar behavior. |
| Ruleset validation | 14 | YAML contract invariants, including the active-rule requirement, assignment-producer ordering, and function contracts. |
| **Total** | **218** | |

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
