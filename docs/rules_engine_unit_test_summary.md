# Rules Engine Unit Test Summary

Source: the current behavioral pytest suite under `tests/`.

The suite contains **250 explicit test functions** and collects **317 pytest cases** after parameter expansion. **24 cases** exercise the live Spark worker boundary and require `RULES_ENGINE_RUN_SPARK_TESTS=1` plus a compatible Spark and Java runtime.

README, notebook execution, and workspace layout checks are outside the unit suite. The Databricks system-test notebook is separate from these counts and is documented in `rules_engine_system_test_summary.md`.

## Coverage Matrix

| Area | Explicit Tests | Current Contract Covered |
| --- | ---: | --- |
| Authoring manifest | 5 | Deterministic JSON-compatible operator, enum, literal-hint, function-hint, and registered function contracts without implementation references. |
| YAML compilation | 28 | Strict mapping shapes and scalar types, exact numerics, known scalar-hint enforcement, typed date/timestamp/boolean normalization, extension-hint retention, operand null defaults, assigned operands, recursively nested function arguments, and duplicate-key protection. |
| YAML export | 6 | Stable round trips, shipped-artifact hashes, nested argument operands, and the exact authoring vocabulary. |
| Governance and audit contract | 4 | Compact/full-audit behavior, Python/Spark worker semantic parity, public coverage imports, and shared engine/function metadata versioning. |
| Publish workflow | 3 | Semantic validation before persistence and publication provenance. |
| Repository persistence | 9 | Dual immutable identity checks, duplicate-safe lifecycle operations, and registry behavior. |
| Repository schema | 6 | Metadata DDL, safe quoted table naming, identifier rejection, and nullability. |
| In-memory runtime | 63 | Exact string-code equality, typed numeric equality, ordered evaluation, actionable bound/conversion errors, explicit assignment outcomes, assignment chaining, atomic same-rule assignments, null defaults, condition identity, audit provenance, worker safety, key metadata, ambiguous-key rejection, and immutable assignment keys. |
| Version serialization | 11 | Deterministic payloads and hashes, exact values, lifecycle separation, and deserialization. |
| Service orchestration | 14 | Public facade behavior across compile, publish, load, describe, evaluate, and retire operations. |
| Spark runtime | 24 | Keyed result separation, scalar and name-aligned atomic-struct assignment application, explicit null clearing, retained values on captured errors, evaluate-once shared persistence, native typed results, column ordering and prefixes, assignment chaining, active-rule preflight, audit output, nested array arguments, optional function defaults, and proven lazy fail-fast execution through a real worker. |
| Spark validation | 40 | Source-field validation, compiled-literal/type-hint agreement, recursive name-based struct compatibility, trailing-zero decimal representability, typed custom-function arguments, derived function return types, common-type conflict rejection, and lossless type compatibility for conditions and assignments. |
| Standard functions | 21 | Text and regex behavior, strict conversion failure policies, exact-decimal arithmetic, null composition, completed calendar periods, business-day month boundaries, arrays, rich registry metadata, and runtime integration. |
| Ruleset validation | 16 | YAML contract invariants, including the active-rule requirement, assignment-producer ordering, enforceable registry metadata, and required/optional typed function arguments with literal constraints. |
| **Total** | **250** | |

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
