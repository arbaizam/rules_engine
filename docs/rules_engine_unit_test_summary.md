# Rules Engine Unit Test Summary

Source: the current behavioral pytest suite under `tests/`.

The suite contains **249 explicit test functions** and collects **316 pytest cases** after parameter expansion. **24 cases** exercise the live Spark worker boundary and require `RULES_ENGINE_RUN_SPARK_TESTS=1` plus a compatible Spark and Java runtime.

README, notebook execution, and workspace layout checks are outside the unit suite. The Databricks system-test notebook is separate from these counts and is documented in `rules_engine_system_test_summary.md`.

## Coverage Matrix

| Area | Explicit Tests | Current Contract Covered |
| --- | ---: | --- |
| Authoring manifest | 5 | Deterministic JSON-compatible operator, enum, literal-hint, function-hint, and registered function contracts without implementation references. |
| YAML compilation | 28 | Strict mapping shapes and scalar types, exact numerics, known scalar-hint enforcement, typed date/timestamp/boolean normalization, extension-hint retention, operand null defaults, assigned operands, recursively nested function arguments, and duplicate-key protection. |
| YAML export | 6 | Stable round trips, shipped-artifact hashes, nested argument operands, and the exact authoring vocabulary. |
| Governance and audit contract | 3 | Strict full-audit option handling, Python/Spark worker semantic parity, public coverage imports, and shared engine/function metadata versioning. |
| Publish workflow | 3 | Semantic validation before persistence and publication provenance. |
| Repository persistence | 9 | Dual immutable identity checks, duplicate-safe lifecycle operations, and registry behavior. |
| Repository schema | 6 | Metadata DDL, safe quoted table naming, identifier rejection, and nullability. |
| In-memory runtime | 63 | Exact string-code equality, typed numeric equality, ordered evaluation, actionable bound/conversion errors, explicit assignment outcomes, assignment chaining, atomic same-rule assignments, null defaults, condition identity, audit provenance, worker safety, explicit/default-all key metadata, ambiguous-key rejection, and immutable assignment keys. |
| Version serialization | 11 | Deterministic payloads and hashes, exact values, lifecycle separation, and deserialization. |
| Service orchestration | 14 | Public facade behavior across compile, publish, load, describe, evaluate, and retire operations. |
| Spark runtime | 24 | Explicit and default-all keyed result separation, scalar and name-aligned atomic-struct assignment application, explicit null clearing, retained values on captured errors, evaluate-once shared persistence, native typed results, column ordering and prefixes, assignment chaining, active-rule preflight, audit output, nested array arguments, optional function defaults, and proven lazy fail-fast execution through a real worker. |
| Spark validation | 40 | Source-field validation, compiled-literal/type-hint agreement, recursive name-based struct compatibility, trailing-zero decimal representability, typed custom-function arguments, derived function return types, common-type conflict rejection, and lossless type compatibility for conditions and assignments. |
| Standard functions | 21 | Text and regex behavior, strict conversion failure policies, exact-decimal arithmetic, null composition, completed calendar periods, business-day month boundaries, arrays, rich registry metadata, and runtime integration. |
| Ruleset validation | 16 | YAML contract invariants, including the active-rule requirement, assignment-producer ordering, enforceable registry metadata, and required/optional typed function arguments with literal constraints. |
| **Total** | **249** | |

## Current-Version Audit

The 2026-08-26 audit reviewed every collected test objective against the
current README contract, its owning implementation boundary, and the separate
Databricks system-test inventory. Layered tests were retained when they prove
different failure boundaries; case count alone was not treated as evidence of
duplication.

Five test functions were removed:

- A governance full-audit payload/schema test duplicated the focused payload
  shape, compact/full parity, and live Spark output-order tests.
- A service retirement-actor test repeated the exact assertion already made by
  the combined facade-delegation test.
- A Spark test for a historical fixed temporary result-column name duplicated
  current arbitrary-source-column preservation and system test ST-015.
- A standalone no-match audit-array test was fully subsumed by the compact/full
  success, no-match, and error parity matrix.
- A basic field `default_if_null` match test was fully subsumed by the stronger
  trace test that proves the match and the original, fallback, effective, and
  application values.

No compiler, exporter, repository, serializer, Spark-validator,
standard-function, or semantic-validator objective was retired. Their apparent
overlap represents distinct compile, validation, persistence, worker, or public
facade boundaries.

## Execution

Run the non-Spark suite:

```bash
python -m pytest tests -q
```

Run the complete suite with Spark enabled:

```bash
RULES_ENGINE_RUN_SPARK_TESTS=1 python -m pytest tests -q
```

The live-Spark cases are skipped unless explicitly enabled. They verify the actual Python-worker serialization and Spark schema boundary instead of mocking it. The two cache-specific cases also probe the active session and skip automatically when Databricks reports that persistence APIs are unavailable on serverless compute.
