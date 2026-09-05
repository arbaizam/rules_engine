# Rules Engine Unit Test Summary

Source: the behavioral pytest suite under `tests/`.

The [generated inventory](rules_engine_test_inventory.md) lists explicit test
functions, collected parameter cases, and live Spark cases by module. These
counts are computed from the checkout and verified in CI. They describe
coverage, not a record of a successful run.

The [Databricks system-test notebook](rules_engine_system_test_summary.md)
is a separate acceptance suite. Local notebook bootstrap tests execute only
root discovery; they do not claim to run Databricks cells or Delta operations.

The [September 2026 review](reviews/2026-09-05-unit-test-review.md) records the
original assessment of all 342 functions and 531 cases. The
[remediation report](reviews/2026-09-05-unit-test-remediation.md) tracks the
implemented recommendations and verification. Those review counts describe
the historical baseline; use the generated inventory for the current suite.

## Coverage Matrix

| Area | Current Contract Covered |
| --- | --- |
| Authoring manifest | Independent exact records for every operator, including uniqueness, arity and shapes; enum, literal-hint, ordered-sequence and registered function contracts. |
| YAML compilation and canonical values | Strict shapes and identities, supported finite scalar/collection values, exact numbers, temporal normalization, argument operands, mapping-key collisions, and rejection of unsupported binary values. |
| YAML export | Model/hash and concrete scalar type round trips through payload and text APIs, binary float tags including signed zero and nested values, type-sensitive function behavior, shipped-artifact hashes, nested operands and escaped argument mappings. |
| Governance and execution parity | Public exports, shared version metadata, strict options, scrambled metadata with independent execution outcomes, and Python/Spark adapter parity in both audit modes. |
| Publish and repository | Validation before writes, actual publication with optional provenance, stateful sibling versions, published name/status/version selection and ambiguity, explicit tables, immutable identities, retirement and staging-view cleanup after failed registry merges. |
| Repository schema | Complete DDL/StructType field, type and nullability parity; default, valid and rejected bootstrap modes; quoted table names and identifier validation. |
| Row runtime | Shared rule execution, stop semantics, atomic assignments, assigned dependencies, prepared snapshots, isolated collections/binary values/error payloads, inactive-rule closure exclusion, exact decimal comparisons, and audit provenance. |
| Canonical persistence | Explicit current format, direct model decoding, lossless literal and argument node encoding, deterministic hashes, unsupported-format rejection, and verified payload/row identity. |
| Service | Text/path publication, exact version forwarding, explicit-model precedence, evaluation/coverage options, descriptions, registry maintenance and retirement. |
| Spark schema and worker boundaries | One prepared schema analysis, public/prepared source projection agreement, timezone-aware timestamp normalization, recursive output type/nullability checks, float32 overflow and rounding, custom return error capture, and audit parity for existing NaN/infinite values. |
| Live Spark | Actual worker input keys and empty-dependency sentinel, typed assignments and exact timestamp schemas, validation before UDF construction, both error/audit modes, installed engine identity, driver manifest/per-row hash agreement, lazy errors, shared persistence, exact first/all-match coverage statistics, error exclusion and empty input. |
| Standard functions | Text/regex, strict conversions and UTC representation, isolated decimal arithmetic, reversed/equal min/max and clamp boundaries, signed completed calendar periods, collection true/false/null cases, deterministic registry metadata and worker integration. |
| Semantic validation | Ownership/identity checks, direct-model literals, same-instance version independence, same-rule/future/inactive producer rejection, exact custom arguments, genuine cross-type coercion, schema-bearing acceptance and effective collection fallbacks. |
| Notebook bootstrap | Standalone checkout discovery from nested directories using tracked layout markers. |

## Execution

Install the pinned development tools and run the non-Spark suite:

```bash
python -m pip install -e '.[dev]'
python -m pytest tests -q
python -m ruff check .
python scripts/test_inventory.py --check
```

Run all tests with Java and real Spark workers:

```bash
export RULES_ENGINE_RUN_SPARK_TESTS=1
export PYSPARK_PYTHON="$(command -v python)"
python -m pytest tests -q
```

PowerShell equivalent:

```powershell
$env:RULES_ENGINE_RUN_SPARK_TESTS = "1"
$env:PYSPARK_PYTHON = (Get-Command python).Source
python -m pytest tests -q
```

Set `JAVA_HOME` to a compatible Java installation. CI uses Java 17 with
Python 3.10 / PySpark 3.5.6 and Python 3.12 / PySpark 4.2.0. Live Spark cases
are opt-in locally; worker-only tests also exercise actual PySpark type
conversion without requiring a JVM. Cache-specific live tests skip when
Databricks serverless explicitly reports that persistence APIs are unavailable.
CI runs both combinations under UTC, America/New_York, Australia/Lord_Howe,
Asia/Kathmandu, and Pacific/Chatham OS timezones. Ruff checks undefined names
in the system and example notebooks, allowing only their Databricks-provided
globals; this static check does not execute notebook cells.

After changing collected tests, regenerate the inventory:

```bash
python scripts/test_inventory.py
```

The CI workflow also builds a wheel and verifies its source bytes and package
version against the checkout. Deployment acceptance still requires running
the complete system notebook on the target Databricks runtime.
