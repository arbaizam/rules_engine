# Rules Engine Unit Test Summary

Source: the behavioral pytest suite under `tests/`.

The [generated inventory](rules_engine_test_inventory.md) lists explicit test
functions, collected parameter cases, and live Spark cases by module. These
counts are computed from the checkout and verified in CI. They describe
coverage, not a record of a successful run.

The [Databricks system-test notebook](rules_engine_system_test_summary.md)
is a separate acceptance suite. Local notebook bootstrap tests execute only
root discovery; they do not claim to run Databricks cells or Delta operations.

## Coverage Matrix

| Area | Current Contract Covered |
| --- | --- |
| Authoring manifest | Deterministic JSON-compatible operator, enum, literal-hint, ordered-sequence, and registered function contracts. |
| YAML compilation and canonical values | Strict shapes and identities, supported finite scalar/collection values, exact numbers, temporal normalization, argument operands, mapping-key collisions, and rejection of unsupported binary values. |
| YAML export | Round trips, shipped-artifact hashes, nested argument operands, and canonical authoring vocabulary. |
| Governance and execution parity | Public exports, shared version metadata, strict option handling, and shared Python/Spark rule ordering. |
| Publish and repository | Validation before writes, publication provenance, explicit tables, immutable identities, parameterized retirement, duplicates, and registry metadata. |
| Repository schema | Metadata DDL, quoted table names, identifier validation, and nullability. |
| Row runtime | Shared rule execution, stop semantics, atomic assignments, assigned dependencies, prepared snapshots, isolated mutable literal arguments, exact decimal comparisons, errors, and audit provenance. |
| Canonical persistence | Explicit current format, direct model decoding, lossless literal and argument node encoding, deterministic hashes, unsupported-format rejection, and verified payload/row identity. |
| Service | Public facade orchestration across compile, publish, load, describe, evaluate, and retire. |
| Spark schema and worker boundaries | One prepared schema analysis, source selection, timezone-aware timestamp normalization, recursive output type/nullability checks, custom return error capture, function dependency manifests, and audit parity for existing NaN/infinite values. |
| Live Spark | Actual worker serialization, typed decimal and collection assignments, timestamp comparisons, compact/full-audit output, lazy errors, keyed projections, shared persistence, and coverage aggregation. |
| Standard functions | Text/regex, strict conversions, isolated decimal arithmetic, null composition, calendar operations, ordered collections, deterministic registry metadata, and worker integration. |
| Semantic validation | Ownership/identity checks, direct-model literal validity, rule ordering, active producers, custom contracts, and effective collection fallbacks. |
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

After changing collected tests, regenerate the inventory:

```bash
python scripts/test_inventory.py
```

The CI workflow also builds a wheel and verifies its source bytes and package
version against the checkout. Deployment acceptance still requires running
the complete system notebook on the target Databricks runtime.
