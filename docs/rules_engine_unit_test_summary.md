# Rules Engine Unit Test Summary

Source: current pytest suite under `tests/`.

The current unit suite covers the supported Spark-first contract:

| Area | Representative Coverage |
| --- | --- |
| YAML compilation | Canonical row-level rules and named expected cases compile; omitted null modes materialize documented defaults; unsupported aliases are rejected; aggregate operands fail with guidance to use precomputed fields; fractional YAML is preserved as exact Decimal; non-finite numbers are rejected. |
| YAML export | Compiled rulesets and expected cases round-trip through canonical, byte-stable YAML without reintroducing aliases or losing Decimal, temporal, tuple, set, or set-of-tuple types. |
| Validation | Ownership metadata, duplicate IDs, operator arity, collection-valued membership, finite Decimal/float values, boolean null defaults, custom-function contracts, expected-assignment keys, and tolerance rules are enforced. |
| Repository schema | Ruleset-version metadata stores payload, hash, lifecycle fields, rule/condition/assignment/custom-function counts, and governance ownership; pinned loads reject duplicate rows for one immutable name/version key. |
| Serialization | Payload JSON is deterministic; exact Decimal, date, datetime, tuple, set, reserved-key mapping, and nested custom-function values survive persistence; malformed type envelopes fail uniformly; lifecycle fields stay outside payload content; and rows deserialize back to canonical models. |
| Spark runtime | Required-column projection, numeric membership, first-match trace construction, self-contained authored assignment expressions, position-based assignment provenance, compact/debug error modes, lazy single-pass fail-fast behavior, audit-level schemas, immutable execution identity, and custom-function cloudpickle preflight are covered without a live Spark session. Gated Spark tests cover one `BatchEvalPython` plan node, Decimal, Date, Timestamp, TimestampNTZ, array/struct, nested nullability, quarantine, fail-fast, audit levels, coverage, and output schema behavior through the real UDF boundary. |
| Spark compatibility validation | Existing/new target inference, Decimal precision/scale, polymorphic function hints, strict timestamp representations, mixed temporal bounds, actionable TimestampNTZ diagnostics, and unresolved types are checked before worker execution. |
| Governance and service facade | Publish/load/evaluate/describe workflows delegate through the Spark repository and runtime components; embedded expected cases block invalid publication; semantic diffs isolate changed condition/assignment leaves while retaining identity and authored expressions; coverage reports dead/broad/closest rules; runtime explanations share the `describe_rules` syntax. |
| Recon translation | Source reconciliation specs translate into canonical rules engine YAML and audit artifacts. |
| Standard functions | Built-in text, numeric, null, and date function specs and implementations are registered, persisted, and usable in conditions and typed assignments, including month-end/leap-year behavior and descriptive date-range overflow errors. |

Aggregate execution is intentionally not part of the package. Tests that need
aggregate-like business facts use columns such as `account_amount_sum`, which
must be produced upstream by Spark before invoking the rules engine.
