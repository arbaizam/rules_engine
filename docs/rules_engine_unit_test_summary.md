# Rules Engine Unit Test Summary

Source: current pytest suite under `tests/`.

The current unit suite covers the supported Spark-first contract:

| Area | Representative Coverage |
| --- | --- |
| YAML compilation | Canonical row-level rules compile; unsupported aliases are rejected; aggregate operands fail compilation with guidance to use precomputed fields. |
| YAML export | Compiled rulesets round-trip through canonical YAML without reintroducing aliases. |
| Validation | Ownership metadata, duplicate IDs, operator arity, collection literals, literal value/type consistency, null defaults, custom-function contracts, and tolerance rules are enforced. |
| Repository schema | Ruleset-version metadata stores payload, hash, lifecycle fields, rule/condition/assignment/custom-function counts, and governance ownership. |
| Serialization | Payload JSON is deterministic, lifecycle fields stay outside payload content, and rows deserialize back to canonical models. |
| Spark runtime | Field/literal rules use native Spark execution without Python evaluation nodes; strict native mode rejects fallback during planning; capability-gate tests cover unsupported null, type, tolerance, collection, mapping, custom-function, literal-hint, and LIKE semantics; native/UDF parity tests cover every operator, null modes, schemas, scalar/range/membership NaN behavior, Decimal and field-to-field comparisons, nested and empty groups, ordered multi-match behavior, native and UDF dotted-column paths, ANSI mode, and stop-rule plan growth; Python custom functions retain an observable compatibility path; assignments and the winning-rule trace remain native Spark structs; default-valued trace fields remain null; mapping literal assignments remain nested structs; inactive rules do not affect active schemas; and winning-rule explanations preserve authored AND/OR logic while omitting failed branches. |
| Spark compatibility validation | The Spark validator preserves the base row-level ruleset contract. |
| Service facade | Publish/load/evaluate/describe workflows delegate through the Spark repository and runtime components; runtime winning-rule explanations share the same author-facing expression syntax as `describe_rules`. |
| Recon translation | Source reconciliation specs translate into canonical rules engine YAML and audit artifacts. |
| Standard functions | Built-in function specs and implementations are registered, persisted, and usable in rules. |

Aggregate execution is intentionally not part of the package. Tests that need
aggregate-like business facts use columns such as `account_amount_sum`, which
must be produced upstream by Spark before invoking the rules engine.
