# Rules Engine Unit Test Summary

Source: current pytest suite under `tests/`.

The current unit suite covers the supported Spark-first contract:

| Area | Representative Coverage |
| --- | --- |
| YAML compilation | Canonical row-level rules compile; unsupported aliases are rejected; aggregate operands fail compilation with guidance to use precomputed fields. |
| YAML export | Compiled rulesets round-trip through canonical YAML without reintroducing aliases. |
| Validation | Ownership metadata, duplicate IDs, operator arity, collection literals, null defaults, custom-function contracts, and tolerance rules are enforced. |
| Repository schema | Ruleset-version metadata stores payload, hash, lifecycle fields, rule/condition/assignment/custom-function counts, and governance ownership. |
| Serialization | Payload JSON is deterministic, lifecycle fields stay outside payload content, and rows deserialize back to canonical models. |
| Spark runtime | Required source-column discovery covers active conditions, nested custom-function arguments, assignments, deduplication, inactive metadata, package export, and literal-only rules; the UDF serializes only required available fields while preserving dotted names and unrelated output columns; active rules are ordered once per evaluator; match-only and traced paths agree that inactive conditions are false in ALL/ANY groups; losing rules avoid trace allocation without hiding later-condition errors; winning custom functions execute once; assignments resolve without operand traces; primitive trace values bypass JSON serialization; result columns use one Spark projection; assignment and winning-rule outputs remain native structs; and continued matching and author-facing explanations retain their documented semantics. |
| Spark compatibility validation | The Spark validator preserves the base row-level ruleset contract. |
| Service facade | Publish/load/evaluate/describe workflows delegate through the Spark repository and runtime components; runtime winning-rule explanations share the same author-facing expression syntax as `describe_rules`. |
| Recon translation | Source reconciliation specs translate into canonical rules engine YAML and audit artifacts. |
| Standard functions | Built-in function specs and implementations are registered, persisted, and usable in rules. |

Aggregate execution is intentionally not part of the package. Tests that need
aggregate-like business facts use columns such as `account_amount_sum`, which
must be produced upstream by Spark before invoking the rules engine.
