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
| Spark runtime | The row UDF returns native Spark structs for assignments and the winning-rule trace; precomputed aggregate facts are handled as ordinary field operands. |
| Spark compatibility validation | The Spark validator preserves the base row-level ruleset contract. |
| Service facade | Publish/load/evaluate/describe workflows delegate through the Spark repository and runtime components. |
| Recon translation | Source reconciliation specs translate into canonical rules engine YAML and audit artifacts. |
| Standard functions | Built-in function specs and implementations are registered, persisted, and usable in rules. |

Aggregate execution is intentionally not part of the package. Tests that need
aggregate-like business facts use columns such as `account_amount_sum`, which
must be produced upstream by Spark before invoking the rules engine.
