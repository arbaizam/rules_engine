# Changelog

This project records public runtime and audit-contract changes here. Package
version and audit-schema version are intentionally separate contracts.

## 2.2 — release candidate (2026-08-28)

### Full-audit contract

- `overridden_by_rule_id` and `overridden_by_assignment_id` identify the
  immediate next assignment to the same target.
- Full-audit assignment elements add non-nullable `final_winning_rule_id` and
  `final_winning_assignment_id` fields for the eventual winner.
- Full-audit output adds `rules_engine_audit_schema_version`; its value for this
  contract is `"2"`.

### Runtime hardening

- Driver and worker package/audit versions are checked inside the worker before
  row evaluation.
- `rules_engine_engine_version` is now sourced from the worker result after the
  driver/worker check.
- Programmatically authored rule and assignment IDs must be non-empty strings,
  preventing non-nullable Spark provenance fields from receiving `None`.
- Assignment-result ordering has a direct regression test.

### Production validation

Read the
[2.2 production checklist](docs/rules_engine_2_2_production_checklist.md)
before deploying. The target Databricks Runtime system-test notebook remains a
release gate.
