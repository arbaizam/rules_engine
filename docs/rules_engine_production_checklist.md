# Rules Engine Production Checklist

## Local gates

- [ ] Install development dependencies with `python -m pip install -e ".[dev]"`.
- [ ] Run the complete pytest suite with real Spark workers enabled on the
  supported CI combinations: Python 3.10 / Spark 3.5.6 and Python 3.12 /
  Spark 4.2.0, using Java 17. CI covers UTC, America/New_York,
  Australia/Lord_Howe, Asia/Kathmandu, and Pacific/Chatham OS timezones.
- [ ] Run `ruff check .`.
- [ ] Run `python scripts/test_inventory.py --check` and update the generated
  inventory when test collection changes.
- [ ] Build the wheel and run `python scripts/check_wheel.py dist` to verify
  its version and source contents.
- [ ] Run `git diff --check` for pending changes and
  `git show --check --format=oneline --diff-merges=first-parent HEAD` for the commit.

## Target Databricks Runtime gates

- [ ] Install the wheel and restart the target cluster.
- [ ] Run all 22 tests in `notebooks/99.rules_engine_system_tests.py` and retain
  the output.
- [ ] Run a bounded canary with `full_audit=true` against representative data,
  including a target assigned by at least three matching rules.

## Canary assertions

- Business assignment values, matches, and row errors meet expected results.
- Assignment events remain in evaluation order.
- Exactly one event is `effective=true` for every assigned row and target.
- Every non-final event points to the immediate next event for its target.
- Every event for a target shares the same non-null `final_winning_*` values.
- The effective event's proposed value equals the emitted assignment value.
- Full-width decimal values and zero-tolerance comparisons remain exact.
- Spark timestamp fields compare with offset-bearing literals by instant.
- Compact and full-audit business results agree, including overwrites of
  existing non-finite values and captured custom-function errors.
- The ruleset payload and referenced-function manifest each have a verifiable
  content hash; persisted identity and payload identity agree.
- Export/recompile preserves the ruleset content hash and concrete numeric kinds,
  including binary floats in literals and function arguments. Retain canonical
  `!rules_engine/float` tags when reviewing exported YAML.

Stop promotion for any business-result drift, null final-winner IDs, worker
serialization errors, or failing system tests.
