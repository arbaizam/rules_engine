# Rules Engine Production Checklist

## Local gates

- [ ] Run the complete pytest suite with real Spark workers enabled.
- [ ] Run `ruff check .`.
- [ ] Build the wheel and inspect its package version.
- [ ] Run `git diff --check`.

## Target Databricks Runtime gates

- [ ] Install the wheel and restart the target cluster.
- [ ] Run all 18 tests in `notebooks/99.rules_engine_system_tests.py` and retain
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

Stop promotion for any business-result drift, null final-winner IDs, worker
serialization errors, or failing system tests.
