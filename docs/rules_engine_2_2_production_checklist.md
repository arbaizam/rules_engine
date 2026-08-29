# Rules Engine 2.2 Production Checklist

This checklist validates the 2.2 candidate on its own contract.

## Local gates

- [x] Run the complete pytest suite with real Spark workers enabled.
- [x] Run `ruff check .`.
- [x] Build the `rules_engine-2.2` wheel and inspect its versioned filename.
- [x] Run `git diff --check`.

## Target Databricks Runtime gates

- [ ] Install the candidate wheel and restart the target cluster.
- [ ] Run all 18 tests in `notebooks/99.rules_engine_system_tests.py` and retain
  the output.
- [ ] Verify the driver and every executor report package version `2.2`.
- [ ] Run a bounded canary with `full_audit=true` against representative data,
  including a target assigned by at least three matching rules.

Executor verification:

```python
import rules_engine

driver_version = rules_engine.__version__
worker_versions = (
    spark.range(max(spark.sparkContext.defaultParallelism, 1))
    .rdd.map(lambda _: __import__("rules_engine").__version__)
    .distinct()
    .collect()
)
assert driver_version == "2.2"
assert worker_versions == [driver_version]
```

## Canary assertions

- Business assignment values, matches, and row errors meet expected results.
- Assignment events remain in evaluation order.
- Exactly one event is `effective=true` for every assigned row and target.
- Every non-final event points to the immediate next event for its target.
- Every event for a target shares the same non-null `final_winning_*` values.
- The effective event's proposed value equals the emitted assignment value.
- The engine version is `2.2` and the audit schema version is `2` on every row.

Stop promotion for any business-result drift, multiple engine versions, a
missing audit-schema marker, null final-winner IDs, worker serialization errors,
or failing system tests.
