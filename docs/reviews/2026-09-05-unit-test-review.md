# Unit test review — 2026-09-05

**Historical baseline.** This report and its evidence describe the review before
changes were authorized. The exporter fix and test recommendations have since
been implemented; see the [remediation report](2026-09-05-unit-test-remediation.md)
for resolutions and fresh verification. Source line links below refer to the
reviewed snapshot and may have moved. The findings are retained as review evidence.

The suite is worth retaining, but **39 of 342 test functions do not fully establish
their stated purpose**. The main weaknesses are fixtures that miss the claimed
case, helpers that select a different execution path, and assertions that can pass
when the intended behavior is broken. This review also exposed one reproducible
current exporter defect. **No tests or production code were changed or removed.**

Reviewed the current working tree based on commit
`ad26d54a8b57fd359b3ff3c0b9addf87f9b43f3f`, including the uncommitted hardening changes
already present when this review began. The 26 source modules and 18 test modules
were byte-hashed before and after review; all 44 `.py` files are unchanged.
Review fingerprint: `7255fbfdb30d3d2ae21ae760fc1f0e86ba8eb71dcdb208582850f8ef8907430b`.

The [complete per-test register](2026-09-05-unit-test-register.md) contains one assessment for
every explicit function, its source location, collected-case count, purpose,
recommendation and rationale. The [evidence JSON](2026-09-05-unit-test-evidence.json) preserves
all 531 collected case identifiers, per-file hashes and isolated probe results.

## Decisions

| Recommendation | Explicit functions | Meaning |
| --- | ---: | --- |
| Keep as written | 298 | Useful current-contract coverage; no change required by this review. |
| Strengthen | 35 | Preserve scenario and improve its fixture, assertions or actual execution path. |
| Replace ineffective fixture | 2 | Preserve the test intent; the present input does not trigger its claimed case. |
| Rename/reclassify | 2 | Keep the behavior check and correct the name or claimed test layer. |
| Optional consolidation | 5 | Combine five functions into two parameterized families while keeping every case/assertion. |
| Retire/delete | 0 | No reviewed function was shown to be wholly unnecessary. |

Purpose assessment: **303 met, 35 partial,
4 not met**. The five optional-consolidation tests meet their
purpose; this explains why purpose and recommendation counts differ. “Not met” is
about the declared purpose, not necessarily a test with no useful assertions.
“Keep” does not assert exhaustive coverage of every branch a test happens to touch.

## Highest-priority findings

1. **Current exporter defect — [CY-08](#cy-08).** Supported raw binary floats survive
   persistence but become Decimal on YAML export/recompile. Model equality can still
   return True while canonical hashes and type-sensitive custom-function results
   change. Fix the behavior and add direct-model float cases before relying on the
   broad lossless-export promise.
2. **Wrong execution paths — [RT-01](#rt-01), [VA-01](#va-01).** The match-only error
   regression actually runs full audit; the Spark validation acceptance test supplies
   no schema and never reaches Spark preparation.
3. **Ineffective test inputs — [CY-01](#cy-01), [VA-02](#va-02), [VA-04](#va-04).**
   Mixed-key sorting sees only one unsupported key; cross-type coercion compares two
   StringTypes; future/inactive assignment-producer claims only test a same-rule case.
4. **Hidden incorrect results — [RT-02](#rt-02), [VA-07](#va-07), [RO-07](#ro-07).**
   Float32 downstream visibility is masked by a second conversion; min/max and
   contains-all lack decisive counterexamples; analytics never checks first-match
   statistics or error exclusion from clean no-match rows.
5. **Unobserved integration contracts — [RO-03](#ro-03), [RO-05](#ro-05),
   [RO-06](#ro-06), [RO-10](#ro-10).** Tests can miss unsorted execution, lost pinned
   versions, wider UDF input payloads and wrong repository selection predicates.

These are test-specific findings. An intentionally broken implementation surviving
a named test does **not** imply it survives the entire suite. The exporter defect
was reproduced against unchanged code, independently of mutation probes.

## Method and execution evidence

- Read every test function, parameter matrix, shared helper and relevant current
  production path; compared names/docstrings with what the setup reaches and what
  assertions can distinguish. Documentation inventories were cross-checked against
  AST definitions and the freshly collected/executed cases.
- Used narrow process-local monkeypatch probes to challenge suspect assertions.
  No mutation was written to a source or test file. Included an explicit control:
  no-op publication survives the omitted-actor test but is caught by its explicit-actor
  sibling. These probes are examples, not an exhaustive mutation score.
- Fresh baseline: **531 passed, 0 failed, 0 skipped**, including **34 live Spark
  cases**, on Python 3.12.14 / PySpark 4.2.0 / Java 17, in **122.08 seconds**.
  The run used `RULES_ENGINE_RUN_SPARK_TESTS=1`, a pinned worker interpreter and
  `pytest tests -q -p no:cacheprovider --durations=20`.
- One upstream PySpark warning concerned the installed pandas 3 compatibility;
  no tests failed. Both supported Python/Spark combinations had passed earlier in
  this thread; only the combination above was rerun for this review.
- Of 531 cases, 497 use no live Spark fixture and 34 do. Many non-live cases are
  multi-module worker/schema tests rather than narrowly isolated units; that is a
  useful layer when labeled accurately.
- Did not execute the Databricks/Delta system notebook or serverless workloads.
  Its 22 acceptance scenarios are separate from these 342 test functions. Notebook
  bootstrap tests execute four root-discovery assignments only. No branch-coverage
  percentage, mutation score or proof of all race/interoperability behavior is claimed.

## Coverage and necessity by module

| Test module | Functions | Cases | Keep | Strengthen/replace/reclassify | Optional consolidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| [test_authoring.py](../../tests/test_authoring.py) | 5 | 5 | 4 | 1 | 0 |
| [test_canonical_contract.py](../../tests/test_canonical_contract.py) | 16 | 42 | 16 | 0 | 0 |
| [test_compiler_yaml.py](../../tests/test_compiler_yaml.py) | 34 | 66 | 28 | 6 | 0 |
| [test_exporter_yaml.py](../../tests/test_exporter_yaml.py) | 7 | 19 | 6 | 1 | 0 |
| [test_governance.py](../../tests/test_governance.py) | 3 | 3 | 2 | 1 | 0 |
| [test_notebook_bootstrap.py](../../tests/test_notebook_bootstrap.py) | 1 | 4 | 1 | 0 | 0 |
| [test_publish.py](../../tests/test_publish.py) | 3 | 3 | 2 | 1 | 0 |
| [test_repository.py](../../tests/test_repository.py) | 15 | 15 | 13 | 2 | 0 |
| [test_repository_schema.py](../../tests/test_repository_schema.py) | 6 | 8 | 6 | 0 | 0 |
| [test_required_source_columns.py](../../tests/test_required_source_columns.py) | 1 | 2 | 1 | 0 | 0 |
| [test_runtime.py](../../tests/test_runtime.py) | 78 | 112 | 70 | 3 | 5 |
| [test_serializer.py](../../tests/test_serializer.py) | 12 | 17 | 10 | 2 | 0 |
| [test_service.py](../../tests/test_service.py) | 14 | 14 | 12 | 2 | 0 |
| [test_spark_boundaries.py](../../tests/test_spark_boundaries.py) | 13 | 50 | 12 | 1 | 0 |
| [test_spark_runtime.py](../../tests/test_spark_runtime.py) | 30 | 34 | 25 | 5 | 0 |
| [test_spark_validator.py](../../tests/test_spark_validator.py) | 51 | 52 | 45 | 6 | 0 |
| [test_standard_functions.py](../../tests/test_standard_functions.py) | 29 | 39 | 25 | 4 | 0 |
| [test_validator.py](../../tests/test_validator.py) | 24 | 46 | 20 | 4 | 0 |

## What should remain even when coverage overlaps

Compiler syntax checks, direct-model validation, canonical codec checks, repository
error translation, worker conversion checks and live Spark round trips protect
different boundaries. Removing one merely because a larger integration test also
touches that code would weaken failure localization or adversarial input coverage.
Keep negative legacy/alias tests while the current contract requires rejection.

The five consolidation candidates are specifically the two reserved-output-name
functions and three ambiguous-source-name functions in [RT-05](#rt-05). This is
maintenance of duplicated scaffolding, not permission to discard any scenario.
Neither a short facade smoke test nor a long live test is unnecessary on size alone.

Negative validator tests that require a particular issue name are sound rejection
oracles here: every issue makes `ValidationResult.has_errors()` true. Adding a second
failure assertion everywhere would be redundant. In contrast, absence of one issue
does not establish success; the five acceptance cases in [VA-06](#va-06) need stronger
positive assertions. UTC representation checks in [CY-04](#cy-04) and [VA-03](#va-03)
are the same assertion weakness at two distinct boundaries, so both should remain.

## Suggested sequence after reviewing this report

1. Resolve CY-08 with explicit type/hash/behavior regressions, preserving all current
   canonical model guarantees or failing unsupported exports explicitly.
2. Correct wrong-path and ineffective-fixture tests, then add decisive independent
   outcomes for assignment ordering, float narrowing, selection functions and analytics.
3. Strengthen public version forwarding, actual worker input projection and published
   repository selection. Keep Databricks execution as the separate deployment gate.
4. Correct overbroad names and optional duplicated scaffolding. Re-run both supported
   Python/Spark combinations and refresh the inventory after any future test edits.

No test change, consolidation, replacement or removal has been applied by this review.

## Detailed findings

### CY-08

**Medium: the round-trip matrix omits binary floats, hiding a current model/hash change**

- **Tests:** [tests/test_exporter_yaml.py:246](../../tests/test_exporter_yaml.py#L246) (`test_operand_shaped_argument_mappings_survive_yaml_round_trip`), related broad compiled-model round trip at [tests/test_exporter_yaml.py:39](../../tests/test_exporter_yaml.py#L39).
- **Production path:** [src/rules_engine/exporter_yaml.py:213](../../src/rules_engine/exporter_yaml.py#L213) `_export_value` preserves untyped float while exporting; the Decimal-aware YAML loader and `_compile_custom_function_arg` turn that YAML numeric token into Decimal when recompiled.
- **Evidence without mutation:** invoking the existing mapping test with one additional valid parameter, `{"field": 1.5}`, fails its content-hash assertion. A direct canonical `LiteralOperand(1.5)` and raw function argument `{"value": 1.5}` also persist exactly through Delta serialization, then export/recompile as `Decimal("1.5")`. Python dataclass equality reports **True** because `1.5 == Decimal("1.5")`, while canonical hashes differ.
- **Why current tests miss it:** the original broad fixtures originate in the compiler, which has already normalized untyped fractions, or explicitly carry a floating hint. The direct-model mapping matrix has no binary-float payload. Its hash assertion is good; the missing input is the gap. `test_persistence_preserves_binary_float_and_decimal_kinds_without_authoring_coercion` already proves the persistence side supports these kinds, but never crosses YAML.
- **Proposed change, not implemented:** add raw direct-model float cases to the exporter matrix, including operand-shaped mappings, collections and an untyped float literal; assert concrete scalar types and canonical hashes. First decide and implement the authoring representation that preserves the existing kind, or a typed unsupported-export error rather than silently changing identity. Merely adding a `value_type` hint can itself change the canonical model and is not a complete identity-preserving solution.

**Independent valid-input confirmation:** three models with ownership, nonempty
conditions and registered functions where needed passed `RulesetValidator` before
persistence, after `DeltaRowSerializer` restoration, and after YAML recompilation.
Serializer restoration preserved concrete float types and hashes. YAML changed
them to Decimal and changed hashes. A registered function that returns its argument's
type name returned `float` before export and `Decimal` afterward. Thus the omitted
case can change execution results as well as identity. This exercised serializer
round trips and the Python evaluator, not a live Delta table. No correction was
implemented during this report-only task.

### RT-01

**Medium: the named match-only error regression never calls match-only evaluation**

Evidence: [tests/test_runtime.py:2034](../../tests/test_runtime.py#L2034) describes preserving errors after a false
condition in the match-only path. Its call at line 2057 uses `_evaluate_worker`
without an audit argument, while that helper defaults `full_audit=True` at line
103. `src/rules_engine/runtime.py:156-161` dispatches this to `_evaluate_rule`,
not `_rule_matches`/`_group_matches`.

False-positive mechanism: compact ALL could resume short-circuiting after its
first false condition and silently hide the later invalid numeric operand, yet
this named regression would stay green. A scratch-only mutation probe replaced
`SparkRowEvaluator._rule_matches` with a deliberate failure: the unchanged test
passed and the patched function was called zero times.

Recommendation: keep the scenario and explicitly run `full_audit=False` (or
parameterize both modes and require the expected error independently). Do not
remove it: the two group evaluators are still separate production paths, and
the existing inactive-group parity test at line 2064 does not cover errors.

### RT-02

**Medium: float32 test proves final output rounding, not downstream visibility**

Evidence: `tests/test_spark_boundaries.py:236-272` sets `target` from a function,
then copies `{assigned: target}` to `copied`. Both output fields resolve as
FloatType; `src/rules_engine/runtime.py:170-174` normalizes every assignment and
`src/rules_engine/spark_runtime.py:611-614` applies the target field type.

False-positive mechanism: if the downstream read returned the original double,
the copy assignment's own FloatType conversion would still round it to the
expected value. Exact target `.hex()` checks signed-zero output but do not
observe what the second rule received. The scratch probe forced every
AssignedOperand read of target to return the original unrounded double. All six
cases (three values x two audit modes) still passed, with one wrong read
observed in each case.

Recommendation: retain the good overflow/output-rounding cases and strengthen
the downstream case with a condition or a custom function that returns the
received float's `.hex()` as a string, or give `copied` an existing DoubleType
column. Assert this observation before any second FloatType conversion. This
tests a legitimate production guarantee without requiring a live Spark session.

### RT-03

**Low: materializing-action test is a direct Python worker unit test**

Evidence: `tests/test_runtime.py:2465-2476` is named
`test_spark_row_evaluator_can_raise_during_materializing_action`, but calls
`_evaluate_worker` synchronously with a FakeSparkRow. The helper builds and
invokes a Python closure at lines 117-123; there is no Spark action or lazy plan.

Recommendation: retain and rename/reclassify as worker fail-fast exception
wrapping. Real `collect()` failure is already checked at
`tests/test_spark_runtime.py:1241-1247`, and lazy plan construction is separately
tested at line 1250. These are useful complementary layers, so no deletion is
warranted merely because both encounter the same exception text.

### RT-04

**Low: condition-once test counts trace construction instead of evaluation**

Evidence: [tests/test_runtime.py:1845](../../tests/test_runtime.py#L1845) claims every condition is evaluated once
and only matched traces are emitted. The spy at lines 1901-1908 wraps
`SparkRowEvaluator._condition_trace`; assertions at lines 1912-1917 count those
trace creations and inspect only `matched_rules[0]`. Actual operand/function
resolution happens earlier in `src/rules_engine/runtime.py:405-420`.

False-positive mechanism: multiple operand/function evaluations could feed a
single trace. A scratch probe resolved both operands once extra for each
condition before the normal `_evaluate_condition` call; the test still passed
with extra resolutions for loser_first, loser_second and first_match_condition.
Checking the first output trace also does not assert the claimed exact list of
matched traces.

Recommendation: retain the valuable ordered trace-count assertion, but instrument
actual callable invocations or `_evaluate_condition`/operand resolution as
appropriate, and assert the complete matched trace ID list. The separate
`test_losing_custom_condition_is_invoked_once_during_full_audit` at line 1919
correctly counts a real callable, so this is a localized weakness rather than a
claim that all function-once coverage is absent. The custom trace test at line
2200 also counts its actual matching callable.

### RT-05

**Informational: two small preflight families can share parameter tables**

The variants themselves are necessary; there are no proposed lost assertions.

- [tests/test_runtime.py:460](../../tests/test_runtime.py#L460) already parameterizes six core output suffixes;
  line 482 duplicates fixture/call/assertion for the two audit-only suffixes.
  All eight exercise the same output conflict branch at
  `src/rules_engine/spark_runtime.py:352-373`. Combine the variants in one table.
- [tests/test_runtime.py:576](../../tests/test_runtime.py#L576), 599 and 622 duplicate the source-ambiguity fixture
  for exact duplicate selected key, case-only selected key and case-only
  unselected source names. All reach the all-source casefold duplicate guard at
  `src/rules_engine/spark_runtime.py:486-497`. Keep all three cases in a table;
  in particular do not lose the unselected-column case, which proves the check
  is broader than key metadata.

This is optional maintenance, not evidence of broken coverage. Most apparent
overlap elsewhere is justified layering: schema preflight versus adversarial
worker returns, public Python results versus Spark-shaped payloads, and
trace text versus human-readable authored syntax.

### VA-01

**Medium: the Spark error_on_null test bypasses Spark schema preflight**

- Test: [tests/test_spark_validator.py:21](../../tests/test_spark_validator.py#L21), particularly `.validate(ruleset)` at line 55.
- Production: [src/rules_engine/spark_validator.py:109](../../src/rules_engine/spark_validator.py#L109)–119 selects `prepare` only when a
  schema is supplied; the no-schema case goes directly to base validation.
- Actual purpose covered: ordinary base metadata permits a binary condition with
  `error_on_null=True`.
- Missing purpose: schema compatibility accepts the supported Spark row-runtime
  configuration. A regression introduced only in `prepare` or schema traversal would
  be invisible.
- Probe: replacing `SparkRulesetCompatibilityValidator.prepare` with an exception
  left the test passing, proving that path was never reached.
- Recommendation: retain and strengthen with a `StructType` containing status and
  assert `prepare(...).validation.passed` or `validate(ruleset, schema).passed`. If a
  schema-free delegation contract is intentional, name it accordingly and keep a
  separate schema-bearing acceptance case. No live JVM is needed for this unit test.

### VA-02

**Medium: the coercion test never uses different inferred operand types**

- Test: [tests/test_spark_validator.py:487](../../tests/test_spark_validator.py#L487).
- The amount field has `StringType`; right literal is the string `"10"`. This fixture
  is compatible even under a hypothetical strict same-type-only schema policy.
- Production: [src/rules_engine/spark_validator.py:805](../../src/rules_engine/spark_validator.py#L805)–883 intentionally rejects
  temporal mismatches while leaving non-temporal row-runtime coercion alone.
- Probe: added a process-local rejection for every unequal pair of known inferred
  types. The existing test passed; changing the test input to integer literal `10`
  caused that mutant to reject the ruleset.
- Recommendation: replace the fixture with an actual StringType/numeric comparison,
  preferably parameterizing the reverse numeric-field/string-literal direction. Keep
  `result.passed`. This is a fixture correction, not a removal recommendation.

### VA-03

**Medium: datetime equality does not establish UTC normalization**

- Test: [tests/test_standard_functions.py:192](../../tests/test_standard_functions.py#L192), assertion at line 193.
- Production: [src/rules_engine/standard_functions.py:321](../../src/rules_engine/standard_functions.py#L321) returns an aware instant
  normalized through `parsed.astimezone(timezone.utc)` at line 330.
- The expected UTC datetime compares equal to the original `01:00+01:00`; Python
  aware-datetime equality compares instants. It cannot prove that the output offset
  or timezone representation was normalized.
- Probe: returning the original parsed offset after ordinary validation still passes
  the entire instant/wall-clock converter test, while the result remains +01:00.
- Recommendation: assert the normalized result has zero UTC offset (or UTC tzinfo if
  that exact object is contractual) as well as expected instant, and explicitly assert
  NTZ output is naive. Keep malformed shape and policy cases.
- Related: the compiler audit identifies the same oracle pattern (CY-04); this is the
  distinct public converter boundary, not duplicate coverage to delete.

### VA-04

**Medium: the assigned producer test claims three invalid cases but creates one**

- Test: [tests/test_validator.py:517](../../tests/test_validator.py#L517); its docstring says “Same-rule, future, and
  inactive assignments cannot satisfy a reference.”
- Actual fixture: one active rule, referring to the bucket it assigns itself. There
  is neither a future rule nor an inactive earlier producer.
- Production: [src/rules_engine/validator.py:208](../../src/rules_engine/validator.py#L208) independently filters active producers
  and requires `producer[0] < rule.rule_order` at line 253.
- Probes: changing the strict order test to unequal order (permitting future
  producers), or removing the active producer filter, both leave the negative test
  passing. The positive earlier-producer test at line 533 also passes the order mutant.
- Recommendation: retain the same-rule case and add separately identified future and
  inactive-earlier producer cases. They should contain an otherwise valid active
  consumer and assert `ASSIGNED_VALUE_PRIOR_PRODUCER_REQUIRED` for its identity.

### VA-05

**Low: custom argument mismatch coverage tests missing names only**

- Test: [tests/test_validator.py:317](../../tests/test_validator.py#L317) claims missing, extra and misspelled arguments.
- Its registry requires x and y and the only call supplies x: only `missing` is
  exercised.
- Production: [src/rules_engine/registry.py:265](../../src/rules_engine/registry.py#L265) checks `missing or extra` independently,
  then [src/rules_engine/validator.py:931](../../src/rules_engine/validator.py#L931) maps the binding exception into a structured
  mismatch issue.
- Probe: binding that silently drops unexpected names leaves the test passing.
- Recommendation: parameterize missing-only and extra-only arguments; a misspelled
  case can also verify the diagnostic required/optional/actual fields. Keep this
  authored-input validation test even though registry constructor tests also exist.

### VA-06

**Low: several acceptance tests check absence of one diagnostic, not acceptance**

Affected tests:

- [tests/test_validator.py:533](../../tests/test_validator.py#L533) — potential prior producer.
- [tests/test_spark_validator.py:104](../../tests/test_spark_validator.py#L104) — numeric fallback.
- [tests/test_spark_validator.py:863](../../tests/test_spark_validator.py#L863) — array membership.
- [tests/test_spark_validator.py:1108](../../tests/test_spark_validator.py#L1108) — NTZ scalar hint.
- [tests/test_spark_validator.py:1128](../../tests/test_spark_validator.py#L1128) — NTZ collection hint.

All fixtures currently validate, but their oracle only excludes one check name. A
false rejection under a different check name would pass. This is different from
negative tests requiring an issue: any issue fails validation, while absence of one
issue does not establish success.

Probe: injecting another error into Spark `validate` leaves all four Spark acceptance
tests green. The prior-producer test has the same structurally weak oracle. Public
`assignment_schema` success is already a sufficient acceptance oracle in other tests,
such as exact target reuse at [tests/test_spark_validator.py:244](../../tests/test_spark_validator.py#L244); those do not need
this change.

Recommendation: retain each fixture and assert `result.passed`, retaining specialized
diagnostic exclusions only if they add useful clarity.

### VA-07

**Medium: selection functions lack decisive counterexamples**

- [tests/test_standard_functions.py:217](../../tests/test_standard_functions.py#L217) only calls `decimal_min("2", "3")` and
  `decimal_max("2", "3")` at lines 227–228. Returning the left/right argument without
  comparing them passes the entire test. Clamp checks only above-maximum behavior;
  it has no below-minimum or unchanged-interior example.
- [tests/test_standard_functions.py:279](../../tests/test_standard_functions.py#L279) calls `array_contains_all` only when every
  candidate is present or the candidates list is empty (lines 285–286). Its scalar
  guard is useful, but an implementation that preserves validation and always returns
  True for valid inputs passes. The live composition test at
  [tests/test_spark_runtime.py:794](../../tests/test_spark_runtime.py#L794) also expects True, so it is not a counterexample.
  Despite the broad null-aware title, contains_any/all have no assertions for None on
  either input side in this test.
- Production functions: [src/rules_engine/standard_functions.py:410](../../src/rules_engine/standard_functions.py#L410), `:422`, `:427`,
  `:597`, `:606`.
- Probes: both “return selected argument instead of min/max” and “always-True valid
  contains_all” mutants passed their entire existing test functions.
- Recommendation: keep the arithmetic/array tests and add reverse/equal min/max,
  lower/interior clamp, nonempty missing-candidate contains_all, and None-input
  predicate cases. Individual parameterized cases improve failure localization; no
  assertion needs to be removed. Decimal return-type assertions on representative
  outputs also prevent numeric equality masking a float/int return change.

### VA-08

**Low: completed-period tests cannot detect lost negative signs**

- Test: [tests/test_standard_functions.py:237](../../tests/test_standard_functions.py#L237).
- The reversed month and year cases both expect zero (`:242`, `:244`). They test
  incomplete periods but cannot distinguish a correct signed implementation from one
  returning absolute periods.
- Production: [src/rules_engine/standard_functions.py:478](../../src/rules_engine/standard_functions.py#L478) and `:492` have distinct
  reverse-order branches returning negated forward results.
- Probe: wrapping both period functions in `abs(...)` leaves the test passing.
- Recommendation: retain the incomplete-period and boundary cases and include a
  reversed pair with at least one fully completed month/year, expecting -1 or less.
  The signed day-difference test covers a separate function and should stay.

### VA-09

**Low: version independence uses fresh validators, missing the reusable instance**

- Test: [tests/test_validator.py:817](../../tests/test_validator.py#L817), final assertions at lines 835–836.
- Its purpose is preventing assignment uniqueness state leaking across versions, but
  it constructs a new `RulesetValidator()` for each version. It cannot catch accidental
  state retained on one validator instance between `validate` calls.
- Production: seen assignment IDs are correctly local inside `_validate_ruleset`
  ([src/rules_engine/validator.py:155](../../src/rules_engine/validator.py#L155)). There is no current bug.
- Recommendation: preserve the two versions and stable assignment identity; reuse one
  validator for both calls (optionally repeat version 1). No separate duplicate test
  is necessary. This observation is static, not included among the mutation probes.

### CY-01

**Medium: the mixed-type sorting regression is not exercised**

- **Test:** [tests/test_compiler_yaml.py:720](../../tests/test_compiler_yaml.py#L720) (`test_mixed_type_unsupported_keys_raise_compilation_error`).
- **Claim:** malformed mixed-type keys must not leak a Python sorting error.
- **Actual fixture:** only integer key `1` is unsupported. Valid string keys are removed when `_reject_unsupported_keys` constructs the unsupported set, leaving one element.
- **Probe:** replacing the production diagnostic ordering with unsafe `sorted(unsupported_keys)` still passes this test. Adding both `1` and `"unexpected"` as unsupported keys to the same payload then raises `TypeError: '<' not supported between instances of 'str' and 'int'` under that mutation.
- **Proposed change:** replace the fixture with at least two unsupported keys of incomparable Python types, and assert a CompilationError that identifies both. Keep the regression; its intent is useful, its current trigger is ineffective.

### CY-02

**Low: the deterministic-payload test never varies dictionary order**

- **Test:** [tests/test_serializer.py:201](../../tests/test_serializer.py#L201) (`test_content_hash_and_payload_json_are_deterministic`).
- **Claim:** catches dictionary ordering causing noncanonical bytes.
- **Actual assertion:** serializes the same unchanged ruleset twice in the same process.
- **Probe:** monkeypatching `_canonical_mapping_dumps` to preserve insertion order rather than sort keys still passes. Under the same mutation, `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` produce different canonical JSON.
- **Proposed change:** keep the repeat-call smoke check, and compare rulesets whose **literal or raw function-argument dictionaries** contain identical data in different insertion orders. Do not reorder rules or assignments, whose order is meaningful. Also directly check canonical set/dictionary output under alternative construction orders; independent expected bytes make ordering intent explicit.

### CY-03

**Low: lifecycle/provenance and summary assertions only cover trivial defaults**

- **Test:** [tests/test_serializer.py:69](../../tests/test_serializer.py#L69) (`test_serializer_stamps_provenance_hash_and_summary_counts`).
- **Claim:** populates lifecycle, publication provenance and summary counts.
- **Actual fixture/assertions:** omits `published_by` and `published_at`, expects only `published_by is None`, never checks status, and has exactly one rule, one condition and one assignment.
- **Probe:** a wrapper returning `status="retired"`, discarding both publication fields, and hard-coding rule/condition/assignment counts to 1 still passes.
- **Proposed change:** pass nondefault publication fields and assert them plus published lifecycle/empty retirement fields. Use multiple rules, nested groups and multiple assignments to produce distinct counts. The separate `test_serializer_counts_nested_custom_function_operands` at line 113 is sound and need not be removed.

### CY-04

**Low: datetime equality does not verify UTC normalization**

- **Test:** [tests/test_compiler_yaml.py:496](../../tests/test_compiler_yaml.py#L496) (`test_explicit_timestamp_literals_normalize_to_datetime`).
- **Related test outside this audit group:** [tests/test_standard_functions.py:192](../../tests/test_standard_functions.py#L192) (`test_timestamp_converters_distinguish_instant_and_wall_clock_values`) uses the same equality pattern.
- **Claim:** compile timestamp text into the canonical declared datetime representation; `standard_functions.to_timestamp` explicitly promises normalization to UTC.
- **Probe:** replacing `to_timestamp` with `datetime.fromisoformat` makes the compiler retain `+05:00`, yet the existing UTC expected-value assertion passes. Aware datetime equality compares instants rather than requiring the same timezone representation.
- **Proposed change:** retain the instant equality assertion and additionally assert zero UTC offset (or the actual UTC tzinfo contract) and canonical clock components. For timestamp_ntz, assert `tzinfo is None` explicitly. This is a representation-oracle weakness, not evidence that current code normalizes incorrectly.

### CY-05

**Low: a compiler-only test claims semantic validation**

- **Test:** [tests/test_compiler_yaml.py:67](../../tests/test_compiler_yaml.py#L67) (`test_valid_simple_row_rule_compiles_and_validates`).
- **Actual execution:** calls `compile_payload`, then inspects ownership, operator and default tolerance. It never invokes `RulesetValidator`.
- **Current architecture:** compiler performs structure parsing; semantic validation is a separate production gate. The test is useful for compilation, but its name promises work it does not do.
- **Proposed change:** rename to describe compilation/default metadata, or explicitly call and assert validation if an integration check is intended. Prefer the name correction because validated compiler/exporter fixtures already exist elsewhere. No test deletion is warranted.

### CY-06

**Low: “every contract level” omits separate group and operand checks**

- **Test:** [tests/test_compiler_yaml.py:689](../../tests/test_compiler_yaml.py#L689) (`test_unknown_mapping_keys_are_rejected_at_every_contract_level`).
- **Actual matrix:** ruleset, rule, condition, assignment and inner custom-function payload.
- **Missing separate production paths:** condition-group allowed keys in `_compile_group_mapping` and operand allowed keys in `_compile_operand` (field, assigned, literal, outer custom_function). Mapping-escape exclusivity has its own separate regression and is covered.
- **Proposed change:** extend this existing matrix with group and each operand-kind mapping, giving each a valid kind plus an extra key so it reaches unsupported-key validation rather than failing operand recognition. Alternatively narrow the “every” claim, but adding these small cases protects independently declared schemas.

### CY-07

**Low: two recursive typed-collection tests only traverse flat lists**

- **Tests:** [tests/test_compiler_yaml.py:417](../../tests/test_compiler_yaml.py#L417) (`test_explicit_decimal_collection_is_normalized_recursively`) and [tests/test_compiler_yaml.py:602](../../tests/test_compiler_yaml.py#L602) (`test_known_scalar_literal_hints_validate_collection_items_recursively`).
- **Actual inputs:** `["0.0425", "0.05"]` and `[1, "2"]`, respectively. These prove one-level element conversion/validation, which remains useful.
- **Gap:** neither contains a nested collection. A one-level-only implementation can satisfy their assertions despite their explicit recursive claim; the production function supports nested list, tuple, set and mapping branches.
- **Proposed change:** add at least a nested list inside a mapping/tuple with an exact decimal leaf and a deeply nested incompatible typed leaf. Assert container shape and leaf type, not only numeric equality. Parameterizing the existing tests can cover these cases without creating duplicate test functions.

### RO-01

**Low: omitted-publication-provenance test also passes when nothing is published**

[tests/test_publish.py:82](../../tests/test_publish.py#L82) calls `PublishService.publish` without an actor, then
asserts that the recording repository's actor is None. That field was already None
before the call. Replacing `publish` with a no-op leaves this test green. The explicit
actor test at line 69 catches that mutation, so this is a localized weak test, not
evidence that publication as a whole is untested.

Keep the optional-actor case, but assert the original ruleset was saved exactly once
and the actor passed to the repository was None. Repository normalization to
`system` is a separate boundary and is not proved by this recording fake.

### RO-02

**Low: the complete operator-contract test has a partly self-derived oracle**

[tests/test_authoring.py:58](../../tests/test_authoring.py#L58) indexes records into a dictionary, checks selected
operator shapes and derives expected arity from the returned `right_operand_shape`.
A deliberately incorrect `is_not_null` record with arity 2 and shape `any` passes.
The dictionary also conceals duplicate records for an already present operator.

Retain the manifest contract test. Compare all operator records against an independent
expected table, including both unary operators, ordinary binary shapes and uniqueness.
Comparing enums remains useful for enum/manifest wiring but does not independently
establish every behavioral attribute.

### RO-03

**Medium: the ordering differential test shares its oracle and never scrambles rules**

[tests/test_governance.py:44](../../tests/test_governance.py#L44) compares the pure row evaluator with the worker closure.
Both now delegate to the same prepared row execution logic. Its authored rules are
already in ascending `rule_order`. A process-only mutation making runtime
`iter_rules` ignore `ordered=True` passes the test.

Keep the useful adapter parity comparison, but add hand-written expected match IDs,
assignments and stop results, and deliberately scramble the metadata list. The runtime
test review found shuffled inputs in dependency-projection tests, which only exercise
traversal, not execution. This audit does not claim every suite case was run under the
sorting mutation; the direct targeted test definitely cannot establish its claim.

### RO-04

**Low: the sibling-version test starts with an empty repository**

[tests/test_repository.py:182](../../tests/test_repository.py#L182) says two published versions of one name may coexist,
but invokes `save_published` only once for version 2 and replaces both identity lookups
with unconditional None. It proves an append when neither identity exists. It cannot
demonstrate that an existing version 1 is allowed alongside version 2.

Retain the policy test with a small stateful fake keyed by both identity contracts.
Publish version 1 and version 2, verify both retained, then reject a duplicate exact
version. The existing duplicate-ID and duplicate-name tests remain useful focused
counterparts. No Delta session is required for the policy fixture itself.

### RO-05

**Medium: service load tests ignore the requested version**

[tests/test_service.py:118](../../tests/test_service.py#L118) and `:281` load version 1 through a fake whose
`load_published` checks only the name (`:38`). Both tests still pass when the actual
facade silently replaces the caller's version with None. Name forwarding, actor
forwarding and exact formatting are otherwise checked correctly.

Record the exact `(ruleset_name, version)` received by the fake and assert it. Keep
the two scenarios: YAML publication/load and loaded nested formatting exercise
different public behavior. This is a forwarding gap, not a request to duplicate
all repository tests in the service module.

### RO-06

**Medium: source-projection tests cannot detect sending every column to workers**

[tests/test_spark_runtime.py:827](../../tests/test_spark_runtime.py#L827) checks the public required-column helper and final
values; `:864` checks a literal-only helper result and successful output. Neither
observes the source struct actually passed into the UDF.

An in-memory change to `_evaluate_attached_dataframe` replacing
`*prepared_schema.required_source_columns` with `*df.columns` leaves both live tests
green. In the second case the intended empty-dependency branch is no longer used.
The separate required-source/prepared-schema invariant tests are valid, but cannot
prove that plan construction consumes those facts.

Retain the real dotted-name and literal-only integration cases. Add an observable
worker-input-key assertion or inspect the constructed UDF argument expression.
Include both audit modes because audit may legitimately add existing assignment
targets for old-value tracing. Assert unrelated columns are absent and the
empty-dependency sentinel path is actually selected when appropriate.

### RO-07

**Medium: coverage analytics tests omit error exclusion and first-match statistics**

[tests/test_spark_runtime.py:1407](../../tests/test_spark_runtime.py#L1407) uses three successful rows, asserts total/no-match
counts, dead/broad IDs and one no-match row. It does not check per-rule match counts,
first-match counts/distribution or rates, and has no error row.

Two separate process-local changes pass this existing live test: forcing every
`first_match_count` to zero, and removing `error_col.isNull()` from the clean-no-match
predicate. These are test-oracle demonstrations; current production code retains
both calculations correctly.

Keep the ANSI/custom-prefix scenarios. Add a row whose custom condition errors,
assert it is counted as an error and absent from all clean no-match rows, assert
the full no-match row-ID set, and check exact per-rule counts, first-match
distribution and rates. Include empty-input behavior in an additional small case.

### RO-08

**Low: the live compact-null-error claim actually runs full audit**

[tests/test_spark_runtime.py:664](../../tests/test_spark_runtime.py#L664) describes a compact error but passes
`full_audit=True` at line 681 and asserts an audit-only trace field. This is useful
audit quarantine coverage, but not a compact-path test.

Keep it and parameterize both modes with mode-appropriate assertions, or rename its
purpose to audit quarantine. This is the same class of helper/mode issue as RT-01,
but at the real Spark boundary and for an independent null-error scenario.

### RO-09

**Low: the pre-UDF-validation test only establishes a construction-time error**

[tests/test_spark_runtime.py:1022](../../tests/test_spark_runtime.py#L1022) requires the expected schema ValidationFailedError
while building an evaluation. It does not observe whether `_build_row_evaluator` or
`F.udf` was invoked first. A future ordering change could build a closure/UDF and
then raise validation while leaving this test green. This finding is from control-flow
inspection, not a mutation probe.

Retain the incompatible-target case and add a fail-if-called spy around UDF/worker
construction, or narrow the title to synchronous schema rejection. The broader
no-hidden-Spark-action test at line 1250 uses a real job tracker and is sound.

### RO-10

**Medium coverage gap: repository fakes do not establish published selection**

The duplicate-load test at [tests/test_repository.py:235](../../tests/test_repository.py#L235) correctly tests its stated
two-row rejection, but `FakePredicate` and `FakeLoadDataFrame.where` at `:75` and
`:89` discard all filtering semantics. The repository module has no successful
`load_published` case proving name/status/version selection, no empty-result case,
and no unpinned-multiple-published case. Current code implements these branches in
[src/rules_engine/repository.py:370](../../src/rules_engine/repository.py#L370) onward; the gap is in unit coverage, not a
demonstrated implementation defect. Service loading uses a different, simpler fake.

Add predicate-recording/semantic fake coverage or a bounded real-Spark table test
for a published row among retired/wrong-name/wrong-version rows, no match, unpinned
ambiguity, and unique successful payload deserialization. Preserve the existing
duplicate-count test. Delta transaction/concurrency correctness still needs the
target Databricks suite; fake SQL capture does not establish it.

### RO-11

**Low: direct writer test overstates its existence-check evidence**

[tests/test_repository.py:482](../../tests/test_repository.py#L482) correctly checks Delta format, append mode and
saveAsTable, but the fake catalog reports the table exists and no call ordering is
recorded. It does not itself exercise the failed-check clause claimed in its
docstring. The public publication missing-table test at `:201` does reach the
actual write guard and would catch removing it; the suite has useful protection.

Either narrow this test's description to the successful writer contract, or add
direct missing-table and recorded-order assertions. Keep its by-name append API
assertions; they protect a different contract from the public failure path.

## Additional bounded follow-ups

These are opportunities exposed by reviewing current code, not a claim that all
possible missing tests have been enumerated:

- Assert exact output TimestampType and installed engine version in the existing
  live output tests; they currently establish the broader value/schema behavior.
- Add DDL-versus-StructType parity for all types/nullability, invalid/default
  bootstrap modes, and staging-view cleanup on failed MERGE. Existing DDL and SQL
  capture tests remain useful unit evidence, not live Delta execution.
- Cover the service's path-based publication, coverage-report option forwarding
  and explicit-ruleset precedence with small recording fakes.
- The long first live smoke test can optionally be split for better diagnosis;
  keep its schema/provenance assertions. Fresh timings are environment-specific and
  are not a reason to remove cases that establish unique guarantees.
