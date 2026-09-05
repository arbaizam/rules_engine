# Unit-test remediation — 2026-09-05

This report tracks the implementation of all **33 findings** and the bounded
follow-ups in the [unit-test review](2026-09-05-unit-test-review.md). The original
review, register and evidence remain historical records of the reviewed snapshot.

The changes fix the demonstrated YAML float round-trip defect and strengthen tests
where fixtures, execution paths or assertions did not establish their stated
purpose. No scenario was retired as unnecessary. Five tests with duplicated setup
were consolidated into two parameterized families while retaining every case and
assertion. Compiler-only and direct-worker tests now describe their actual layer.

The current suite has **355 explicit functions / 615 collected cases**, including
**43 live Spark cases**. This adds 84 cases to the reviewed baseline; the lower
function count in the consolidated runtime families reflects retained parameter
cases rather than removed coverage. The [generated inventory](../rules_engine_test_inventory.md)
lists counts by module.

Links below identify the current test functions; names are used instead of line
numbers so later additions do not invalidate the references.

## Compiler, exporter and serializer

| Finding | Implemented change and current test |
| --- | --- |
| CY-01 | [test_mixed_type_unsupported_keys_raise_compilation_error](../../tests/test_compiler_yaml.py) supplies both unsupported integer and string keys, forcing the mixed-type diagnostic ordering case and checking both keys in the error. |
| CY-02 | [test_content_hash_and_payload_json_are_deterministic](../../tests/test_serializer.py) retains the repeated-call check and adds differently constructed nested mappings/sets, independent expected canonical bytes, and identical persisted payload/hash assertions. |
| CY-03 | [test_serializer_stamps_provenance_hash_and_summary_counts](../../tests/test_serializer.py) checks explicit publication actor/time, published status, empty retirement fields, and distinct totals from two rules, five conditions and three assignments. Omitted provenance remains checked. |
| CY-04 | [test_explicit_timestamp_literals_normalize_to_datetime](../../tests/test_compiler_yaml.py) checks UTC/naive timezone representation and clock components in addition to datetime equality. |
| CY-05 | The useful compilation test is now [test_valid_simple_row_rule_compiles_with_owner_and_default_metadata](../../tests/test_compiler_yaml.py); its name no longer claims semantic validation. |
| CY-06 | [test_unknown_mapping_keys_are_rejected_at_every_contract_level](../../tests/test_compiler_yaml.py) includes condition groups and field, assigned, literal and outer custom-function operands, preserving the original five contexts. |
| CY-07 | [test_explicit_decimal_collection_is_normalized_recursively](../../tests/test_compiler_yaml.py) and [test_known_scalar_literal_hints_validate_collection_items_recursively](../../tests/test_compiler_yaml.py) retain flat cases and add genuinely nested mapping/list/tuple/set cases, including concrete Decimal leaves and deep invalid leaves. |
| CY-08 | [test_yaml_export_preserves_untyped_float_literals_and_null_defaults](../../tests/test_exporter_yaml.py), [test_yaml_export_preserves_raw_float_argument_types_and_function_behavior](../../tests/test_exporter_yaml.py), [test_yaml_export_preserves_float_literal_type_hint_metadata](../../tests/test_exporter_yaml.py), and the expanded operand-shaped mapping matrix verify scalar kinds, signed zero, nested collections, type hints, canonical hashes and observable function results through persistence, YAML text and direct exported-payload recompilation. |

The CY-08 production fix is in [YamlRulesetExporter](../../src/rules_engine/exporter_yaml.py),
[YamlRulesetCompiler](../../src/rules_engine/compiler_yaml.py), and
[canonical literal normalization](../../src/rules_engine/canonical_values.py). Exported binary
floats use the explicit safe YAML tag `!rules_engine/float`, preserving their Python
kind without adding a `value_type` field to the canonical model. Ordinary untagged
fractions still compile as Decimal; existing explicit type hints retain their
conversion semantics. An internal float marker also preserves kind through direct
`export_payload` → `compile_payload`, then normalizes to an ordinary Python float in
the model. Compiler tests also reject malformed and nonfinite tagged
values. Reading this extended export format requires support for its engine tags.

## Runtime and Spark boundaries

| Finding | Implemented change and current test |
| --- | --- |
| RT-01 | [test_losing_rule_preserves_later_condition_errors_in_both_audit_modes](../../tests/test_runtime.py) explicitly runs compact and full audit, so the later condition error is checked in both production evaluators. |
| RT-02 | [test_float32_rounding_is_visible_to_later_rules](../../tests/test_spark_boundaries.py) observes the value received by a later function as a hexadecimal string before another FloatType conversion could conceal an unrounded read. Existing output-rounding, overflow and audit-mode cases remain. |
| RT-03 | [test_spark_row_evaluator_wraps_fail_fast_errors_in_the_worker](../../tests/test_runtime.py) accurately identifies direct worker exception wrapping. Separate live materialization and laziness tests remain. |
| RT-04 | [test_full_audit_evaluates_each_condition_once_and_emits_only_matched_traces](../../tests/test_runtime.py) counts actual custom-function calls as well as trace creation and checks the complete matched-rule list. |
| RT-05 | [test_dataframe_evaluation_reserves_every_output_name_in_compact_mode](../../tests/test_runtime.py) combines all eight reserved suffixes. [test_dataframe_evaluation_rejects_ambiguous_source_columns](../../tests/test_runtime.py) combines exact selected-key, case-only selected-key and case-only unselected-source collisions. Five functions become two families without losing scenarios. |

## Base and Spark validation, standard functions

| Finding | Implemented change and current test |
| --- | --- |
| VA-01 | [test_spark_validator_allows_error_on_null_for_udf_row_path](../../tests/test_spark_validator.py) supplies the source schema and calls `prepare`, exercising Spark preflight instead of base-only delegation. |
| VA-02 | [test_spark_validator_does_not_guess_condition_coercion_semantics](../../tests/test_spark_validator.py) keeps the original string/string case and adds string/integer, string/Decimal and integer/string comparisons. |
| VA-03 | [test_timestamp_converters_distinguish_instant_and_wall_clock_values](../../tests/test_standard_functions.py) separately asserts zero UTC offset and naive NTZ output; equal instants alone no longer satisfy the test. |
| VA-04 | [test_assigned_operand_requires_an_active_lower_order_producer](../../tests/test_validator.py) now constructs same-rule, future and inactive-earlier producers separately and checks the consumer's diagnostic identity and details. |
| VA-05 | [test_custom_function_args_mismatch_fails_validation](../../tests/test_validator.py) covers missing-only, extra-only and misspelled arguments and checks required/optional/actual argument details. |
| VA-06 | The potential-prior-producer test in [test_validator.py](../../tests/test_validator.py), and numeric-default, array-membership and both NTZ-hint acceptance tests in [test_spark_validator.py](../../tests/test_spark_validator.py), assert overall validation success. Negative tests continue to use sufficient issue-name assertions. |
| VA-07 | [test_decimal_selection_functions_compare_values_and_preserve_decimal_type](../../tests/test_standard_functions.py) retains the original min/max/clamp values and adds reversed/equal inputs, lower/interior clamp cases and exact Decimal types. [test_array_functions_are_null_aware_and_reject_scalar_inputs](../../tests/test_standard_functions.py) adds missing-candidate false results and None on either predicate input. |
| VA-08 | [test_calendar_boundary_and_completed_period_functions_are_explicit](../../tests/test_standard_functions.py) now includes negative completed month and year results, retaining incomplete-period and boundary cases. |
| VA-09 | [test_assignment_id_may_be_reused_when_versions_are_validated_independently](../../tests/test_validator.py) validates both versions and then repeats the first using one validator instance, detecting leaked uniqueness state. |

## Public services, repository policy and live integration

| Finding | Implemented change and current test |
| --- | --- |
| RO-01 | [test_publish_allows_omitted_provenance](../../tests/test_publish.py) verifies the exact ruleset is saved once with a None actor; an untouched fake or no-op publication cannot pass. |
| RO-02 | [test_manifest_exposes_the_complete_engine_operator_contract](../../tests/test_authoring.py) compares the ordered records against an independent table of all 18 operators, including both unary records, and checks uniqueness. |
| RO-03 | [test_python_evaluator_and_spark_worker_share_rule_ordering_semantics](../../tests/test_governance.py) deliberately shuffles the compiled rules and compares both adapters against explicit merge/stop/no-match outcomes in both audit modes. |
| RO-04 | [test_save_published_allows_distinct_versions_for_same_ruleset_name](../../tests/test_repository.py) uses stateful identity lookups, saves versions 1 and 2, verifies both survive, and rejects exact identity/name-version duplicates. |
| RO-05 | Existing publication/load and loaded-description tests in [test_service.py](../../tests/test_service.py) record the exact requested name/version. The new service resolution matrix checks pinned loading and explicit-model precedence. |
| RO-06 | [test_spark_runtime_serializes_only_required_literal_source_columns](../../tests/test_spark_runtime.py) and [test_spark_runtime_evaluates_literal_only_rule_without_source_dependencies](../../tests/test_spark_runtime.py) assert the actual worker input keys, including the empty-input sentinel and full-audit old-value dependencies. Unrelated source columns remain in the applied output. |
| RO-07 | [test_coverage_report_finds_dead_broad_and_clean_no_match_rows](../../tests/test_spark_runtime.py) checks error exclusion, the full clean no-match ID set, exact per-rule/first-match counts, rates and broad thresholds. [test_coverage_report_handles_empty_input_with_zero_counts_and_rates](../../tests/test_spark_runtime.py) covers empty input. ANSI and custom-prefix scenarios remain. |
| RO-08 | [test_spark_runtime_quarantines_error_on_null](../../tests/test_spark_runtime.py) runs both audit modes and checks the appropriate presence or absence of audit-only fields. |
| RO-09 | [test_spark_runtime_validates_schema_before_building_udf](../../tests/test_spark_runtime.py) fails if either worker construction or `F.udf` is reached before the expected schema rejection. |
| RO-10 | The published-load tests in [test_repository.py](../../tests/test_repository.py) use a predicate-evaluating fake and realistic rows to cover name/status/version selection, distractors before the selected row, successful deserialization, no match and unpinned ambiguity. Existing explicit-version duplicate rejection remains. |
| RO-11 | [test_write_rows_appends_by_name_only_after_table_existence_check](../../tests/test_repository.py) records existence-check/frame-creation ordering and covers both existing and missing tables, retaining the Delta append/by-name writer assertions. |

## Bounded follow-ups

| Follow-up from the review | Implementation |
| --- | --- |
| Exact live timestamp schema and installed engine version | [test_spark_runtime_preserves_timestamp_assignment_type](../../tests/test_spark_runtime.py) checks TimestampType in both assignment payload and applied output. [test_full_audit_emits_ordered_optional_detail_and_identity](../../tests/test_spark_runtime.py) checks the actual installed version in compact and full audit. |
| DDL/schema parity and bootstrap modes | [test_bootstrap_ddl_matches_every_struct_field_and_mode](../../tests/test_repository_schema.py) compares every declared field, type and nullability across default/supported modes. [test_bootstrap_rejects_invalid_modes_before_any_sql](../../tests/test_repository_schema.py) checks rejection before SQL. |
| Failed-MERGE staging cleanup | [test_registry_merge_failure_drops_its_staging_view](../../tests/test_repository.py) checks cleanup of the exact staging view after an injected MERGE failure in both registry update modes. |
| Service path publication and option/resolution forwarding | [test_service_publishes_yaml_path_with_provenance](../../tests/test_service.py) reads a real temporary YAML file. [test_service_resolves_pinned_or_supplied_ruleset_and_forwards_options](../../tests/test_service.py) covers describe/evaluate/coverage calls, explicit-model precedence and nondefault options. |
| Split the long live smoke test without dropping coverage | Multi-match override/provenance coverage now has [test_full_audit_tracks_overridden_assignments_across_multiple_matches](../../tests/test_spark_runtime.py); [test_spark_runtime_evaluates_row_rule](../../tests/test_spark_runtime.py) retains its original single-rule schema/provenance checks. |

## Verification

Focused runs and narrowly scoped process-local mutation checks were used during
implementation. The revised tests rejected 39 targeted regressions across the
compiler, runtime and validator checks; these are not a whole-suite mutation
score. An independent exporter probe also passed 532 payload/text round trips
over 133 finite float values, including signed zeros, subnormals and finite
extremes in nested literals and raw arguments.

| Check | Final result |
| --- | --- |
| Full suite: Python 3.10 / PySpark 3.5.6, live Spark enabled | **615 passed**, including 43 live cases; 0 failures or skips. |
| Full suite: Python 3.12 / PySpark 4.2.0, live Spark enabled | **615 passed**, including 43 live cases; 0 failures or skips. |
| Ruff, whitespace and generated test inventory | Passed. |
| Package build and source verification | Wheel built; 26 source modules and package version 3.0 verified against the checkout. |

The final assertion-only exporter refinement was rechecked separately on Python
3.12 (**35 passed**) after that full run loaded its tests; the Python 3.10 full
run includes it. The Spark 4.2 run emitted one upstream pandas compatibility
warning. Java 17 was used for both runs. Per-case results and source hashes are
recorded in the [remediation evidence](2026-09-05-unit-test-remediation-evidence.json).

The changes preserve the distinction between direct Python/codec tests and live
Spark tests. Repository predicate and SQL-capture fakes establish local selection,
write policy and cleanup contracts; they do not prove Delta transaction or
concurrency behavior. Databricks/Delta notebook execution and serverless acceptance
remain checks for the target environment. No deployment or publication is part of
this remediation.
