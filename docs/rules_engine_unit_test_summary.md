# Rules Engine Unit Test Summary

Source: current behavioral pytest suite under `tests/`.

The suite contains **212 explicit test functions** and currently collects **236 pytest cases** after parameter expansion. **16 cases** require `RULES_ENGINE_RUN_SPARK_TESTS=1` and a live Spark runtime.

Standalone wheel-building, Databricks bundle-layout, package-version alignment, README, and notebook source-layout tests are intentionally excluded. Those surfaces differ between this repository and the integrated work Databricks deployment and are validated separately during packaging and deployment.

Ruleset version comparison remains covered because it is engine behavior rather than package or repository version alignment.

## Coverage Rollup

| Area | Explicit Tests | Representative Coverage |
| --- | ---: | --- |
| Governance and change control | 15 | Expected cases, audit levels, semantic diffs, and coverage diagnostics. |
| In-memory runtime | 44 | Row evaluation, errors, provenance, and worker-safe behavior without Spark. |
| Publish workflow | 4 | Validation and governance gates before repository persistence. |
| Reconciliation translation | 11 | Translation of legacy reconciliation definitions into canonical YAML. |
| Repository persistence | 9 | Delta persistence, immutable identity, lifecycle, and registry behavior. |
| Repository schema | 5 | Metadata DDL, table names, and nullability contracts. |
| Ruleset validation | 15 | Semantic invariants that fail before publication or execution. |
| Ruleset version comparison | 2 | Deterministic ordering and validation of ruleset version identifiers. |
| Service orchestration | 14 | Public facade delegation across compile, publish, evaluate, and lifecycle APIs. |
| Spark runtime | 16 | Typed execution and audit output through the real Spark worker boundary. |
| Spark validation | 31 | Preflight rejection of unsupported or incompatible Spark schemas. |
| Standard functions | 12 | Registered text, numeric, null, and calendar functions. |
| Version serialization | 13 | Deterministic payloads, hashes, exact values, and deserialization. |
| YAML compilation | 18 | Canonical authoring, defaults, aliases, exact numerics, and unsupported constructs. |
| YAML export | 3 | Stable, reviewable canonical YAML round trips. |

## Detailed Test Inventory

| Test ID | Area | Priority | Test | Plain-English Coverage | Execution |
| --- | --- | --- | --- | --- | --- |
| UT-001 | YAML compilation | Critical | `test_compiler_yaml.py::test_compile_text_preserves_untyped_fractional_yaml_as_decimal` | Financial YAML literals must not silently become binary floats. | Local and Databricks |
| UT-002 | YAML compilation | High | `test_compiler_yaml.py::test_valid_simple_row_rule_compiles_and_validates` | Compiles a minimal row-level YAML rule with owner metadata. | Local and Databricks |
| UT-003 | YAML compilation | Medium | `test_compiler_yaml.py::test_condition_null_modes_use_documented_authoring_defaults` | Concise YAML materializes explicit null semantics in canonical models. | Local and Databricks |
| UT-004 | YAML compilation | Medium | `test_compiler_yaml.py::test_nonfinite_yaml_numbers_fail_compilation` | NaN and infinities cannot enter comparison or persistence paths. | Local and Databricks |
| UT-005 | YAML compilation | Medium | `test_compiler_yaml.py::test_nonfinite_explicit_numeric_payloads_fail_compilation` | Explicit floating hints cannot bypass the finite-number invariant. | Local and Databricks |
| UT-006 | YAML compilation | Critical | `test_compiler_yaml.py::test_explicit_decimal_collection_is_normalized_recursively` | A collection hint no longer bypasses exact Decimal normalization. | Local and Databricks |
| UT-007 | YAML compilation | Medium | `test_compiler_yaml.py::test_precomputed_aggregate_field_compiles_as_row_field` | Compiles a rule that references an upstream aggregate column as a field. | Local and Databricks |
| UT-008 | YAML compilation | Medium | `test_compiler_yaml.py::test_canonical_string_operators_compile` | Compiles all canonical string operators. | Local and Databricks |
| UT-009 | YAML compilation | High | `test_compiler_yaml.py::test_value_operand_alias_is_rejected` | Rejects the non-canonical operand key value. | Local and Databricks |
| UT-010 | YAML compilation | High | `test_compiler_yaml.py::test_assignments_rule_alias_is_rejected` | Rejects the non-canonical rule key assignments. | Local and Databricks |
| UT-011 | YAML compilation | High | `test_compiler_yaml.py::test_aggregate_operand_is_rejected` | Rejects aggregate operands. | Local and Databricks |
| UT-012 | YAML compilation | High | `test_compiler_yaml.py::test_aggregate_operand_inside_custom_function_arg_is_rejected` | Rejects aggregate operands nested inside custom-function args. | Local and Databricks |
| UT-013 | YAML compilation | Medium | `test_compiler_yaml.py::test_generated_assignment_ids_are_stable_by_rule_and_target` | Generated IDs do not change when assignment order changes. | Local and Databricks |
| UT-014 | YAML compilation | Medium | `test_compiler_yaml.py::test_explicit_assignment_list_uses_stable_generated_id_when_omitted` | List-form assignments use the same rule-and-target ID contract. | Local and Databricks |
| UT-015 | YAML compilation | High | `test_compiler_yaml.py::test_duplicate_yaml_mapping_keys_fail_compilation` | Duplicate keys are rejected before the YAML parser drops a value. | Local and Databricks |
| UT-016 | YAML compilation | Medium | `test_compiler_yaml.py::test_yaml_merge_allows_explicit_key_override` | A legal YAML merge may be overridden by an explicit mapping key. | Local and Databricks |
| UT-017 | YAML compilation | Medium | `test_compiler_yaml.py::test_yaml_loader_preserves_recursive_alias_construction` | Two-phase mapping construction supports legal recursive YAML aliases. | Local and Databricks |
| UT-018 | YAML export | Medium | `test_exporter_yaml.py::test_yaml_export_round_trips_compiled_ruleset` | Exports a compiled ruleset to YAML and recompiles it. | Local and Databricks |
| UT-019 | YAML export | Medium | `test_exporter_yaml.py::test_yaml_export_text_is_stable_after_recompilation` | Canonical key order produces byte-stable review artifacts. | Local and Databricks |
| UT-020 | YAML export | Medium | `test_exporter_yaml.py::test_yaml_export_uses_canonical_keys` | Verifies YAML export emits canonical authoring keys only. | Local and Databricks |
| UT-021 | Governance and change control | Critical | `test_governance.py::test_expected_cases_round_trip_and_preserve_exact_decimals` | Expected cases round trip and preserve exact decimals. | Local and Databricks |
| UT-022 | Governance and change control | Medium | `test_governance.py::test_ruleset_tester_supports_assignment_shorthand_and_no_match_cases` | Ruleset tester supports assignment shorthand and no match cases. | Local and Databricks |
| UT-023 | Governance and change control | Critical | `test_governance.py::test_expected_case_failure_blocks_publish_before_repository_write` | Expected case failure blocks publish before repository write. | Local and Databricks |
| UT-024 | Governance and change control | Critical | `test_governance.py::test_passing_expected_cases_publish_normally` | Passing expected cases publish normally. | Local and Databricks |
| UT-025 | Governance and change control | High | `test_governance.py::test_expected_assign_shape_is_validated_before_execution` | Expected assign shape is validated before execution. | Local and Databricks |
| UT-026 | Governance and change control | High | `test_governance.py::test_expected_case_rejects_misspelled_assignment_keys` | Expected case rejects misspelled assignment keys. | Local and Databricks |
| UT-027 | Governance and change control | Medium | `test_governance.py::test_reserved_result_name_can_be_asserted_as_explicit_assignment` | Reserved result name can be asserted as explicit assignment. | Local and Databricks |
| UT-028 | Governance and change control | Medium | `test_governance.py::test_closest_rule_diagnostic_reports_failed_condition_ids` | Closest rule diagnostic reports failed condition ids. | Local and Databricks |
| UT-029 | Governance and change control | High | `test_governance.py::test_semantic_diff_highlights_order_logic_and_assignment_changes` | Semantic diff highlights order logic and assignment changes. | Local and Databricks |
| UT-030 | Governance and change control | Critical | `test_governance.py::test_semantic_diff_detects_null_behavior_and_identity_changes` | Semantic diff detects null behavior and identity changes. | Local and Databricks |
| UT-031 | Governance and change control | High | `test_governance.py::test_semantic_diff_renders_only_changed_condition_leaves` | Semantic diff renders only changed condition leaves. | Local and Databricks |
| UT-032 | Governance and change control | High | `test_governance.py::test_semantic_diff_reports_expected_cases_individually_by_name` | Semantic diff reports expected cases individually by name. | Local and Databricks |
| UT-033 | Governance and change control | High | `test_governance.py::test_audit_levels_have_distinct_schemas_and_payloads` | Audit levels have distinct schemas and payloads. | Local and Databricks |
| UT-034 | Governance and change control | High | `test_governance.py::test_invalid_audit_level_fails_before_spark_execution` | Invalid audit level fails before spark execution. | Local and Databricks |
| UT-035 | Governance and change control | Critical | `test_governance.py::test_publish_evaluator_and_spark_worker_share_rule_ordering_semantics` | Publish evaluator and spark worker share rule ordering semantics. | Local and Databricks |
| UT-036 | Ruleset version comparison | Medium | `test_pipeline_versioning.py::test_compare_versions_orders_numeric_dot_versions` | Compares numeric dot-notation versions using integer segments. | Local and Databricks |
| UT-037 | Ruleset version comparison | High | `test_pipeline_versioning.py::test_parse_numeric_version_rejects_tags_and_dates` | Rejects non-numeric versions for automatic retirement. | Local and Databricks |
| UT-038 | Publish workflow | Critical | `test_publish.py::test_publish_requires_published_status` | Rejects publish when the incoming ruleset status is not published. | Local and Databricks |
| UT-039 | Publish workflow | Critical | `test_publish.py::test_publish_passes_provenance_to_repository` | Passes published_by during direct publication. | Local and Databricks |
| UT-040 | Publish workflow | Critical | `test_publish.py::test_publish_passes_effective_dates_to_repository` | Passes effective date overrides during direct publication. | Local and Databricks |
| UT-041 | Publish workflow | Critical | `test_publish.py::test_publish_allows_omitted_provenance` | Allows publish callers to omit actor metadata. | Local and Databricks |
| UT-042 | Reconciliation translation | Medium | `test_recon_translation.py::test_all_and_source_rule_translates_correctly` | Translates a flat all-AND reconciliation rule into canonical YAML. | Local and Databricks |
| UT-043 | Reconciliation translation | Medium | `test_recon_translation.py::test_translator_can_disable_stop_on_match` | Verifies translator-level stop_on_match can be disabled. | Local and Databricks |
| UT-044 | Reconciliation translation | Medium | `test_recon_translation.py::test_mixed_join_chain_translates_left_to_right` | Translates a mixed AND/OR chain using confirmed left-to-right semantics. | Local and Databricks |
| UT-045 | Reconciliation translation | Medium | `test_recon_translation.py::test_group_sequence_translates_with_group_join_operator` | Translates grouped source criteria using GroupSequence and GroupJoinOperator. | Local and Databricks |
| UT-046 | Reconciliation translation | Medium | `test_recon_translation.py::test_operator_mapping_for_all_supported_source_operators` | Maps every supported source ValueOperator to its canonical operator. | Local and Databricks |
| UT-047 | Reconciliation translation | High | `test_recon_translation.py::test_slug_collision_emits_unique_rule_ids_and_audit_warning` | Deduplicates translated rule IDs when source rule names slug-collide. | Local and Databricks |
| UT-048 | Reconciliation translation | High | `test_recon_translation.py::test_invalid_source_operator_fails_translation` | Rejects an unsupported source ValueOperator. | Local and Databricks |
| UT-049 | Reconciliation translation | Medium | `test_recon_translation.py::test_null_join_before_final_criterion_fails_translation` | Rejects a non-final source row with a null JoinType. | Local and Databricks |
| UT-050 | Reconciliation translation | Medium | `test_recon_translation.py::test_last_group_join_operator_fails_translation` | Rejects a final source group that still declares a GroupJoinOperator. | Local and Databricks |
| UT-051 | Reconciliation translation | Medium | `test_recon_translation.py::test_output_yaml_aligns_to_engine_authoring_format` | Emits YAML text using the rules engine authoring vocabulary. | Local and Databricks |
| UT-052 | Reconciliation translation | High | `test_recon_translation.py::test_audit_artifact_contains_expected_fields` | Writes translation audit records with required governance fields. | Local and Databricks |
| UT-053 | Repository persistence | Critical | `test_repository.py::test_save_published_checks_duplicate_ruleset_name_and_version` | Uses ruleset_name/version as the duplicate publish boundary. | Local and Databricks |
| UT-054 | Repository persistence | Critical | `test_repository.py::test_save_published_allows_distinct_versions_for_same_ruleset_name` | Allows two published versions with the same ruleset_name when their versions differ. | Local and Databricks |
| UT-055 | Repository persistence | Critical | `test_repository.py::test_save_published_persists_effective_dates` | Persists explicit effective dates on the ruleset version row. | Local and Databricks |
| UT-056 | Repository persistence | Critical | `test_repository.py::test_load_published_rejects_duplicate_rows_for_explicit_version` | Pinned loads fail loudly when the immutable version key is not unique. | Local and Databricks |
| UT-057 | Repository persistence | High | `test_repository.py::test_retire_closes_effective_window` | Retirement updates lifecycle status and effective_end_date. | Local and Databricks |
| UT-058 | Repository persistence | High | `test_repository.py::test_retire_allows_explicit_effective_end_date` | Allows retirement callers to close the effective window explicitly. | Local and Databricks |
| UT-059 | Repository persistence | High | `test_repository.py::test_retire_rejects_already_retired_version` | Rejects a second retirement for an already retired version. | Local and Databricks |
| UT-060 | Repository persistence | High | `test_repository.py::test_save_function_registry_rows_skips_existing_rows_when_update_disabled` | Emits an insert-only merge when update_existing is disabled. | Local and Databricks |
| UT-061 | Repository persistence | Medium | `test_repository.py::test_save_function_registry_rows_upserts_by_default` | Keeps custom function registry saves as upserts by default. | Local and Databricks |
| UT-062 | Repository schema | Medium | `test_repository_schema.py::test_ruleset_version_schema_contains_payload_provenance_and_hash_fields` | Verifies the ruleset version schema exposes payload, count, provenance, and hash columns. | Local and Databricks |
| UT-063 | Repository schema | Medium | `test_repository_schema.py::test_function_registry_model_and_table_field_names_stay_aligned` | Registry model renames cannot silently drift from the Delta schema. | Local and Databricks |
| UT-064 | Repository schema | Medium | `test_repository_schema.py::test_table_names_can_be_built_from_schema` | Builds the standard two-table registry footprint from one schema name. | Local and Databricks |
| UT-065 | Repository schema | Medium | `test_repository_schema.py::test_create_base_tables_uses_explicit_delta_ddl_with_not_null_columns` | Creates metadata tables through SQL DDL with NOT NULL columns. | Local and Databricks |
| UT-066 | Repository schema | Medium | `test_repository_schema.py::test_create_base_tables_uses_if_not_exists_for_ignore_mode` | Uses CREATE TABLE IF NOT EXISTS for idempotent bootstrap. | Local and Databricks |
| UT-067 | In-memory runtime | Critical | `test_runtime.py::test_numeric_membership_uses_decimal_equality` | Worker floats match exact Decimal collection literals consistently. | Local and Databricks |
| UT-068 | In-memory runtime | Medium | `test_runtime.py::test_string_membership_semantics_are_unchanged` | Membership still applies exact equality to ordinary strings. | Local and Databricks |
| UT-069 | In-memory runtime | Medium | `test_runtime.py::test_scalar_string_membership_fails_with_contains_guidance` | IN rejects scalar strings instead of silently applying character matching. | Local and Databricks |
| UT-070 | In-memory runtime | Medium | `test_runtime.py::test_result_payload_keys_are_derived_from_the_declared_schema` | Success and error payloads cannot drift from the Spark result schema. | Local and Databricks |
| UT-071 | In-memory runtime | Medium | `test_runtime.py::test_assignment_provenance_uses_stable_event_positions` | Last-assignment precedence does not depend on dictionary identity. | Local and Databricks |
| UT-072 | In-memory runtime | Medium | `test_runtime.py::test_set_trace_text_is_deterministic` | Unordered values produce stable audit text across worker processes. | Local and Databricks |
| UT-073 | In-memory runtime | Medium | `test_runtime.py::test_human_readable_values_sort_sets_and_use_iso_temporal_text` | Authored audit expressions are deterministic across Python workers. | Local and Databricks |
| UT-074 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_orders_date_operands` | Spark row evaluator orders date operands. | Local and Databricks |
| UT-075 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_orders_date_ranges` | Spark row evaluator orders date ranges. | Local and Databricks |
| UT-076 | In-memory runtime | Critical | `test_runtime.py::test_spark_row_evaluator_orders_timezone_aware_timestamps` | Spark row evaluator orders timezone aware timestamps. | Local and Databricks |
| UT-077 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_rejects_ambiguous_temporal_comparisons` | Spark row evaluator rejects ambiguous temporal comparisons. | Local and Databricks |
| UT-078 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_rejects_numeric_tolerance_for_dates` | Spark row evaluator rejects numeric tolerance for dates. | Local and Databricks |
| UT-079 | In-memory runtime | Medium | `test_runtime.py::test_required_source_columns_returns_only_active_runtime_dependencies` | Reports active condition, custom-function, and assignment source fields. | Local and Databricks |
| UT-080 | In-memory runtime | Medium | `test_runtime.py::test_required_source_columns_can_return_no_dependencies` | Returns an empty tuple for literal-only rules and assignments. | Local and Databricks |
| UT-081 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_returns_native_winning_rule_trace` | Returns assignment and winning-rule trace payloads through the Spark row UDF. | Local and Databricks |
| UT-082 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_trace_keeps_default_options_null` | Leaves default condition options null in the winning-rule Spark trace. | Local and Databricks |
| UT-083 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_assignment_struct_includes_unassigned_fields_as_null` | Returns all assignment struct fields with nulls for fields not assigned. | Local and Databricks |
| UT-084 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_explanation_uses_any_joiner` | Uses OR when a winning root any group has multiple passed conditions. | Local and Databricks |
| UT-085 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_explanation_drops_failing_any_branches` | Omits failed OR branches from the winning-rule explanation. | Local and Databricks |
| UT-086 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_explanation_preserves_nested_groups` | Preserves parentheses and OR joiners for nested winning groups. | Local and Databricks |
| UT-087 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_explanation_drops_failing_nested_or_arm` | Omits a failed nested OR arm while preserving the passed nested path. | Local and Databricks |
| UT-088 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_explanation_matches_service_formatter` | Uses the same author-facing syntax as the service helper when all branches pass. | Local and Databricks |
| UT-089 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_preserves_mapping_literal_assignment_as_struct` | Preserves a mapping literal assignment as a nested struct payload. | Local and Databricks |
| UT-090 | In-memory runtime | Medium | `test_runtime.py::test_spark_assignment_schema_ignores_inactive_rules` | Infers assignment schema from active rules only. | Local and Databricks |
| UT-091 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_merges_assignments_when_stop_on_match_false` | Merges assignments from multiple matching rules when evaluation continues. | Local and Databricks |
| UT-092 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_no_match_returns_empty_audit_arrays` | No-match rows use empty summary/provenance arrays and null rule structs. | Local and Databricks |
| UT-093 | In-memory runtime | Medium | `test_runtime.py::test_rule_summaries_are_precomputed_once_per_row_evaluator` | Static human-readable rule descriptions are not rebuilt per row. | Local and Databricks |
| UT-094 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_rejects_lossy_assignment_coercion` | Typed assignment values fail instead of being silently truncated. | Local and Databricks |
| UT-095 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_stop_on_match_excludes_later_summaries_and_assignments` | stop_on_match prevents later rules from appearing in either audit array. | Local and Databricks |
| UT-096 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_builds_condition_traces_only_for_winner` | Builds condition trace objects only for the first matching rule. | Local and Databricks |
| UT-097 | In-memory runtime | High | `test_runtime.py::test_match_only_losing_rule_preserves_later_condition_errors` | Evaluates every condition in a losing group when a later one errors. | Local and Databricks |
| UT-098 | In-memory runtime | Medium | `test_runtime.py::test_match_only_and_traced_paths_agree_on_inactive_condition_groups` | Pins inactive conditions as false in both ALL and ANY groups. | Local and Databricks |
| UT-099 | In-memory runtime | High | `test_runtime.py::test_spark_assignment_schema_rejects_incompatible_same_target_assignments` | Rejects incompatible active assignments to one target. | Local and Databricks |
| UT-100 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_trace_includes_precomputed_aggregate_field` | Emits precomputed aggregate columns like ordinary field operands. | Local and Databricks |
| UT-101 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_winning_rule_trace_includes_custom_function_args` | Emits custom-function argument summaries in the winning-rule trace. | Local and Databricks |
| UT-102 | In-memory runtime | High | `test_runtime.py::test_trace_value_returns_common_scalars_without_json_serialization` | Returns primitive trace values without invoking the JSON encoder. | Local and Databricks |
| UT-103 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_like_uses_sql_wildcard_semantics` | Evaluates SQL LIKE percent wildcard behavior in the Spark row evaluator. | Local and Databricks |
| UT-104 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_null_result_default_controls_condition_result` | Evaluates null_result_mode=default on a null comparison result. | Local and Databricks |
| UT-105 | In-memory runtime | High | `test_runtime.py::test_spark_row_evaluator_rejects_non_boolean_null_default_at_runtime` | Direct callers cannot bypass the validator into Python truthiness. | Local and Databricks |
| UT-106 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_can_include_debug_traceback` | Spark row evaluator can include debug traceback. | Local and Databricks |
| UT-107 | In-memory runtime | Medium | `test_runtime.py::test_spark_row_evaluator_can_raise_during_materializing_action` | Spark row evaluator can raise during materializing action. | Local and Databricks |
| UT-108 | In-memory runtime | Medium | `test_runtime.py::test_assignment_changed_compares_spark_normalized_values` | Date audit changes reflect the stored value, not Python input classes. | Local and Databricks |
| UT-109 | In-memory runtime | High | `test_runtime.py::test_spark_runtime_preflights_custom_function_serialization` | Spark runtime preflights custom function serialization. | Local and Databricks |
| UT-110 | In-memory runtime | High | `test_runtime.py::test_spark_runtime_accepts_serializable_worker_evaluator` | Spark runtime accepts serializable worker evaluator. | Local and Databricks |
| UT-111 | Version serialization | High | `test_serializer.py::test_serializer_persists_canonical_payload_with_explicit_fields` | Serializes a ruleset and inspects explicit canonical payload fields. | Local and Databricks |
| UT-112 | Version serialization | High | `test_serializer.py::test_serializer_stamps_provenance_hash_and_summary_counts` | Serializes owner, lifecycle, hash, and payload summary metadata. | Local and Databricks |
| UT-113 | Version serialization | High | `test_serializer.py::test_serializer_counts_nested_custom_function_operands` | Counts custom functions nested inside custom-function arguments. | Local and Databricks |
| UT-114 | Version serialization | Critical | `test_serializer.py::test_content_hash_equals_sha256_of_payload_json` | Compares content_hash to SHA-256 of persisted payload_json bytes. | Local and Databricks |
| UT-115 | Version serialization | Critical | `test_serializer.py::test_content_hash_and_payload_json_are_deterministic` | Serializes the same ruleset twice and compares payload/hash output. | Local and Databricks |
| UT-116 | Version serialization | High | `test_serializer.py::test_deserializer_reconstructs_canonical_models` | Deserializes a persisted ruleset version back to canonical dataclasses. | Local and Databricks |
| UT-117 | Version serialization | Critical | `test_serializer.py::test_serializer_round_trips_exact_decimal_scalars_and_collections` | Persisted JSON keeps financial Decimals numeric and lossless. | Local and Databricks |
| UT-118 | Version serialization | High | `test_serializer.py::test_malformed_extended_json_envelopes_fail_uniformly` | Corrupt persisted values produce one diagnosable ValueError contract. | Local and Databricks |
| UT-119 | Version serialization | High | `test_serializer.py::test_canonical_json_rejects_nonfinite_float` | The persistence encoder never emits non-standard Infinity or NaN tokens. | Local and Databricks |
| UT-120 | Version serialization | High | `test_serializer.py::test_serializer_round_trips_temporal_and_python_collection_literals` | Extended JSON preserves supported values that plain JSON cannot encode. | Local and Databricks |
| UT-121 | Version serialization | High | `test_serializer.py::test_persisted_payload_excludes_lifecycle_status` | Confirms payload_json does not duplicate lifecycle status. | Local and Databricks |
| UT-122 | Version serialization | High | `test_serializer.py::test_serializer_accepts_explicit_effective_dates_outside_payload` | Persists effective dates as row metadata while keeping payload content canonical. | Local and Databricks |
| UT-123 | Version serialization | Critical | `test_serializer.py::test_serializer_defaults_effective_dates_from_publish_metadata` | Defaults the effective window when no explicit dates are supplied. | Local and Databricks |
| UT-124 | Service orchestration | Medium | `test_service.py::test_service_runtime_reuses_injected_compatibility_validator` | Publish and runtime schema checks share the configured validator. | Local and Databricks |
| UT-125 | Service orchestration | Medium | `test_service.py::test_service_from_schema_uses_standard_table_names` | Builds a service from schema and checks repository table names. | Local and Databricks |
| UT-126 | Service orchestration | Medium | `test_service.py::test_service_from_schema_accepts_custom_table_names` | Builds a service from schema while overriding metadata table names. | Local and Databricks |
| UT-127 | Service orchestration | Critical | `test_service.py::test_service_publish_yaml_text_and_loads_published_ruleset` | Publishes YAML through the facade and loads it back. | Local and Databricks |
| UT-128 | Service orchestration | High | `test_service.py::test_service_create_tables_save_standard_functions_and_retire` | Exercises table creation, standard function registry save, and retire facade calls. | Local and Databricks |
| UT-129 | Service orchestration | Medium | `test_service.py::test_service_saves_supplied_function_registry_rows` | Saves caller-supplied function registry rows through the facade. | Local and Databricks |
| UT-130 | Service orchestration | Medium | `test_service.py::test_service_can_preserve_standard_function_registry_when_requested` | Allows callers to preserve existing standard registry rows explicitly. | Local and Databricks |
| UT-131 | Service orchestration | Medium | `test_service.py::test_service_evaluate_dataframe_requires_ruleset_or_name` | Rejects evaluate calls without a ruleset or ruleset name. | Local and Databricks |
| UT-132 | Service orchestration | High | `test_service.py::test_service_passes_runtime_error_options_through` | Service passes runtime error options through. | Local and Databricks |
| UT-133 | Service orchestration | Medium | `test_service.py::test_service_describe_rules_formats_supplied_ruleset` | Formats compiled rule metadata into readable table-shaped rows. | Local and Databricks |
| UT-134 | Service orchestration | Critical | `test_service.py::test_service_describe_rules_loads_published_ruleset_and_formats_nested_logic` | Loads a published ruleset and renders nested condition groups. | Local and Databricks |
| UT-135 | Service orchestration | Medium | `test_service.py::test_service_describe_rules_requires_ruleset_or_name` | Rejects describe calls without a ruleset or ruleset name. | Local and Databricks |
| UT-136 | Service orchestration | Critical | `test_service.py::test_service_publish_accepts_compiled_ruleset` | Publishes an already compiled ruleset through the facade. | Local and Databricks |
| UT-137 | Service orchestration | Critical | `test_service.py::test_service_publish_and_retire_pass_effective_dates` | Passes effective date overrides through the facade. | Local and Databricks |
| UT-138 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_evaluates_row_rule` | Evaluates a row-level rule through Spark DataFrame runtime. | Databricks Spark |
| UT-139 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_builds_one_python_udf` | The runtime creates one UDF whose result struct feeds every output field. | Databricks Spark |
| UT-140 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_evaluates_precomputed_aggregate_field` | Evaluates a rule using an upstream aggregate column. | Databricks Spark |
| UT-141 | Spark runtime | High | `test_spark_runtime.py::test_spark_runtime_evaluates_and_assigns_standard_date_functions` | Date arithmetic remains typed through the real Spark UDF boundary. | Databricks Spark |
| UT-142 | Spark runtime | High | `test_spark_runtime.py::test_spark_runtime_serializes_only_required_literal_source_columns` | Evaluates a dotted source field while retaining an unrelated input column. | Databricks Spark |
| UT-143 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_evaluates_literal_only_rule_without_source_dependencies` | Evaluates a literal-only rule with an empty dependency set. | Databricks Spark |
| UT-144 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_applies_column_prefix_to_all_new_outputs` | Every additive audit output respects the configured column prefix. | Databricks Spark |
| UT-145 | Spark runtime | High | `test_spark_runtime.py::test_spark_runtime_validates_schema_before_building_udf` | An incompatible existing target fails before row evaluation. | Databricks Spark |
| UT-146 | Spark runtime | Medium | `test_spark_runtime.py::test_spark_runtime_preserves_mapping_literal_assignment_as_struct` | Emits mapping literal assignments as nested Spark structs. | Databricks Spark |
| UT-147 | Spark runtime | Critical | `test_spark_runtime.py::test_spark_runtime_preserves_decimal_and_array_assignments` | Financial values stay exact across the real Python UDF boundary. | Databricks Spark |
| UT-148 | Spark runtime | High | `test_spark_runtime.py::test_spark_runtime_quarantines_errors_without_failing_job` | Production quarantine mode retains good rows and marks bad rows. | Databricks Spark |
| UT-149 | Spark runtime | Critical | `test_spark_runtime.py::test_fail_on_error_remains_lazy_until_callers_action` | Building output does not hide a separate full-data validation action. | Databricks Spark |
| UT-150 | Spark runtime | Critical | `test_spark_runtime.py::test_spark_runtime_preserves_timestamp_assignment_type` | Timestamp assignment values survive the real worker serialization path. | Databricks Spark |
| UT-151 | Spark runtime | Critical | `test_spark_runtime.py::test_spark_runtime_preserves_timestamp_ntz_assignment_type` | TimestampNTZ survives schema inference and live worker serialization. | Databricks Spark |
| UT-152 | Spark runtime | Critical | `test_spark_runtime.py::test_audit_levels_emit_identity_and_only_requested_detail` | Every level is attributable while lighter levels omit expensive fields. | Databricks Spark |
| UT-153 | Spark runtime | High | `test_spark_runtime.py::test_coverage_report_finds_dead_broad_and_closest_rules` | Coverage aggregates matches and diagnoses clean no-match rows. | Databricks Spark |
| UT-154 | Spark validation | High | `test_spark_validator.py::test_spark_validator_allows_condition_null_result_error_for_udf_row_path` | Allows condition-level null_result_mode=error for ordinary row UDF checks. | Local and Databricks |
| UT-155 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_missing_condition_field` | Spark validator rejects missing condition field. | Local and Databricks |
| UT-156 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_missing_assignment_source_field` | Spark validator rejects missing assignment source field. | Local and Databricks |
| UT-157 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_incompatible_existing_target_type` | Spark validator rejects incompatible existing target type. | Local and Databricks |
| UT-158 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_incompatible_new_target_assignments` | Spark validator rejects incompatible new target assignments. | Local and Databricks |
| UT-159 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_existing_target_supplies_null_literal_type` | Spark validator existing target supplies null literal type. | Local and Databricks |
| UT-160 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_new_null_literal_requires_value_type` | Spark validator new null literal requires value type. | Local and Databricks |
| UT-161 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_new_field_operand_inherits_source_type` | Spark validator new field operand inherits source type. | Local and Databricks |
| UT-162 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_new_custom_assignment_requires_return_type_hint` | Spark validator new custom assignment requires return type hint. | Local and Databricks |
| UT-163 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_polymorphic_assignment_uses_existing_target_type` | Spark validator polymorphic assignment uses existing target type. | Local and Databricks |
| UT-164 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_polymorphic_assignment_requires_new_target_type` | Spark validator polymorphic assignment requires new target type. | Local and Databricks |
| UT-165 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_does_not_guess_condition_coercion_semantics` | Runtime-supported string-to-number comparisons are not rejected early. | Local and Databricks |
| UT-166 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_unifies_decimal_literals_without_float_fallback` | Spark validator unifies decimal literals without float fallback. | Local and Databricks |
| UT-167 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_rejects_lossy_decimal_scale_narrowing` | Spark validator rejects lossy decimal scale narrowing. | Local and Databricks |
| UT-168 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_accepts_safe_decimal_widening` | Spark validator accepts safe decimal widening. | Local and Databricks |
| UT-169 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_accepts_yaml_fraction_for_existing_decimal_target` | Spark validator accepts yaml fraction for existing decimal target. | Local and Databricks |
| UT-170 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_infers_decimal_for_new_yaml_fraction_target` | Spark validator infers decimal for new yaml fraction target. | Local and Databricks |
| UT-171 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_accepts_to_number_for_existing_decimal_target` | Spark validator accepts to number for existing decimal target. | Local and Databricks |
| UT-172 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_infers_decimal_for_new_to_number_target` | Spark validator infers decimal for new to number target. | Local and Databricks |
| UT-173 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_date_compared_with_quoted_string` | Spark validator rejects date compared with quoted string. | Local and Databricks |
| UT-174 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_scalar_string_membership_field` | IN/NOT_IN require collection-valued fields at schema preflight. | Local and Databricks |
| UT-175 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_allows_array_membership_field` | A Spark array remains a valid field-backed IN operand. | Local and Databricks |
| UT-176 | Spark validation | Medium | `test_spark_validator.py::test_mapping_literal_schema_order_is_stable_after_persistence` | Canonical struct field order does not depend on JSON mapping order. | Local and Databricks |
| UT-177 | Spark validation | Medium | `test_spark_validator.py::test_spark_validator_reports_unsupported_typed_null_precisely` | Spark validator reports unsupported typed null precisely. | Local and Databricks |
| UT-178 | Spark validation | High | `test_spark_validator.py::test_spark_validator_rejects_untyped_nulltype_assignment_source` | Spark validator rejects untyped nulltype assignment source. | Local and Databricks |
| UT-179 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_rejects_timestamp_representation_assignment_change` | Spark validator rejects timestamp representation assignment change. | Local and Databricks |
| UT-180 | Spark validation | Critical | `test_spark_validator.py::test_spark_validator_rejects_timestamp_representation_condition_change` | Spark validator rejects timestamp representation condition change. | Local and Databricks |
| UT-181 | Spark validation | Critical | `test_spark_validator.py::test_timestamp_ntz_literal_mismatch_explains_value_type_fix` | A bare datetime diagnostic names the supported NTZ authoring hint. | Local and Databricks |
| UT-182 | Spark validation | Critical | `test_spark_validator.py::test_timestamp_ntz_literal_hint_matches_ntz_field` | An explicit NTZ hint resolves the representation mismatch preflight. | Local and Databricks |
| UT-183 | Spark validation | Critical | `test_spark_validator.py::test_timestamp_ntz_collection_hint_matches_ntz_field` | The temporal hint applies to every normalized collection element. | Local and Databricks |
| UT-184 | Spark validation | Medium | `test_spark_validator.py::test_mixed_temporal_between_bounds_fail_preflight` | A date/timestamp bound pair cannot evade temporal validation. | Local and Databricks |
| UT-185 | Standard functions | Medium | `test_standard_functions.py::test_substring_uses_sql_style_start_position` | Verifies substring uses a 1-based start index. | Local and Databricks |
| UT-186 | Standard functions | High | `test_standard_functions.py::test_to_date_accepts_iso_date_values_and_propagates_nulls` | To date accepts iso date values and propagates nulls. | Local and Databricks |
| UT-187 | Standard functions | Critical | `test_standard_functions.py::test_to_date_uses_aware_timestamp_own_calendar_date` | To date uses aware timestamp own calendar date. | Local and Databricks |
| UT-188 | Standard functions | High | `test_standard_functions.py::test_to_date_rejects_ambiguous_or_invalid_values` | To date rejects ambiguous or invalid values. | Local and Databricks |
| UT-189 | Standard functions | High | `test_standard_functions.py::test_date_add_days_supports_positive_and_negative_offsets` | Date add days supports positive and negative offsets. | Local and Databricks |
| UT-190 | Standard functions | High | `test_standard_functions.py::test_date_add_days_reports_out_of_range_offsets` | Date add days reports out of range offsets. | Local and Databricks |
| UT-191 | Standard functions | High | `test_standard_functions.py::test_date_add_months_clamps_to_target_month_end` | Date add months clamps to target month end. | Local and Databricks |
| UT-192 | Standard functions | High | `test_standard_functions.py::test_date_add_years_clamps_leap_day_and_rejects_fractional_offsets` | Date add years clamps leap day and rejects fractional offsets. | Local and Databricks |
| UT-193 | Standard functions | High | `test_standard_functions.py::test_date_diff_and_month_boundaries_have_explicit_calendar_semantics` | Date diff and month boundaries have explicit calendar semantics. | Local and Databricks |
| UT-194 | Standard functions | Medium | `test_standard_functions.py::test_standard_functions_can_be_registered_for_runtime_field_args` | Registers standard functions and evaluates substring against row fields. | Local and Databricks |
| UT-195 | Standard functions | High | `test_standard_functions.py::test_standard_date_functions_work_in_conditions_and_typed_assignments` | Standard date functions work in conditions and typed assignments. | Local and Databricks |
| UT-196 | Standard functions | Medium | `test_standard_functions.py::test_standard_function_rows_expose_registry_metadata` | Creates persisted metadata rows for standard functions. | Local and Databricks |
| UT-197 | Ruleset validation | Medium | `test_validator.py::test_missing_owner_metadata_fails_validation` | Validates that owner and owner_department are required. | Local and Databricks |
| UT-198 | Ruleset validation | Medium | `test_validator.py::test_custom_function_args_mismatch_fails_validation` | Validates custom function argument names against the registry contract. | Local and Databricks |
| UT-199 | Ruleset validation | Medium | `test_validator.py::test_null_result_mode_default_without_default_fails_validation` | Validates that null_result_mode=default requires null_default_value. | Local and Databricks |
| UT-200 | Ruleset validation | High | `test_validator.py::test_null_result_mode_default_rejects_string_boolean` | Quoted YAML booleans must not become truthy defaults at runtime. | Local and Databricks |
| UT-201 | Ruleset validation | High | `test_validator.py::test_valid_string_operators_validate` | Validates a condition using a canonical string operator. | Local and Databricks |
| UT-202 | Ruleset validation | Medium | `test_validator.py::test_between_with_nonzero_tolerance_fails_validation` | Validates that between/not_between cannot use non-zero tolerance. | Local and Databricks |
| UT-203 | Ruleset validation | High | `test_validator.py::test_duplicate_condition_ids_fail_validation` | Validates that condition_id values are unique within a ruleset. | Local and Databricks |
| UT-204 | Ruleset validation | High | `test_validator.py::test_duplicate_condition_group_ids_fail_validation` | Validates that condition_group_id values are unique within a ruleset. | Local and Databricks |
| UT-205 | Ruleset validation | High | `test_validator.py::test_duplicate_assignment_target_within_rule_fails_validation` | One rule cannot use list order to resolve duplicate target fields. | Local and Databricks |
| UT-206 | Ruleset validation | High | `test_validator.py::test_duplicate_assignment_id_across_rules_fails_validation` | Assignment identity is unique across one ruleset version. | Local and Databricks |
| UT-207 | Ruleset validation | High | `test_validator.py::test_duplicate_assignment_id_within_rule_has_clear_location` | A same-rule duplicate does not claim two differently named rules. | Local and Databricks |
| UT-208 | Ruleset validation | High | `test_validator.py::test_assignment_id_may_be_reused_when_versions_are_validated_independently` | The uniqueness boundary does not leak across ruleset versions. | Local and Databricks |
| UT-209 | Ruleset validation | Critical | `test_validator.py::test_code_authored_nonfinite_decimal_literal_fails_validation` | Dataclass authoring cannot bypass the compiler's finite-number guard. | Local and Databricks |
| UT-210 | Ruleset validation | Medium | `test_validator.py::test_code_authored_nonfinite_tolerance_fails_validation_cleanly` | NaN tolerances produce a validation issue instead of Decimal failure. | Local and Databricks |
| UT-211 | Ruleset validation | Medium | `test_validator.py::test_code_authored_nonfinite_float_literal_fails_validation` | Dataclass authoring cannot bypass finite floating-point validation. | Local and Databricks |
| UT-212 | YAML compilation | High | `test_compiler_yaml.py::test_explicit_date_literal_normalizes_quoted_iso_text` | A date hint turns portable quoted ISO authoring text into a Python date. | Local and Databricks |

## Execution

Local/non-Spark suite:

```bash
python -m pytest tests -q -p no:cacheprovider
```

Full Databricks suite:

```bash
RULES_ENGINE_RUN_SPARK_TESTS=1 python -m pytest tests -q -p no:cacheprovider
```

Aggregate execution remains outside the package. Tests needing aggregate-like facts use upstream Spark columns such as `account_amount_sum`.
