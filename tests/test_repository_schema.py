from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


def _repository():
    return SparkDeltaRulesetRepository(
        None,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
            ruleset_validation_logs="ruleset_validation_logs",
        ),
    )


def _field_names(schema):
    return {field.name for field in schema.fields}


def test_ruleset_version_schema_contains_payload_provenance_and_hash_fields():
    """
    What: Verifies the ruleset version schema exposes payload, count, provenance, and hash columns.
    Why: These columns are the queryable contract for persisted ruleset metadata.
    Fails when: Schema refactors drop or rename required top-level fields.
    """
    fields = _field_names(_repository().ruleset_version_schema)

    assert {
        "payload_json",
        "content_hash",
        "rule_count",
        "condition_count",
        "assignment_count",
        "aggregate_count",
        "custom_function_count",
        "owner",
        "owner_department",
        "published_by",
        "published_at",
        "retired_by",
        "retired_at",
    } <= fields


def test_table_names_can_be_built_from_schema():
    """
    What: Builds the standard three-table registry footprint from one schema name.
    Why: Production pipeline configuration should not repeat fixed table names.
    Fails when: The schema helper stops producing the agreed registry table names.
    """
    table_names = RulesEngineTableNames.from_schema("catalog.schema")

    assert table_names.ruleset_versions == "catalog.schema.ruleset_versions"
    assert table_names.function_registry == "catalog.schema.function_registry"
    assert table_names.ruleset_validation_logs == "catalog.schema.ruleset_validation_logs"


def test_ruleset_validation_log_schema_contains_publish_pipeline_fields():
    """
    What: Verifies the repository owns the validation/publish log schema.
    Why: The log table is part of the standard rules engine registry footprint.
    Fails when: Dashboard or pipeline audit fields are dropped from the schema.
    """
    fields = _field_names(_repository().ruleset_validation_log_schema)

    assert {
        "pipeline_run_id",
        "event_time",
        "operation",
        "status",
        "reason",
        "ruleset_id",
        "ruleset_name",
        "version",
        "content_hash",
        "source_yaml_path",
        "canonical_yaml_path",
        "original_yaml_archive_path",
        "published_by",
        "retire_existing_published",
        "require_newer_version",
        "retired_ruleset_id",
        "retired_version",
        "validation_issue_count",
        "validation_issues_json",
        "error_message",
        "error_traceback",
    } <= fields


def test_ruleset_validation_log_schema_allows_partial_failure_rows():
    """
    What: Verifies validation log fields are nullable for failure-path logging.
    Why: Pipeline failure logs may be written before a ruleset is parsed.
    Fails when: Spark rejects failure logs because optional context is missing.
    """
    schema = _repository().ruleset_validation_log_schema

    assert all(field.nullable for field in schema.fields)
