from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


def _repository():
    return SparkDeltaRulesetRepository(
        None,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )


def _field_names(schema):
    return {field.name for field in schema.fields}


def test_ruleset_version_schema_contains_payload_provenance_and_hash_fields():
    fields = _field_names(_repository().ruleset_version_schema)

    assert {
        "payload_json",
        "created_by",
        "created_at",
        "published_by",
        "published_at",
        "retired_by",
        "retired_at",
        "content_hash",
    } <= fields


def test_ruleset_version_schema_contains_summary_counts():
    fields = _field_names(_repository().ruleset_version_schema)

    assert {
        "rule_count",
        "condition_count",
        "assignment_count",
        "aggregate_count",
        "custom_function_count",
    } <= fields
