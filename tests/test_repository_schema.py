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
    """
    What: Verifies the ruleset version schema exposes payload, metadata, and hash columns.
    Why: These columns are the queryable contract for persisted ruleset metadata.
    Fails when: Schema refactors drop or rename required top-level fields.
    """
    fields = _field_names(_repository().ruleset_version_schema)

    assert {
        "payload_json",
        "payload_metadata",
        "user_metadata",
        "content_hash",
    } <= fields


def test_ruleset_version_schema_contains_summary_counts():
    """
    What: Verifies payload_metadata contains all summary count fields.
    Why: Count metadata supports lightweight governance queries without parsing payload JSON.
    Fails when: Payload metadata struct shape no longer matches serializer output.
    """
    payload_metadata = _repository().ruleset_version_schema["payload_metadata"].dataType
    fields = _field_names(payload_metadata)

    assert {
        "rule_count",
        "condition_count",
        "assignment_count",
        "aggregate_count",
        "custom_function_count",
    } <= fields


def test_ruleset_version_schema_contains_user_metadata_fields():
    """
    What: Verifies user_metadata contains owner and lifecycle actor fields.
    Why: Audit workflows need ownership and lifecycle provenance in one nested column.
    Fails when: User metadata struct drops owner or lifecycle audit fields.
    """
    user_metadata = _repository().ruleset_version_schema["user_metadata"].dataType
    fields = _field_names(user_metadata)

    assert {
        "owner",
        "owner_department",
        "created_by",
        "created_at",
        "published_by",
        "published_at",
        "retired_by",
        "retired_at",
    } <= fields
