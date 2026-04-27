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
    What: Builds the standard two-table registry footprint from one schema name.
    Why: Production pipeline configuration should not repeat fixed table names.
    Fails when: The schema helper stops producing the agreed registry table names.
    """
    table_names = RulesEngineTableNames.from_schema("catalog.schema")

    assert table_names.ruleset_versions == "catalog.schema.ruleset_versions"
    assert table_names.function_registry == "catalog.schema.function_registry"
