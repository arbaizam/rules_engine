from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


def _repository():
    return SparkDeltaRulesetRepository(
        None,
        RulesEngineTableNames(
            rulesets="rulesets",
            rules="rules",
            condition_groups="condition_groups",
            conditions="conditions",
            assignments="assignments",
            function_registry="function_registry",
            validation_results="validation_results",
        ),
    )


def _field_names(schema):
    return {field.name for field in schema.fields}


def test_ruleset_schema_contains_provenance_and_hash_fields():
    fields = _field_names(_repository().ruleset_schema)

    assert {
        "created_by",
        "created_at",
        "published_by",
        "published_at",
        "content_hash",
    } <= fields


def test_child_metadata_schemas_contain_created_provenance():
    repository = _repository()

    for schema in [
        repository.rule_schema,
        repository.condition_group_schema,
        repository.condition_schema,
        repository.assignment_schema,
    ]:
        assert {"created_by", "created_at"} <= _field_names(schema)


def test_validation_result_schema_contains_run_at():
    assert "run_at" in _field_names(_repository().validation_result_schema)
