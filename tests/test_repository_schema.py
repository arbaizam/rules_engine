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


class FakeSpark:
    def __init__(self):
        self.created_frames = []
        self.queries = []

    def createDataFrame(self, data, schema=None):
        self.created_frames.append((data, schema))
        raise AssertionError("create_base_tables should use explicit DDL, not empty DataFrame writes")

    def sql(self, query):
        self.queries.append(query)


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
        "effective_start_date",
        "effective_end_date",
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


def test_create_base_tables_uses_explicit_delta_ddl_with_not_null_columns():
    """
    What: Creates metadata tables through SQL DDL with NOT NULL columns.
    Why: Empty Spark DataFrame writes may drop nullable=False metadata in the catalog.
    Fails when: Table bootstrap returns to DataFrame-based creation or omits required nullability.
    """
    spark = FakeSpark()
    repository = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    repository.create_base_tables(mode="overwrite")

    assert spark.created_frames == []
    assert any("DROP TABLE IF EXISTS ruleset_versions" in query for query in spark.queries)
    assert any("DROP TABLE IF EXISTS function_registry" in query for query in spark.queries)
    assert any("CREATE TABLE ruleset_versions" in query for query in spark.queries)
    assert any("CREATE TABLE function_registry" in query for query in spark.queries)
    assert any("ruleset_id STRING NOT NULL" in query for query in spark.queries)
    assert any("effective_start_date STRING NOT NULL" in query for query in spark.queries)
    assert any("effective_end_date STRING NOT NULL" in query for query in spark.queries)
    assert any("payload_json STRING NOT NULL" in query for query in spark.queries)
    assert any("function_name STRING NOT NULL" in query for query in spark.queries)
    assert any("active_flag BOOLEAN NOT NULL" in query for query in spark.queries)
    assert all("USING DELTA" in query for query in spark.queries if "CREATE TABLE" in query)


def test_create_base_tables_uses_if_not_exists_for_ignore_mode():
    """
    What: Uses CREATE TABLE IF NOT EXISTS for idempotent bootstrap.
    Why: Deployment setup should be safely rerunnable when requested.
    Fails when: Ignore mode emits destructive or non-idempotent DDL.
    """
    spark = FakeSpark()
    repository = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    repository.create_base_tables(mode="ignore")

    assert not any("DROP TABLE" in query for query in spark.queries)
    assert any("CREATE TABLE IF NOT EXISTS ruleset_versions" in query for query in spark.queries)
    assert any("CREATE TABLE IF NOT EXISTS function_registry" in query for query in spark.queries)
