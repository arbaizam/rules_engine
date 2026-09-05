import re
from dataclasses import fields

import pytest

from rules_engine.exceptions import RepositoryError
from rules_engine.models import FunctionRegistryRow
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
        raise AssertionError(
            "create_base_tables should use explicit DDL, not empty DataFrame writes"
        )

    def sql(self, query):
        self.queries.append(query)


@pytest.mark.parametrize("mode", [None, "error", "errorifexists", "ERROR", "ignore", "overwrite"])
def test_bootstrap_ddl_matches_every_struct_field_and_mode(mode):
    """Emitted DDL preserves every schema field, type and nullability in each supported mode."""
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark, RulesEngineTableNames("ruleset_versions", "function_registry"),
    )
    if mode is None:
        repo.create_base_tables()
    else:
        repo.create_base_tables(mode=mode)

    creates = [query for query in spark.queries if "CREATE TABLE" in query]
    assert len(creates) == 2
    assert len(spark.queries) == (4 if mode == "overwrite" else 2)
    for query, name, schema in zip(
        creates,
        ("ruleset_versions", "function_registry"),
        (repo.ruleset_version_schema, repo.function_registry_schema),
        strict=True,
    ):
        match = re.fullmatch(
            r"\s*CREATE TABLE (IF NOT EXISTS )?`(\w+)`\s*\((.*?)\)\s*USING DELTA\s*",
            query, flags=re.DOTALL,
        )
        assert match is not None
        assert match[1] == ("IF NOT EXISTS " if mode == "ignore" else None)
        assert match[2] == name
        actual = []
        for declaration in match[3].split(","):
            tokens = declaration.split()
            assert tokens[2:] in ([], ["NOT", "NULL"])
            actual.append((tokens[0], tokens[1].lower(), tokens[2:] == []))
        assert actual == [
            (field.name, field.dataType.simpleString(), field.nullable)
            for field in schema.fields
        ]


@pytest.mark.parametrize("mode", ["append", "", "replace"])
def test_bootstrap_rejects_invalid_modes_before_any_sql(mode):
    """Unknown write modes fail without creating or dropping metadata tables."""
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark, RulesEngineTableNames("ruleset_versions", "function_registry"),
    )
    with pytest.raises(RepositoryError, match="Base table creation mode"):
        repo.create_base_tables(mode=mode)
    assert spark.queries == []
    assert spark.created_frames == []


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
        "custom_function_count",
        "owner",
        "owner_department",
        "published_by",
        "published_at",
        "retired_by",
        "retired_at",
    } <= fields


def test_function_registry_model_and_table_field_names_stay_aligned():
    """Registry model renames cannot silently drift from the Delta schema."""
    model_fields = {item.name for item in fields(FunctionRegistryRow)}
    model_fields.remove("arg_contract_payload")
    model_fields.add("arg_contract_payload_json")

    assert model_fields == _field_names(_repository().function_registry_schema)


def test_table_names_can_be_built_from_schema():
    """
    What: Builds the standard two-table registry footprint from one schema name.
    Why: Production pipeline configuration should not repeat fixed table names.
    Fails when: The schema helper stops producing the agreed registry table names.
    """
    table_names = RulesEngineTableNames.from_schema("catalog.schema")

    assert table_names.ruleset_versions == "catalog.schema.ruleset_versions"
    assert table_names.function_registry == "catalog.schema.function_registry"


@pytest.mark.parametrize(
    "table_name",
    ["catalog.schema.table;DROP_TABLE", "catalog..table", "four.part.table.name"],
)
def test_table_names_reject_unsafe_spark_identifiers(table_name):
    """Operator-provided table names cannot escape into generated Spark SQL."""
    with pytest.raises(RepositoryError, match="safe one-, two-, or three-part"):
        RulesEngineTableNames(table_name, "function_registry")


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
    assert any("DROP TABLE IF EXISTS `ruleset_versions`" in query for query in spark.queries)
    assert any("DROP TABLE IF EXISTS `function_registry`" in query for query in spark.queries)
    assert any("CREATE TABLE `ruleset_versions`" in query for query in spark.queries)
    assert any("CREATE TABLE `function_registry`" in query for query in spark.queries)
    assert any("ruleset_id STRING NOT NULL" in query for query in spark.queries)
    assert any("payload_json STRING NOT NULL" in query for query in spark.queries)
    assert any("function_name STRING NOT NULL" in query for query in spark.queries)
    assert any("active_flag BOOLEAN NOT NULL" in query for query in spark.queries)
    assert all("USING DELTA" in query for query in spark.queries if "CREATE TABLE" in query)


def test_create_base_tables_uses_if_not_exists_for_ignore_mode():
    """
    What: Uses CREATE TABLE IF NOT EXISTS for idempotent bootstrap.
    Why: Metadata initialization should be safely rerunnable when requested.
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
    assert any(
        "CREATE TABLE IF NOT EXISTS `ruleset_versions`" in query for query in spark.queries
    )
    assert any(
        "CREATE TABLE IF NOT EXISTS `function_registry`" in query for query in spark.queries
    )
