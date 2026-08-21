from types import SimpleNamespace

import pytest

import rules_engine.repository as repository_module
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
from rules_engine.standard_functions import standard_function_rows


class FakeCatalog:
    def __init__(self):
        self.dropped_views = []

    def tableExists(self, table_name):
        return True

    def dropTempView(self, view_name):
        self.dropped_views.append(view_name)


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows
        self.view_name = None

    def createOrReplaceTempView(self, view_name):
        self.view_name = view_name


class FakeSpark:
    def __init__(self):
        self.catalog = FakeCatalog()
        self.created_frames = []
        self.queries = []

    def createDataFrame(self, data, schema=None):
        frame = FakeDataFrame(data)
        frame.schema = schema
        self.created_frames.append(frame)
        return frame

    def sql(self, query):
        self.queries.append(query)


class FakePredicate:
    def __eq__(self, other):
        return self

    def __and__(self, other):
        return self


class FakeLoadDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def where(self, predicate):
        return self

    def limit(self, count):
        assert count == 2
        return self

    def collect(self):
        return self.rows


class FakeLoadSpark:
    def __init__(self, rows):
        self.catalog = FakeCatalog()
        self.frame = FakeLoadDataFrame(rows)
        self.queries = []

    def table(self, table_name):
        assert table_name == "ruleset_versions"
        return self.frame

    def sql(self, query):
        self.queries.append(query)


def _repository():
    return SparkDeltaRulesetRepository(
        None,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )


def _ruleset(*, ruleset_id="rs1", ruleset_name="Ruleset", version="1"):
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": ruleset_id,
            "ruleset_name": ruleset_name,
            "version": version,
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )


def test_save_published_rejects_duplicate_ruleset_name_and_version():
    """
    What: Rejects duplicate caller-facing ruleset identities.
    Why: Name-based loading must resolve one immutable version.
    Fails when: A second ID can claim an existing name/version.
    """
    repo = _repository()
    repo._ruleset_row_dict = lambda ruleset_id, version: None
    repo._ruleset_row_dict_by_name_version = lambda ruleset_name, version: {
        "status": "published"
    }

    with pytest.raises(RepositoryError, match="ruleset_name=Ruleset, version=1"):
        repo.save_published(
            _ruleset(
                ruleset_id="generated-id",
                ruleset_name="Ruleset",
                version="1",
            )
        )



def test_save_published_rejects_duplicate_ruleset_id_and_version():
    """Stable ruleset IDs cannot identify more than one row per version."""
    repo = _repository()
    repo._ruleset_row_dict = lambda ruleset_id, version: {"status": "published"}
    repo._ruleset_row_dict_by_name_version = lambda ruleset_name, version: None

    with pytest.raises(RepositoryError, match="ruleset_id=rs1, version=1"):
        repo.save_published(
            _ruleset(
                ruleset_id="rs1",
                ruleset_name="Different name",
                version="1",
            )
        )


def test_save_published_allows_distinct_versions_for_same_ruleset_name():
    """
    What: Allows two published versions with the same ruleset_name when their versions differ.
    Why: Candidate rulesets need to be published side by side for testing.
    Fails when: The repository still enforces a single published sibling per ruleset_name.
    """
    repo = _repository()
    saved_versions = []
    repo._ruleset_row_dict = lambda ruleset_id, version: None
    repo._ruleset_row_dict_by_name_version = lambda ruleset_name, version: None
    repo._write_rows = lambda table_name, rows, schema: saved_versions.extend(
        row["version"] for row in rows
    )

    repo.save_published(_ruleset(ruleset_name="Ruleset", version="2"))

    assert saved_versions == ["2"]


def test_load_published_rejects_duplicate_rows_for_explicit_version(monkeypatch):
    """Pinned loads fail loudly when the immutable version key is not unique."""
    monkeypatch.setattr(
        repository_module,
        "F",
        SimpleNamespace(col=lambda name: FakePredicate()),
    )
    repo = SparkDeltaRulesetRepository(
        FakeLoadSpark([object(), object()]),
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    with pytest.raises(
        RepositoryError,
        match=r"immutable ruleset version: ruleset_name=Ruleset, version=1",
    ):
        repo.load_published("Ruleset", version="1")


def test_retire_rejects_duplicate_stable_identity_before_update(monkeypatch):
    """Retirement cannot update several rows sharing one stable identity."""
    monkeypatch.setattr(
        repository_module,
        "F",
        SimpleNamespace(col=lambda name: FakePredicate()),
    )
    spark = FakeLoadSpark([object(), object()])
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )
    with pytest.raises(
        RepositoryError,
        match=r"ruleset_id=rs1, version=1",
    ):
        repo.retire("rs1", "1")

    assert spark.queries == []


def test_retire_records_lifecycle_state_and_actor():
    """
    What: Retirement updates lifecycle status and audit fields.
    Why: Retired versions should retain who retired them and when.
    Fails when: Retirement omits lifecycle audit metadata.
    """
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )
    rows = iter([{"status": "published"}, {"status": "retired"}])
    repo._ruleset_row_dict = lambda ruleset_id, version: next(rows)
    repo._utc_now = lambda: "2026-04-30T23:59:59+00:00"

    repo.retire("rs1", "1", retired_by="engineer")

    update_sql = "\n".join(spark.queries)
    assert "status = 'retired'" in update_sql
    assert "retired_by = 'engineer'" in update_sql
    assert "retired_at = '2026-04-30T23:59:59+00:00'" in update_sql


def test_retire_rejects_already_retired_version():
    """
    What: Rejects a second retirement for an already retired version.
    Why: Retirement audit fields should preserve the original retirement event.
    Fails when: A second retirement silently overwrites retirement audit fields.
    """
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )
    repo._ruleset_row_dict = lambda ruleset_id, version: {"status": "retired"}

    with pytest.raises(RepositoryError, match="already retired"):
        repo.retire("rs1", "1", retired_by="engineer")

    assert spark.queries == []


def test_save_function_registry_rows_skips_existing_rows_when_update_disabled():
    """
    What: Emits an insert-only merge when update_existing is disabled.
    Why: Callers may intentionally preserve existing function metadata.
    Fails when: Existing function registry rows are overwritten in insert-only mode.
    """
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    repo.save_function_registry_rows(
        standard_function_rows()[:1],
        update_existing=False,
    )

    merge_sql = "\n".join(spark.queries)
    assert "MERGE INTO function_registry" in merge_sql
    assert "WHEN MATCHED THEN UPDATE" not in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert spark.catalog.dropped_views


def test_save_function_registry_rows_upserts_by_default():
    """
    What: Keeps custom function registry saves as upserts by default.
    Why: Existing custom registry maintenance workflows may rely on replacement semantics.
    Fails when: The default save path stops updating matched function rows.
    """
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    repo.save_function_registry_rows(standard_function_rows()[:1])

    merge_sql = "\n".join(spark.queries)
    assert "WHEN MATCHED THEN UPDATE" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
