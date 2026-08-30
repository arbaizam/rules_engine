from types import SimpleNamespace

import pytest

import rules_engine.repository as repository_module
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
from rules_engine.standard_functions import standard_function_rows


class FakeCatalog:
    def __init__(self, *, tables_exist=True):
        self.dropped_views = []
        self.tables_exist = tables_exist

    def tableExists(self, table_name):
        return self.tables_exist

    def dropTempView(self, view_name):
        self.dropped_views.append(view_name)


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows
        self.view_name = None
        self.write = FakeDataFrameWriter()

    def createOrReplaceTempView(self, view_name):
        self.view_name = view_name


class FakeDataFrameWriter:
    def __init__(self):
        self.format_name = None
        self.mode_name = None
        self.saved_table = None

    def format(self, format_name):
        self.format_name = format_name
        return self

    def mode(self, mode_name):
        self.mode_name = mode_name
        return self

    def saveAsTable(self, table_name):
        self.saved_table = table_name


class FakeSpark:
    def __init__(self, *, tables_exist=True):
        self.catalog = FakeCatalog(tables_exist=tables_exist)
        self.created_frames = []
        self.queries = []
        self.sql_calls = []

    def createDataFrame(self, data, schema=None):
        frame = FakeDataFrame(data)
        frame.schema = schema
        self.created_frames.append(frame)
        return frame

    def sql(self, query, args=None):
        self.queries.append(query)
        self.sql_calls.append((query, args))


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
    repo._ruleset_row_dict_by_name_version = lambda ruleset_name, version: {"status": "published"}

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
    Fails when: The repository enforces a single published sibling per ruleset_name.
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


def test_save_published_requires_explicit_table_creation():
    """Publication cannot create the ruleset metadata table as a write side effect."""
    spark = FakeSpark(tables_exist=False)
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    with pytest.raises(RepositoryError, match="metadata table does not exist"):
        repo.save_published(_ruleset())

    assert spark.created_frames == []


def test_load_published_requires_explicit_table_creation():
    """Loading a missing metadata table fails through the repository contract."""
    spark = FakeSpark(tables_exist=False)
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    with pytest.raises(RepositoryError, match="metadata table does not exist"):
        repo.load_published("Ruleset", version="1")

    assert spark.created_frames == []


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
    rows = iter(
        [
            {"status": "published"},
            {
                "status": "retired",
                "retired_by": "engineer",
                "retired_at": "2026-04-30T23:59:59+00:00",
            },
        ]
    )
    repo._ruleset_row_dict = lambda ruleset_id, version: next(rows)
    repo._utc_now = lambda: "2026-04-30T23:59:59+00:00"

    repo.retire("rs1", "1", retired_by="engineer")

    update_sql, args = spark.sql_calls[0]
    assert "UPDATE `ruleset_versions`" in update_sql
    assert "status = :status" in update_sql
    assert "retired_by = :retired_by" in update_sql
    assert "retired_at = :retired_at" in update_sql
    assert "AND status = :published_status" in update_sql
    assert args == {
        "status": "retired",
        "retired_by": "engineer",
        "retired_at": "2026-04-30T23:59:59+00:00",
        "ruleset_id": "rs1",
        "version": "1",
        "published_status": "published",
    }


def test_retire_binds_backslashes_and_quotes_as_sql_parameters():
    """Authored values remain data even when Spark string-literal escaping would be unsafe."""
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )
    ruleset_id = "rs\\' OR true --"
    version = "1\\"
    retired_by = "O'Brien\\"
    rows = iter(
        [
            {"status": "published"},
            {
                "status": "retired",
                "retired_by": retired_by,
                "retired_at": "2026-04-30T23:59:59+00:00",
            },
        ]
    )
    repo._ruleset_row_dict = lambda actual_id, actual_version: next(rows)
    repo._utc_now = lambda: "2026-04-30T23:59:59+00:00"

    repo.retire(ruleset_id, version, retired_by=retired_by)

    update_sql, args = spark.sql_calls[0]
    assert ruleset_id not in update_sql
    assert version not in update_sql
    assert retired_by not in update_sql
    assert args["ruleset_id"] == ruleset_id
    assert args["version"] == version
    assert args["retired_by"] == retired_by


def test_retire_does_not_overwrite_a_concurrent_retirement():
    """A caller that loses the published-to-retired race fails without replacing audit data."""
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )
    rows = iter(
        [
            {"status": "published"},
            {
                "status": "retired",
                "retired_by": "first-engineer",
                "retired_at": "2026-04-30T23:59:58+00:00",
            },
        ]
    )
    repo._ruleset_row_dict = lambda ruleset_id, version: next(rows)
    repo._utc_now = lambda: "2026-04-30T23:59:59+00:00"

    with pytest.raises(RepositoryError, match="Retirement failed"):
        repo.retire("rs1", "1", retired_by="second-engineer")

    update_sql, args = spark.sql_calls[0]
    assert "AND status = :published_status" in update_sql
    assert args["published_status"] == "published"


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
    assert "MERGE INTO `function_registry`" in merge_sql
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


def test_save_function_registry_rows_requires_explicit_table_creation():
    """Registry persistence cannot create its Delta table as a write side effect."""
    spark = FakeSpark(tables_exist=False)
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    with pytest.raises(RepositoryError, match="metadata table does not exist"):
        repo.save_function_registry_rows(standard_function_rows()[:1])

    assert spark.created_frames == []


def test_write_rows_appends_by_name_only_after_table_existence_check():
    """Existing metadata writes align columns by name and cannot create on a failed check."""
    spark = FakeSpark()
    repo = SparkDeltaRulesetRepository(
        spark,
        RulesEngineTableNames(
            ruleset_versions="ruleset_versions",
            function_registry="function_registry",
        ),
    )

    repo._write_rows("ruleset_versions", [{"ruleset_id": "rs1"}], repo.ruleset_version_schema)

    writer = spark.created_frames[0].write
    assert writer.format_name == "delta"
    assert writer.mode_name == "append"
    assert writer.saved_table == "ruleset_versions"
