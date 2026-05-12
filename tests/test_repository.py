import pytest

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
            "status": "published",
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
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )


def test_save_published_checks_duplicate_ruleset_name_and_version():
    """
    What: Uses ruleset_name/version as the duplicate publish boundary.
    Why: Testing workflows may publish multiple ruleset IDs under the same user-facing ruleset/version identity.
    Fails when: The repository checks ruleset_id/version instead of ruleset_name/version.
    """
    repo = _repository()
    checked = []
    repo._existing_ruleset_status = lambda ruleset_name, version: checked.append(
        (ruleset_name, version)
    ) or "published"

    with pytest.raises(RepositoryError, match="ruleset_name=Ruleset, version=1"):
        repo.save_published(
            _ruleset(
                ruleset_id="generated-id",
                ruleset_name="Ruleset",
                version="1",
            )
        )

    assert checked == [("Ruleset", "1")]


def test_save_published_allows_distinct_versions_for_same_ruleset_name():
    """
    What: Allows two published versions with the same ruleset_name when their versions differ.
    Why: Candidate rulesets need to be published side by side for testing.
    Fails when: The repository still enforces a single published sibling per ruleset_name.
    """
    repo = _repository()
    saved_versions = []
    existing_versions = {("Ruleset", "1")}
    repo._existing_ruleset_status = lambda ruleset_name, version: (
        "published" if (ruleset_name, version) in existing_versions else None
    )
    repo._write_rows = lambda table_name, rows, schema: saved_versions.extend(
        row["version"] for row in rows
    )

    repo.save_published(_ruleset(ruleset_name="Ruleset", version="2"))

    assert saved_versions == ["2"]


def test_save_published_persists_effective_dates():
    """
    What: Persists explicit effective dates on the ruleset version row.
    Why: Effective dating is queryable lifecycle metadata for published versions.
    Fails when: Publish drops effective-date overrides before serialization.
    """
    repo = _repository()
    saved_rows = []
    repo._existing_ruleset_status = lambda ruleset_name, version: None
    repo._utc_now = lambda: "2026-04-26T12:00:00+00:00"
    repo._write_rows = lambda table_name, rows, schema: saved_rows.extend(rows)

    repo.save_published(
        _ruleset(ruleset_name="Ruleset", version="2"),
        effective_start_date="2026-05-01",
        effective_end_date="2026-12-31",
    )

    assert saved_rows[0]["effective_start_date"] == "2026-05-01"
    assert saved_rows[0]["effective_end_date"] == "2026-12-31"


def test_retire_closes_effective_window():
    """
    What: Retirement updates lifecycle status and effective_end_date.
    Why: Retired versions should retain the date their effective window closed.
    Fails when: Retirement leaves effective_end_date open-ended.
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
    assert "effective_end_date = '2026-04-30'" in update_sql


def test_retire_allows_explicit_effective_end_date():
    """
    What: Allows retirement callers to close the effective window explicitly.
    Why: Backdated or end-of-business retirements may differ from the audit timestamp.
    Fails when: Explicit retirement effective_end_date is ignored.
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

    repo.retire("rs1", "1", effective_end_date="2026-04-25")

    assert "effective_end_date = '2026-04-25'" in "\n".join(spark.queries)


def test_save_function_registry_rows_skips_existing_rows_when_update_disabled():
    """
    What: Emits an insert-only merge when update_existing is disabled.
    Why: Deployment setup should load standard functions only when missing.
    Fails when: Existing function registry rows are overwritten during setup.
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
