import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


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
