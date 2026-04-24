from dataclasses import asdict, replace

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
from rules_engine.serializer import DeltaRowSerializer


def _ruleset(status="draft"):
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": status,
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


class RowBackedRepository(SparkDeltaRulesetRepository):
    def __init__(self, row):
        super().__init__(
            None,
            RulesEngineTableNames(
                ruleset_versions="ruleset_versions",
                function_registry="function_registry",
                ruleset_validation_logs="ruleset_validation_logs",
            ),
        )
        self._row = row

    def _ruleset_row_dict(self, ruleset_id, version):
        if self._row is None:
            return None
        if self._row["ruleset_id"] == ruleset_id and self._row["version"] == version:
            return self._row
        return None


def _row_for(status):
    row = DeltaRowSerializer().serialize_ruleset_version(_ruleset("draft"))
    return asdict(replace(row, status=status.value))


def test_load_draft_for_testing_loads_exact_draft_identity():
    """
    What: Loads a persisted draft by ruleset_id and version.
    Why: Authors need registry-backed drafts for test execution.
    Fails when: Draft metadata cannot be reconstructed through the repository.
    """
    repository = RowBackedRepository(_row_for(RulesetStatus.DRAFT))

    ruleset = repository.load_draft_for_testing("rs1", "1")

    assert ruleset.ruleset_id == "rs1"
    assert ruleset.version == "1"
    assert ruleset.status is RulesetStatus.DRAFT


def test_load_draft_for_testing_requires_exact_identity():
    """
    What: Rejects draft loads when the requested ruleset_id/version is absent.
    Why: Draft testing should never resolve latest-by-name or fall back implicitly.
    Fails when: Non-exact draft requests can load unrelated metadata.
    """
    repository = RowBackedRepository(_row_for(RulesetStatus.DRAFT))

    with pytest.raises(RepositoryError, match="Draft ruleset not found"):
        repository.load_draft_for_testing("rs1", "2")


def test_load_draft_for_testing_rejects_published_metadata():
    """
    What: Rejects published rows through the draft testing load path.
    Why: Draft and production load paths must remain explicit and separate.
    Fails when: The draft API can load non-draft lifecycle states.
    """
    repository = RowBackedRepository(_row_for(RulesetStatus.PUBLISHED))

    with pytest.raises(RepositoryError, match="not draft"):
        repository.load_draft_for_testing("rs1", "1")
