import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.publish import PublishService
from rules_engine.registry import FunctionRegistry
from rules_engine.validator import RulesetValidator


class RecordingRepository:
    def save_published(self, ruleset, *, published_by=None):
        self.saved = ruleset
        self.published_by = published_by

    def retire(self, ruleset_id, version, *, retired_by=None):
        raise NotImplementedError

    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


def _ruleset(status):
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


def _service():
    return PublishService(
        repository=RecordingRepository(),
        validator=RulesetValidator(FunctionRegistry()),
        normalizer=RulesetNormalizer(),
    )


def test_publish_requires_published_status():
    """
    What: Rejects publish when the incoming ruleset status is not published.
    Why: The lifecycle now supports only published and retired persisted states.
    Fails when: Retired models can be written through the publish path.
    """
    with pytest.raises(ValidationFailedError, match="status=published"):
        _service().publish(_ruleset("retired"), published_by="tester")


def test_publish_passes_provenance_to_repository():
    """
    What: Passes published_by during direct publication.
    Why: Publication writes the publication actor in one step.
    Fails when: The actor value is lost across the service/repository boundary.
    """
    service = _service()

    service.publish(_ruleset("published"), published_by="approver")

    assert service._repository.published_by == "approver"


def test_publish_allows_omitted_provenance():
    """
    What: Allows publish callers to omit actor metadata.
    Why: Repository defaults omitted actor values for locked-down production jobs.
    Fails when: Optional provenance becomes mandatory at service level.
    """
    service = _service()

    service.publish(_ruleset("published"))

    assert service._repository.published_by is None
