import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.publish import PublishService
from rules_engine.registry import FunctionRegistry
from rules_engine.validator import RulesetValidator


class RecordingRepository:
    def save_draft(self, ruleset, *, created_by=None):
        self.saved = ruleset
        self.created_by = created_by

    def publish(self, ruleset_id, version, *, published_by=None):
        self.published = (ruleset_id, version)
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


def test_save_draft_requires_draft_status():
    """
    What: Rejects save_draft when the ruleset status is not draft.
    Why: Draft saves must not overwrite published or retired lifecycle states.
    Fails when: Non-draft rulesets can be persisted through the draft path.
    """
    with pytest.raises(ValidationFailedError, match="status=draft"):
        _service().save_draft(_ruleset("published"), created_by="tester")


def test_publish_requires_draft_status():
    """
    What: Rejects publish when the incoming ruleset status is not draft.
    Why: Publish performs draft save then lifecycle promotion from a draft model.
    Fails when: Already-published or retired models can bypass lifecycle guards.
    """
    with pytest.raises(ValidationFailedError, match="status=draft"):
        _service().publish(_ruleset("published"), created_by="tester", published_by="tester")


def test_save_draft_passes_created_by_to_repository():
    """
    What: Passes created_by from PublishService.save_draft to the repository.
    Why: Draft author provenance is assigned at persistence time.
    Fails when: Save-draft provenance is dropped before repository persistence.
    """
    service = _service()

    service.save_draft(_ruleset("draft"), created_by="tester")

    assert service._repository.created_by == "tester"


def test_publish_passes_provenance_to_repository():
    """
    What: Passes created_by and published_by during publish.
    Why: Publish writes both draft provenance and publication approval metadata.
    Fails when: Either actor value is lost across the service/repository boundary.
    """
    service = _service()

    service.publish(_ruleset("draft"), created_by="author", published_by="approver")

    assert service._repository.created_by == "author"
    assert service._repository.published_by == "approver"


def test_publish_allows_omitted_provenance():
    """
    What: Allows publish callers to omit actor metadata.
    Why: Repository defaults omitted actor values for locked-down production jobs.
    Fails when: Optional provenance becomes mandatory at service level.
    """
    service = _service()

    service.publish(_ruleset("draft"))

    assert service._repository.created_by is None
    assert service._repository.published_by is None
