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
    with pytest.raises(ValidationFailedError, match="status=draft"):
        _service().save_draft(_ruleset("published"), created_by="tester")


def test_publish_requires_draft_status():
    with pytest.raises(ValidationFailedError, match="status=draft"):
        _service().publish(_ruleset("published"), created_by="tester", published_by="tester")


def test_save_draft_passes_created_by_to_repository():
    service = _service()

    service.save_draft(_ruleset("draft"), created_by="tester")

    assert service._repository.created_by == "tester"


def test_publish_passes_provenance_to_repository():
    service = _service()

    service.publish(_ruleset("draft"), created_by="author", published_by="approver")

    assert service._repository.created_by == "author"
    assert service._repository.published_by == "approver"


def test_publish_allows_omitted_provenance():
    service = _service()

    service.publish(_ruleset("draft"))

    assert service._repository.created_by is None
    assert service._repository.published_by is None
