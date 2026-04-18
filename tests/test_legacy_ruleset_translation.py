from rules_engine.compiler_yaml import YamlRulesetCompiler
from tools.legacy_ruleset_translation.translate_legacy_rulesets import (
    LegacyRulesetTranslator,
)


def _legacy_payload(condition):
    return {
        "rule_set_id": "legacy",
        "rule_set_name": "Legacy",
        "version": "1",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "priority": 10,
                "enabled": True,
                "when": {"all": [condition]},
                "then": {"assign": {"leaf_key": "10110"}},
            }
        ],
    }


def test_legacy_substring_equality_translates_to_starts_with():
    payload = LegacyRulesetTranslator().translate_payload(
        _legacy_payload(
            {
                "op": "=",
                "left": {
                    "func": {
                        "name": "substring",
                        "args": [
                            {"field": "BK_PositionID"},
                            {"value": 1, "type": "integer"},
                            {"value": 3, "type": "integer"},
                        ],
                    }
                },
                "right": {"value": "INV", "type": "string"},
            }
        )
    )

    condition = payload["rules"][0]["when"]["all"][0]

    assert condition["left"] == {"field": "BK_PositionID"}
    assert condition["operator"] == "starts_with"
    assert condition["right"] == {"literal": "INV", "value_type": "string"}
    assert payload["rules"][0]["stop_on_match"] is True
    YamlRulesetCompiler().compile_payload(payload)


def test_legacy_substring_inequality_with_longer_length_translates_to_exact_not_like():
    payload = LegacyRulesetTranslator().translate_payload(
        _legacy_payload(
            {
                "op": "!=",
                "left": {
                    "func": {
                        "name": "substring",
                        "args": [
                            {"field": "BK_AccountID"},
                            {"value": 1, "type": "integer"},
                            {"value": 5, "type": "integer"},
                        ],
                    }
                },
                "right": {"value": "INV", "type": "string"},
            }
        )
    )

    condition = payload["rules"][0]["when"]["all"][0]

    assert condition["operator"] == "not_like"
    assert condition["right"] == {"literal": "INV", "value_type": "string"}
    YamlRulesetCompiler().compile_payload(payload)


def test_legacy_in_operand_translates_to_collection_literal():
    payload = LegacyRulesetTranslator().translate_payload(
        _legacy_payload(
            {
                "op": "in",
                "left": {"field": "BK_AccountID"},
                "right": {
                    "values": [
                        {"value": "MFX", "type": "string"},
                        {"value": "DUS", "type": "string"},
                    ]
                },
            }
        )
    )

    condition = payload["rules"][0]["when"]["all"][0]

    assert condition["operator"] == "in"
    assert condition["right"] == {"literal": ["MFX", "DUS"], "value_type": "list"}
    YamlRulesetCompiler().compile_payload(payload)
