import hashlib
import json

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.serializer import DeltaRowSerializer


def _compile(payload):
    return YamlRulesetCompiler().compile_payload(payload)


def test_serializer_persists_canonical_payload_with_explicit_fields():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "aggregate": {
                                        "function": "sum",
                                        "field": "amount",
                                        "scope": "dataset",
                                        "null_input_mode": "ignore",
                                        "null_result_mode": "null",
                                    }
                                },
                                "operator": "gt",
                                "right": {"literal": 1},
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

    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)
    payload = json.loads(row.payload_json)
    condition = payload["rules"][0]["when"]["all"][0]
    aggregate = condition["left"]["aggregate"]

    assert aggregate["scope"] == "dataset"
    assert aggregate["by"] == []
    assert condition["tolerance_abs"] == "0"
    assert condition["null_input_mode"] == "propagate"
    assert condition["null_result_mode"] == "null"


def test_serializer_stamps_provenance_hash_and_summary_counts():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    row = DeltaRowSerializer().serialize_ruleset_version(
        ruleset,
        created_by="tester",
        created_at="2026-04-18T00:00:00+00:00",
    )

    assert row.created_by == "tester"
    assert row.created_at == "2026-04-18T00:00:00+00:00"
    assert row.published_by is None
    assert len(row.content_hash) == 64
    assert row.rule_count == 1
    assert row.condition_count == 1
    assert row.assignment_count == 1


def test_content_hash_equals_sha256_of_payload_json():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)

    assert hashlib.sha256(row.payload_json.encode("utf-8")).hexdigest() == row.content_hash


def test_content_hash_and_payload_json_are_deterministic():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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
    serializer = DeltaRowSerializer()

    first = serializer.serialize_ruleset_version(ruleset)
    second = serializer.serialize_ruleset_version(ruleset)

    assert first.payload_json == second.payload_json
    assert first.content_hash == second.content_hash


def test_serializer_defaults_created_by_to_system():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)

    assert row.created_by == "system"


def test_deserializer_reconstructs_canonical_models():
    original = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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
    serializer = DeltaRowSerializer()
    reconstructed = serializer.deserialize_ruleset_version(
        serializer.serialize_ruleset_version(original)
    )

    assert reconstructed == original


def test_persisted_payload_excludes_lifecycle_status():
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(ruleset)
    payload = json.loads(row.payload_json)

    assert "status" not in payload
