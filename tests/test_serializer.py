import hashlib
import json

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.serializer import DeltaRowSerializer


def _compile(payload):
    return YamlRulesetCompiler().compile_payload(payload)


def test_serializer_persists_canonical_payload_with_explicit_fields():
    """
    What: Serializes a ruleset and inspects explicit canonical payload fields.
    Why: Persisted payload_json is the audit source for runtime reconstruction.
    Fails when: Scope, tolerance, or null fields are omitted or renamed.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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
    """
    What: Serializes owner, lifecycle, hash, and payload summary metadata.
    Why: The ruleset_versions row must expose queryable audit metadata.
    Fails when: user_metadata, payload_metadata, or content_hash are not populated.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    row = DeltaRowSerializer().serialize_ruleset_version(
        ruleset,
        created_by="tester",
        created_at="2026-04-18T00:00:00+00:00",
    )

    assert row.user_metadata.owner == "Rules Team"
    assert row.user_metadata.owner_department == "ALM Engineering"
    assert row.user_metadata.created_by == "tester"
    assert row.user_metadata.created_at == "2026-04-18T00:00:00+00:00"
    assert row.user_metadata.published_by is None
    assert len(row.content_hash) == 64
    assert row.payload_metadata.rule_count == 1
    assert row.payload_metadata.condition_count == 1
    assert row.payload_metadata.assignment_count == 1


def test_content_hash_equals_sha256_of_payload_json():
    """
    What: Compares content_hash to SHA-256 of persisted payload_json bytes.
    Why: Auditors must be able to independently recompute the integrity hash.
    Fails when: The hash is computed from a different or noncanonical payload.
    """
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
    """
    What: Serializes the same ruleset twice and compares payload/hash output.
    Why: Promotion and audit comparisons require stable serialization.
    Fails when: Dict ordering or serialization paths produce nondeterministic bytes.
    """
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
    """
    What: Serializes without created_by and checks the system actor default.
    Why: Locked-down production jobs may omit user actors but metadata must be explicit.
    Fails when: Missing actors persist as null or empty strings.
    """
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

    assert row.user_metadata.created_by == "system"


def test_deserializer_reconstructs_canonical_models():
    """
    What: Deserializes a persisted ruleset version back to canonical dataclasses.
    Why: Runtime loading depends on payload_json round-tripping exactly.
    Fails when: Persistence loses fields or reconstructs a different ruleset.
    """
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
    """
    What: Confirms payload_json does not duplicate lifecycle status.
    Why: The table status column is authoritative for draft/publish/retire state.
    Fails when: Payload content and row lifecycle status can diverge.
    """
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
