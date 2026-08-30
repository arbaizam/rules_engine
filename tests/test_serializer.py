import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.serializer import (
    DeltaRowSerializer,
    _canonical_json_dumps,
    _decode_json_types,
)


def _compile(payload):
    return YamlRulesetCompiler().compile_payload(payload)


def test_serializer_persists_canonical_payload_with_explicit_fields():
    """
    What: Serializes a ruleset and inspects explicit canonical payload fields.
    Why: Persisted payload_json is the audit source for runtime reconstruction.
    Fails when: Operand, null-default, error, or tolerance fields are omitted or renamed.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
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
                                    "field": "account_balance",
                                    "default_if_null": 0,
                                },
                                "operator": "gt",
                                "right": {"literal": 1},
                                "error_on_null": True,
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

    assert condition["left"] == {
        "field": "account_balance",
        "default_if_null": 0,
    }
    assert condition["tolerance_abs"] == "0"
    assert condition["error_on_null"] is True


def test_serializer_stamps_provenance_hash_and_summary_counts():
    """
    What: Serializes owner, lifecycle, hash, and payload summary metadata.
    Why: The ruleset_versions row must expose queryable audit metadata.
    Fails when provenance, summary counts, or content_hash are not populated.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
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

    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)

    assert row.owner == "Rules Team"
    assert row.owner_department == "ALM Engineering"
    assert row.published_by is None
    assert len(row.content_hash) == 64
    assert row.rule_count == 1
    assert row.condition_count == 1
    assert row.assignment_count == 1


def test_serializer_counts_nested_custom_function_operands():
    """
    What: Counts custom functions nested inside custom-function arguments.
    Why: Summary metadata should reflect the full supported operand tree.
    Fails when: Custom function counts only include top-level operands.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
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
                                    "custom_function": {
                                        "name": "outer_score",
                                        "args": {
                                            "values": [
                                                {
                                                    "custom_function": {
                                                        "name": "inner_score",
                                                        "args": {"value": {"field": "account"}},
                                                    }
                                                }
                                            ]
                                        },
                                    }
                                },
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

    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)

    assert row.custom_function_count == 2


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
    serializer = DeltaRowSerializer()

    first = serializer.serialize_ruleset_version(ruleset)
    second = serializer.serialize_ruleset_version(ruleset)

    assert first.payload_json == second.payload_json
    assert first.content_hash == second.content_hash


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
    serializer = DeltaRowSerializer()
    reconstructed = serializer.deserialize_ruleset_version(
        serializer.serialize_ruleset_version(original)
    )

    assert reconstructed == original


def test_serializer_round_trips_exact_decimal_scalars_and_collections():
    """Persisted JSON keeps financial Decimals numeric and lossless."""
    original = _compile(
        {
            "ruleset_id": "decimal_rules",
            "ruleset_name": "Decimal Rules",
            "version": "1",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Exact membership",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "rate"},
                                "operator": "in",
                                "right": {
                                    "literal": [
                                        Decimal("0.042500000000000000001"),
                                        Decimal(1),
                                    ]
                                },
                            }
                        ]
                    },
                    "assign": {
                        "factors": [
                            Decimal("0.10"),
                            Decimal("0.250000000000000000001"),
                        ]
                    },
                }
            ],
        }
    )
    serializer = DeltaRowSerializer()

    row = serializer.serialize_ruleset_version(original)
    payload = json.loads(row.payload_json, parse_float=Decimal)
    reconstructed = serializer.deserialize_ruleset_version(row)

    condition_values = payload["rules"][0]["when"]["all"][0]["right"]["literal"]
    assignment_values = payload["rules"][0]["assign"][0]["value"]["literal"]
    assert condition_values == [Decimal("0.042500000000000000001"), Decimal(1)]
    assert isinstance(condition_values[1], Decimal)
    assert condition_values[1].as_tuple() == Decimal(1).as_tuple()
    assert assignment_values == [
        Decimal("0.10"),
        Decimal("0.250000000000000000001"),
    ]
    assert reconstructed == original
    reconstructed_values = reconstructed.rules[0].root_group.conditions[0].right.value
    assert reconstructed_values[1].as_tuple() == Decimal(1).as_tuple()
    assert len(row.content_hash) == 64
    assert serializer.content_hash(reconstructed) == row.content_hash


@pytest.mark.parametrize(
    ("envelope", "type_name"),
    [
        ({"$rules_engine_type": "date", "value": 123}, "date"),
        ({"$rules_engine_type": "date", "value": "not-a-date"}, "date"),
        ({"$rules_engine_type": "datetime", "value": []}, "datetime"),
        ({"$rules_engine_type": "tuple", "value": "abc"}, "tuple"),
        ({"$rules_engine_type": "set", "value": [[1, 2]]}, "set"),
        ({"$rules_engine_type": "mapping", "value": []}, "mapping"),
    ],
)
def test_malformed_extended_json_envelopes_fail_uniformly(envelope, type_name):
    """Corrupt persisted values produce one diagnosable ValueError contract."""
    with pytest.raises(ValueError, match=type_name):
        _decode_json_types(envelope)


def test_canonical_json_rejects_nonfinite_float():
    """The persistence encoder never emits non-standard Infinity or NaN tokens."""
    with pytest.raises(ValueError, match="finite"):
        _canonical_json_dumps(float("inf"))


def test_canonical_json_rejects_keys_that_normalize_to_the_same_string():
    """Canonical JSON cannot emit duplicate object keys and alter semantics."""
    with pytest.raises(ValueError, match="both normalize to '1'"):
        _canonical_json_dumps({1: "integer", "1": "string"})


def test_serializer_round_trips_temporal_and_python_collection_literals():
    """Extended JSON preserves supported values that plain JSON cannot encode."""
    event_at = datetime(2026, 5, 1, 14, 30, tzinfo=timezone.utc)
    reserved_mapping = {
        "$rules_engine_type": "date",
        "value": "ordinary user metadata",
    }
    original = _compile(
        {
            "ruleset_id": "typed_literals",
            "ruleset_name": "Typed Literals",
            "version": "1",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Typed persistence",
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "typed_probe",
                                        "args": {
                                            "as_of_date": date(2026, 5, 1),
                                            "event_at": event_at,
                                            "codes": {"A", "B"},
                                            "window": (
                                                date(2026, 1, 1),
                                                date(2026, 12, 31),
                                            ),
                                            "metadata": reserved_mapping,
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {
                        "review_date": date(2026, 5, 1),
                        "event_at": event_at,
                        "codes": {"A", "B"},
                        "bounds": (date(2026, 1, 1), date(2026, 12, 31)),
                    },
                }
            ],
        }
    )
    serializer = DeltaRowSerializer()

    row = serializer.serialize_ruleset_version(original)
    reconstructed = serializer.deserialize_ruleset_version(row)

    assert "$rules_engine_type" in row.payload_json
    assert reconstructed == original
    assert serializer.content_hash(reconstructed) == row.content_hash


def test_persisted_payload_excludes_lifecycle_status():
    """
    What: Confirms payload_json does not duplicate lifecycle status.
    Why: The table status column is authoritative for publish/retire state.
    Fails when: Payload content and row lifecycle status can diverge.
    """
    ruleset = _compile(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
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

    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(ruleset)
    payload = json.loads(row.payload_json)

    assert "status" not in payload
