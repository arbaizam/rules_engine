from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.validator import RulesetValidator

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SHIPPED_RULESETS = tuple(
    sorted(
        [*_REPOSITORY_ROOT.joinpath("examples", "rulesets").glob("*.yaml")]
        + [*_REPOSITORY_ROOT.joinpath("outputs").glob("*.yaml")]
    )
)


@pytest.mark.parametrize("path", _SHIPPED_RULESETS, ids=lambda path: path.name)
def test_shipped_rulesets_validate_and_round_trip_hash_stably(path):
    """Every shipped YAML artifact stays aligned with the canonical contract."""
    compiler = YamlRulesetCompiler()
    serializer = DeltaRowSerializer()
    ruleset = compiler.compile_path(path)

    assert RulesetValidator().validate(ruleset).passed

    reconstructed = compiler.compile_text(YamlRulesetExporter().export_text(ruleset))

    assert reconstructed == ruleset
    assert serializer.content_hash(reconstructed) == serializer.content_hash(ruleset)


def test_yaml_export_round_trips_compiled_ruleset():
    """
    What: Exports a compiled ruleset to YAML and recompiles it.
    Why: Governance workflows need stable YAML round-trip authoring support.
    Fails when: Exported YAML loses metadata, nested groups, operands, or assignments.
    """
    compiler = YamlRulesetCompiler()
    original = compiler.compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "description": "Round-trip fixture",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "active_flag": True,
                    "stop_on_match": True,
                    "description": "Rule description",
                    "when": {
                        "condition_group_id": "root",
                        "all": [
                            {
                                "condition_id": "c1",
                                "left": {
                                    "field": "account",
                                    "default_if_null": "UNKNOWN",
                                },
                                "operator": "eq",
                                "right": {"literal": "A", "value_type": "string"},
                                "tolerance_abs": "0",
                                "error_on_null": True,
                                "active_flag": True,
                            },
                            {
                                "condition_group_id": "nested",
                                "any": [
                                    {
                                        "condition_id": "c2",
                                        "left": {"field": "account_open_amount_sum"},
                                        "operator": "gt",
                                        "right": {"literal": 100, "value_type": "number"},
                                        "tolerance_abs": "0",
                                    }
                                ],
                            },
                        ],
                    },
                    "assign": [
                        {
                            "assignment_id": "a1",
                            "target_field": "bucket",
                            "value": {"literal": "A", "value_type": "string"},
                        },
                        {
                            "assignment_id": "a2",
                            "target_field": "score",
                            "value": {
                                "custom_function": {
                                    "name": "score_account",
                                    "args": {"threshold": 10},
                                }
                            },
                        },
                        {
                            "assignment_id": "a3",
                            "target_field": "rate",
                            "value": {"literal": Decimal("0.042500000000000000001")},
                        },
                    ],
                }
            ],
        }
    )

    yaml_text = YamlRulesetExporter().export_text(original)
    exported = yaml.safe_load(yaml_text)
    reconstructed = compiler.compile_text(yaml_text)

    assert exported["owner"] == "Rules Team"
    assert exported["owner_department"] == "ALM Engineering"
    assert "status" not in exported
    assert reconstructed == original


def test_yaml_export_text_is_stable_after_recompilation():
    """Canonical key order produces byte-stable review artifacts."""
    compiler = YamlRulesetCompiler()
    original = compiler.compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "when": {
                        "all": [
                            {
                                "condition_id": "c1",
                                "left": {"field": "rate"},
                                "operator": "ge",
                                "right": {"literal": Decimal("0.0425")},
                            }
                        ]
                    },
                    "assign": {
                        "bucket": "A",
                        "tags": {"beta", "alpha"},
                        "bounds": (date(2026, 1, 1), date(2026, 12, 31)),
                        "pairs": {("A", "B"), ("C", "D")},
                    },
                }
            ],
        }
    )
    exporter = YamlRulesetExporter()

    first = exporter.export_text(original)
    reconstructed = compiler.compile_text(first)
    second = exporter.export_text(reconstructed)
    condition = exporter.export_payload(original)["rules"][0]["when"]["all"][0]

    assert first == second
    assert reconstructed == original
    assert "!rules_engine/tuple" in first
    assert DeltaRowSerializer().content_hash(reconstructed) == (
        DeltaRowSerializer().content_hash(original)
    )
    assert list(condition) == [
        "condition_id",
        "left",
        "operator",
        "right",
        "tolerance_abs",
        "active_flag",
    ]


def test_yaml_exporter_preserves_operands_inside_function_argument_arrays():
    compiler = YamlRulesetCompiler()
    payload = {
        "ruleset_id": "nested",
        "ruleset_name": "Nested arguments",
        "version": "1",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Compose",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "left": {"literal": True},
                            "operator": "eq",
                            "right": {"literal": True},
                        }
                    ]
                },
                "assign": {
                    "selected": {
                        "custom_function": {
                            "name": "coalesce",
                            "args": {
                                "values": [
                                    {"field": "primary"},
                                    {"field": "secondary"},
                                ]
                            },
                        }
                    }
                },
            }
        ],
    }
    original = compiler.compile_payload(payload)

    reconstructed = compiler.compile_text(YamlRulesetExporter().export_text(original))

    assert reconstructed == original


def test_yaml_export_round_trips_assigned_operands():
    """The explicit prior-assignment reference remains canonical YAML."""
    compiler = YamlRulesetCompiler()
    ruleset = compiler.compile_payload(
        {
            "ruleset_id": "chain",
            "ruleset_name": "Chain",
            "version": "1",
            "rules": [
                {
                    "rule_id": "producer",
                    "rule_name": "Producer",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                },
                {
                    "rule_id": "consumer",
                    "rule_name": "Consumer",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "assigned": "bucket",
                                    "default_if_null": "MISSING",
                                },
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"copy": {"assigned": "bucket"}},
                },
            ],
        }
    )

    exported = YamlRulesetExporter().export_text(ruleset)

    assert compiler.compile_text(exported) == ruleset
    assert "assigned: bucket" in exported


def test_yaml_export_emits_the_exact_rule_contract():
    """Exported mappings contain the complete declared authoring shape."""
    ruleset = YamlRulesetCompiler().compile_payload(
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

    payload = yaml.safe_load(YamlRulesetExporter().export_text(ruleset))
    condition = payload["rules"][0]["when"]["all"][0]
    assignment = payload["rules"][0]["assign"][0]

    assert set(payload["rules"][0]) == {
        "rule_id",
        "rule_name",
        "rule_order",
        "active_flag",
        "stop_on_match",
        "when",
        "assign",
    }
    assert set(condition) == {
        "condition_id",
        "left",
        "operator",
        "right",
        "tolerance_abs",
        "active_flag",
    }
    assert condition["right"] == {"literal": "A"}
    assert assignment["value"] == {"literal": "A"}
    assert payload["rules"][0]["when"]["condition_group_id"] == "cg:r1:root"
