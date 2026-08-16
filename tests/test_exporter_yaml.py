from datetime import date
from decimal import Decimal

import yaml

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.serializer import DeltaRowSerializer


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
            "status": "published",
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
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A", "value_type": "string"},
                                "tolerance_abs": "0",
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
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
                                        "null_input_mode": "propagate",
                                        "null_result_mode": "null",
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
                            "value": {
                                "literal": Decimal("0.042500000000000000001")
                            },
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
        "null_input_mode",
        "null_result_mode",
        "active_flag",
    ]


def test_yaml_export_uses_canonical_keys():
    """
    What: Verifies YAML export emits canonical authoring keys only.
    Why: Exported YAML must not reintroduce legacy aliases or internal dataclass names.
    Fails when: Export uses value/assignments aliases or omits canonical group IDs.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
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

    payload = yaml.safe_load(YamlRulesetExporter().export_text(ruleset))
    condition = payload["rules"][0]["when"]["all"][0]
    assignment = payload["rules"][0]["assign"][0]

    assert "value" not in condition["right"]
    assert "assignments" not in payload["rules"][0]
    assert condition["right"]["literal"] == "A"
    assert assignment["value"]["literal"] == "A"
    assert payload["rules"][0]["when"]["condition_group_id"] == "cg:r1:root"
