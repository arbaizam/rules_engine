from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.models import AssignedOperand, CustomFunctionOperand, FieldOperand, LiteralOperand
from rules_engine.registry import CustomFunctionArgSpec, CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.validator import RulesetValidator

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SHIPPED_RULESETS = tuple(
    sorted(
        [*_REPOSITORY_ROOT.joinpath("examples", "rulesets").glob("*.yaml")]
        + [*_REPOSITORY_ROOT.joinpath("outputs").glob("*.yaml")]
    )
)


def _ruleset_with_operand(operand):
    """Build a valid owned ruleset without authoring-normalizing the tested operand."""
    ruleset = YamlRulesetCompiler().compile_payload({
        "ruleset_id": "types",
        "ruleset_name": "Typed export",
        "version": "1",
        "owner": "Rules Team",
        "owner_department": "Engineering",
        "rules": [{
            "rule_name": "Preserve value",
            "when": {"all": [{"left": {"literal": True}, "operator": "eq", "right": {"literal": True}}]},
            "assign": {"result": "placeholder"},
        }],
    })
    rule = ruleset.rules[0]
    assignment = replace(rule.assignments[0], value=operand)
    return replace(ruleset, rules=(replace(rule, assignments=(assignment,)),))


def _assert_same_value_and_types(actual, expected):
    """Compare recursive Python kinds, including signed zero, without a codec oracle."""
    assert type(actual) is type(expected)
    assert actual == expected
    if isinstance(expected, float):
        assert actual.hex() == expected.hex()
    elif isinstance(expected, dict):
        for key, value in expected.items():
            _assert_same_value_and_types(actual[key], value)
    elif isinstance(expected, (list, tuple)):
        for actual_item, expected_item in zip(actual, expected, strict=True):
            _assert_same_value_and_types(actual_item, expected_item)
    elif isinstance(expected, set):
        for expected_item in expected:
            actual_item = next(item for item in actual if item == expected_item)
            _assert_same_value_and_types(actual_item, expected_item)


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

    exporter = YamlRulesetExporter()
    yaml_text = exporter.export_text(original)
    exported = exporter.export_payload(original)
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


@pytest.mark.parametrize(
    "mapping",
    [
        {"field": "legacy_name"},
        {"field": 1.5},
        {"field": {"nested": [1.5, (2.5, {"values": {3.5, 4.5}})]}},
        {"assigned": "ordinary data"},
        {"literal": {"custom_function": "ordinary nested data"}},
        {"custom_function": {"name": "ordinary data"}},
        {"other": FieldOperand("x"), "field": 1},
        {"field": FieldOperand("x", default_if_null=LiteralOperand("missing"))},
        {"$rules_engine_mapping": "ordinary reserved data"},
        {"$rules_engine_mapping": {"field": "nested reserved data"}},
        {
            "field": [
                {"assigned": "data"},
                (AssignedOperand("prior"), LiteralOperand({"field": "literal data"})),
            ],
            "nested": CustomFunctionOperand("lookup", {"mapping": {"field": "data"}}),
        },
    ],
)
def test_operand_shaped_argument_mappings_survive_yaml_round_trip(mapping):
    """Reviewing persisted raw mappings in YAML preserves model and hash identity."""
    compiler = YamlRulesetCompiler()
    original = compiler.compile_payload(
        {
            "ruleset_id": "mapping",
            "ruleset_name": "Argument mappings",
            "version": "1",
            "rules": [
                {
                    "rule_name": "Lookup",
                    "when": {"all": []},
                    "assign": {"result": "placeholder"},
                }
            ],
        }
    )
    rule = original.rules[0]
    assignment = replace(
        rule.assignments[0], value=CustomFunctionOperand("lookup", {"column_map": mapping})
    )
    original = replace(original, rules=(replace(rule, assignments=(assignment,)),))
    serializer = DeltaRowSerializer()
    persisted = serializer.deserialize_ruleset_version(
        serializer.serialize_ruleset_version(original)
    )

    exporter = YamlRulesetExporter()
    exported = exporter.export_text(persisted)
    reconstructed = compiler.compile_text(exported)
    from_payload = compiler.compile_payload(exporter.export_payload(persisted))

    assert reconstructed == original
    assert from_payload == original
    assert serializer.content_hash(reconstructed) == serializer.content_hash(original)
    assert serializer.content_hash(from_payload) == serializer.content_hash(original)
    assert exporter.export_text(reconstructed) == exported
    assert isinstance(reconstructed.rules[0].assignments[0].value.args["column_map"], dict)


@pytest.mark.parametrize(
    "value",
    [
        1.5,
        -0.0,
        0.0,
        5e-324,
        1.7976931348623157e308,
        {"mixed": [1.5, Decimal("1.50"), 1], "nested": (2.5, {3.5, 4.5})},
        {(1.5, 2.5), (3.5, 4.5)},
    ],
)
def test_yaml_export_preserves_untyped_float_literals_and_null_defaults(value):
    """A persisted float keeps its type, sign, metadata and hash through YAML review."""
    original = _ruleset_with_operand(LiteralOperand(value))
    original_rule = original.rules[0]
    assignment = original_rule.assignments[0]
    original = replace(original, rules=(replace(
        original_rule,
        assignments=(
            assignment,
            replace(
                assignment,
                assignment_id="fallback",
                target_field="fallback",
                value=LiteralOperand(None, default_if_null=LiteralOperand(value)),
            ),
        ),
    ),))
    serializer = DeltaRowSerializer()
    persisted = serializer.deserialize_ruleset_version(serializer.serialize_ruleset_version(original))
    exporter = YamlRulesetExporter()
    text = exporter.export_text(persisted)
    recompiled = YamlRulesetCompiler().compile_text(text)
    from_payload = YamlRulesetCompiler().compile_payload(exporter.export_payload(persisted))

    assert RulesetValidator().validate(original).passed
    assert RulesetValidator().validate(recompiled).passed
    assert recompiled == original
    assert serializer.content_hash(recompiled) == serializer.content_hash(original)
    assert exporter.export_text(recompiled) == text
    for model in (persisted, recompiled, from_payload):
        assert RulesetValidator().validate(model).passed
        assert serializer.content_hash(model) == serializer.content_hash(original)
        actual = model.rules[0].assignments[0].value
        assert actual.value_type is None
        _assert_same_value_and_types(actual.value, value)
        fallback = model.rules[0].assignments[1].value.default_if_null
        assert fallback.value_type is None
        _assert_same_value_and_types(fallback.value, value)
    assert "!rules_engine/float" in text


@pytest.mark.parametrize("value_type", ["float", "double", "number", "extension"])
def test_yaml_export_preserves_float_literal_type_hint_metadata(value_type):
    """Preserving float kind must neither add nor replace authored type metadata."""
    original = _ruleset_with_operand(LiteralOperand(1.5, value_type=value_type))
    exporter = YamlRulesetExporter()
    compiler = YamlRulesetCompiler()
    for restored in (
        compiler.compile_text(exporter.export_text(original)),
        compiler.compile_payload(exporter.export_payload(original)),
    ):
        operand = restored.rules[0].assignments[0].value
        assert type(operand.value) is float
        assert operand.value_type == value_type
        assert DeltaRowSerializer().content_hash(restored) == DeltaRowSerializer().content_hash(original)


@pytest.mark.parametrize(
    ("value", "expected_result"),
    [
        (1.5, "float"),
        ({"field": 1.5}, "float"),
        ({"literal": [1.5, (2.5, {3.5, 4.5})]}, "float,float,float,float"),
    ],
)
def test_yaml_export_preserves_raw_float_argument_types_and_function_behavior(value, expected_result):
    """Valid registry-backed models retain observable argument kinds through export."""
    def numeric_kinds(value):
        if isinstance(value, dict):
            return [kind for item in value.values() for kind in numeric_kinds(item)]
        if isinstance(value, (list, tuple, set)):
            return [kind for item in value for kind in numeric_kinds(item)]
        return [type(value).__name__]

    def describe_kinds(value):
        return ",".join(numeric_kinds(value))

    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            "describe_kinds", "tests.describe_kinds", (CustomFunctionArgSpec("value"),),
            True, True, return_type_hint="string",
        ),
        describe_kinds,
    )
    original = _ruleset_with_operand(CustomFunctionOperand("describe_kinds", {"value": value}))
    serializer = DeltaRowSerializer()
    persisted = serializer.deserialize_ruleset_version(serializer.serialize_ruleset_version(original))
    exporter = YamlRulesetExporter()
    compiler = YamlRulesetCompiler()
    recompiled = compiler.compile_text(exporter.export_text(persisted))
    from_payload = compiler.compile_payload(exporter.export_payload(persisted))
    for model in (original, persisted, recompiled, from_payload):
        assert RulesetValidator(registry).validate(model).passed
        _assert_same_value_and_types(model.rules[0].assignments[0].value.args["value"], value)
        result = SparkRowEvaluator(registry).evaluate_row(model, {})
        assert result["assign"]["result"] == {"applied": True, "value": expected_result}
        assert serializer.content_hash(model) == serializer.content_hash(original)


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
