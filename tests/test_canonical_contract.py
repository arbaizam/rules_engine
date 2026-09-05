"""Regression coverage for the shared authoring and persistence boundary."""

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from rules_engine.canonical_values import canonical_json_value, decode_json_types
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import CompilationError, RepositoryError
from rules_engine.model_codec import PERSISTENCE_FORMAT_VERSION
from rules_engine.models import CustomFunctionOperand, FieldOperand, LiteralOperand
from rules_engine.registry import CustomFunctionArgSpec, CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.traversal import iter_conditions, iter_ruleset_operands
from rules_engine.validator import RulesetValidator


def _ruleset():
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "contract",
            "ruleset_name": "Canonical contract",
            "version": "1",
            "owner": "Rules Team",
            "owner_department": "Engineering",
            "rules": [
                {
                    "rule_name": "Rule",
                    "when": {
                        "all": [{"left": {"literal": 1}, "operator": "eq", "right": {"literal": 1}}]
                    },
                    "assign": {"result": "A"},
                }
            ],
        }
    )


def _with_assignment_value(value):
    ruleset = _ruleset()
    rule = ruleset.rules[0]
    assignment = replace(rule.assignments[0], value=value)
    return replace(ruleset, rules=(replace(rule, assignments=(assignment,)),))


@pytest.mark.parametrize(
    ("operator", "matched"),
    [("in", True), ("not_in", False), ("between", True), ("not_between", False)],
)
def test_collection_shape_validation_uses_effective_literal_fallback(operator, matched):
    """The publish gate accepts the same effective collection the worker evaluates."""
    ruleset = YamlRulesetCompiler().compile_text(
        f"""ruleset_id: fallback
ruleset_name: Fallback
version: '1'
owner: Rules Team
owner_department: Engineering
rules:
- rule_name: Rule
  when:
    all:
    - left: {{literal: 1}}
      operator: {operator}
      right: {{literal: null, default_if_null: [1, 2]}}
  assign: {{result: A}}
"""
    )
    assert RulesetValidator().validate(ruleset).passed
    assert SparkRowEvaluator(FunctionRegistry()).evaluate_row(ruleset, {})["matched"] is matched


def test_yaml_binary_literal_is_rejected_before_publication():
    with pytest.raises(CompilationError, match="Unsupported literal type: bytes"):
        YamlRulesetCompiler().compile_text(
            """ruleset_id: binary
ruleset_name: Binary
version: '1'
rules:
- rule_name: Rule
  when: {all: [{left: {literal: true}, operator: eq, right: {literal: true}}]}
  assign: {result: {literal: !!binary SGVsbG8=}}
"""
        )


@pytest.mark.parametrize("version", [None, "", "  ", 42, []])
def test_direct_models_require_a_nonempty_version(version):
    result = RulesetValidator().validate(replace(_ruleset(), version=version))
    assert "RULESET_VERSION_INVALID" in {issue.check_name for issue in result.issues}


@pytest.mark.parametrize(
    "operand",
    [
        LiteralOperand(b"hello"),
        LiteralOperand({"nested": [object()]}),
        LiteralOperand(float("nan")),
        LiteralOperand(Decimal("Infinity")),
        LiteralOperand("oops", "integer"),
        LiteralOperand(1.5, "integer"),
        LiteralOperand("2026-01-01", "date"),
        LiteralOperand([1, "2"], "integer"),
    ],
)
def test_direct_literal_errors_are_structured_validation_issues(operand):
    result = RulesetValidator().validate(_with_assignment_value(operand))
    assert "LITERAL_VALUE_INVALID" in {issue.check_name for issue in result.issues}


@pytest.mark.parametrize("value", [b"hello", float("inf"), {"nested": [object()]}])
def test_raw_function_argument_values_receive_the_same_finite_literal_validation(value):
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec("probe", "tests.probe", (CustomFunctionArgSpec("value"),), True, True)
    )
    ruleset = _with_assignment_value(CustomFunctionOperand("probe", {"value": value}))
    result = RulesetValidator(registry).validate(ruleset)
    assert "CUSTOM_FUNCTION_ARG_VALUE_INVALID" in {issue.check_name for issue in result.issues}


@pytest.mark.parametrize("wrapped", [False, True])
def test_direct_mapping_keys_cannot_change_meaning_during_publication(wrapped):
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            "probe",
            "tests.probe",
            (CustomFunctionArgSpec("value", type_hint="mapping"),),
            True,
            True,
        )
    )
    value = LiteralOperand({1: "value"}) if wrapped else {1: "value"}
    ruleset = _with_assignment_value(CustomFunctionOperand("probe", {"value": value}))
    result = RulesetValidator(registry).validate(ruleset)
    assert "MAPPING_KEY_INVALID" in {issue.check_name for issue in result.issues}
    with pytest.raises(ValueError, match="mapping keys must be strings"):
        DeltaRowSerializer().serialize_ruleset_version(ruleset)


def test_persistence_disambiguates_mapping_data_and_dynamic_argument_operands():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            "probe",
            "tests.probe",
            (CustomFunctionArgSpec("metadata", type_hint="mapping"),),
            True,
            True,
        )
    )
    operand = CustomFunctionOperand(
        "probe",
        {
            "metadata": {
                "field": "ordinary data",
                "literal": {"custom_function": "ordinary nested data"},
                "$rules_engine_arg": "ordinary reserved data",
                "dynamic": FieldOperand("source"),
                "tuple": (FieldOperand("another"), LiteralOperand(2)),
            }
        },
    )
    original = _with_assignment_value(operand)
    assert RulesetValidator(registry).validate(original).passed
    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(original)
    restored = serializer.deserialize_ruleset_version(row)
    assert restored == original
    assert serializer.content_hash(restored) == row.content_hash
    assert isinstance(restored.rules[0].assignments[0].value.args["metadata"], dict)
    assert json.loads(row.payload_json)["$rules_engine_format"] == PERSISTENCE_FORMAT_VERSION


def test_persistence_preserves_binary_float_and_decimal_kinds_without_authoring_coercion():
    original = _with_assignment_value(LiteralOperand([1.5, Decimal("1.5"), 1]))
    serializer = DeltaRowSerializer()
    restored = serializer.deserialize_ruleset_version(
        serializer.serialize_ruleset_version(original)
    )
    values = restored.rules[0].assignments[0].value.value
    assert [type(value) for value in values] == [float, Decimal, int]
    assert restored == original


def test_json_ready_graph_preserves_decimal_precision_and_all_literal_kinds():
    exact = Decimal("12345678901234567890.123456789012345678")
    original = {
        "values": (exact, Decimal(1), Decimal("1.00"), 1, 1.5),
        "set": {"a", "b"},
        "reserved": {"$rules_engine_type": "decimal", "value": "ordinary data"},
    }
    graph = canonical_json_value(original)
    encoded = json.dumps(graph, sort_keys=True, allow_nan=False)
    restored = decode_json_types(json.loads(encoded))
    assert restored == original
    assert [type(item) for item in restored["values"]] == [Decimal, Decimal, Decimal, int, float]
    assert restored["values"][0].as_tuple() == exact.as_tuple()
    assert restored["values"][2].as_tuple() == Decimal("1.00").as_tuple()


@pytest.mark.parametrize("value", [1, "NaN", "Infinity", "not-a-number"])
def test_json_ready_decimal_envelope_rejects_malformed_or_nonfinite_values(value):
    with pytest.raises(ValueError, match="decimal"):
        decode_json_types({"$rules_engine_type": "decimal", "value": value})


@pytest.mark.parametrize("field", ["ruleset_id", "ruleset_name", "version"])
def test_persistence_rejects_row_identity_disagreement(field):
    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(_ruleset())
    with pytest.raises(RepositoryError, match=f"{field} disagrees"):
        serializer.deserialize_ruleset_version(replace(row, **{field: "different"}))


def test_persistence_verifies_hash_before_deserializing_payload():
    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(_ruleset())
    with pytest.raises(RepositoryError, match="content_hash does not match"):
        serializer.deserialize_ruleset_version(replace(row, content_hash="0" * 64))
    with pytest.raises(RepositoryError, match="content_hash does not match"):
        serializer.deserialize_ruleset_version(replace(row, payload_json="invalid JSON"))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda payload: payload.update({"$rules_engine_format": 999}), "Unsupported ruleset"),
        (lambda payload: payload.pop("$rules_engine_format"), "Unsupported ruleset"),
        (lambda payload: payload.update({"unexpected": True}), "not canonical"),
        (lambda payload: payload["rules"][0].pop("rule_id"), "Cannot load persisted"),
    ],
)
def test_persistence_reports_unsupported_or_malformed_documents(change, message):
    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(_ruleset())
    payload = json.loads(row.payload_json)
    change(payload)
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    corrupt = replace(
        row, payload_json=text, content_hash=serializer.content_hash_from_payload_json(text)
    )
    with pytest.raises(RepositoryError, match=message):
        serializer.deserialize_ruleset_version(corrupt)


def test_persistence_rejects_duplicate_json_keys_even_with_a_matching_hash():
    serializer = DeltaRowSerializer()
    row = serializer.serialize_ruleset_version(_ruleset())
    text = row.payload_json[:-1] + ',"version":"2"}'
    corrupt = replace(
        row, payload_json=text, content_hash=serializer.content_hash_from_payload_json(text)
    )
    with pytest.raises(RepositoryError, match="Duplicate persisted JSON key"):
        serializer.deserialize_ruleset_version(corrupt)


def test_shared_traversal_respects_active_filters_and_literal_boundaries():
    ruleset = _with_assignment_value(
        CustomFunctionOperand("probe", {"data": LiteralOperand({"field": "data"})})
    )
    rule = ruleset.rules[0]
    condition = rule.root_group.conditions[0]
    group = replace(rule.root_group, conditions=(replace(condition, active_flag=False),))
    ruleset = replace(ruleset, rules=(replace(rule, root_group=group),))
    assert len(list(iter_conditions(group))) == 1
    assert not list(iter_conditions(group, active_only=True))
    operands = list(iter_ruleset_operands(ruleset, active_only=True))
    assert len(operands) == 2
    assert isinstance(operands[0], CustomFunctionOperand)
    assert isinstance(operands[1], LiteralOperand)
