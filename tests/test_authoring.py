"""Public authoring-manifest contract tests."""

from __future__ import annotations

import json

import pytest

from rules_engine import build_authoring_manifest
from rules_engine.authoring import (
    AUTHORING_MANIFEST_VERSION,
    literal_type_hint_names,
)
from rules_engine.enums import ComparisonOperator, LogicalOperator, OperandKind
from rules_engine.registry import (
    DYNAMIC_RETURN_TYPE_HINT_TEMPLATES,
    SUPPORTED_ARGUMENT_TYPE_HINTS,
    SUPPORTED_RETURN_TYPE_HINTS,
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    FunctionRegistry,
)
from rules_engine.spark_types import TIMESTAMP_NTZ_TYPE
from rules_engine.spark_validator import SPARK_TYPE_HINTS
from rules_engine.standard_functions import register_standard_functions
from rules_engine.version import __version__


def _specification(
    function_name: str,
    *,
    active: bool = True,
) -> CustomFunctionSpec:
    """Return a representative authoring-visible function specification."""
    return CustomFunctionSpec(
        function_name=function_name,
        implementation_reference=f"functions.{function_name}",
        arguments=(
            CustomFunctionArgSpec("value", type_hint="string"),
            CustomFunctionArgSpec(
                "mode",
                required=False,
                default="strict",
                type_hint="string",
                allowed_values=("strict", "lenient"),
                literal_only=True,
            ),
        ),
        return_type_hint="string",
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        active_flag=active,
        description=f"Apply {function_name}.",
        version="1",
    )


def test_manifest_exposes_the_complete_engine_operator_contract():
    """Every comparison enum must have one exact authoring behavior record."""
    manifest = build_authoring_manifest(FunctionRegistry())
    expected = [
        ("eq", 2, "any", True),
        ("ne", 2, "any", True),
        ("gt", 2, "any", True),
        ("ge", 2, "any", True),
        ("lt", 2, "any", True),
        ("le", 2, "any", True),
        ("in", 2, "collection", True),
        ("not_in", 2, "collection", True),
        ("between", 2, "pair", False),
        ("not_between", 2, "pair", False),
        ("like", 2, "any", False),
        ("not_like", 2, "any", False),
        ("contains", 2, "any", False),
        ("not_contains", 2, "any", False),
        ("starts_with", 2, "any", False),
        ("ends_with", 2, "any", False),
        ("is_null", 1, "none", False),
        ("is_not_null", 1, "none", False),
    ]
    records = manifest["comparison_operators"]
    assert [item["name"] for item in records] == [item.value for item in ComparisonOperator]
    assert len(records) == len({item["name"] for item in records})
    assert records == [
        {"name": name, "arity": arity, "right_operand_shape": shape,
         "supports_tolerance": tolerance}
        for name, arity, shape, tolerance in expected
    ]


def test_manifest_exposes_enums_literal_hints_and_build_identity():
    """Static authoring choices must come from the installed engine contract."""
    manifest = build_authoring_manifest(FunctionRegistry())

    assert {
        "manifest_version",
        "engine_version",
        "comparison_operators",
        "logical_operators",
        "operand_kinds",
        "literal_type_hints",
        "function_argument_type_hints",
        "function_return_type_hints",
        "functions",
    } <= set(manifest)
    assert manifest["manifest_version"] == AUTHORING_MANIFEST_VERSION
    assert manifest["engine_version"] == __version__
    assert manifest["logical_operators"] == [item.value for item in LogicalOperator]
    assert manifest["operand_kinds"] == [item.value for item in OperandKind]
    assert manifest["literal_type_hints"] == [
        {"name": "string", "aliases": ["str"]},
        {"name": "integer", "aliases": ["int", "long"]},
        {"name": "decimal", "aliases": []},
        {"name": "double", "aliases": ["float", "number"]},
        {"name": "boolean", "aliases": ["bool"]},
        {"name": "date", "aliases": []},
        {"name": "timestamp", "aliases": []},
        {"name": "timestamp_ntz", "aliases": []},
    ]
    assert manifest["function_argument_type_hints"] == sorted(
        SUPPORTED_ARGUMENT_TYPE_HINTS
    )
    assert manifest["function_return_type_hints"] == {
        "fixed": sorted(SUPPORTED_RETURN_TYPE_HINTS),
        "dynamic_templates": list(DYNAMIC_RETURN_TYPE_HINT_TEMPLATES),
    }

    expected_spark_hints = set(literal_type_hint_names())
    if TIMESTAMP_NTZ_TYPE is None:
        expected_spark_hints.remove("timestamp_ntz")
    assert set(SPARK_TYPE_HINTS) == expected_spark_hints


def test_manifest_serializes_registered_function_contracts_deterministically():
    """Function metadata must be ordered, complete, and safe to send as JSON."""
    registry = FunctionRegistry()
    registry.register(_specification("zeta", active=False))
    registry.register(_specification("alpha"))

    assert [item.function_name for item in registry.specs()] == ["alpha", "zeta"]

    first = build_authoring_manifest(registry)
    second = build_authoring_manifest(registry)
    assert first == second
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    assert [item["function_name"] for item in first["functions"]] == ["alpha", "zeta"]

    alpha = first["functions"][0]
    assert alpha == {
        "function_name": "alpha",
        "arguments": [
            {
                "name": "value",
                "required": True,
                "type_hint": "string",
                "literal_only": False,
            },
            {
                "name": "mode",
                "required": False,
                "type_hint": "string",
                "literal_only": True,
                "default": "strict",
                "allowed_values": ["strict", "lenient"],
            },
        ],
        "return_type_hint": "string",
        "allowed_in_condition_flag": True,
        "allowed_in_assignment_flag": False,
        "active_flag": True,
        "description": "Apply alpha.",
        "version": "1",
    }
    assert "implementation_reference" not in alpha
    assert first["functions"][1]["active_flag"] is False


def test_manifest_serializes_the_complete_standard_function_registry():
    """The production standard registry must be directly consumable by authoring tools."""
    registry = register_standard_functions(FunctionRegistry())

    manifest = build_authoring_manifest(registry)

    assert len(manifest["functions"]) == 58
    assert [item["function_name"] for item in manifest["functions"]] == sorted(
        item["function_name"] for item in manifest["functions"]
    )
    argument_hints = set(manifest["function_argument_type_hints"])
    return_hints = set(manifest["function_return_type_hints"]["fixed"])
    dynamic_prefixes = {
        template.partition(":")[0]
        for template in manifest["function_return_type_hints"]["dynamic_templates"]
    }
    assert all(
        argument["type_hint"] in argument_hints
        for function in manifest["functions"]
        for argument in function["arguments"]
    )
    assert all(
        return_type_hint in return_hints
        or return_type_hint.partition(":")[0] in dynamic_prefixes
        for function in manifest["functions"]
        if (return_type_hint := function["return_type_hint"]) is not None
    )
    json.dumps(manifest, sort_keys=True)


def test_manifest_requires_an_explicit_registry():
    """Callers must supply the registry whose function choices they expose."""
    with pytest.raises(TypeError, match="registry must be a FunctionRegistry"):
        build_authoring_manifest(None)  # type: ignore[arg-type]
