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
    operators = {item["name"]: item for item in manifest["comparison_operators"]}

    assert list(operators) == [operator.value for operator in ComparisonOperator]
    assert operators["is_null"] == {
        "name": "is_null",
        "arity": 1,
        "right_operand_shape": "none",
        "supports_tolerance": False,
    }
    assert operators["in"]["right_operand_shape"] == "collection"
    assert operators["not_in"]["right_operand_shape"] == "collection"
    assert operators["between"]["right_operand_shape"] == "pair"
    assert operators["not_between"]["right_operand_shape"] == "pair"
    assert {
        name for name, item in operators.items() if item["supports_tolerance"]
    } == {"eq", "ne", "gt", "ge", "lt", "le", "in", "not_in"}
    assert all(
        item["arity"] == (1 if item["right_operand_shape"] == "none" else 2)
        for item in operators.values()
    )


def test_manifest_exposes_enums_literal_hints_and_build_identity():
    """Static authoring choices must come from the installed engine contract."""
    manifest = build_authoring_manifest(FunctionRegistry())

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
    json.dumps(manifest, sort_keys=True)


def test_manifest_requires_an_explicit_registry():
    """Callers must supply the registry whose function choices they expose."""
    with pytest.raises(TypeError, match="registry must be a FunctionRegistry"):
        build_authoring_manifest(None)  # type: ignore[arg-type]
