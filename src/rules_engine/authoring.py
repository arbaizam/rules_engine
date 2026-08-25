"""Public authoring contract for rules-engine consumers."""

from __future__ import annotations

from typing import Any

from rules_engine.enums import (
    COLLECTION_LITERAL_OPERATORS,
    UNARY_OPERATORS,
    ComparisonOperator,
    LogicalOperator,
    OperandKind,
)
from rules_engine.registry import (
    DYNAMIC_RETURN_TYPE_HINT_TEMPLATES,
    SUPPORTED_ARGUMENT_TYPE_HINTS,
    SUPPORTED_RETURN_TYPE_HINTS,
    FunctionRegistry,
)
from rules_engine.version import __version__

AUTHORING_MANIFEST_VERSION = 1

# Canonical editor choices and their accepted persisted aliases. Collection
# shapes and null are represented by literal values, not by ``value_type``.
LITERAL_TYPE_HINTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("string", ("str",)),
    ("integer", ("int", "long")),
    ("decimal", ()),
    ("double", ("float", "number")),
    ("boolean", ("bool",)),
    ("date", ()),
    ("timestamp", ()),
    ("timestamp_ntz", ()),
)

_PAIR_OPERATORS = {
    ComparisonOperator.BETWEEN,
    ComparisonOperator.NOT_BETWEEN,
}
_TOLERANCE_OPERATORS = {
    ComparisonOperator.EQ,
    ComparisonOperator.NE,
    ComparisonOperator.GT,
    ComparisonOperator.GE,
    ComparisonOperator.LT,
    ComparisonOperator.LE,
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
}


def literal_type_hint_names() -> tuple[str, ...]:
    """Return every canonical and alias literal type hint."""
    return tuple(
        hint
        for canonical_name, aliases in LITERAL_TYPE_HINTS
        for hint in (canonical_name, *aliases)
    )


def build_authoring_manifest(registry: FunctionRegistry) -> dict[str, Any]:
    """Return the deterministic JSON-compatible authoring contract.

    The manifest owns validation-relevant choices only. Applications retain
    responsibility for display labels, help text, layout, and mutable draft
    state.
    """
    if not isinstance(registry, FunctionRegistry):
        raise TypeError("registry must be a FunctionRegistry.")

    return {
        "manifest_version": AUTHORING_MANIFEST_VERSION,
        "engine_version": __version__,
        "comparison_operators": [
            {
                "name": operator.value,
                "arity": 1 if operator in UNARY_OPERATORS else 2,
                "right_operand_shape": _right_operand_shape(operator),
                "supports_tolerance": operator in _TOLERANCE_OPERATORS,
            }
            for operator in ComparisonOperator
        ],
        "logical_operators": [operator.value for operator in LogicalOperator],
        "operand_kinds": [kind.value for kind in OperandKind],
        "literal_type_hints": [
            {
                "name": canonical_name,
                "aliases": list(aliases),
            }
            for canonical_name, aliases in LITERAL_TYPE_HINTS
        ],
        "function_argument_type_hints": sorted(SUPPORTED_ARGUMENT_TYPE_HINTS),
        "function_return_type_hints": {
            "fixed": sorted(SUPPORTED_RETURN_TYPE_HINTS),
            "dynamic_templates": list(DYNAMIC_RETURN_TYPE_HINT_TEMPLATES),
        },
        "functions": [specification.to_authoring_payload() for specification in registry.specs()],
    }


def _right_operand_shape(operator: ComparisonOperator) -> str:
    """Return the authoring shape required for an operator's right operand."""
    if operator in UNARY_OPERATORS:
        return "none"
    if operator in _PAIR_OPERATORS:
        return "pair"
    if operator in COLLECTION_LITERAL_OPERATORS:
        return "collection"
    return "any"


__all__ = [
    "AUTHORING_MANIFEST_VERSION",
    "LITERAL_TYPE_HINTS",
    "build_authoring_manifest",
    "literal_type_hint_names",
]
