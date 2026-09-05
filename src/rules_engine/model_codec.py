"""Versioned persistence codec for canonical models, independent of YAML syntax.

The format contains fully explicit identities and never generates authoring
defaults. Function argument nodes distinguish data mappings from operands,
including mappings whose keys happen to match the YAML operand vocabulary.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from rules_engine.canonical_values import (
    normalize_literal,
    normalize_mapping_keys,
    validate_literal,
    validate_string_mapping_keys,
)
from rules_engine.enums import ComparisonOperator, LogicalOperator
from rules_engine.models import (
    AssignedOperand,
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Rule,
    Ruleset,
)

PERSISTENCE_FORMAT_VERSION = 1
_FORMAT_KEY = "$rules_engine_format"
_ARG_KEY = "$rules_engine_arg"
_OPERAND_KEYS = {"field", "assigned", "literal", "custom_function"}


def encode_ruleset(ruleset: Ruleset) -> dict[str, Any]:
    """Return a deterministic, typed persistence document for a ruleset."""
    return {
        _FORMAT_KEY: PERSISTENCE_FORMAT_VERSION,
        "ruleset_id": ruleset.ruleset_id,
        "ruleset_name": ruleset.ruleset_name,
        "version": ruleset.version,
        "description": ruleset.description,
        "owner": ruleset.owner,
        "owner_department": ruleset.owner_department,
        "rules": [_encode_rule(rule) for rule in ruleset.rules],
    }


def decode_ruleset(payload: Any) -> Ruleset:
    """Reconstruct only the explicitly supported persisted format."""
    data = _mapping(payload, "ruleset")
    version = data.get(_FORMAT_KEY)
    if type(version) is not int or version != PERSISTENCE_FORMAT_VERSION:
        raise ValueError(f"Unsupported ruleset persistence format: {version!r}.")
    return Ruleset(
        ruleset_id=_text(data, "ruleset_id"),
        ruleset_name=_text(data, "ruleset_name"),
        version=_text(data, "version"),
        description=_optional_text(data, "description"),
        owner=_optional_text(data, "owner"),
        owner_department=_optional_text(data, "owner_department"),
        rules=tuple(_decode_rule(rule) for rule in _list(data, "rules")),
    )


def _encode_rule(rule: Rule) -> dict[str, Any]:
    return {
        "rule_id": rule.rule_id,
        "rule_name": rule.rule_name,
        "rule_order": rule.rule_order,
        "description": rule.description,
        "active_flag": rule.active_flag,
        "stop_on_match": rule.stop_on_match,
        "when": _encode_group(rule.root_group),
        "assign": [
            {
                "assignment_id": assignment.assignment_id,
                "target_field": assignment.target_field,
                "value": _encode_operand(assignment.value),
            }
            for assignment in rule.assignments
        ],
    }


def _decode_rule(payload: Any) -> Rule:
    data = _mapping(payload, "rule")
    order = data["rule_order"]
    if type(order) is not int:
        raise ValueError("Persisted rule_order must be an integer.")
    assignments = []
    for item in _list(data, "assign"):
        assignment = _mapping(item, "assignment")
        assignments.append(
            Assignment(
                _text(assignment, "assignment_id"),
                _text(assignment, "target_field"),
                _decode_operand(assignment["value"]),
            )
        )
    return Rule(
        rule_id=_text(data, "rule_id"),
        rule_name=_text(data, "rule_name"),
        rule_order=order,
        root_group=_decode_group(data["when"]),
        assignments=tuple(assignments),
        active_flag=_boolean(data, "active_flag"),
        stop_on_match=_boolean(data, "stop_on_match"),
        description=_optional_text(data, "description"),
    )


def _encode_group(group: ConditionGroup) -> dict[str, Any]:
    return {
        "condition_group_id": group.condition_group_id,
        group.logical_operator.value: [
            *[_encode_condition(condition) for condition in group.conditions],
            *[_encode_group(child) for child in group.groups],
        ],
    }


def _decode_group(payload: Any) -> ConditionGroup:
    data = _mapping(payload, "condition group")
    logical_keys = set(data) & {operator.value for operator in LogicalOperator}
    if len(logical_keys) != 1:
        raise ValueError("Persisted condition group requires exactly one logical operator.")
    operator = next(iter(logical_keys))
    conditions = []
    groups = []
    for child in _list(data, operator):
        child = _mapping(child, "group child")
        if "condition_group_id" in child:
            groups.append(_decode_group(child))
        else:
            conditions.append(_decode_condition(child))
    return ConditionGroup(
        _text(data, "condition_group_id"),
        LogicalOperator(operator),
        tuple(conditions),
        tuple(groups),
    )


def _encode_condition(condition: Condition) -> dict[str, Any]:
    payload = {
        "condition_id": condition.condition_id,
        "left": _encode_operand(condition.left),
        "operator": condition.operator.value,
        "tolerance_abs": format(condition.tolerance_abs, "f"),
        "error_on_null": condition.error_on_null,
        "active_flag": condition.active_flag,
    }
    if condition.right is not None:
        payload["right"] = _encode_operand(condition.right)
    return payload


def _decode_condition(payload: Any) -> Condition:
    data = _mapping(payload, "condition")
    tolerance = Decimal(data["tolerance_abs"])
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("Persisted tolerance_abs must be a finite non-negative Decimal.")
    return Condition(
        condition_id=_text(data, "condition_id"),
        left=_decode_operand(data["left"]),
        operator=ComparisonOperator(data["operator"]),
        right=_decode_operand(data["right"]) if "right" in data else None,
        tolerance_abs=tolerance,
        error_on_null=_boolean(data, "error_on_null"),
        active_flag=_boolean(data, "active_flag"),
    )


def _literal(value: Any, value_type: str | None = None) -> Any:
    validate_literal(value, value_type)
    return normalize_literal(value, normalize_untyped_float=False)


def _encode_operand(operand: Operand) -> dict[str, Any]:
    if isinstance(operand, FieldOperand):
        payload = {"field": operand.field_name}
    elif isinstance(operand, AssignedOperand):
        payload = {"assigned": operand.target_field}
    elif isinstance(operand, LiteralOperand):
        payload = {"literal": _literal(operand.value, operand.value_type)}
        if operand.value_type is not None:
            payload["value_type"] = operand.value_type
    elif isinstance(operand, CustomFunctionOperand):
        validate_string_mapping_keys(operand.args)
        payload = {
            "custom_function": {
                "name": operand.function_name,
                "args": normalize_mapping_keys(operand.args, _encode_argument, "Function args"),
            }
        }
    else:
        raise ValueError(f"Unsupported operand type: {type(operand).__name__}.")
    default = operand.default_if_null
    if default is not None:
        if not isinstance(default, LiteralOperand) or default.value is None:
            raise ValueError("default_if_null must be a non-null LiteralOperand.")
        if default.default_if_null is not None:
            raise ValueError("default_if_null cannot contain another default_if_null.")
        payload["default_if_null"] = (
            _encode_operand(default)
            if default.value_type is not None or isinstance(default.value, Mapping)
            else _literal(default.value)
        )
    return payload


def _decode_operand(payload: Any) -> Operand:
    data = _mapping(payload, "operand")
    kinds = set(data) & _OPERAND_KEYS
    if len(kinds) != 1:
        raise ValueError("Persisted operand must contain exactly one operand kind.")
    kind = next(iter(kinds))
    default = None
    if "default_if_null" in data:
        raw = data["default_if_null"]
        if isinstance(raw, Mapping):
            default = _decode_operand(raw)
        else:
            default = LiteralOperand(_literal(raw))
        if not isinstance(default, LiteralOperand) or default.value is None:
            raise ValueError("Persisted default_if_null must be a non-null literal.")
        if default.default_if_null is not None:
            raise ValueError("Persisted default_if_null cannot be nested.")
    if kind == "field":
        return FieldOperand(_text(data, kind), default)
    if kind == "assigned":
        return AssignedOperand(_text(data, kind), default)
    if kind == "literal":
        value_type = _optional_text(data, "value_type")
        return LiteralOperand(_literal(data[kind], value_type), value_type, default)
    function = _mapping(data[kind], "custom function")
    args = _mapping(function["args"], "function arguments")
    return CustomFunctionOperand(
        _text(function, "name"),
        normalize_mapping_keys(args, _decode_argument, "Function args"),
        default,
    )


def _encode_argument(value: Any) -> Any:
    if isinstance(value, Operand):
        return {_ARG_KEY: "operand", "value": _encode_operand(value)}
    if isinstance(value, Mapping):
        validate_string_mapping_keys(value)
        return {
            _ARG_KEY: "mapping",
            "value": normalize_mapping_keys(value, _encode_argument, "Argument mapping"),
        }
    if isinstance(value, (tuple, set)):
        items = sorted(value, key=repr) if isinstance(value, set) else value
        return {_ARG_KEY: type(value).__name__, "value": [_encode_argument(x) for x in items]}
    if isinstance(value, list):
        return [_encode_argument(item) for item in value]
    return _literal(value)


def _decode_argument(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_argument(item) for item in value]
    if not isinstance(value, Mapping):
        return _literal(value)
    if set(value) != {_ARG_KEY, "value"}:
        raise ValueError("Persisted function argument requires an explicit node tag.")
    kind, raw = value[_ARG_KEY], value["value"]
    if kind == "operand":
        return _decode_operand(raw)
    if kind == "mapping":
        return normalize_mapping_keys(_mapping(raw, "argument mapping"), _decode_argument, "Args")
    if kind in {"tuple", "set"}:
        if not isinstance(raw, list):
            raise ValueError(f"Persisted {kind} argument requires an array.")
        decoded = [_decode_argument(item) for item in raw]
        if kind == "tuple":
            return tuple(decoded)
        try:
            return set(decoded)
        except TypeError as exc:
            raise ValueError("Persisted set argument must contain only hashable values.") from exc
    raise ValueError(f"Unsupported function argument node: {kind!r}.")


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"Persisted {label} must be a mapping with string keys.")
    return value


def _text(data: Mapping[str, Any], key: str) -> str:
    value = data[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Persisted {key} must be a non-empty string.")
    return value


def _optional_text(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"Persisted {key} must be a string or null.")
    return value


def _boolean(data: Mapping[str, Any], key: str) -> bool:
    value = data[key]
    if not isinstance(value, bool):
        raise ValueError(f"Persisted {key} must be a boolean.")
    return value


def _list(data: Mapping[str, Any], key: str) -> list[Any]:
    value = data[key]
    if not isinstance(value, list):
        raise ValueError(f"Persisted {key} must be an array.")
    return value
