"""Canonical finite literal values and deterministic extended JSON.

This module owns supported scalar/collection types, type-hint normalization,
string mapping keys, and lossless JSON envelopes. Authoring syntax and operand
recognition belong to the YAML compiler and model codec respectively.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

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

_LITERAL_TYPE_HINT_CANONICAL_NAMES = {
    hint: canonical_name
    for canonical_name, aliases in LITERAL_TYPE_HINTS
    for hint in (canonical_name, *aliases)
}
_LONG_MIN = -(2**63)
_LONG_MAX = 2**63 - 1
_JSON_TYPE_KEY = "$rules_engine_type"
_JSON_VALUE_KEY = "value"


class _PreservedFloat(float):
    """Mark an exported authoring scalar whose binary-float kind must survive."""


def normalize_literal(
    value: Any,
    value_type: str | None = None,
    *,
    normalize_untyped_float: bool = True,
) -> Any:
    """Preserve YAML-authored fractional numbers as exact decimals.

    PyYAML normally parses an unquoted fractional literal as ``float``.
    Financial rules must not silently switch to binary floating-point, so
    untyped floats are normalized recursively through their YAML text
    representation. Explicit floating-point hints retain float semantics.
    """
    if value_type is not None and (not isinstance(value_type, str) or not value_type.strip()):
        raise ValueError("Literal value_type must be a non-empty string when provided.")
    if isinstance(value, _PreservedFloat):
        value = float(value)
        normalize_untyped_float = False
    normalized_type = value_type.lower() if isinstance(value_type, str) else None
    if isinstance(value, list):
        return [
            normalize_literal(item, value_type, normalize_untyped_float=normalize_untyped_float)
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            normalize_literal(item, value_type, normalize_untyped_float=normalize_untyped_float)
            for item in value
        )
    if isinstance(value, set):
        return {
            normalize_literal(item, value_type, normalize_untyped_float=normalize_untyped_float)
            for item in value
        }
    if isinstance(value, Mapping):
        return normalize_mapping_keys(
            value,
            lambda item: normalize_literal(
                item, value_type, normalize_untyped_float=normalize_untyped_float
            ),
            "Literal mapping",
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Numeric literals must be finite.")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ValueError("Decimal literals must be finite.")
    if value is not None and not isinstance(value, (str, bool, int, float, Decimal, date)):
        raise ValueError(f"Unsupported literal type: {type(value).__name__}.")
    if normalized_type is not None:
        return _normalize_typed_literal(value, normalized_type)
    if isinstance(value, float) and normalize_untyped_float:
        return Decimal(str(value))
    return value


def validate_literal(value: Any, value_type: str | None = None) -> None:
    """Require supported finite values and truthful scalar type metadata.

    Unlike authoring normalization, validation does not coerce direct models.
    A declared known type must already describe the stored Python value.
    """
    normalized = normalize_literal(value, value_type, normalize_untyped_float=False)
    validate_string_mapping_keys(value)
    if value_type is not None and value_type.lower() in _LITERAL_TYPE_HINT_CANONICAL_NAMES:
        if not _same_scalar_types(value, normalized):
            raise ValueError(
                f"Literal value does not have the declared {value_type!r} type; "
                "normalize the value before constructing the operand."
            )


def validate_string_mapping_keys(value: Any) -> None:
    """Require canonical data mappings to already have string keys.

    Authoring normalization may convert YAML keys. A canonical model must not
    change the meaning of a literal mapping when it is published and loaded.
    Operand objects encountered inside raw argument collections are opaque.
    """
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"Canonical mapping keys must be strings, found {key!r}.")
            validate_string_mapping_keys(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            validate_string_mapping_keys(item)


def _same_scalar_types(original: Any, normalized: Any) -> bool:
    """Compare recursive scalar types while allowing normalized mapping keys."""
    if isinstance(original, Mapping):
        return all(_same_scalar_types(item, normalized[str(key)]) for key, item in original.items())
    if isinstance(original, (list, tuple)):
        return all(_same_scalar_types(a, b) for a, b in zip(original, normalized, strict=True))
    if isinstance(original, set):
        return {type(item) for item in original} == {type(item) for item in normalized}
    return type(original) is type(normalized)


def _normalize_typed_literal(value: Any, normalized_type: str) -> Any:
    """Normalize one non-collection literal according to its declared type."""
    if value is None:
        return None
    canonical_type = _LITERAL_TYPE_HINT_CANONICAL_NAMES.get(
        normalized_type,
        normalized_type,
    )
    if canonical_type == "string":
        return _normalize_string_literal(value)
    if canonical_type == "integer":
        return _normalize_integer_literal(value)
    if canonical_type == "double":
        return _normalize_double_literal(value)
    if canonical_type == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            try:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
                    raise ValueError("Expected ISO date format YYYY-MM-DD.")
                return date.fromisoformat(text)
            except ValueError as exc:
                raise ValueError(
                    f"Date literal must use ISO YYYY-MM-DD format, found {value!r}."
                ) from exc
        raise ValueError(f"Date literal must be a date or ISO YYYY-MM-DD string, found {value!r}.")
    if canonical_type in {"timestamp", "timestamp_ntz"}:
        from rules_engine.standard_functions import to_timestamp, to_timestamp_ntz

        converter = to_timestamp if canonical_type == "timestamp" else to_timestamp_ntz
        try:
            return converter(value)
        except (TypeError, ValueError) as exc:
            representation = (
                "an ISO timestamp with a UTC offset"
                if canonical_type == "timestamp"
                else "an ISO timestamp without a UTC offset"
            )
            raise ValueError(
                f"{canonical_type} literal must be a datetime or {representation}, found {value!r}."
            ) from exc
    if canonical_type == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"Boolean literal must be an actual boolean, found {value!r}.")
        return value
    if canonical_type == "decimal":
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"Decimal literal must be numeric, found {value!r}.") from exc
        if not decimal_value.is_finite():
            raise ValueError("Decimal literals must be finite.")
        return decimal_value
    return value


def _normalize_string_literal(value: Any) -> str:
    """Require one explicitly string-typed literal value."""
    if not isinstance(value, str):
        raise ValueError(f"String literal must be a string, found {value!r}.")
    return value


def _normalize_integer_literal(value: Any) -> int:
    """Return one lossless signed 64-bit integer literal value."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"Integer literal must be numeric, found {value!r}.")
    converted = int(value)
    if value != converted:
        raise ValueError(f"Integer literal must not have a fractional component, found {value!r}.")
    if not _LONG_MIN <= converted <= _LONG_MAX:
        raise ValueError(f"Integer literal must fit a signed 64-bit value, found {value!r}.")
    return converted


def _normalize_double_literal(value: Any) -> float:
    """Return one finite explicitly floating-point literal value."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"Floating-point literal must be numeric, found {value!r}.")
    try:
        converted = float(value)
    except OverflowError as exc:
        raise ValueError("Floating-point literals must be finite.") from exc
    if not math.isfinite(converted):
        raise ValueError("Floating-point literals must be finite.")
    return converted


def normalize_mapping_keys(
    value: Mapping[Any, Any],
    normalize_value: Callable[[Any], Any],
    label: str,
) -> dict[str, Any]:
    """Normalize persisted mapping keys without silently merging values."""
    normalized: dict[str, Any] = {}
    original_keys: dict[str, Any] = {}
    for original_key, item in value.items():
        key = str(original_key)
        if key in normalized:
            first_key = original_keys[key]
            raise ValueError(
                f"{label} contains keys {first_key!r} and {original_key!r} "
                f"that both normalize to {key!r}."
            )
        normalized[key] = normalize_value(item)
        original_keys[key] = original_key
    return normalized


def canonical_json_value(value: Any) -> Any:
    """Return a detached JSON-ready graph without losing Python numeric kinds.

    Unlike the numeric Decimal tokens in persisted model documents, Decimal
    values in this graph use explicit envelopes. Consumers may safely pass the
    result to ordinary ``json.dumps`` without rounding through binary floats.
    ``decode_json_types`` restores the original literal types.
    """
    return json.loads(
        canonical_json_dumps(value),
        parse_float=lambda token: {_JSON_TYPE_KEY: "decimal", _JSON_VALUE_KEY: token},
    )


def canonical_json_dumps(value: Any) -> str:
    """Encode deterministic JSON while preserving supported Python types.

    Decimal values are emitted as JSON numbers rather than strings. Integral
    Decimals retain a fractional marker so ``parse_float=Decimal`` restores
    their numeric kind during deserialization. Types absent from JSON use
    reserved, collision-safe envelopes decoded by :func:`decode_json_types`.
    """
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Persisted Decimal values must be finite.")
        text = str(value)
        # ``e0`` forces json.loads to route an integral Decimal through
        # parse_float=Decimal instead of silently restoring it as an int.
        return text if "." in text or "e" in text.lower() else f"{text}e0"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Persisted float values must be finite.")
        return _tagged_json("float", json.dumps(repr(value)))
    if isinstance(value, datetime):
        return _tagged_json("datetime", json.dumps(value.isoformat()))
    if isinstance(value, date):
        return _tagged_json("date", json.dumps(value.isoformat()))
    if isinstance(value, tuple):
        items = ",".join(canonical_json_dumps(item) for item in value)
        return _tagged_json("tuple", f"[{items}]")
    if isinstance(value, set):
        items = sorted(canonical_json_dumps(item) for item in value)
        return _tagged_json("set", "[" + ",".join(items) + "]")
    if isinstance(value, Mapping):
        if _JSON_TYPE_KEY in value:
            return _tagged_json("mapping", _canonical_mapping_dumps(value))
        return _canonical_mapping_dumps(value)
    if isinstance(value, list):
        return "[" + ",".join(canonical_json_dumps(item) for item in value) + "]"
    if value is not None and not isinstance(value, (str, bool, int)):
        raise ValueError(f"Unsupported persisted literal type: {type(value).__name__}.")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_mapping_dumps(value: Mapping[Any, Any]) -> str:
    """Encode mapping contents without interpreting reserved persistence keys."""
    normalized = normalize_mapping_keys(value, lambda item: item, "Persisted mapping")
    encoded_items = (
        f"{json.dumps(key, separators=(',', ':'))}:{canonical_json_dumps(item)}"
        for key, item in sorted(normalized.items())
    )
    return "{" + ",".join(encoded_items) + "}"


def _tagged_json(type_name: str, encoded_value: str) -> str:
    """Return one deterministic extended-JSON value envelope."""
    return (
        "{"
        f"{json.dumps(_JSON_TYPE_KEY)}:{json.dumps(type_name)},"
        f"{json.dumps(_JSON_VALUE_KEY)}:{encoded_value}"
        "}"
    )


def decode_json_types(value: Any) -> Any:
    """Restore Python-only literal types from persisted extended JSON."""
    if isinstance(value, list):
        return [decode_json_types(item) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) != {_JSON_TYPE_KEY, _JSON_VALUE_KEY}:
        return {key: decode_json_types(item) for key, item in value.items()}

    type_name = value[_JSON_TYPE_KEY]
    encoded_value = value[_JSON_VALUE_KEY]
    if type_name == "decimal":
        return _decode_decimal(encoded_value)
    if type_name == "float":
        return _decode_float(encoded_value)
    if type_name == "date":
        if not isinstance(encoded_value, str):
            raise _invalid_envelope(type_name, "value must be an ISO string")
        try:
            return date.fromisoformat(encoded_value)
        except ValueError as exc:
            raise _invalid_envelope(type_name, "value must be a valid ISO date") from exc
    if type_name == "datetime":
        if not isinstance(encoded_value, str):
            raise _invalid_envelope(type_name, "value must be an ISO string")
        try:
            return datetime.fromisoformat(encoded_value)
        except ValueError as exc:
            raise _invalid_envelope(
                type_name,
                "value must be a valid ISO datetime",
            ) from exc
    if type_name == "tuple":
        if not isinstance(encoded_value, list):
            raise _invalid_envelope(type_name, "value must be an array")
        return tuple(decode_json_types(item) for item in encoded_value)
    if type_name == "set":
        if not isinstance(encoded_value, list):
            raise _invalid_envelope(type_name, "value must be an array")
        try:
            return {decode_json_types(item) for item in encoded_value}
        except TypeError as exc:
            raise _invalid_envelope(
                type_name,
                "decoded items must be hashable",
            ) from exc
    if type_name == "mapping":
        if not isinstance(encoded_value, dict):
            raise _invalid_envelope(type_name, "value must be an object")
        # Decode the mapping's values only. Recursing on the inner mapping as a
        # whole would reinterpret an escaped user-owned $rules_engine_type key.
        encoded_items = ((key, decode_json_types(item)) for key, item in encoded_value.items())
        return dict(encoded_items)
    raise ValueError(f"Unsupported persisted literal type envelope: {type_name!r}.")


def _decode_float(value: Any) -> float:
    """Restore a finite binary float without changing it into a Decimal."""
    if not isinstance(value, str):
        raise _invalid_envelope("float", "value must be a numeric string")
    try:
        restored = float(value)
    except ValueError as exc:
        raise _invalid_envelope("float", "value must be numeric") from exc
    if not math.isfinite(restored):
        raise _invalid_envelope("float", "value must be finite")
    return restored


def _decode_decimal(value: Any) -> Decimal:
    """Restore a finite exact Decimal from a JSON-ready graph."""
    if not isinstance(value, str):
        raise _invalid_envelope("decimal", "value must be a numeric string")
    try:
        restored = Decimal(value)
    except InvalidOperation as exc:
        raise _invalid_envelope("decimal", "value must be numeric") from exc
    if not restored.is_finite():
        raise _invalid_envelope("decimal", "value must be finite")
    return restored


def _invalid_envelope(type_name: Any, reason: str) -> ValueError:
    """Return one uniform corruption error for an extended-JSON envelope."""
    return ValueError(f"Invalid persisted {type_name!r} envelope: {reason}.")
