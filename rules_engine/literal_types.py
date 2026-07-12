"""Validation helpers for explicit literal value type hints."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def literal_value_type_issue(value: Any, value_type: str | None) -> str | None:
    """Return a validation message when a literal contradicts its type hint."""
    if value_type is None or value is None:
        return None
    normalized = value_type.lower()
    if normalized in {"string", "str"}:
        matches = isinstance(value, str)
    elif normalized in {"integer", "int", "long"}:
        matches = isinstance(value, int) and not isinstance(value, bool)
    elif normalized in {"number", "float", "double", "decimal"}:
        matches = isinstance(value, (int, float, Decimal)) and not isinstance(
            value,
            bool,
        )
    elif normalized in {"boolean", "bool"}:
        matches = isinstance(value, bool)
    elif normalized == "date":
        matches = isinstance(value, date) and not isinstance(value, datetime)
    elif normalized == "timestamp":
        matches = isinstance(value, datetime)
    elif normalized == "list":
        matches = isinstance(value, (list, tuple))
    elif normalized == "any":
        matches = True
    else:
        return f"Unsupported literal value_type: {value_type}."
    if matches:
        return None
    return (
        f"Literal value {value!r} does not match declared value_type "
        f"{value_type!r}."
    )
