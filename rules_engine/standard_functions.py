"""
Reusable custom functions for common ruleset authoring needs.

The functions are plain Python callables and can be registered into a
``FunctionRegistry`` for runtime use. They intentionally return ``None`` for
``None`` inputs unless the function is explicitly about defaulting.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from rules_engine.registry import CustomFunctionSpec, FunctionRegistry

STANDARD_FUNCTION_VERSION = "1.1.0"


def substring(value: Any, start: int, length: int | None = None) -> str | None:
    """
    Return a SQL-style substring using a 1-based start position.
    """
    if value is None:
        return None
    text = str(value)
    start_index = max(int(start) - 1, 0)
    if length is None:
        return text[start_index:]
    return text[start_index : start_index + max(int(length), 0)]


def left(value: Any, length: int) -> str | None:
    """
    Return the leftmost ``length`` characters.
    """
    if value is None:
        return None
    return str(value)[: max(int(length), 0)]


def right(value: Any, length: int) -> str | None:
    """
    Return the rightmost ``length`` characters.
    """
    if value is None:
        return None
    count = max(int(length), 0)
    if count == 0:
        return ""
    return str(value)[-count:]


def trim(value: Any) -> str | None:
    """
    Strip leading and trailing whitespace.
    """
    return None if value is None else str(value).strip()


def upper(value: Any) -> str | None:
    """
    Convert a value to uppercase text.
    """
    return None if value is None else str(value).upper()


def lower(value: Any) -> str | None:
    """
    Convert a value to lowercase text.
    """
    return None if value is None else str(value).lower()


def normalize_whitespace(value: Any) -> str | None:
    """
    Trim text and collapse internal whitespace to a single space.
    """
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value).strip())


def length(value: Any) -> int | None:
    """
    Return string length.
    """
    return None if value is None else len(str(value))


def regex_extract(value: Any, pattern: str, group: int = 1) -> str | None:
    """
    Return one regex capture group, or ``None`` when there is no match.
    """
    if value is None:
        return None
    match = re.search(pattern, str(value))
    if match is None:
        return None
    return match.group(int(group))


def regex_replace(value: Any, pattern: str, replacement: str) -> str | None:
    """
    Replace regex matches in text.
    """
    if value is None:
        return None
    return re.sub(pattern, replacement, str(value))


def contains_any(value: Any, candidates: list[Any] | tuple[Any, ...]) -> bool | None:
    """
    Return whether text contains any candidate value.
    """
    if value is None:
        return None
    text = str(value)
    return any(str(candidate) in text for candidate in candidates)


def default_if_null(value: Any, default: Any) -> Any:
    """
    Return ``default`` when value is ``None``.
    """
    return default if value is None else value


def null_if(value: Any, compare_to: Any) -> Any | None:
    """
    Return ``None`` when value equals ``compare_to``.
    """
    return None if value == compare_to else value


def to_number(value: Any) -> Decimal | None:
    """
    Convert a value to ``Decimal``; return ``None`` for null or blank values.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Cannot convert value to number: {value!r}") from exc


def to_date(value: Any) -> date | None:
    """Return an ISO date value, dropping the time from a datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
            raise ValueError(
                f"Cannot convert value to ISO date (YYYY-MM-DD): {value!r}"
            )
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"Cannot convert value to ISO date (YYYY-MM-DD): {value!r}"
            ) from exc
    raise ValueError(f"Cannot convert value to date: {value!r}")


def date_add_days(value: Any, days: Any) -> date | None:
    """Add an integral number of calendar days to a date."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return parsed + timedelta(days=_integer_offset(days, "days"))


def date_add_months(value: Any, months: Any) -> date | None:
    """Add calendar months, clamping the day to the target month's end."""
    parsed = to_date(value)
    if parsed is None:
        return None
    offset = _integer_offset(months, "months")
    zero_based_month = parsed.year * 12 + parsed.month - 1 + offset
    target_year, target_month_index = divmod(zero_based_month, 12)
    if not 1 <= target_year <= 9999:
        raise ValueError(
            f"Adding {offset} months to {parsed.isoformat()} exceeds the date range."
        )
    target_month = target_month_index + 1
    target_day = min(
        parsed.day,
        calendar.monthrange(target_year, target_month)[1],
    )
    return date(target_year, target_month, target_day)


def date_add_years(value: Any, years: Any) -> date | None:
    """Add calendar years using the same month-end clamping as date_add_months."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return date_add_months(parsed, _integer_offset(years, "years") * 12)


def date_diff_days(start: Any, end: Any) -> int | None:
    """Return end minus start in calendar days."""
    parsed_start = to_date(start)
    parsed_end = to_date(end)
    if parsed_start is None or parsed_end is None:
        return None
    return (parsed_end - parsed_start).days


def month_start(value: Any) -> date | None:
    """Return the first day of a date's month."""
    parsed = to_date(value)
    return None if parsed is None else parsed.replace(day=1)


def month_end(value: Any) -> date | None:
    """Return the final day of a date's month."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return parsed.replace(day=calendar.monthrange(parsed.year, parsed.month)[1])


def _integer_offset(value: Any, label: str) -> int:
    """Return a lossless integral date offset."""
    if isinstance(value, bool):
        raise TypeError(f"{label} must be an integer, not a boolean.")
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label} must be an integer: {value!r}") from exc
    if not numeric.is_finite() or numeric != numeric.to_integral_value():
        raise ValueError(f"{label} must be an integer: {value!r}")
    return int(numeric)


STANDARD_FUNCTION_SPECS = (
    CustomFunctionSpec(
        function_name="substring",
        implementation_reference="rules_engine.standard_functions.substring",
        arg_names=("value", "start", "length"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="SQL-style 1-based substring extraction.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="left",
        implementation_reference="rules_engine.standard_functions.left",
        arg_names=("value", "length"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Leftmost characters from a text value.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="right",
        implementation_reference="rules_engine.standard_functions.right",
        arg_names=("value", "length"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Rightmost characters from a text value.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="trim",
        implementation_reference="rules_engine.standard_functions.trim",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Trim leading and trailing whitespace.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="upper",
        implementation_reference="rules_engine.standard_functions.upper",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Uppercase text conversion.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="lower",
        implementation_reference="rules_engine.standard_functions.lower",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Lowercase text conversion.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="normalize_whitespace",
        implementation_reference="rules_engine.standard_functions.normalize_whitespace",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Trim and collapse repeated whitespace.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="length",
        implementation_reference="rules_engine.standard_functions.length",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="integer",
        description="String length.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="regex_extract",
        implementation_reference="rules_engine.standard_functions.regex_extract",
        arg_names=("value", "pattern", "group"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Extract a regex capture group from text.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="regex_replace",
        implementation_reference="rules_engine.standard_functions.regex_replace",
        arg_names=("value", "pattern", "replacement"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="string",
        description="Replace regex matches in text.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="contains_any",
        implementation_reference="rules_engine.standard_functions.contains_any",
        arg_names=("value", "candidates"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=False,
        return_type_hint="boolean",
        description="Check whether text contains any candidate string.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="default_if_null",
        implementation_reference="rules_engine.standard_functions.default_if_null",
        arg_names=("value", "default"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="any",
        description="Replace null values with a default.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="null_if",
        implementation_reference="rules_engine.standard_functions.null_if",
        arg_names=("value", "compare_to"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="any",
        description="Return null when a value equals a comparison value.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="to_number",
        implementation_reference="rules_engine.standard_functions.to_number",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="decimal",
        description="Convert text or numeric input to Decimal.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="to_date",
        implementation_reference="rules_engine.standard_functions.to_date",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Convert an ISO YYYY-MM-DD value to a date.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="date_add_days",
        implementation_reference="rules_engine.standard_functions.date_add_days",
        arg_names=("value", "days"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Add integral calendar days to a date.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="date_add_months",
        implementation_reference="rules_engine.standard_functions.date_add_months",
        arg_names=("value", "months"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Add calendar months with month-end clamping.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="date_add_years",
        implementation_reference="rules_engine.standard_functions.date_add_years",
        arg_names=("value", "years"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Add calendar years with leap-day clamping.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="date_diff_days",
        implementation_reference="rules_engine.standard_functions.date_diff_days",
        arg_names=("start", "end"),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="integer",
        description="Return end minus start in calendar days.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="month_start",
        implementation_reference="rules_engine.standard_functions.month_start",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Return the first day of a date's month.",
        version=STANDARD_FUNCTION_VERSION,
    ),
    CustomFunctionSpec(
        function_name="month_end",
        implementation_reference="rules_engine.standard_functions.month_end",
        arg_names=("value",),
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint="date",
        description="Return the final day of a date's month.",
        version=STANDARD_FUNCTION_VERSION,
    ),
)


STANDARD_FUNCTION_IMPLEMENTATIONS = {
    "substring": lambda **kwargs: substring(
        kwargs["value"],
        kwargs["start"],
        kwargs["length"],
    ),
    "left": lambda **kwargs: left(kwargs["value"], kwargs["length"]),
    "right": lambda **kwargs: right(kwargs["value"], kwargs["length"]),
    "trim": lambda **kwargs: trim(kwargs["value"]),
    "upper": lambda **kwargs: upper(kwargs["value"]),
    "lower": lambda **kwargs: lower(kwargs["value"]),
    "normalize_whitespace": lambda **kwargs: normalize_whitespace(kwargs["value"]),
    "length": lambda **kwargs: length(kwargs["value"]),
    "regex_extract": lambda **kwargs: regex_extract(
        kwargs["value"],
        kwargs["pattern"],
        kwargs["group"],
    ),
    "regex_replace": lambda **kwargs: regex_replace(
        kwargs["value"],
        kwargs["pattern"],
        kwargs["replacement"],
    ),
    "contains_any": lambda **kwargs: contains_any(
        kwargs["value"],
        kwargs["candidates"],
    ),
    "default_if_null": lambda **kwargs: default_if_null(
        kwargs["value"],
        kwargs["default"],
    ),
    "null_if": lambda **kwargs: null_if(kwargs["value"], kwargs["compare_to"]),
    "to_number": lambda **kwargs: to_number(kwargs["value"]),
    "to_date": lambda **kwargs: to_date(kwargs["value"]),
    "date_add_days": lambda **kwargs: date_add_days(
        kwargs["value"],
        kwargs["days"],
    ),
    "date_add_months": lambda **kwargs: date_add_months(
        kwargs["value"],
        kwargs["months"],
    ),
    "date_add_years": lambda **kwargs: date_add_years(
        kwargs["value"],
        kwargs["years"],
    ),
    "date_diff_days": lambda **kwargs: date_diff_days(
        kwargs["start"],
        kwargs["end"],
    ),
    "month_start": lambda **kwargs: month_start(kwargs["value"]),
    "month_end": lambda **kwargs: month_end(kwargs["value"]),
}


def register_standard_functions(registry: FunctionRegistry) -> FunctionRegistry:
    """
    Register all standard functions and return the supplied registry.
    """
    for spec in STANDARD_FUNCTION_SPECS:
        registry.register(spec, STANDARD_FUNCTION_IMPLEMENTATIONS[spec.function_name])
    return registry


def standard_function_rows():
    """
    Return persisted metadata rows for the standard function specs.
    """
    return [spec.to_row() for spec in STANDARD_FUNCTION_SPECS]
