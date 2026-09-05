"""Deterministic standard functions available to YAML-authored rules."""

from __future__ import annotations

import calendar
import re
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
    Decimal,
    InvalidOperation,
    localcontext,
)
from typing import Any

from rules_engine.decimal_math import decimal_context
from rules_engine.models import FunctionRegistryRow
from rules_engine.registry import (
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    FunctionRegistry,
)
from rules_engine.version import __version__

STANDARD_FUNCTION_VERSION = __version__
_ON_ERROR_VALUES = ("error", "null")
_ROUNDING_MODES = {
    "half_up": ROUND_HALF_UP,
    "half_even": ROUND_HALF_EVEN,
    "half_down": ROUND_HALF_DOWN,
    "up": ROUND_UP,
    "down": ROUND_DOWN,
    "ceiling": ROUND_CEILING,
    "floor": ROUND_FLOOR,
}


def substring(value: Any, start: int, length: int | None = None) -> str | None:
    """Return a SQL-style substring using a 1-based start position."""
    if value is None:
        return None
    text = str(value)
    start_index = max(_integer(start, "start") - 1, 0)
    if length is None:
        return text[start_index:]
    return text[start_index : start_index + max(_integer(length, "length"), 0)]


def left(value: Any, length: int) -> str | None:
    """Return the leftmost ``length`` characters."""
    if value is None:
        return None
    return str(value)[: max(_integer(length, "length"), 0)]


def right(value: Any, length: int) -> str | None:
    """Return the rightmost ``length`` characters."""
    if value is None:
        return None
    count = max(_integer(length, "length"), 0)
    return "" if count == 0 else str(value)[-count:]


def trim(value: Any) -> str | None:
    """Strip leading and trailing whitespace."""
    return None if value is None else str(value).strip()


def ltrim(value: Any) -> str | None:
    """Strip leading whitespace."""
    return None if value is None else str(value).lstrip()


def rtrim(value: Any) -> str | None:
    """Strip trailing whitespace."""
    return None if value is None else str(value).rstrip()


def upper(value: Any) -> str | None:
    """Convert a value to uppercase text."""
    return None if value is None else str(value).upper()


def lower(value: Any) -> str | None:
    """Convert a value to lowercase text."""
    return None if value is None else str(value).lower()


def normalize_whitespace(value: Any) -> str | None:
    """Trim text and collapse internal whitespace to one space."""
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value).strip())


def text_length(value: Any) -> int | None:
    """Return the length of a value after text conversion."""
    return None if value is None else len(str(value))


def replace(value: Any, old: str, new: str) -> str | None:
    """Replace literal text occurrences."""
    return None if value is None else str(value).replace(old, new)


def split_part(value: Any, delimiter: str, part: int) -> str | None:
    """Return a 1-based delimited part, or null when it does not exist."""
    if value is None:
        return None
    if delimiter == "":
        raise ValueError("delimiter must be non-empty.")
    position = _integer(part, "part")
    if position < 1:
        raise ValueError("part must be at least 1.")
    parts = str(value).split(delimiter)
    return parts[position - 1] if position <= len(parts) else None


def pad_left(value: Any, length: int, pad: str = " ") -> str | None:
    """Pad or truncate text on the left to an exact length."""
    return _pad(value, length, pad, left_side=True)


def pad_right(value: Any, length: int, pad: str = " ") -> str | None:
    """Pad or truncate text on the right to an exact length."""
    return _pad(value, length, pad, left_side=False)


def concat_ws(
    values: Any,
    separator: str,
    skip_nulls: bool = True,
) -> str | None:
    """Join an array of values with explicit null handling."""
    if not isinstance(skip_nulls, bool):
        raise TypeError("skip_nulls must be a boolean.")
    if values is None:
        return None
    items = _ordered_sequence(values, "values")
    if not skip_nulls and any(item is None for item in items):
        return None
    return separator.join(to_string(item) for item in items if item is not None)


def regex_extract(value: Any, pattern: str, group: int = 1) -> str | None:
    """Return one regex capture group, or null when there is no match."""
    if value is None:
        return None
    match = re.search(pattern, str(value))
    if match is None:
        return None
    try:
        return match.group(_integer(group, "group"))
    except IndexError as exc:
        raise ValueError(f"Regex group does not exist: {group!r}") from exc


def regex_replace(value: Any, pattern: str, replacement: str) -> str | None:
    """Replace regex matches in text."""
    return None if value is None else re.sub(pattern, replacement, str(value))


def regex_match(value: Any, pattern: str) -> bool | None:
    """Return whether a regex matches anywhere in the text."""
    return None if value is None else re.search(pattern, str(value)) is not None


def text_contains_any(value: Any, candidates: Any) -> bool | None:
    """Return whether text contains at least one candidate string."""
    if value is None or candidates is None:
        return None
    text = str(value)
    return any(
        candidate in text
        for candidate in _sequence(candidates, "candidates")
        if candidate is not None
    )


def is_blank(value: Any) -> bool:
    """Return true for null or whitespace-only text."""
    return value is None or not str(value).strip()


def null_if(value: Any, compare_to: Any) -> Any | None:
    """Return null when ``value`` equals ``compare_to``."""
    return None if value == compare_to else value


def coalesce(values: Any) -> Any:
    """Return the first non-null item in an ordered array."""
    if values is None:
        return None
    return next(
        (value for value in _ordered_sequence(values, "values") if value is not None),
        None,
    )


def to_string(value: Any, on_error: str = "error") -> str | None:
    """Convert a scalar value to deterministic text."""
    _validate_on_error(on_error)
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (Mapping, list, tuple, set)):
            raise ValueError("Collections cannot be converted to scalar text.")
        return str(value)
    except (TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to string: {value!r}",
            exc,
        )


def to_decimal(value: Any, on_error: str = "error") -> Decimal | None:
    """Convert a scalar value to ``Decimal``."""
    _validate_on_error(on_error)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if isinstance(value, bool):
            raise ValueError("Booleans are not decimal values.")
        parsed = Decimal(text)
        if not parsed.is_finite():
            raise ValueError("Non-finite decimals are not supported.")
        return parsed
    except (InvalidOperation, TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to decimal: {value!r}",
            exc,
        )


def to_integer(value: Any, on_error: str = "error") -> int | None:
    """Convert a scalar value to an integer without rounding."""
    _validate_on_error(on_error)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        if isinstance(value, bool):
            raise ValueError("Booleans are not integer values.")
        parsed = Decimal(str(value).strip())
        if not parsed.is_finite() or parsed != parsed.to_integral_value():
            raise ValueError("The value is not an exact integer.")
        return int(parsed)
    except (InvalidOperation, TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to integer: {value!r}",
            exc,
        )


def to_boolean(value: Any, on_error: str = "error") -> bool | None:
    """Convert common explicit boolean representations to a boolean."""
    _validate_on_error(on_error)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
        raise ValueError("Unsupported boolean representation.")
    except (TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to boolean: {value!r}",
            exc,
        )


def to_date(value: Any, on_error: str = "error") -> date | None:
    """Convert an ISO ``YYYY-MM-DD`` value to a date."""
    _validate_on_error(on_error)
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
                raise ValueError("Expected ISO date format YYYY-MM-DD.")
            return date.fromisoformat(text)
        raise ValueError("Unsupported date input type.")
    except (TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to date; expected ISO YYYY-MM-DD: {value!r}",
            exc,
        )


def to_timestamp(value: Any, on_error: str = "error") -> datetime | None:
    """Convert an ISO timestamp with an offset and normalize it to UTC."""
    _validate_on_error(on_error)
    if value is None:
        return None
    try:
        parsed = _parse_timestamp(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Timestamp must include a UTC offset.")
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to timestamp: {value!r}",
            exc,
        )


def to_timestamp_ntz(value: Any, on_error: str = "error") -> datetime | None:
    """Convert an ISO timestamp that intentionally has no time-zone offset."""
    _validate_on_error(on_error)
    if value is None:
        return None
    try:
        parsed = _parse_timestamp(value)
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            raise ValueError("Timestamp must not include a UTC offset.")
        return parsed.replace(tzinfo=None)
    except (TypeError, ValueError) as exc:
        return _conversion_failure(
            on_error,
            f"Cannot convert value to timestamp_ntz: {value!r}",
            exc,
        )


def decimal_abs(value: Any) -> Decimal | None:
    """Return the absolute decimal value."""
    parsed = _decimal_operand(value, "value")
    return None if parsed is None else parsed.copy_abs()


def decimal_add(left: Any, right: Any) -> Decimal | None:
    """Add two decimal values."""
    return _decimal_binary(left, right, "add", lambda x, y: x + y)


def decimal_subtract(left: Any, right: Any) -> Decimal | None:
    """Subtract ``right`` from ``left``."""
    return _decimal_binary(left, right, "subtract", lambda x, y: x - y)


def decimal_multiply(left: Any, right: Any) -> Decimal | None:
    """Multiply two decimal values."""
    return _decimal_binary(left, right, "multiply", lambda x, y: x * y)


def decimal_divide(
    numerator: Any,
    denominator: Any,
    scale: int = 18,
    rounding_mode: str = "half_up",
) -> Decimal | None:
    """Divide decimals and round to the requested scale; zero is an error."""
    return _divide(numerator, denominator, scale, rounding_mode, safe=False)


def decimal_safe_divide(
    numerator: Any,
    denominator: Any,
    scale: int = 18,
    rounding_mode: str = "half_up",
) -> Decimal | None:
    """Divide decimals and return null when the denominator is zero."""
    return _divide(numerator, denominator, scale, rounding_mode, safe=True)


def decimal_round(
    value: Any,
    scale: int,
    rounding_mode: str = "half_up",
) -> Decimal | None:
    """Round a decimal to an explicit scale and rounding mode."""
    parsed = _decimal_operand(value, "value")
    if parsed is None:
        return None
    return _quantize(parsed, scale, rounding_mode)


def decimal_clamp(value: Any, minimum: Any, maximum: Any) -> Decimal | None:
    """Constrain a decimal to an inclusive minimum and maximum."""
    parsed = _decimal_operand(value, "value")
    lower = _decimal_operand(minimum, "minimum")
    upper = _decimal_operand(maximum, "maximum")
    if parsed is None or lower is None or upper is None:
        return None
    if lower > upper:
        raise ValueError("minimum cannot be greater than maximum.")
    return min(max(parsed, lower), upper)


def decimal_min(left: Any, right: Any) -> Decimal | None:
    """Return the smaller of two decimals."""
    return _decimal_binary(left, right, "minimum", min)


def decimal_max(left: Any, right: Any) -> Decimal | None:
    """Return the larger of two decimals."""
    return _decimal_binary(left, right, "maximum", max)


def date_add_days(value: Any, days: Any) -> date | None:
    """Add an integral number of calendar days to a date."""
    parsed = to_date(value)
    if parsed is None:
        return None
    offset = _integer(days, "days")
    try:
        return parsed + timedelta(days=offset)
    except OverflowError as exc:
        raise ValueError(
            f"Adding {offset} days to {parsed.isoformat()} exceeds the date range."
        ) from exc


def date_add_months(value: Any, months: Any) -> date | None:
    """Add calendar months, clamping to the target month's end."""
    parsed = to_date(value)
    if parsed is None:
        return None
    offset = _integer(months, "months")
    zero_based_month = parsed.year * 12 + parsed.month - 1 + offset
    target_year, target_month_index = divmod(zero_based_month, 12)
    if not 1 <= target_year <= 9999:
        raise ValueError(f"Adding {offset} months to {parsed.isoformat()} exceeds the date range.")
    target_month = target_month_index + 1
    target_day = min(parsed.day, calendar.monthrange(target_year, target_month)[1])
    return date(target_year, target_month, target_day)


def date_add_years(value: Any, years: Any) -> date | None:
    """Add calendar years with leap-day clamping."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return date_add_months(parsed, _integer(years, "years") * 12)


def date_diff_days(start: Any, end: Any) -> int | None:
    """Return ``end`` minus ``start`` in calendar days."""
    parsed_start = to_date(start)
    parsed_end = to_date(end)
    if parsed_start is None or parsed_end is None:
        return None
    return (parsed_end - parsed_start).days


def date_diff_months(start: Any, end: Any) -> int | None:
    """Return completed whole calendar months from ``start`` to ``end``."""
    parsed_start = to_date(start)
    parsed_end = to_date(end)
    if parsed_start is None or parsed_end is None:
        return None
    if parsed_end < parsed_start:
        return -date_diff_months(parsed_end, parsed_start)
    months = (parsed_end.year - parsed_start.year) * 12 + parsed_end.month - parsed_start.month
    if date_add_months(parsed_start, months) > parsed_end:
        months -= 1
    return months


def date_diff_years(start: Any, end: Any) -> int | None:
    """Return completed whole calendar years from ``start`` to ``end``."""
    parsed_start = to_date(start)
    parsed_end = to_date(end)
    if parsed_start is None or parsed_end is None:
        return None
    if parsed_end < parsed_start:
        return -date_diff_years(parsed_end, parsed_start)
    years = parsed_end.year - parsed_start.year
    if date_add_years(parsed_start, years) > parsed_end:
        years -= 1
    return years


def date_part(value: Any, part: str) -> int | None:
    """Return a named calendar component from a date."""
    parsed = to_date(value)
    if parsed is None:
        return None
    parts = {
        "year": parsed.year,
        "quarter": (parsed.month - 1) // 3 + 1,
        "month": parsed.month,
        "day": parsed.day,
        "day_of_week": parsed.isoweekday(),
        "day_of_year": parsed.timetuple().tm_yday,
    }
    try:
        return parts[part]
    except KeyError as exc:
        raise ValueError(f"Unsupported date part: {part!r}") from exc


def month_start(value: Any) -> date | None:
    """Return the first calendar day of a date's month."""
    parsed = to_date(value)
    return None if parsed is None else parsed.replace(day=1)


def month_end(value: Any) -> date | None:
    """Return the final calendar day of a date's month."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return parsed.replace(day=calendar.monthrange(parsed.year, parsed.month)[1])


def quarter_start(value: Any) -> date | None:
    """Return the first calendar day of a date's quarter."""
    parsed = to_date(value)
    if parsed is None:
        return None
    return date(parsed.year, ((parsed.month - 1) // 3) * 3 + 1, 1)


def quarter_end(value: Any) -> date | None:
    """Return the final calendar day of a date's quarter."""
    parsed = to_date(value)
    if parsed is None:
        return None
    end_month = ((parsed.month - 1) // 3 + 1) * 3
    return date(parsed.year, end_month, calendar.monthrange(parsed.year, end_month)[1])


def year_start(value: Any) -> date | None:
    """Return the first calendar day of a date's year."""
    parsed = to_date(value)
    return None if parsed is None else date(parsed.year, 1, 1)


def year_end(value: Any) -> date | None:
    """Return the final calendar day of a date's year."""
    parsed = to_date(value)
    return None if parsed is None else date(parsed.year, 12, 31)


def first_business_day_of_month(
    value: Any,
    holidays: Any,
    weekend_days: Any = (6, 7),
) -> date | None:
    """Return the first non-weekend, non-holiday day of the month."""
    first = month_start(value)
    if first is None:
        return None
    return _month_business_day(first, holidays, weekend_days, step=1)


def last_business_day_of_month(
    value: Any,
    holidays: Any,
    weekend_days: Any = (6, 7),
) -> date | None:
    """Return the last non-weekend, non-holiday day of the month."""
    last = month_end(value)
    if last is None:
        return None
    return _month_business_day(last, holidays, weekend_days, step=-1)


def array_size(values: Any) -> int | None:
    """Return the number of items in an array."""
    return None if values is None else len(_sequence(values, "values"))


def array_contains_any(values: Any, candidates: Any) -> bool | None:
    """Return whether an array contains at least one candidate."""
    if values is None or candidates is None:
        return None
    items = _sequence(values, "values")
    sought = _sequence(candidates, "candidates")
    return any(any(item == candidate for item in items) for candidate in sought)


def array_contains_all(values: Any, candidates: Any) -> bool | None:
    """Return whether an array contains every candidate."""
    if values is None or candidates is None:
        return None
    items = _sequence(values, "values")
    sought = _sequence(candidates, "candidates")
    return all(any(item == candidate for item in items) for candidate in sought)


def array_join(
    values: Any,
    separator: str,
    skip_nulls: bool = True,
) -> str | None:
    """Join array items as text with explicit null behavior."""
    return concat_ws(values, separator, skip_nulls)


def _pad(value: Any, length: Any, pad: str, *, left_side: bool) -> str | None:
    """Implement exact-width left and right padding."""
    if value is None:
        return None
    width = max(_integer(length, "length"), 0)
    if not pad:
        raise ValueError("pad must be non-empty.")
    text = str(value)
    if len(text) >= width:
        return text[-width:] if left_side and width else text[:width]
    count = width - len(text)
    repeated = (pad * ((count + len(pad) - 1) // len(pad)))[:count]
    return repeated + text if left_side else text + repeated


def _sequence(value: Any, label: str) -> list[Any] | tuple[Any, ...] | set[Any]:
    """Return a real array-like value and reject strings and mappings."""
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not isinstance(
        value,
        (list, tuple, set),
    ):
        raise TypeError(f"{label} must be an array.")
    return value


def _ordered_sequence(value: Any, label: str) -> list[Any] | tuple[Any, ...]:
    """Require authored order for first-value and string-join operations."""
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{label} must be an ordered array (list or tuple).")
    return value


def _integer(value: Any, label: str) -> int:
    """Return a lossless integral value."""
    if isinstance(value, bool):
        raise TypeError(f"{label} must be an integer, not a boolean.")
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label} must be an integer: {value!r}") from exc
    if not numeric.is_finite() or numeric != numeric.to_integral_value():
        raise ValueError(f"{label} must be an integer: {value!r}")
    return int(numeric)


def _conversion_failure(
    on_error: str,
    message: str,
    cause: Exception,
) -> None:
    """Apply the shared converter failure policy."""
    if on_error == "null":
        return None
    raise ValueError(message) from cause


def _validate_on_error(on_error: str) -> None:
    """Require one supported conversion failure policy before reading data."""
    if on_error not in _ON_ERROR_VALUES:
        raise ValueError("on_error must be 'error' or 'null'.")


def _parse_timestamp(value: Any) -> datetime:
    """Parse one datetime or strict ISO timestamp string."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected an ISO timestamp string or datetime.")
    text = value.strip()
    if re.search(r"[T ]\d{2}:\d{2}", text) is None:
        raise ValueError("Expected an ISO timestamp containing a time.")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def _decimal_operand(value: Any, label: str) -> Decimal | None:
    """Return a finite decimal operand with null propagation."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f"{label} must be decimal-compatible, not boolean.")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be decimal-compatible: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"{label} must be a finite decimal.")
    return parsed


def _decimal_binary(
    left: Any,
    right: Any,
    label: str,
    operation: Any,
) -> Decimal | None:
    """Apply a null-propagating binary decimal operation."""
    parsed_left = _decimal_operand(left, "left")
    parsed_right = _decimal_operand(right, "right")
    if parsed_left is None or parsed_right is None:
        return None
    with localcontext(decimal_context(parsed_left, parsed_right)):
        try:
            return operation(parsed_left, parsed_right)
        except InvalidOperation as exc:
            raise ValueError(f"Cannot compute decimal {label}.") from exc


def _divide(
    numerator: Any,
    denominator: Any,
    scale: Any,
    rounding_mode: str,
    *,
    safe: bool,
) -> Decimal | None:
    """Implement strict and safe decimal division."""
    parsed_numerator = _decimal_operand(numerator, "numerator")
    parsed_denominator = _decimal_operand(denominator, "denominator")
    if parsed_numerator is None or parsed_denominator is None:
        return None
    if parsed_denominator == 0:
        if safe:
            return None
        raise ZeroDivisionError("denominator cannot be zero.")
    places = _integer(scale, "scale")
    context = decimal_context(parsed_numerator, parsed_denominator)
    quotient_digits = max(1, parsed_numerator.adjusted() - parsed_denominator.adjusted() + 1)
    context.prec = max(
        context.prec,
        quotient_digits + max(places, 0) + len(parsed_denominator.as_tuple().digits) + 2,
    )
    with localcontext(context):
        return _quantize(
            parsed_numerator / parsed_denominator,
            places,
            rounding_mode,
        )


def _quantize(value: Decimal, scale: Any, rounding_mode: str) -> Decimal:
    """Quantize a decimal using an explicit supported mode."""
    places = _integer(scale, "scale")
    if not -38 <= places <= 18:
        raise ValueError("scale must be between -38 and 18.")
    try:
        rounding = _ROUNDING_MODES[rounding_mode]
    except KeyError as exc:
        raise ValueError(f"rounding_mode must be one of {sorted(_ROUNDING_MODES)}.") from exc
    quantum = Decimal((0, (1,), -places))
    with localcontext(decimal_context(value, quantum)):
        try:
            return value.quantize(quantum, rounding=rounding)
        except InvalidOperation as exc:
            raise ValueError("Rounded decimal exceeds the supported precision.") from exc


def _month_business_day(
    candidate: date,
    holidays: Any,
    weekend_days: Any,
    *,
    step: int,
) -> date:
    """Search one month for a configured business day."""
    holiday_values = _sequence(holidays, "holidays")
    weekend_values = _sequence(weekend_days, "weekend_days")
    holiday_dates = {to_date(value) for value in holiday_values}
    if None in holiday_dates:
        raise ValueError("holidays cannot contain null or blank values.")
    weekends = {_integer(value, "weekend day") for value in weekend_values}
    if any(day < 1 or day > 7 for day in weekends):
        raise ValueError("weekend_days must contain ISO weekdays from 1 through 7.")
    if len(weekends) == 7:
        raise ValueError("weekend_days cannot mark every weekday as a weekend.")
    target_month = (candidate.year, candidate.month)
    while (candidate.year, candidate.month) == target_month:
        if candidate.isoweekday() not in weekends and candidate not in holiday_dates:
            return candidate
        candidate += timedelta(days=step)
    raise ValueError("The month contains no available business day.")


Arg = CustomFunctionArgSpec


def _spec(
    name: str,
    arguments: tuple[CustomFunctionArgSpec, ...],
    return_type_hint: str,
    description: str,
) -> CustomFunctionSpec:
    """Build one standard function registry contract."""
    return CustomFunctionSpec(
        function_name=name,
        implementation_reference=f"rules_engine.standard_functions.{name}",
        arguments=arguments,
        allowed_in_condition_flag=True,
        allowed_in_assignment_flag=True,
        return_type_hint=return_type_hint,
        description=description,
        version=STANDARD_FUNCTION_VERSION,
    )


_VALUE = Arg("value")
_ON_ERROR = Arg(
    "on_error",
    required=False,
    default="error",
    type_hint="string",
    allowed_values=_ON_ERROR_VALUES,
    literal_only=True,
)
_ROUNDING = Arg(
    "rounding_mode",
    required=False,
    default="half_up",
    type_hint="string",
    allowed_values=tuple(_ROUNDING_MODES),
    literal_only=True,
)

STANDARD_FUNCTION_SPECS = (
    _spec(
        "substring",
        (_VALUE, Arg("start", type_hint="integer"), Arg("length", False, None, "integer")),
        "string",
        "SQL-style 1-based substring extraction.",
    ),
    _spec(
        "left",
        (_VALUE, Arg("length", type_hint="integer")),
        "string",
        "Leftmost characters from text.",
    ),
    _spec(
        "right",
        (_VALUE, Arg("length", type_hint="integer")),
        "string",
        "Rightmost characters from text.",
    ),
    _spec("trim", (_VALUE,), "string", "Trim leading and trailing whitespace."),
    _spec("ltrim", (_VALUE,), "string", "Trim leading whitespace."),
    _spec("rtrim", (_VALUE,), "string", "Trim trailing whitespace."),
    _spec("upper", (_VALUE,), "string", "Convert text to uppercase."),
    _spec("lower", (_VALUE,), "string", "Convert text to lowercase."),
    _spec("normalize_whitespace", (_VALUE,), "string", "Trim and collapse repeated whitespace."),
    _spec("text_length", (_VALUE,), "integer", "Return text length."),
    _spec(
        "replace",
        (_VALUE, Arg("old", type_hint="string"), Arg("new", type_hint="string")),
        "string",
        "Replace literal text.",
    ),
    _spec(
        "split_part",
        (_VALUE, Arg("delimiter", type_hint="string"), Arg("part", type_hint="integer")),
        "string",
        "Return a 1-based delimited text part.",
    ),
    _spec(
        "pad_left",
        (_VALUE, Arg("length", type_hint="integer"), Arg("pad", False, " ", "string")),
        "string",
        "Left-pad or truncate text to an exact width.",
    ),
    _spec(
        "pad_right",
        (_VALUE, Arg("length", type_hint="integer"), Arg("pad", False, " ", "string")),
        "string",
        "Right-pad or truncate text to an exact width.",
    ),
    _spec(
        "concat_ws",
        (
            Arg("values", type_hint="ordered_sequence"),
            Arg("separator", type_hint="string"),
            Arg("skip_nulls", False, True, "boolean", literal_only=True),
        ),
        "string",
        "Join array items with explicit null handling.",
    ),
    _spec(
        "regex_extract",
        (_VALUE, Arg("pattern", type_hint="string"), Arg("group", False, 1, "integer")),
        "string",
        "Extract a regex capture group.",
    ),
    _spec(
        "regex_replace",
        (_VALUE, Arg("pattern", type_hint="string"), Arg("replacement", type_hint="string")),
        "string",
        "Replace regex matches.",
    ),
    _spec(
        "regex_match",
        (_VALUE, Arg("pattern", type_hint="string")),
        "boolean",
        "Test whether a regex matches text.",
    ),
    _spec(
        "text_contains_any",
        (_VALUE, Arg("candidates", type_hint="string_sequence")),
        "boolean",
        "Test whether text contains any candidate.",
    ),
    _spec("is_blank", (_VALUE,), "boolean", "Test for null or whitespace-only text."),
    _spec(
        "null_if",
        (_VALUE, Arg("compare_to")),
        "same_as:value",
        "Return null when two values are equal.",
    ),
    _spec(
        "coalesce",
        (Arg("values", type_hint="ordered_sequence"),),
        "common_type:values",
        "Return the first non-null array item.",
    ),
    _spec("to_string", (_VALUE, _ON_ERROR), "string", "Convert a scalar value to text."),
    _spec("to_decimal", (_VALUE, _ON_ERROR), "decimal", "Convert a scalar value to Decimal."),
    _spec(
        "to_integer", (_VALUE, _ON_ERROR), "integer", "Convert a scalar value to an exact integer."
    ),
    _spec(
        "to_boolean", (_VALUE, _ON_ERROR), "boolean", "Convert an explicit boolean representation."
    ),
    _spec("to_date", (_VALUE, _ON_ERROR), "date", "Convert an ISO YYYY-MM-DD value to a date."),
    _spec(
        "to_timestamp", (_VALUE, _ON_ERROR), "timestamp", "Convert an offset ISO timestamp to UTC."
    ),
    _spec(
        "to_timestamp_ntz",
        (_VALUE, _ON_ERROR),
        "timestamp_ntz",
        "Convert an ISO timestamp without a time zone.",
    ),
    _spec("decimal_abs", (_VALUE,), "decimal", "Return an absolute decimal value."),
    _spec("decimal_add", (Arg("left"), Arg("right")), "decimal", "Add two decimal values."),
    _spec(
        "decimal_subtract", (Arg("left"), Arg("right")), "decimal", "Subtract two decimal values."
    ),
    _spec(
        "decimal_multiply", (Arg("left"), Arg("right")), "decimal", "Multiply two decimal values."
    ),
    _spec(
        "decimal_divide",
        (Arg("numerator"), Arg("denominator"), Arg("scale", False, 18, "integer"), _ROUNDING),
        "decimal",
        "Divide and round decimal values; zero is an error.",
    ),
    _spec(
        "decimal_safe_divide",
        (Arg("numerator"), Arg("denominator"), Arg("scale", False, 18, "integer"), _ROUNDING),
        "decimal",
        "Divide decimals; zero returns null.",
    ),
    _spec(
        "decimal_round",
        (_VALUE, Arg("scale", type_hint="integer"), _ROUNDING),
        "decimal",
        "Round a decimal using an explicit mode.",
    ),
    _spec(
        "decimal_clamp",
        (_VALUE, Arg("minimum"), Arg("maximum")),
        "decimal",
        "Clamp a decimal to inclusive bounds.",
    ),
    _spec("decimal_min", (Arg("left"), Arg("right")), "decimal", "Return the smaller decimal."),
    _spec("decimal_max", (Arg("left"), Arg("right")), "decimal", "Return the larger decimal."),
    _spec(
        "date_add_days",
        (_VALUE, Arg("days", type_hint="integer")),
        "date",
        "Add integral calendar days.",
    ),
    _spec(
        "date_add_months",
        (_VALUE, Arg("months", type_hint="integer")),
        "date",
        "Add calendar months with month-end clamping.",
    ),
    _spec(
        "date_add_years",
        (_VALUE, Arg("years", type_hint="integer")),
        "date",
        "Add calendar years with leap-day clamping.",
    ),
    _spec("date_diff_days", (Arg("start"), Arg("end")), "integer", "Return elapsed calendar days."),
    _spec(
        "date_diff_months",
        (Arg("start"), Arg("end")),
        "integer",
        "Return completed whole calendar months.",
    ),
    _spec(
        "date_diff_years",
        (Arg("start"), Arg("end")),
        "integer",
        "Return completed whole calendar years.",
    ),
    _spec(
        "date_part",
        (
            _VALUE,
            Arg(
                "part",
                type_hint="string",
                allowed_values=("year", "quarter", "month", "day", "day_of_week", "day_of_year"),
                literal_only=True,
            ),
        ),
        "integer",
        "Return a named calendar component.",
    ),
    _spec("month_start", (_VALUE,), "date", "Return the first calendar day of a month."),
    _spec("month_end", (_VALUE,), "date", "Return the final calendar day of a month."),
    _spec("quarter_start", (_VALUE,), "date", "Return the first calendar day of a quarter."),
    _spec("quarter_end", (_VALUE,), "date", "Return the final calendar day of a quarter."),
    _spec("year_start", (_VALUE,), "date", "Return the first calendar day of a year."),
    _spec("year_end", (_VALUE,), "date", "Return the final calendar day of a year."),
    _spec(
        "first_business_day_of_month",
        (
            _VALUE,
            Arg("holidays", type_hint="sequence"),
            Arg("weekend_days", False, (6, 7), "integer_sequence", literal_only=True),
        ),
        "date",
        "Return the month's first configured business day.",
    ),
    _spec(
        "last_business_day_of_month",
        (
            _VALUE,
            Arg("holidays", type_hint="sequence"),
            Arg("weekend_days", False, (6, 7), "integer_sequence", literal_only=True),
        ),
        "date",
        "Return the month's last configured business day.",
    ),
    _spec(
        "array_size",
        (Arg("values", type_hint="sequence"),),
        "integer",
        "Return an array's item count.",
    ),
    _spec(
        "array_contains_any",
        (Arg("values", type_hint="sequence"), Arg("candidates", type_hint="sequence")),
        "boolean",
        "Test whether an array contains any candidate.",
    ),
    _spec(
        "array_contains_all",
        (Arg("values", type_hint="sequence"), Arg("candidates", type_hint="sequence")),
        "boolean",
        "Test whether an array contains all candidates.",
    ),
    _spec(
        "array_join",
        (
            Arg("values", type_hint="ordered_sequence"),
            Arg("separator", type_hint="string"),
            Arg("skip_nulls", False, True, "boolean", literal_only=True),
        ),
        "string",
        "Join array items as text.",
    ),
)

STANDARD_FUNCTION_IMPLEMENTATIONS = {
    spec.function_name: globals()[spec.function_name] for spec in STANDARD_FUNCTION_SPECS
}


def register_standard_functions(registry: FunctionRegistry) -> FunctionRegistry:
    """Register every standard function and return the supplied registry."""
    for spec in STANDARD_FUNCTION_SPECS:
        registry.register(spec, STANDARD_FUNCTION_IMPLEMENTATIONS[spec.function_name])
    return registry


def standard_function_rows() -> list[FunctionRegistryRow]:
    """Return persisted metadata rows for all standard function specs."""
    return [spec.to_row() for spec in STANDARD_FUNCTION_SPECS]
