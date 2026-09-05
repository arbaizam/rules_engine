"""Shared Spark decimal inference and exact-fit helpers."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import types as T

INTEGRAL_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
INTEGRAL_RANK = {
    T.ByteType: 0,
    T.ShortType: 1,
    T.IntegerType: 2,
    T.LongType: 3,
}
INTEGRAL_LIMITS = {
    T.ByteType: (-(2**7), 2**7 - 1),
    T.ShortType: (-(2**15), 2**15 - 1),
    T.IntegerType: (-(2**31), 2**31 - 1),
    T.LongType: (-(2**63), 2**63 - 1),
}
INTEGRAL_DECIMAL_DIGITS = {
    T.ByteType: 3,
    T.ShortType: 5,
    T.IntegerType: 10,
    T.LongType: 19,
}
TIMESTAMP_NTZ_TYPE = getattr(T, "TimestampNTZType", None)
TIMESTAMP_TYPES = (
    (T.TimestampType, TIMESTAMP_NTZ_TYPE) if TIMESTAMP_NTZ_TYPE is not None else (T.TimestampType,)
)
TEMPORAL_TYPES = (T.DateType, *TIMESTAMP_TYPES)


def decimal_literal_type(value: Decimal) -> T.DecimalType | None:
    """Return the smallest Spark decimal type that exactly holds ``value``."""
    if not value.is_finite():
        return None
    _, digits, exponent = value.as_tuple()
    scale = max(-exponent, 0)
    integral_digits = max(len(digits) + exponent, 0)
    precision = max(integral_digits + scale, 1)
    if precision > 38:
        return None
    return T.DecimalType(precision, scale)


def decimal_value_fits(value: Decimal, data_type: T.DecimalType) -> bool:
    """Return whether ``value`` fits a Spark decimal without loss."""
    if not value.is_finite():
        return False
    if value.is_zero():
        return True
    _, authored_digits, authored_exponent = value.as_tuple()
    digits = list(authored_digits)
    exponent = authored_exponent
    while digits and digits[-1] == 0:
        digits.pop()
        exponent += 1
    integral_digits = max(len(digits) + exponent, 0)
    return exponent >= -data_type.scale and integral_digits <= data_type.precision - data_type.scale
