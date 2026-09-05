"""Decimal arithmetic isolated from caller and worker context settings."""

from decimal import (
    MAX_EMAX,
    MIN_EMIN,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)


def decimal_context(*values: Decimal) -> Context:
    """Provide enough coefficient precision for exact finite add/multiply work.

    Include the exponent span because aligned addition can require more digits
    than either input coefficient. A fresh context also isolates rounding modes,
    exponent limits, and traps changed by an unrelated caller or custom function.
    """
    parts = [value.as_tuple() for value in values]
    exponents = [part.exponent for part in parts]
    precision = sum(len(part.digits) for part in parts)
    if exponents:
        precision += max(exponents) - min(exponents)
    return Context(
        prec=max(76, precision + 2),
        rounding=ROUND_HALF_EVEN,
        Emin=MIN_EMIN,
        Emax=MAX_EMAX,
        traps=[InvalidOperation, DivisionByZero, Overflow],
    )


def subtract_exact(left: Decimal, right: Decimal) -> Decimal:
    """Subtract finite decimals without ambient-context rounding."""
    with localcontext(decimal_context(left, right)):
        return left - right
