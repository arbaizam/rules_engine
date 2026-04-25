"""
Canonical enumerations for rules engine metadata.

Only values defined in this module are valid. The compiler and validator do
not accept aliases because persisted metadata must be deterministic and
auditable.
"""

from __future__ import annotations

from enum import Enum


class RulesetStatus(str, Enum):
    """Lifecycle states supported by ruleset metadata."""

    PUBLISHED = "published"
    RETIRED = "retired"


class LogicalOperator(str, Enum):
    """Logical group operators."""

    ALL = "all"
    ANY = "any"


class OperandKind(str, Enum):
    """Kinds of operands that can appear in conditions and assignments."""

    FIELD = "field"
    LITERAL = "literal"
    AGGREGATE = "aggregate"
    CUSTOM_FUNCTION = "custom_function"


class AggregateScope(str, Enum):
    """Supported aggregate scopes."""

    GROUP = "group"
    DATASET = "dataset"


class ComparisonOperator(str, Enum):
    """Canonical comparison operators."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"
    LIKE = "like"
    NOT_LIKE = "not_like"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class NullInputMode(str, Enum):
    """How an operand handles null inputs before comparison or aggregation."""

    IGNORE = "ignore"
    ZERO = "zero"
    PROPAGATE = "propagate"
    ERROR = "error"


class NullResultMode(str, Enum):
    """How a condition handles a null comparison result."""

    NULL = "null"
    DEFAULT = "default"
    ERROR = "error"


class AggregateFunction(str, Enum):
    """Scalar aggregate functions supported by v1 metadata."""

    SUM = "sum"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    QUANTILE = "quantile"
    MEDIAN = "median"
    STDDEV = "stddev"
    VARIANCE = "variance"
    FIRST = "first"
    LAST = "last"


class ValidationSeverity(str, Enum):
    """Validation issue severities."""

    ERROR = "ERROR"


class ObjectType(str, Enum):
    """Object types used in validation diagnostics."""

    RULESET = "ruleset"
    RULE = "rule"
    CONDITION_GROUP = "condition_group"
    CONDITION = "condition"
    ASSIGNMENT = "assignment"
    FUNCTION = "function"


ORDER_SENSITIVE_AGGREGATES = {AggregateFunction.FIRST, AggregateFunction.LAST}
UNARY_OPERATORS = {ComparisonOperator.IS_NULL, ComparisonOperator.IS_NOT_NULL}
BINARY_OPERATORS = set(ComparisonOperator) - UNARY_OPERATORS
COLLECTION_LITERAL_OPERATORS = {
    ComparisonOperator.IN,
    ComparisonOperator.NOT_IN,
    ComparisonOperator.BETWEEN,
    ComparisonOperator.NOT_BETWEEN,
}
