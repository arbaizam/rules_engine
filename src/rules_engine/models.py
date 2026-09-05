"""
Domain models for rules engine metadata.

These dataclasses define the canonical in-memory representation of a ruleset
and the row objects used for Delta persistence.

Design notes
------------
The YAML authoring format is tree-shaped because condition groups are easiest
to read that way.

The persisted representation treats a ruleset version as one immutable
metadata document with selected queryable columns for lifecycle, provenance,
hashing, and summary counts.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    ObjectType,
    OperandKind,
)


@dataclass
class ValidationIssue:
    """
    One validation issue produced by the rules engine.

    Parameters
    ----------
    check_name : str
        Stable identifier for the validation check.
    message : str
        Human-readable issue description.
    object_type : ObjectType
        Type of object that produced the issue.
    object_id : str
        Identifier of the object that produced the issue.
    details : dict[str, Any] | None
        Optional structured diagnostics.
    """

    check_name: str
    message: str
    object_type: ObjectType
    object_id: str
    details: dict[str, Any] | None = None


@dataclass
class ValidationResult:
    """
    Structured validation result for a ruleset validation run.

    Notes
    -----
    ``passed`` is derived from the current issue list so callers cannot observe
    stale pass/fail state.
    """

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """
        Return whether validation has no issues.

        Returns
        -------
        bool
            True when no issues were collected.
        """
        return not self.has_errors()

    def add_issue(
        self,
        check_name: str,
        message: str,
        object_type: ObjectType,
        object_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Add one validation issue.

        Parameters
        ----------
        check_name : str
            Stable validation check name.
        message : str
            Human-readable issue text.
        object_type : ObjectType
            Object type for diagnostics.
        object_id : str
            Object identifier for diagnostics.
        details : dict[str, Any] | None, default None
            Optional structured context.
        """
        self.issues.append(
            ValidationIssue(
                check_name=check_name,
                message=message,
                object_type=object_type,
                object_id=object_id,
                details=details,
            )
        )

    def has_errors(self) -> bool:
        """
        Return whether any validation issue exists.

        Returns
        -------
        bool
            True when one or more issues exist.
        """
        return bool(self.issues)

    def to_text(self) -> str:
        """
        Render validation issues as readable multi-line text.

        Returns
        -------
        str
            Human-readable validation summary.
        """
        if not self.issues:
            return "Validation passed with no issues."
        lines = [f"Validation failed with {len(self.issues)} issue(s):"]
        for issue in self.issues:
            details_text = f" | details={issue.details}" if issue.details else ""
            lines.append(f"[ERROR] {issue.check_name}: {issue.message}{details_text}")
        return "\n".join(lines)


@dataclass(frozen=True)
class FieldOperand:
    """
    Field-reference operand resolved against the incoming row set.
    """

    field_name: str
    default_if_null: LiteralOperand | None = None
    kind: OperandKind = field(default=OperandKind.FIELD, init=False)


@dataclass(frozen=True)
class AssignedOperand:
    """Reference to the latest value committed by an earlier matched rule."""

    target_field: str
    default_if_null: LiteralOperand | None = None
    kind: OperandKind = field(default=OperandKind.ASSIGNED, init=False)


@dataclass(frozen=True)
class LiteralOperand:
    """
    Literal operand.
    """

    value: Any
    value_type: str | None = None
    default_if_null: LiteralOperand | None = None
    kind: OperandKind = field(default=OperandKind.LITERAL, init=False)


@dataclass(frozen=True)
class CustomFunctionOperand:
    """
    Operand resolved through the custom function registry.

    Parameters
    ----------
    function_name : str
        Name registered in ``FunctionRegistry``.
    args : Mapping[str, Any]
        Keyword arguments supplied to the function. Arguments are metadata and
        are validated against the registry contract before publish.
    """

    function_name: str
    args: Mapping[str, Any] = field(default_factory=dict)
    default_if_null: LiteralOperand | None = None
    kind: OperandKind = field(default=OperandKind.CUSTOM_FUNCTION, init=False)


Operand = AssignedOperand | FieldOperand | LiteralOperand | CustomFunctionOperand


def iter_nested_operands(value: Any) -> Iterator[Operand]:
    """Yield operands inside argument collections using the shared traversal."""
    from rules_engine.traversal import iter_nested_operands as walk

    yield from walk(value)


@dataclass(frozen=True)
class Condition:
    """
    One comparison condition.
    """

    condition_id: str
    left: Operand
    operator: ComparisonOperator
    right: Operand | None
    tolerance_abs: Decimal
    error_on_null: bool = False
    active_flag: bool = True


@dataclass(frozen=True)
class ConditionGroup:
    """
    Logical condition group.
    """

    condition_group_id: str
    logical_operator: LogicalOperator
    conditions: tuple[Condition, ...] = ()
    groups: tuple[ConditionGroup, ...] = ()


@dataclass(frozen=True)
class Assignment:
    """
    Rule assignment emitted when a rule matches.
    """

    assignment_id: str
    target_field: str
    value: Operand


@dataclass(frozen=True)
class Rule:
    """
    Compiled rule metadata.
    """

    rule_id: str
    rule_name: str
    rule_order: int
    root_group: ConditionGroup
    assignments: tuple[Assignment, ...]
    active_flag: bool = True
    stop_on_match: bool = False
    description: str | None = None


@dataclass(frozen=True)
class Ruleset:
    """
    Compiled ruleset metadata.
    """

    ruleset_id: str
    ruleset_name: str
    version: str
    rules: tuple[Rule, ...]
    description: str | None = None
    owner: str | None = None
    owner_department: str | None = None


@dataclass(frozen=True)
class RulesetVersionRow:
    """Authoritative ruleset version table row."""

    ruleset_id: str
    ruleset_name: str
    version: str
    status: str
    description: str | None
    payload_json: str
    content_hash: str
    rule_count: int
    condition_count: int
    assignment_count: int
    custom_function_count: int
    owner: str | None
    owner_department: str | None
    published_by: str | None
    published_at: str | None
    retired_by: str | None
    retired_at: str | None


@dataclass(frozen=True)
class FunctionRegistryRow:
    """Persisted custom function registry metadata row."""

    function_name: str
    implementation_reference: str
    arg_contract_payload: dict[str, Any]
    return_type_hint: str | None
    allowed_in_condition_flag: bool
    allowed_in_assignment_flag: bool
    active_flag: bool
    description: str | None
    version: str | None


@dataclass(frozen=True)
class ResolvedConditionTrace:
    """Runtime condition trace emitted for one evaluated condition."""

    condition_id: str
    passed: bool
    condition_group_id: str | None = None
    condition_group_operator: str | None = None
    active_flag: bool = True
    operator: str | None = None
    tolerance_abs: str | None = None
    left: Mapping[str, Any] | None = None
    right: Mapping[str, Any] | None = None
    comparison_result: bool | None = None


@dataclass(frozen=True)
class RuleExecutionTrace:
    """Runtime trace emitted for one evaluated rule."""

    rule_id: str
    condition_traces: tuple[ResolvedConditionTrace, ...]
    assignments_applied: tuple[str, ...]
    rule_name: str | None = None
    rule_order: int | None = None
