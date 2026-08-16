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

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
    ObjectType,
    OperandKind,
    RulesetStatus,
    ValidationSeverity,
)


@dataclass
class ValidationIssue:
    """
    One validation issue produced by the rules engine.

    Parameters
    ----------
    severity : ValidationSeverity
        Severity classification for the issue.
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

    severity: ValidationSeverity
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
        Return whether validation has no error-severity issues.

        Returns
        -------
        bool
            True when no collected issue has severity ``ERROR``.
        """
        return not self.has_errors()

    def add_issue(
        self,
        severity: ValidationSeverity,
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
        severity : ValidationSeverity
            Issue severity.
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
                severity=severity,
                check_name=check_name,
                message=message,
                object_type=object_type,
                object_id=object_id,
                details=details,
            )
        )

    def has_errors(self) -> bool:
        """
        Return whether any validation issue is an error.

        Returns
        -------
        bool
            True when one or more issues have severity ``ERROR``.
        """
        return any(issue.severity is ValidationSeverity.ERROR for issue in self.issues)

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
        lines = [f"Validation passed: {self.passed}"]
        for issue in self.issues:
            details_text = f" | details={issue.details}" if issue.details else ""
            lines.append(
                f"[{issue.severity.value}] {issue.check_name}: "
                f"{issue.message}{details_text}"
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class FieldOperand:
    """
    Field-reference operand resolved against the incoming row set.
    """

    field_name: str
    kind: OperandKind = field(default=OperandKind.FIELD, init=False)


@dataclass(frozen=True)
class LiteralOperand:
    """
    Literal operand.
    """

    value: Any
    value_type: str | None = None
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
    kind: OperandKind = field(default=OperandKind.CUSTOM_FUNCTION, init=False)


Operand = FieldOperand | LiteralOperand | CustomFunctionOperand


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
    null_input_mode: NullInputMode
    null_result_mode: NullResultMode
    null_default_value: Any | None = None
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
    status: RulesetStatus
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
    effective_start_date: str
    effective_end_date: str
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
    null_input_mode: str | None = None
    null_result_mode: str | None = None
    null_default_value: Any | None = None
    left: Mapping[str, Any] | None = None
    right: Mapping[str, Any] | None = None
    comparison_result: bool | None = None


@dataclass(frozen=True)
class RuleExecutionTrace:
    """Runtime trace emitted for one evaluated rule."""

    rule_id: str
    condition_traces: tuple[ResolvedConditionTrace, ...]
    assignments_applied: tuple[str, ...]
    matched: bool
    rule_name: str | None = None
    rule_order: int | None = None
