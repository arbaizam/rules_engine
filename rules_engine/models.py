"""
Domain models for rules engine metadata.

These dataclasses define the canonical in-memory representation of a ruleset
and the flattened row objects used for Delta persistence.

Design notes
------------
The YAML authoring format is tree-shaped because condition groups are easiest
to read that way.

The persisted representation is relational because ruleset metadata needs to
be queryable in Databricks. Variable operand shapes are stored as structured
payloads while stable fields remain first-class columns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from rules_engine.enums import (
    AggregateFunction,
    AggregateScope,
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
    The result object mirrors the hierarchy engine pattern: callers can inspect
    issues programmatically, render a readable text summary, and persist the
    same details as validation-result metadata rows.
    """

    passed: bool = True
    issues: list[ValidationIssue] = field(default_factory=list)

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

    def finalize(self) -> "ValidationResult":
        """
        Finalize the pass/fail flag from collected issues.

        Returns
        -------
        ValidationResult
            The same object with ``passed`` updated.
        """
        self.passed = not self.has_errors()
        return self

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
class OrderBySpec:
    """
    Explicit ordering specification for order-sensitive aggregates.

    Parameters
    ----------
    field : str
        Field used for aggregate ordering.
    direction : str
        Ordering direction. Valid values are validated separately.
    """

    field: str
    direction: str


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


@dataclass(frozen=True)
class RowFilterPredicate:
    """
    Row-level predicate used inside a filtered aggregate.

    Notes
    -----
    Aggregate filters intentionally contain only row-level predicates. Nested
    aggregates are rejected by validation.
    """

    left: Operand
    operator: ComparisonOperator
    right: Operand | None
    tolerance_abs: Decimal
    null_input_mode: NullInputMode
    null_result_mode: NullResultMode
    null_default_value: Any | None = None


@dataclass(frozen=True)
class AggregateFilter:
    """
    Filter applied before aggregate calculation.
    """

    logical_operator: LogicalOperator
    predicates: tuple[RowFilterPredicate, ...]


@dataclass(frozen=True)
class AggregateOperand:
    """
    Scalar aggregate operand.

    Parameters
    ----------
    function : AggregateFunction
        Aggregate function name.
    field_name : str
        Field aggregated over the incoming row set.
    scope : AggregateScope
        Explicit aggregate scope, either ``group`` or ``dataset``.
    by : tuple[str, ...]
        Grouping fields for ``group`` scope. Empty for ``dataset`` scope.
    args : Mapping[str, Any]
        Function-specific arguments such as ``q`` for quantile.
    filter : AggregateFilter | None
        Optional row-level aggregate filter.
    order_by : tuple[OrderBySpec, ...]
        Explicit ordering for order-sensitive aggregates.
    null_input_mode : NullInputMode
        Null handling for aggregate inputs.
    null_result_mode : NullResultMode
        Null handling for aggregate results.
    """

    function: AggregateFunction
    field_name: str
    scope: AggregateScope
    by: tuple[str, ...]
    args: Mapping[str, Any]
    filter: AggregateFilter | None
    order_by: tuple[OrderBySpec, ...]
    null_input_mode: NullInputMode
    null_result_mode: NullResultMode
    null_default_value: Any | None = None
    kind: OperandKind = field(default=OperandKind.AGGREGATE, init=False)

    @classmethod
    def build(
        cls,
        function: AggregateFunction,
        field_name: str,
        scope: AggregateScope,
        by: Sequence[str] | None,
        args: Mapping[str, Any] | None,
        filter_: AggregateFilter | None,
        order_by: Sequence[OrderBySpec] | None,
        null_input_mode: NullInputMode,
        null_result_mode: NullResultMode,
        null_default_value: Any | None = None,
    ) -> "AggregateOperand":
        """
        Build an aggregate operand while materializing sequence fields.
        """
        return cls(
            function=function,
            field_name=field_name,
            scope=scope,
            by=tuple(by or ()),
            args=dict(args or {}),
            filter=filter_,
            order_by=tuple(order_by or ()),
            null_input_mode=null_input_mode,
            null_result_mode=null_result_mode,
            null_default_value=null_default_value,
        )


Operand = FieldOperand | LiteralOperand | AggregateOperand | CustomFunctionOperand


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
    groups: tuple["ConditionGroup", ...] = ()


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


@dataclass(frozen=True)
class RulesetRow:
    """Ruleset table row."""

    ruleset_id: str
    ruleset_name: str
    version: str
    status: str
    description: str | None
    created_by: str
    created_at: str
    published_by: str | None
    published_at: str | None
    content_hash: str


@dataclass(frozen=True)
class RuleRow:
    """Rule table row."""

    rule_id: str
    ruleset_id: str
    ruleset_version: str
    rule_name: str
    rule_order: int
    active_flag: bool
    stop_on_match: bool
    description: str | None
    created_by: str
    created_at: str


@dataclass(frozen=True)
class ConditionGroupRow:
    """Condition-group table row."""

    condition_group_id: str
    ruleset_id: str
    ruleset_version: str
    rule_id: str
    parent_condition_group_id: str | None
    logical_operator: str
    group_order: int
    created_by: str
    created_at: str


@dataclass(frozen=True)
class ConditionRow:
    """Condition table row."""

    condition_id: str
    ruleset_id: str
    ruleset_version: str
    rule_id: str
    condition_group_id: str
    condition_order: int
    left_operand_kind: str
    left_operand_payload: dict[str, Any]
    operator: str
    right_operand_kind: str | None
    right_operand_payload: dict[str, Any] | None
    aggregate_scope: str | None
    tolerance_abs: str
    null_input_mode: str
    null_result_mode: str
    null_default_value: Any | None
    active_flag: bool
    created_by: str
    created_at: str


@dataclass(frozen=True)
class AssignmentRow:
    """Assignment table row."""

    assignment_id: str
    ruleset_id: str
    ruleset_version: str
    rule_id: str
    assignment_order: int
    target_field: str
    assign_operand_kind: str
    assign_operand_payload: dict[str, Any]
    created_by: str
    created_at: str


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
class ValidationResultRow:
    """Persisted validation issue row."""

    ruleset_id: str
    version: str
    severity: str
    check_name: str
    message: str
    object_type: str
    object_id: str
    details_payload: dict[str, Any] | None
    run_at: str


@dataclass(frozen=True)
class DeltaRows:
    """
    Complete relational row set for one ruleset.
    """

    ruleset_row: RulesetRow
    rule_rows: list[RuleRow]
    condition_group_rows: list[ConditionGroupRow]
    condition_rows: list[ConditionRow]
    assignment_rows: list[AssignmentRow]


@dataclass(frozen=True)
class ResolvedConditionTrace:
    """Runtime condition trace placeholder for future evaluation support."""

    condition_id: str
    passed: bool


@dataclass(frozen=True)
class RuleExecutionTrace:
    """Runtime rule trace placeholder for future evaluation support."""

    rule_id: str
    condition_traces: tuple[ResolvedConditionTrace, ...]
    assignments_applied: tuple[str, ...]
    matched: bool
