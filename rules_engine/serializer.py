"""
Serialization between canonical models and Delta row payloads.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from enum import Enum
import hashlib
import json
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
from rules_engine.models import (
    AggregateFilter,
    AggregateOperand,
    Assignment,
    AssignmentRow,
    Condition,
    ConditionGroup,
    ConditionGroupRow,
    ConditionRow,
    CustomFunctionOperand,
    DeltaRows,
    FieldOperand,
    FunctionRegistryRow,
    LiteralOperand,
    Operand,
    OrderBySpec,
    RowFilterPredicate,
    Rule,
    RuleRow,
    Ruleset,
    RulesetRow,
    ValidationResult,
    ValidationResultRow,
)
from rules_engine.registry import CustomFunctionSpec


class DeltaRowSerializer:
    """
    Convert canonical ruleset models to and from flattened row objects.
    """

    def serialize_ruleset(
        self,
        ruleset: Ruleset,
        *,
        created_by: str = "unknown",
        created_at: str = "unknown",
        published_by: str | None = None,
        published_at: str | None = None,
    ) -> DeltaRows:
        """
        Serialize one ruleset to Delta-shaped row dataclasses.
        """
        rule_rows: list[RuleRow] = []
        group_rows: list[ConditionGroupRow] = []
        condition_rows: list[ConditionRow] = []
        assignment_rows: list[AssignmentRow] = []

        for rule in ruleset.rules:
            rule_rows.append(
                RuleRow(
                    rule_id=rule.rule_id,
                    ruleset_id=ruleset.ruleset_id,
                    ruleset_version=ruleset.version,
                    rule_name=rule.rule_name,
                    rule_order=rule.rule_order,
                    active_flag=rule.active_flag,
                    stop_on_match=rule.stop_on_match,
                    description=rule.description,
                    created_by=created_by,
                    created_at=created_at,
                )
            )
            self._serialize_group(
                rule.rule_id,
                ruleset.ruleset_id,
                ruleset.version,
                rule.root_group,
                None,
                1,
                group_rows,
                condition_rows,
                created_by,
                created_at,
            )
            for assignment_order, assignment in enumerate(rule.assignments, start=1):
                assignment_rows.append(
                    AssignmentRow(
                        assignment_id=assignment.assignment_id,
                        ruleset_id=ruleset.ruleset_id,
                        ruleset_version=ruleset.version,
                        rule_id=rule.rule_id,
                        assignment_order=assignment_order,
                        target_field=assignment.target_field,
                        assign_operand_kind=assignment.value.kind.value,
                        assign_operand_payload=self.serialize_operand(assignment.value),
                        created_by=created_by,
                        created_at=created_at,
                    )
                )

        return DeltaRows(
            ruleset_row=RulesetRow(
                ruleset_id=ruleset.ruleset_id,
                ruleset_name=ruleset.ruleset_name,
                version=ruleset.version,
                status=ruleset.status.value,
                description=ruleset.description,
                created_by=created_by,
                created_at=created_at,
                published_by=published_by,
                published_at=published_at,
                content_hash=self.content_hash(ruleset),
            ),
            rule_rows=rule_rows,
            condition_group_rows=group_rows,
            condition_rows=condition_rows,
            assignment_rows=assignment_rows,
        )

    def deserialize_ruleset(self, rows: DeltaRows) -> Ruleset:
        """
        Reconstruct a canonical ruleset from Delta-shaped rows.
        """
        conditions_by_group: dict[str, list[ConditionRow]] = defaultdict(list)
        for row in rows.condition_rows:
            conditions_by_group[row.condition_group_id].append(row)
        for condition_list in conditions_by_group.values():
            condition_list.sort(key=lambda item: item.condition_order)

        groups_by_parent: dict[tuple[str, str | None], list[ConditionGroupRow]] = defaultdict(list)
        for row in rows.condition_group_rows:
            groups_by_parent[(row.rule_id, row.parent_condition_group_id)].append(row)
        for group_list in groups_by_parent.values():
            group_list.sort(key=lambda item: item.group_order)

        assignments_by_rule: dict[str, list[AssignmentRow]] = defaultdict(list)
        for row in rows.assignment_rows:
            assignments_by_rule[row.rule_id].append(row)
        for assignment_list in assignments_by_rule.values():
            assignment_list.sort(key=lambda item: item.assignment_order)

        rules: list[Rule] = []
        for rule_row in sorted(rows.rule_rows, key=lambda item: item.rule_order):
            root_rows = groups_by_parent.get((rule_row.rule_id, None), [])
            if len(root_rows) != 1:
                raise ValueError(f"Rule {rule_row.rule_id} must have exactly one root group.")
            root_group = self._deserialize_group(
                root_rows[0],
                groups_by_parent,
                conditions_by_group,
            )
            assignments = tuple(
                Assignment(
                    assignment_id=row.assignment_id,
                    target_field=row.target_field,
                    value=self.deserialize_operand(
                        OperandKind(row.assign_operand_kind),
                        row.assign_operand_payload,
                    ),
                )
                for row in assignments_by_rule.get(rule_row.rule_id, [])
            )
            rules.append(
                Rule(
                    rule_id=rule_row.rule_id,
                    rule_name=rule_row.rule_name,
                    rule_order=rule_row.rule_order,
                    root_group=root_group,
                    assignments=assignments,
                    active_flag=rule_row.active_flag,
                    stop_on_match=rule_row.stop_on_match,
                    description=rule_row.description,
                )
            )

        return Ruleset(
            ruleset_id=rows.ruleset_row.ruleset_id,
            ruleset_name=rows.ruleset_row.ruleset_name,
            version=rows.ruleset_row.version,
            status=RulesetStatus(rows.ruleset_row.status),
            rules=tuple(rules),
            description=rows.ruleset_row.description,
        )

    def serialize_validation_result(
        self,
        ruleset_id: str,
        version: str,
        validation_result: ValidationResult,
        *,
        run_at: str = "unknown",
    ) -> list[ValidationResultRow]:
        """
        Serialize validation results to persisted rows.

        A clean validation still emits one positive audit row so the persisted
        table proves validation ran rather than relying on absence of issues.
        """
        if not validation_result.issues:
            return [
                ValidationResultRow(
                    ruleset_id=ruleset_id,
                    version=version,
                    severity=ValidationSeverity.INFO.value,
                    check_name="VALIDATION_PASSED",
                    message="Validation passed with no issues.",
                    object_type=ObjectType.RULESET.value,
                    object_id=ruleset_id,
                    details_payload={"issue_count": 0},
                    run_at=run_at,
                )
            ]
        return [
            ValidationResultRow(
                ruleset_id=ruleset_id,
                version=version,
                severity=issue.severity.value,
                check_name=issue.check_name,
                message=issue.message,
                object_type=issue.object_type.value,
                object_id=issue.object_id,
                details_payload=issue.details,
                run_at=run_at,
            )
            for issue in validation_result.issues
        ]

    def content_hash(self, ruleset: Ruleset) -> str:
        """
        Return a deterministic SHA-256 hash of canonical ruleset content.

        Lifecycle provenance is intentionally excluded. The hash changes when
        rule semantics or authoring metadata change, not when a draft is saved
        or published by a different operator.
        """
        payload = self._json_safe(asdict(ruleset))
        payload.pop("status", None)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def serialize_function_spec(self, spec: CustomFunctionSpec) -> FunctionRegistryRow:
        """
        Serialize a custom function spec to a registry row.
        """
        return spec.to_row()

    def deserialize_function_spec(self, row: FunctionRegistryRow) -> CustomFunctionSpec:
        """
        Reconstruct a custom function spec from a registry row.
        """
        return CustomFunctionSpec.from_row(row)

    def _serialize_group(
        self,
        rule_id: str,
        ruleset_id: str,
        ruleset_version: str,
        group: ConditionGroup,
        parent_group_id: str | None,
        group_order: int,
        group_rows: list[ConditionGroupRow],
        condition_rows: list[ConditionRow],
        created_by: str,
        created_at: str,
    ) -> None:
        group_rows.append(
            ConditionGroupRow(
                condition_group_id=group.condition_group_id,
                ruleset_id=ruleset_id,
                ruleset_version=ruleset_version,
                rule_id=rule_id,
                parent_condition_group_id=parent_group_id,
                logical_operator=group.logical_operator.value,
                group_order=group_order,
                created_by=created_by,
                created_at=created_at,
            )
        )
        for condition_order, condition in enumerate(group.conditions, start=1):
            condition_rows.append(
                ConditionRow(
                    condition_id=condition.condition_id,
                    ruleset_id=ruleset_id,
                    ruleset_version=ruleset_version,
                    rule_id=rule_id,
                    condition_group_id=group.condition_group_id,
                    condition_order=condition_order,
                    left_operand_kind=condition.left.kind.value,
                    left_operand_payload=self.serialize_operand(condition.left),
                    operator=condition.operator.value,
                    right_operand_kind=(
                        condition.right.kind.value if condition.right is not None else None
                    ),
                    right_operand_payload=(
                        self.serialize_operand(condition.right)
                        if condition.right is not None
                        else None
                    ),
                    aggregate_scope=self._condition_aggregate_scope(condition),
                    tolerance_abs=str(condition.tolerance_abs),
                    null_input_mode=condition.null_input_mode.value,
                    null_result_mode=condition.null_result_mode.value,
                    null_default_value=condition.null_default_value,
                    active_flag=condition.active_flag,
                    created_by=created_by,
                    created_at=created_at,
                )
            )
        for nested_order, nested_group in enumerate(group.groups, start=1):
            self._serialize_group(
                rule_id,
                ruleset_id,
                ruleset_version,
                nested_group,
                group.condition_group_id,
                nested_order,
                group_rows,
                condition_rows,
                created_by,
                created_at,
            )

    def _deserialize_group(
        self,
        row: ConditionGroupRow,
        groups_by_parent: dict[tuple[str, str | None], list[ConditionGroupRow]],
        conditions_by_group: dict[str, list[ConditionRow]],
    ) -> ConditionGroup:
        return ConditionGroup(
            condition_group_id=row.condition_group_id,
            logical_operator=LogicalOperator(row.logical_operator),
            conditions=tuple(
                self._deserialize_condition(condition_row)
                for condition_row in conditions_by_group.get(row.condition_group_id, [])
            ),
            groups=tuple(
                self._deserialize_group(child, groups_by_parent, conditions_by_group)
                for child in groups_by_parent.get((row.rule_id, row.condition_group_id), [])
            ),
        )

    def _deserialize_condition(self, row: ConditionRow) -> Condition:
        return Condition(
            condition_id=row.condition_id,
            left=self.deserialize_operand(OperandKind(row.left_operand_kind), row.left_operand_payload),
            operator=ComparisonOperator(row.operator),
            right=(
                self.deserialize_operand(OperandKind(row.right_operand_kind), row.right_operand_payload)
                if row.right_operand_kind is not None and row.right_operand_payload is not None
                else None
            ),
            tolerance_abs=Decimal(row.tolerance_abs),
            null_input_mode=NullInputMode(row.null_input_mode),
            null_result_mode=NullResultMode(row.null_result_mode),
            null_default_value=row.null_default_value,
            active_flag=row.active_flag,
        )

    def serialize_operand(self, operand: Operand) -> dict[str, Any]:
        """
        Serialize an operand into a structured payload.
        """
        if isinstance(operand, FieldOperand):
            return {"field_name": operand.field_name}
        if isinstance(operand, LiteralOperand):
            return {"value": operand.value, "value_type": operand.value_type}
        if isinstance(operand, CustomFunctionOperand):
            return {"function_name": operand.function_name, "args": dict(operand.args)}
        if isinstance(operand, AggregateOperand):
            return {
                "function": operand.function.value,
                "field_name": operand.field_name,
                "scope": operand.scope.value,
                "by": list(operand.by),
                "args": dict(operand.args),
                "filter": self._serialize_aggregate_filter(operand.filter),
                "order_by": [
                    {"field": item.field, "direction": item.direction}
                    for item in operand.order_by
                ],
                "null_input_mode": operand.null_input_mode.value,
                "null_result_mode": operand.null_result_mode.value,
                "null_default_value": operand.null_default_value,
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def deserialize_operand(self, kind: OperandKind, payload: dict[str, Any]) -> Operand:
        """
        Reconstruct an operand from kind and structured payload.
        """
        if kind is OperandKind.FIELD:
            return FieldOperand(field_name=str(payload["field_name"]))
        if kind is OperandKind.LITERAL:
            return LiteralOperand(value=payload.get("value"), value_type=payload.get("value_type"))
        if kind is OperandKind.CUSTOM_FUNCTION:
            return CustomFunctionOperand(
                function_name=str(payload["function_name"]),
                args=dict(payload.get("args", {})),
            )
        if kind is OperandKind.AGGREGATE:
            return AggregateOperand.build(
                function=AggregateFunction(payload["function"]),
                field_name=str(payload["field_name"]),
                scope=AggregateScope(payload["scope"]),
                by=tuple(payload.get("by", [])),
                args=dict(payload.get("args", {})),
                filter_=self._deserialize_aggregate_filter(payload.get("filter")),
                order_by=tuple(
                    OrderBySpec(field=item["field"], direction=item["direction"])
                    for item in payload.get("order_by", [])
                ),
                null_input_mode=NullInputMode(payload["null_input_mode"]),
                null_result_mode=NullResultMode(payload["null_result_mode"]),
                null_default_value=payload.get("null_default_value"),
            )
        raise ValueError(f"Unsupported operand kind: {kind.value}")

    def _serialize_aggregate_filter(
        self,
        aggregate_filter: AggregateFilter | None,
    ) -> dict[str, Any] | None:
        if aggregate_filter is None:
            return None
        return {
            "logical_operator": aggregate_filter.logical_operator.value,
            "predicates": [
                {
                    "left_kind": predicate.left.kind.value,
                    "left": self.serialize_operand(predicate.left),
                    "operator": predicate.operator.value,
                    "right_kind": (
                        predicate.right.kind.value if predicate.right is not None else None
                    ),
                    "right": (
                        self.serialize_operand(predicate.right)
                        if predicate.right is not None
                        else None
                    ),
                    "tolerance_abs": str(predicate.tolerance_abs),
                    "null_input_mode": predicate.null_input_mode.value,
                    "null_result_mode": predicate.null_result_mode.value,
                    "null_default_value": predicate.null_default_value,
                }
                for predicate in aggregate_filter.predicates
            ],
        }

    def _deserialize_aggregate_filter(self, payload: dict[str, Any] | None) -> AggregateFilter | None:
        if payload is None:
            return None
        return AggregateFilter(
            logical_operator=LogicalOperator(payload["logical_operator"]),
            predicates=tuple(
                RowFilterPredicate(
                    left=self.deserialize_operand(
                        OperandKind(item["left_kind"]),
                        item["left"],
                    ),
                    operator=ComparisonOperator(item["operator"]),
                    right=(
                        self.deserialize_operand(OperandKind(item["right_kind"]), item["right"])
                        if item.get("right_kind") is not None and item.get("right") is not None
                        else None
                    ),
                    tolerance_abs=Decimal(item["tolerance_abs"]),
                    null_input_mode=NullInputMode(item["null_input_mode"]),
                    null_result_mode=NullResultMode(item["null_result_mode"]),
                    null_default_value=item.get("null_default_value"),
                )
                for item in payload.get("predicates", [])
            ),
        )

    def _condition_aggregate_scope(self, condition: Condition) -> str | None:
        for operand in (condition.left, condition.right):
            if isinstance(operand, AggregateOperand):
                return operand.scope.value
        return None

    def _json_safe(self, value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, Decimal):
            return str(value)
        if is_dataclass(value):
            return self._json_safe(asdict(value))
        if isinstance(value, Mapping):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return [self._json_safe(item) for item in value]
        if isinstance(value, list):
            return [self._json_safe(item) for item in value]
        if isinstance(value, set):
            return sorted(self._json_safe(item) for item in value)
        return value
