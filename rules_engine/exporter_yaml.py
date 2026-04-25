"""
YAML exporter for canonical ruleset metadata.

The exporter writes the same authoring vocabulary accepted by
``YamlRulesetCompiler``. It is intended for governance workflows that need to
round-trip compiled or code-authored metadata back into reviewable YAML.
"""

from __future__ import annotations

from dataclasses import is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from rules_engine.models import (
    AggregateFilter,
    AggregateOperand,
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    OrderBySpec,
    RowFilterPredicate,
    Rule,
    Ruleset,
)


class YamlRulesetExporter:
    """
    Export ruleset dataclasses into canonical YAML authoring payloads.
    """

    def export_payload(self, ruleset: Ruleset) -> dict[str, Any]:
        """
        Convert a ruleset model into a YAML-safe dictionary.

        Parameters
        ----------
        ruleset : Ruleset
            Ruleset metadata to export.

        Returns
        -------
        dict[str, Any]
            Canonical authoring payload suitable for ``yaml.safe_dump``.
        """
        payload: dict[str, Any] = {
            "ruleset_id": ruleset.ruleset_id,
            "ruleset_name": ruleset.ruleset_name,
            "version": ruleset.version,
        }
        if ruleset.description is not None:
            payload["description"] = ruleset.description
        if ruleset.owner is not None:
            payload["owner"] = ruleset.owner
        if ruleset.owner_department is not None:
            payload["owner_department"] = ruleset.owner_department
        payload["rules"] = [self._export_rule(rule) for rule in ruleset.rules]
        return payload

    def export_text(self, ruleset: Ruleset) -> str:
        """
        Render a ruleset model as YAML text.
        """
        return yaml.safe_dump(
            self.export_payload(ruleset),
            sort_keys=False,
            allow_unicode=True,
        )

    def export_path(self, ruleset: Ruleset, path: str | Path) -> None:
        """
        Write a ruleset model to a YAML file.
        """
        Path(path).write_text(self.export_text(ruleset), encoding="utf-8")

    def _export_rule(self, rule: Rule) -> dict[str, Any]:
        """
        Export one rule into canonical YAML rule syntax.
        """
        payload: dict[str, Any] = {
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "rule_order": rule.rule_order,
            "active_flag": rule.active_flag,
            "stop_on_match": rule.stop_on_match,
        }
        if rule.description is not None:
            payload["description"] = rule.description
        payload["when"] = self._export_group(rule.root_group)
        payload["assign"] = [
            self._export_assignment(assignment)
            for assignment in rule.assignments
        ]
        return payload

    def _export_group(self, group: ConditionGroup) -> dict[str, Any]:
        """
        Export a condition group, preserving its logical operator and children.
        """
        return {
            "condition_group_id": group.condition_group_id,
            group.logical_operator.value: [
                *[self._export_condition(condition) for condition in group.conditions],
                *[self._export_group(child_group) for child_group in group.groups],
            ],
        }

    def _export_condition(self, condition: Condition) -> dict[str, Any]:
        """
        Export one condition with explicit tolerance and null behavior fields.
        """
        payload: dict[str, Any] = {
            "condition_id": condition.condition_id,
            "left": self._export_operand(condition.left),
            "operator": condition.operator.value,
            "tolerance_abs": self._export_decimal(condition.tolerance_abs),
            "null_input_mode": condition.null_input_mode.value,
            "null_result_mode": condition.null_result_mode.value,
            "active_flag": condition.active_flag,
        }
        if condition.right is not None:
            payload["right"] = self._export_operand(condition.right)
        if condition.null_default_value is not None:
            payload["null_default_value"] = self._export_value(condition.null_default_value)
        return payload

    def _export_assignment(self, assignment: Assignment) -> dict[str, Any]:
        """
        Export one rule assignment in canonical list-entry form.
        """
        return {
            "assignment_id": assignment.assignment_id,
            "target_field": assignment.target_field,
            "value": self._export_operand(assignment.value),
        }

    def _export_operand(self, operand: Operand) -> dict[str, Any]:
        """
        Export an operand using the canonical operand key for its kind.
        """
        if isinstance(operand, FieldOperand):
            return {"field": operand.field_name}
        if isinstance(operand, LiteralOperand):
            payload = {"literal": self._export_value(operand.value)}
            if operand.value_type is not None:
                payload["value_type"] = operand.value_type
            return payload
        if isinstance(operand, CustomFunctionOperand):
            return {
                "custom_function": {
                    "name": operand.function_name,
                    "args": self._export_value(dict(operand.args)),
                }
            }
        if isinstance(operand, AggregateOperand):
            return {"aggregate": self._export_aggregate(operand)}
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _export_aggregate(self, aggregate: AggregateOperand) -> dict[str, Any]:
        """
        Export an aggregate operand with explicit scope and runtime metadata.
        """
        payload: dict[str, Any] = {
            "function": aggregate.function.value,
            "field": aggregate.field_name,
            "scope": aggregate.scope.value,
            "by": list(aggregate.by),
            "args": self._export_value(dict(aggregate.args)),
            "order_by": [
                self._export_order_by(order_by)
                for order_by in aggregate.order_by
            ],
            "null_input_mode": aggregate.null_input_mode.value,
            "null_result_mode": aggregate.null_result_mode.value,
        }
        if aggregate.filter is not None:
            payload["filter"] = self._export_filter(aggregate.filter)
        if aggregate.null_default_value is not None:
            payload["null_default_value"] = self._export_value(aggregate.null_default_value)
        return payload

    def _export_filter(self, aggregate_filter: AggregateFilter) -> dict[str, Any]:
        """
        Export a filtered aggregate predicate group.
        """
        return {
            aggregate_filter.logical_operator.value: [
                self._export_filter_predicate(predicate)
                for predicate in aggregate_filter.predicates
            ]
        }

    def _export_filter_predicate(self, predicate: RowFilterPredicate) -> dict[str, Any]:
        """
        Export one row-level filtered aggregate predicate.
        """
        payload: dict[str, Any] = {
            "left": self._export_operand(predicate.left),
            "operator": predicate.operator.value,
            "tolerance_abs": self._export_decimal(predicate.tolerance_abs),
            "null_input_mode": predicate.null_input_mode.value,
            "null_result_mode": predicate.null_result_mode.value,
        }
        if predicate.right is not None:
            payload["right"] = self._export_operand(predicate.right)
        if predicate.null_default_value is not None:
            payload["null_default_value"] = self._export_value(predicate.null_default_value)
        return payload

    def _export_order_by(self, order_by: OrderBySpec) -> dict[str, str]:
        """
        Export one aggregate order-by specification.
        """
        return {
            "field": order_by.field,
            "direction": order_by.direction,
        }

    def _export_decimal(self, value: Decimal) -> str:
        """
        Export a decimal as a non-scientific string.
        """
        return format(value, "f")

    def _export_value(self, value: Any) -> Any:
        """
        Recursively convert Python values into YAML-safe scalar/list/dict values.
        """
        if isinstance(value, Decimal):
            return self._export_decimal(value)
        if isinstance(value, tuple):
            return [self._export_value(item) for item in value]
        if isinstance(value, list):
            return [self._export_value(item) for item in value]
        if isinstance(value, dict):
            return {
                str(key): self._export_value(item)
                for key, item in value.items()
            }
        if is_dataclass(value):
            raise TypeError(
                f"Dataclass values are not YAML-authoring literals: {type(value).__name__}"
            )
        return value
