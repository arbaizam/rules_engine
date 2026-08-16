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
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Rule,
    Ruleset,
)


class _DecimalSafeDumper(yaml.SafeDumper):
    """Safe dumper that emits Decimal values as YAML numeric scalars."""


def _represent_decimal(dumper: _DecimalSafeDumper, value: Decimal):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:float",
        str(value),
    )


def _represent_set(dumper: _DecimalSafeDumper, value: set[Any]):
    """Emit sets deterministically while retaining the YAML set type."""
    return dumper.represent_mapping(
        "tag:yaml.org,2002:set",
        [(item, None) for item in sorted(value, key=repr)],
    )


def _represent_tuple(dumper: _DecimalSafeDumper, value: tuple[Any, ...]):
    """Emit a safe application tag so tuple literals round-trip exactly."""
    return dumper.represent_sequence("!rules_engine/tuple", value)


_DecimalSafeDumper.add_representer(Decimal, _represent_decimal)
_DecimalSafeDumper.add_representer(set, _represent_set)
_DecimalSafeDumper.add_representer(tuple, _represent_tuple)


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
            Canonical authoring payload used by the Decimal-aware
            :meth:`export_text` renderer.
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
        return yaml.dump(
            self.export_payload(ruleset),
            Dumper=_DecimalSafeDumper,
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
        }
        if condition.right is not None:
            payload["right"] = self._export_operand(condition.right)
        payload["tolerance_abs"] = self._export_decimal(condition.tolerance_abs)
        payload["null_input_mode"] = condition.null_input_mode.value
        payload["null_result_mode"] = condition.null_result_mode.value
        if condition.null_default_value is not None:
            payload["null_default_value"] = self._export_value(condition.null_default_value)
        payload["active_flag"] = condition.active_flag
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
                    "args": {
                        str(key): self._export_arg_value(value)
                        for key, value in operand.args.items()
                    },
                }
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

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
            return value
        if isinstance(value, tuple):
            return tuple(self._export_value(item) for item in value)
        if isinstance(value, list):
            return [self._export_value(item) for item in value]
        if isinstance(value, set):
            return {self._export_value(item) for item in value}
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

    def _export_arg_value(self, value: Any) -> Any:
        """
        Export custom-function args, preserving nested operand references.
        """
        if isinstance(value, (FieldOperand, LiteralOperand, CustomFunctionOperand)):
            return self._export_operand(value)
        return self._export_value(value)
