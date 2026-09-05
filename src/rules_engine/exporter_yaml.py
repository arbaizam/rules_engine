"""
YAML exporter for canonical ruleset metadata.

The exporter writes the same authoring vocabulary accepted by
``YamlRulesetCompiler`` for deterministic authoring and review. Persisted
ruleset documents are encoded separately by ``model_codec.py``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from rules_engine.canonical_values import (
    _PreservedFloat,
    normalize_mapping_keys,
    validate_literal,
    validate_string_mapping_keys,
)
from rules_engine.models import (
    AssignedOperand,
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
    """Safe dumper that keeps Decimal and binary-float kinds distinguishable."""


def _represent_decimal(dumper: _DecimalSafeDumper, value: Decimal):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:float",
        str(value),
    )


def _represent_float(dumper: _DecimalSafeDumper, value: float):
    """Preserve binary-float identity without inventing a literal type hint."""
    return dumper.represent_scalar("!rules_engine/float", repr(value))


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
_DecimalSafeDumper.add_representer(float, _represent_float)
_DecimalSafeDumper.add_representer(_PreservedFloat, _represent_float)
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
            Canonical authoring payload that preserves numeric kinds through
            ``compile_payload`` or the :meth:`export_text` renderer.
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
        payload["assign"] = [self._export_assignment(assignment) for assignment in rule.assignments]
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
        Export one condition with explicit tolerance and optional null errors.
        """
        payload: dict[str, Any] = {
            "condition_id": condition.condition_id,
            "left": self._export_operand(condition.left),
            "operator": condition.operator.value,
        }
        if condition.right is not None:
            payload["right"] = self._export_operand(condition.right)
        payload["tolerance_abs"] = self._export_decimal(condition.tolerance_abs)
        if condition.error_on_null:
            payload["error_on_null"] = True
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
            payload: dict[str, Any] = {"field": operand.field_name}
        elif isinstance(operand, AssignedOperand):
            payload = {"assigned": operand.target_field}
        elif isinstance(operand, LiteralOperand):
            validate_literal(operand.value, operand.value_type)
            payload = {"literal": self._export_value(operand.value)}
            if operand.value_type is not None:
                payload["value_type"] = operand.value_type
        elif isinstance(operand, CustomFunctionOperand):
            payload = {
                "custom_function": {
                    "name": operand.function_name,
                    "args": self._export_mapping(
                        operand.args,
                        self._export_arg_value,
                    ),
                }
            }
        else:
            raise TypeError(f"Unsupported operand type: {type(operand).__name__}")
        if operand.default_if_null is not None:
            default = operand.default_if_null
            if default.value_type is not None or isinstance(default.value, Mapping):
                payload["default_if_null"] = self._export_operand(default)
            else:
                payload["default_if_null"] = self._export_value(default.value)
        return payload

    def _export_decimal(self, value: Decimal) -> str:
        """
        Export a decimal as a non-scientific string.
        """
        return format(value, "f")

    def _export_value(self, value: Any) -> Any:
        """
        Recursively convert Python values into YAML-safe scalar/list/dict values.
        """
        validate_literal(value)

        def preserve_float_kind(item: Any) -> Any:
            if isinstance(item, float):
                return _PreservedFloat(item)
            if isinstance(item, Mapping):
                return {key: preserve_float_kind(child) for key, child in item.items()}
            if isinstance(item, list):
                return [preserve_float_kind(child) for child in item]
            if isinstance(item, tuple):
                return tuple(preserve_float_kind(child) for child in item)
            if isinstance(item, set):
                return {preserve_float_kind(child) for child in item}
            return item

        return preserve_float_kind(value)

    def _export_arg_value(self, value: Any) -> Any:
        """
        Export custom-function args, preserving nested operand references.
        """
        if isinstance(
            value,
            (AssignedOperand, FieldOperand, LiteralOperand, CustomFunctionOperand),
        ):
            return self._export_operand(value)
        if isinstance(value, tuple):
            return tuple(self._export_arg_value(item) for item in value)
        if isinstance(value, list):
            return [self._export_arg_value(item) for item in value]
        if isinstance(value, set):
            return {self._export_arg_value(item) for item in value}
        if isinstance(value, Mapping):
            exported = self._export_mapping(value, self._export_arg_value)
            if set(value) & {
                "field", "assigned", "literal", "custom_function", "$rules_engine_mapping"
            }:
                # Preserve raw mappings separately from LiteralOperand, including
                # nested operands and data that uses the escape key itself.
                return {"$rules_engine_mapping": exported}
            return exported
        return self._export_value(value)

    def _export_mapping(
        self,
        value: Mapping[Any, Any],
        export_value: Callable[[Any], Any],
    ) -> dict[str, Any]:
        """String-normalize mapping keys without silently merging values."""
        validate_string_mapping_keys(value)
        return normalize_mapping_keys(value, export_value, "Exported mapping")
