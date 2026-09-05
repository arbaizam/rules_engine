"""
YAML compiler for canonical ruleset metadata.

The compiler performs shape checks and enum parsing. Semantic checks remain in
``validator.py`` so compiled rulesets pass one validation gate before publishing.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError

from rules_engine.canonical_values import normalize_literal, normalize_mapping_keys
from rules_engine.enums import ComparisonOperator, LogicalOperator
from rules_engine.exceptions import CompilationError
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


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
):
    """Construct a mapping with unique explicit keys and normal YAML merges."""
    explicit_keys: set[Any] = set()
    for key_node, _ in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            continue
        key = loader.construct_object(key_node, deep=False)
        try:
            duplicate = key in explicit_keys
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        explicit_keys.add(key)

    mapping: dict[Any, Any] = {}
    yield mapping
    loader.flatten_mapping(node)
    mapping.update(loader.construct_mapping(node))


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _construct_yaml_decimal(
    loader: _UniqueKeySafeLoader,
    node: yaml.ScalarNode,
) -> Decimal:
    """Parse a YAML float token without first passing through binary float."""
    value = loader.construct_scalar(node).replace("_", "")
    special_values = {
        ".inf": "Infinity",
        "+.inf": "Infinity",
        "-.inf": "-Infinity",
        ".nan": "NaN",
    }
    decimal_text = special_values.get(value.lower(), value)
    try:
        decimal_value = Decimal(decimal_text)
    except InvalidOperation as exc:
        raise ConstructorError(
            "while constructing a decimal",
            node.start_mark,
            f"unsupported YAML numeric literal {value!r}",
            node.start_mark,
        ) from exc
    if not decimal_value.is_finite():
        raise ConstructorError(
            "while constructing a decimal",
            node.start_mark,
            f"numeric literal {value!r} must be finite",
            node.start_mark,
        )
    return decimal_value


_UniqueKeySafeLoader.add_constructor(
    "tag:yaml.org,2002:float",
    _construct_yaml_decimal,
)


def _construct_yaml_tuple(
    loader: _UniqueKeySafeLoader,
    node: yaml.SequenceNode,
) -> tuple[Any, ...]:
    """Restore a rules-engine tuple without enabling Python object loading."""
    return tuple(loader.construct_sequence(node, deep=True))


_UniqueKeySafeLoader.add_constructor(
    "!rules_engine/tuple",
    _construct_yaml_tuple,
)


class YamlRulesetCompiler:
    """
    Compile canonical YAML payloads into rules engine dataclasses.
    """

    def compile_text(self, yaml_text: str) -> Ruleset:
        """
        Compile a YAML text document.

        Parameters
        ----------
        yaml_text : str
            YAML ruleset document.

        Returns
        -------
        Ruleset
            Compiled ruleset model.
        """
        try:
            payload = yaml.load(yaml_text, Loader=_UniqueKeySafeLoader)
        except yaml.YAMLError as exc:
            raise CompilationError(f"Failed to parse YAML: {exc}") from exc
        return self.compile_payload(payload)

    def compile_path(self, path: str | Path) -> Ruleset:
        """
        Compile a YAML document from disk.
        """
        path_obj = Path(path)
        if not path_obj.exists():
            raise CompilationError(f"Ruleset YAML file not found: {path_obj}")
        return self.compile_text(path_obj.read_text(encoding="utf-8"))

    def compile_payload(self, payload: Any) -> Ruleset:
        """
        Compile a parsed YAML payload.
        """
        payload = self._ensure_mapping(payload, "root payload")
        self._reject_unsupported_keys(
            payload,
            {
                "ruleset_id",
                "ruleset_name",
                "version",
                "description",
                "owner",
                "owner_department",
                "rules",
            },
            "Ruleset",
        )

        ruleset_id = self._require_str(payload, "ruleset_id")
        ruleset_name = self._require_str(payload, "ruleset_name")
        version = self._require_str(payload, "version")

        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, list):
            raise CompilationError("rules must be a list.")

        rules = tuple(
            self._compile_rule(raw_rule, index) for index, raw_rule in enumerate(raw_rules, start=1)
        )
        return Ruleset(
            ruleset_id=ruleset_id,
            ruleset_name=ruleset_name,
            version=version,
            rules=rules,
            description=self._optional_str(payload, "description"),
            owner=self._optional_str(payload, "owner"),
            owner_department=self._optional_str(payload, "owner_department"),
        )

    def _compile_rule(self, payload: Any, index: int) -> Rule:
        """
        Compile one rule mapping into a ``Rule`` dataclass.

        Only contract-defined structural defaults are applied here, such as a
        generated rule identifier or order. Unknown keys are rejected so later
        persistence always reflects the declared authoring contract.
        """
        payload = self._ensure_mapping(payload, f"rule at index {index}")
        self._reject_unsupported_keys(
            payload,
            {
                "rule_id",
                "rule_name",
                "rule_order",
                "description",
                "when",
                "assign",
                "active_flag",
                "stop_on_match",
            },
            f"Rule at index {index}",
        )
        rule_name = self._require_str(payload, "rule_name")
        rule_id = self._str_or_default(payload, "rule_id", f"rule:{index}")
        rule_order = self._int_or_default(payload, "rule_order", index)

        when_payload = self._require_mapping(payload, "when")
        root_group = self._compile_group_mapping(when_payload, f"cg:{rule_id}:root")

        assignments_payload = payload.get("assign")
        if assignments_payload is None:
            raise CompilationError(f"Rule {rule_id} must define assign.")
        assignments = self._compile_assignments(assignments_payload, rule_id)

        return Rule(
            rule_id=rule_id,
            rule_name=rule_name,
            rule_order=rule_order,
            root_group=root_group,
            assignments=assignments,
            active_flag=self._bool_or_default(payload, "active_flag", True),
            stop_on_match=self._bool_or_default(payload, "stop_on_match", False),
            description=self._optional_str(payload, "description"),
        )

    def _compile_group_mapping(self, payload: Mapping[str, Any], group_id: str) -> ConditionGroup:
        """
        Compile a condition-group mapping with exactly one logical operator.

        Group keys must match the canonical ``LogicalOperator`` values. Extra
        keys are rejected because group shape is persisted and audited exactly
        as authored.
        """
        logical_keys = set(payload) & {member.value for member in LogicalOperator}
        allowed_keys = logical_keys | {"condition_group_id"}
        self._reject_unsupported_keys(
            payload,
            allowed_keys,
            f"Condition group {group_id}",
        )
        if len(logical_keys) != 1:
            raise CompilationError(
                f"Condition group {group_id} must define exactly one logical operator."
            )
        logical_key = next(iter(logical_keys))
        logical_operator = self._enum(LogicalOperator, logical_key, f"group {group_id}")
        raw_items = payload[logical_key]
        if not isinstance(raw_items, list):
            raise CompilationError(f"Condition group {group_id} must contain a list.")
        explicit_group_id = payload.get("condition_group_id", group_id)
        if not isinstance(explicit_group_id, str) or not explicit_group_id.strip():
            raise CompilationError("condition_group_id must be a non-empty string when provided.")
        return self._compile_condition_group(logical_operator, raw_items, explicit_group_id)

    def _compile_condition_group(
        self,
        logical_operator: LogicalOperator,
        items: list[Any],
        group_id: str,
    ) -> ConditionGroup:
        """
        Compile a group item list into conditions and child groups.

        Each item is inspected for a logical-operator key. Items with such a
        key become nested groups; all others are compiled as conditions.
        """
        conditions: list[Condition] = []
        groups: list[ConditionGroup] = []
        for index, item in enumerate(items, start=1):
            item_map = self._ensure_mapping(item, f"condition/group item in {group_id}")
            logical_keys = set(item_map) & {member.value for member in LogicalOperator}
            if logical_keys:
                groups.append(self._compile_group_mapping(item_map, f"{group_id}:g{index}"))
            else:
                conditions.append(self._compile_condition(item_map, f"{group_id}:c{index}"))
        return ConditionGroup(
            condition_group_id=group_id,
            logical_operator=logical_operator,
            conditions=tuple(conditions),
            groups=tuple(groups),
        )

    def _compile_condition(self, payload: Mapping[str, Any], condition_id: str) -> Condition:
        """
        Compile one condition mapping into a canonical condition model.

        Null defaults belong to operands. Conditions only choose whether a
        remaining null should fail the condition or raise an error.
        """
        allowed_keys = {
            "condition_id",
            "left",
            "operator",
            "right",
            "tolerance_abs",
            "error_on_null",
            "active_flag",
        }
        self._reject_unsupported_keys(
            payload,
            allowed_keys,
            f"Condition {condition_id}",
        )
        left = self._compile_operand(self._require_mapping(payload, "left"))
        operator = self._enum(
            ComparisonOperator,
            self._require_str(payload, "operator"),
            f"operator for {condition_id}",
        )
        right_payload = payload.get("right")
        right = (
            self._compile_operand(self._ensure_mapping(right_payload, "right"))
            if right_payload is not None
            else None
        )
        error_on_null = self._bool_or_default(payload, "error_on_null", False)
        return Condition(
            condition_id=self._str_or_default(
                payload,
                "condition_id",
                condition_id,
            ),
            left=left,
            operator=operator,
            right=right,
            tolerance_abs=self._decimal(payload.get("tolerance_abs", "0"), "tolerance_abs"),
            error_on_null=error_on_null,
            active_flag=self._bool_or_default(payload, "active_flag", True),
        )

    def _compile_assignments(self, payload: Any, rule_id: str) -> tuple[Assignment, ...]:
        """
        Compile rule assignment authoring into assignment models.

        List form preserves explicit assignment IDs. Mapping form is the
        documented shorthand where keys are target fields and values are
        literal or operand payloads.
        """
        assignments: list[Assignment] = []
        if isinstance(payload, Mapping):
            for target_field, raw_value in payload.items():
                if not isinstance(target_field, str) or not target_field.strip():
                    raise CompilationError("Assignment target fields must be non-empty strings.")
                assignments.append(
                    Assignment(
                        assignment_id=f"assignment:{rule_id}:{target_field}",
                        target_field=target_field,
                        value=self._coerce_assignment_value(raw_value),
                    )
                )
            return tuple(assignments)
        if not isinstance(payload, list):
            raise CompilationError("assign must be a list or mapping.")
        for index, raw_assignment in enumerate(payload, start=1):
            assignment = self._ensure_mapping(raw_assignment, "assignment")
            self._reject_unsupported_keys(
                assignment,
                {"assignment_id", "target_field", "value"},
                f"Assignment at index {index} in rule {rule_id}",
            )
            target_field = self._require_str(assignment, "target_field")
            assignments.append(
                Assignment(
                    assignment_id=self._str_or_default(
                        assignment,
                        "assignment_id",
                        f"assignment:{rule_id}:{target_field}",
                    ),
                    target_field=target_field,
                    value=self._compile_operand(self._require_mapping(assignment, "value")),
                )
            )
        return tuple(assignments)

    def _coerce_assignment_value(self, raw_value: Any) -> Operand:
        """
        Convert shorthand assignment values into operands.

        Mapping values are compiled as explicit operand payloads. Scalar values
        are wrapped as literal operands.
        """
        if isinstance(raw_value, Mapping):
            return self._compile_operand(raw_value)
        return LiteralOperand(self._normalize_literal_value(raw_value))

    def _compile_operand(self, payload: Mapping[str, Any]) -> Operand:
        """
        Compile one operand payload.

        Exactly one operand kind is allowed. The accepted keys are canonical:
        ``field``, ``assigned``, ``literal``, and ``custom_function``.
        """
        operand_keys = [
            key for key in ("field", "assigned", "literal", "custom_function") if key in payload
        ]
        if len(operand_keys) != 1:
            raise CompilationError(
                f"Operand must define exactly one operand kind, found: {operand_keys}"
            )
        key = operand_keys[0]
        allowed_keys = {
            "field": {"field", "default_if_null"},
            "assigned": {"assigned", "default_if_null"},
            "literal": {"literal", "value_type", "default_if_null"},
            "custom_function": {"custom_function", "default_if_null"},
        }[key]
        self._reject_unsupported_keys(payload, allowed_keys, f"{key} operand")
        default_if_null = (
            self._compile_default_if_null(payload["default_if_null"])
            if "default_if_null" in payload
            else None
        )
        if key == "field":
            return FieldOperand(
                self._require_str(payload, "field"),
                default_if_null=default_if_null,
            )
        if key == "assigned":
            return AssignedOperand(
                self._require_str(payload, "assigned"),
                default_if_null=default_if_null,
            )
        if key == "literal":
            value_type = (
                self._require_str(payload, "value_type") if "value_type" in payload else None
            )
            return LiteralOperand(
                self._normalize_literal_value(payload[key], value_type),
                value_type,
                default_if_null,
            )
        if key == "custom_function":
            fn_payload = self._require_mapping(payload, "custom_function")
            self._reject_unsupported_keys(
                fn_payload,
                {"name", "args"},
                "Custom function operand",
            )
            return CustomFunctionOperand(
                function_name=self._require_str(fn_payload, "name"),
                args=self._normalize_mapping_keys(
                    self._optional_mapping(fn_payload, "args"),
                    self._compile_custom_function_arg,
                    "Custom function arguments",
                ),
                default_if_null=default_if_null,
            )
        raise CompilationError(f"Unsupported operand kind: {key}")

    def _compile_default_if_null(self, value: Any) -> LiteralOperand:
        """Compile an operand's non-null literal fallback."""
        value_type = None
        if isinstance(value, Mapping):
            unsupported_keys = set(value) - {"literal", "value_type"}
            if "literal" not in value or unsupported_keys:
                raise CompilationError(
                    "default_if_null must be a scalar/list literal or a mapping "
                    "containing only literal and optional value_type."
                )
            raw_value = value["literal"]
            value_type = self._require_str(value, "value_type") if "value_type" in value else None
        else:
            raw_value = value
        if raw_value is None:
            raise CompilationError("default_if_null cannot itself be null.")
        return LiteralOperand(
            self._normalize_literal_value(raw_value, value_type),
            value_type,
        )

    def _compile_custom_function_arg(self, value: Any) -> Any:
        """
        Compile operand-shaped args recursively through lists and mappings.
        """
        if isinstance(value, Mapping):
            operand_keys = {
                key for key in ("field", "assigned", "literal", "custom_function") if key in value
            }
            if operand_keys:
                return self._compile_operand(value)
            return self._normalize_mapping_keys(
                value,
                self._compile_custom_function_arg,
                "Custom function argument mapping",
            )
        if isinstance(value, list):
            return [self._compile_custom_function_arg(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._compile_custom_function_arg(item) for item in value)
        if isinstance(value, set):
            return {self._compile_custom_function_arg(item) for item in value}
        return self._normalize_literal_value(value)

    def _normalize_literal_value(self, value: Any, value_type: str | None = None) -> Any:
        """Normalize authored data through the canonical literal contract."""
        try:
            return normalize_literal(value, value_type)
        except (ValueError, TypeError, RecursionError) as exc:
            raise CompilationError(str(exc)) from exc

    def _enum(self, enum_type: type, value: str, label: str) -> Any:
        """
        Parse a string into one canonical enum value.
        """
        try:
            return enum_type(value)
        except (TypeError, ValueError) as exc:
            valid = ", ".join(member.value for member in enum_type)
            raise CompilationError(f"Invalid {label}: {value}. Valid values: {valid}.") from exc

    def _decimal(self, value: Any, label: str) -> Decimal:
        """
        Parse a numeric authoring value as ``Decimal`` for tolerance handling.
        """
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise CompilationError(f"{label} must be numeric.") from exc
        if not decimal_value.is_finite():
            raise CompilationError(f"{label} must be finite.")
        return decimal_value

    def _require_mapping(self, payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
        """
        Read a required nested mapping field from a parent payload.
        """
        value = payload.get(key)
        if not isinstance(value, Mapping):
            raise CompilationError(f"{key} must be a mapping.")
        return value

    def _optional_mapping(self, payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
        """
        Read an optional nested mapping field, defaulting to an empty mapping.
        """
        value = payload.get(key, {})
        if not isinstance(value, Mapping):
            raise CompilationError(f"{key} must be a mapping when provided.")
        return value

    def _ensure_mapping(self, value: Any, label: str) -> Mapping[str, Any]:
        """
        Require an arbitrary value to be a mapping for the given context label.
        """
        if not isinstance(value, Mapping):
            raise CompilationError(f"{label} must be a mapping.")
        return value

    def _normalize_mapping_keys(
        self,
        value: Mapping[Any, Any],
        normalize_value: Callable[[Any], Any],
        label: str,
    ) -> dict[str, Any]:
        """Normalize persisted mapping keys without silently merging values."""
        try:
            return normalize_mapping_keys(value, normalize_value, label)
        except ValueError as exc:
            raise CompilationError(str(exc)) from exc

    def _require_str(self, payload: Mapping[str, Any], key: str) -> str:
        """
        Read a required non-empty string field from a mapping.
        """
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise CompilationError(f"{key} must be a non-empty string.")
        return value

    def _str_or_default(
        self,
        payload: Mapping[str, Any],
        key: str,
        default: str,
    ) -> str:
        """Read a non-empty string or materialize its authoring default."""
        return default if key not in payload else self._require_str(payload, key)

    def _int_or_default(
        self,
        payload: Mapping[str, Any],
        key: str,
        default: int,
    ) -> int:
        """Read an integer without coercing strings, floats, or booleans."""
        if key not in payload:
            return default
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise CompilationError(f"{key} must be an integer when provided.")
        return value

    def _bool_or_default(
        self,
        payload: Mapping[str, Any],
        key: str,
        default: bool,
    ) -> bool:
        """Read a boolean without applying Python truthiness coercion."""
        if key not in payload:
            return default
        value = payload[key]
        if not isinstance(value, bool):
            raise CompilationError(f"{key} must be a boolean when provided.")
        return value

    def _reject_unsupported_keys(
        self,
        payload: Mapping[str, Any],
        allowed_keys: set[str],
        label: str,
    ) -> None:
        """Reject keys outside one explicitly declared mapping contract."""
        unsupported_keys = set(payload) - allowed_keys
        if unsupported_keys:
            rendered_keys = ", ".join(sorted(repr(key) for key in unsupported_keys))
            raise CompilationError(f"{label} contains unsupported keys: [{rendered_keys}].")

    def _optional_str(self, payload: Mapping[str, Any], key: str) -> str | None:
        """
        Read an optional string field from a mapping.
        """
        value = payload.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise CompilationError(f"{key} must be a string when provided.")
        return value
