"""
YAML compiler for canonical ruleset metadata.

The compiler performs shape checks and enum parsing. Semantic checks remain in
``validator.py`` so that YAML and code-based authoring share one validation
gate before publishing.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

import yaml

from rules_engine.enums import (
    AggregateFunction,
    AggregateScope,
    ComparisonOperator,
    LogicalOperator,
    NullInputMode,
    NullResultMode,
    RulesetStatus,
)
from rules_engine.exceptions import CompilationError
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
            payload = yaml.safe_load(yaml_text)
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
        if "ruleset" in payload:
            payload = self._require_mapping(payload, "ruleset")

        ruleset_id = self._require_str(payload, "ruleset_id")
        ruleset_name = self._require_str(payload, "ruleset_name")
        version = self._require_str(payload, "version")
        status = self._enum(RulesetStatus, self._require_str(payload, "status"), "status")

        raw_rules = payload.get("rules")
        if not isinstance(raw_rules, list):
            raise CompilationError("rules must be a list.")

        rules = tuple(
            self._compile_rule(raw_rule, index)
            for index, raw_rule in enumerate(raw_rules, start=1)
        )
        return Ruleset(
            ruleset_id=ruleset_id,
            ruleset_name=ruleset_name,
            version=version,
            status=status,
            rules=rules,
            description=self._optional_str(payload, "description"),
            owner=self._optional_str(payload, "owner"),
            owner_department=self._optional_str(payload, "owner_department"),
        )

    def _compile_rule(self, payload: Any, index: int) -> Rule:
        """
        Compile one rule mapping into a ``Rule`` dataclass.

        Only contract-defined structural defaults are applied here, such as a
        generated rule identifier or order. Unsupported aliases are rejected so
        later persistence always reflects canonical authoring vocabulary.
        """
        payload = self._ensure_mapping(payload, f"rule at index {index}")
        rule_name = self._require_str(payload, "rule_name")
        rule_id = str(payload.get("rule_id", f"rule:{index}"))
        rule_order = int(payload.get("rule_order", index))

        when_payload = self._require_mapping(payload, "when")
        root_group = self._compile_group_mapping(when_payload, f"cg:{rule_id}:root")

        if "assignments" in payload:
            raise CompilationError("Unsupported rule key: assignments. Use canonical key: assign.")
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
            active_flag=bool(payload.get("active_flag", True)),
            stop_on_match=bool(payload.get("stop_on_match", False)),
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
        unsupported_keys = set(payload) - allowed_keys
        if unsupported_keys:
            raise CompilationError(
                f"Condition group {group_id} contains unsupported keys: {sorted(unsupported_keys)}."
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
        if not isinstance(explicit_group_id, str) or not explicit_group_id:
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

        The compiler materializes explicit tolerance and null-mode fields so
        downstream validation and runtime execution do not rely on hidden
        runtime defaults.
        """
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
        return Condition(
            condition_id=str(payload.get("condition_id", condition_id)),
            left=left,
            operator=operator,
            right=right,
            tolerance_abs=self._decimal(payload.get("tolerance_abs", "0"), "tolerance_abs"),
            null_input_mode=self._enum(
                NullInputMode,
                self._require_str(payload, "null_input_mode"),
                "null_input_mode",
            ),
            null_result_mode=self._enum(
                NullResultMode,
                self._require_str(payload, "null_result_mode"),
                "null_result_mode",
            ),
            null_default_value=payload.get("null_default_value"),
            active_flag=bool(payload.get("active_flag", True)),
        )

    def _compile_assignments(self, payload: Any, rule_id: str) -> tuple[Assignment, ...]:
        """
        Compile rule assignment authoring into assignment models.

        List form preserves explicit assignment IDs. Mapping form is the
        documented shorthand where keys are target fields and values are
        literal or operand payloads.
        """
        if isinstance(payload, Mapping):
            return tuple(
                Assignment(
                    assignment_id=f"assignment:{rule_id}:{index}",
                    target_field=str(target_field),
                    value=self._coerce_assignment_value(raw_value),
                )
                for index, (target_field, raw_value) in enumerate(payload.items(), start=1)
            )
        if not isinstance(payload, list):
            raise CompilationError("assign must be a list or mapping.")
        assignments: list[Assignment] = []
        for index, raw_assignment in enumerate(payload, start=1):
            assignment = self._ensure_mapping(raw_assignment, "assignment")
            assignments.append(
                Assignment(
                    assignment_id=str(
                        assignment.get("assignment_id", f"assignment:{rule_id}:{index}")
                    ),
                    target_field=self._require_str(assignment, "target_field"),
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
        return LiteralOperand(raw_value)

    def _compile_operand(self, payload: Mapping[str, Any]) -> Operand:
        """
        Compile one operand payload.

        Exactly one operand kind is allowed. The accepted keys are canonical:
        ``field``, ``literal``, ``aggregate``, and ``custom_function``.
        """
        if "value" in payload:
            raise CompilationError("Unsupported operand key: value. Use canonical key: literal.")
        operand_keys = [key for key in ("field", "literal", "aggregate", "custom_function") if key in payload]
        if len(operand_keys) != 1:
            raise CompilationError(
                f"Operand must define exactly one operand kind, found: {operand_keys}"
            )
        key = operand_keys[0]
        if key == "field":
            return FieldOperand(self._require_str(payload, "field"))
        if key == "literal":
            return LiteralOperand(payload[key], payload.get("value_type"))
        if key == "custom_function":
            fn_payload = self._require_mapping(payload, "custom_function")
            return CustomFunctionOperand(
                function_name=self._require_str(fn_payload, "name"),
                args=dict(self._optional_mapping(fn_payload, "args")),
            )
        aggregate_payload = self._require_mapping(payload, "aggregate")
        return self._compile_aggregate(aggregate_payload)

    def _compile_aggregate(self, payload: Mapping[str, Any]) -> AggregateOperand:
        """
        Compile an aggregate operand payload.

        This parses function, field, explicit scope, grouping fields, optional
        filters, order-by metadata, arguments, and aggregate null behavior.
        """
        function = self._enum(
            AggregateFunction,
            self._require_str(payload, "function"),
            "aggregate function",
        )
        if "field_name" in payload:
            raise CompilationError("Unsupported aggregate key: field_name. Use canonical key: field.")
        field_name = self._require_str(payload, "field")
        scope = self._enum(AggregateScope, self._require_str(payload, "scope"), "aggregate scope")
        by = payload.get("by", [])
        if not isinstance(by, list):
            raise CompilationError("aggregate by must be a list when provided.")
        order_by_payload = payload.get("order_by", [])
        if not isinstance(order_by_payload, list):
            raise CompilationError("aggregate order_by must be a list when provided.")
        filter_payload = payload.get("filter")
        aggregate_filter = (
            self._compile_aggregate_filter(filter_payload)
            if filter_payload is not None
            else None
        )
        return AggregateOperand.build(
            function=function,
            field_name=field_name,
            scope=scope,
            by=tuple(str(item) for item in by),
            args=self._optional_mapping(payload, "args"),
            filter_=aggregate_filter,
            order_by=tuple(self._compile_order_by(item) for item in order_by_payload),
            null_input_mode=self._enum(
                NullInputMode,
                self._require_str(payload, "null_input_mode"),
                "aggregate null_input_mode",
            ),
            null_result_mode=self._enum(
                NullResultMode,
                self._require_str(payload, "null_result_mode"),
                "aggregate null_result_mode",
            ),
            null_default_value=payload.get("null_default_value"),
        )

    def _compile_aggregate_filter(self, payload: Any) -> AggregateFilter:
        """
        Compile a filtered aggregate logical predicate group.

        The filter structure is intentionally limited to one logical operator
        containing row-level predicates.
        """
        payload = self._ensure_mapping(payload, "aggregate filter")
        if len(payload) != 1:
            raise CompilationError("Aggregate filter must define exactly one logical operator.")
        logical_key = next(iter(payload.keys()))
        logical_operator = self._enum(LogicalOperator, logical_key, "aggregate filter")
        raw_predicates = payload[logical_key]
        if not isinstance(raw_predicates, list):
            raise CompilationError("Aggregate filter predicates must be a list.")
        return AggregateFilter(
            logical_operator=logical_operator,
            predicates=tuple(
                self._compile_row_filter_predicate(item, index)
                for index, item in enumerate(raw_predicates, start=1)
            ),
        )

    def _compile_row_filter_predicate(self, payload: Any, index: int) -> RowFilterPredicate:
        """
        Compile one row-level predicate inside an aggregate filter.

        Predicate operands use the same compiler path as ordinary conditions,
        while nested aggregate rejection remains a validator responsibility.
        """
        payload = self._ensure_mapping(payload, f"aggregate filter predicate {index}")
        right_payload = payload.get("right")
        return RowFilterPredicate(
            left=self._compile_operand(self._require_mapping(payload, "left")),
            operator=self._enum(
                ComparisonOperator,
                self._require_str(payload, "operator"),
                f"aggregate filter predicate {index} operator",
            ),
            right=(
                self._compile_operand(self._ensure_mapping(right_payload, "right"))
                if right_payload is not None
                else None
            ),
            tolerance_abs=self._decimal(payload.get("tolerance_abs", "0"), "tolerance_abs"),
            null_input_mode=self._enum(
                NullInputMode,
                self._require_str(payload, "null_input_mode"),
                "aggregate filter null_input_mode",
            ),
            null_result_mode=self._enum(
                NullResultMode,
                self._require_str(payload, "null_result_mode"),
                "aggregate filter null_result_mode",
            ),
            null_default_value=payload.get("null_default_value"),
        )

    def _compile_order_by(self, payload: Any) -> OrderBySpec:
        """
        Compile one order-by entry for an order-sensitive aggregate.
        """
        payload = self._ensure_mapping(payload, "order_by entry")
        return OrderBySpec(
            field=self._require_str(payload, "field"),
            direction=self._require_str(payload, "direction"),
        )

    def _enum(self, enum_type: type, value: str, label: str) -> Any:
        """
        Parse a string into one canonical enum value.
        """
        try:
            return enum_type(value)
        except ValueError as exc:
            valid = ", ".join(member.value for member in enum_type)
            raise CompilationError(f"Invalid {label}: {value}. Valid values: {valid}.") from exc

    def _decimal(self, value: Any, label: str) -> Decimal:
        """
        Parse a numeric authoring value as ``Decimal`` for tolerance handling.
        """
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise CompilationError(f"{label} must be numeric.") from exc

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

    def _require_str(self, payload: Mapping[str, Any], key: str) -> str:
        """
        Read a required non-empty string field from a mapping.
        """
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise CompilationError(f"{key} must be a non-empty string.")
        return value

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
