"""
Human-readable formatting for compiled ruleset metadata.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from rules_engine.enums import (
    ComparisonOperator,
    LogicalOperator,
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


class HumanReadableRulesetFormatter:
    """
    Render compiled ruleset metadata into compact audit-friendly rows.
    """

    def describe_rules(self, ruleset: Ruleset) -> list[dict[str, str]]:
        """
        Return one readable metadata row per rule.
        """
        return [
            self.describe_rule(rule)
            for rule in sorted(ruleset.rules, key=lambda item: item.rule_order)
        ]

    def describe_rule(self, rule: Rule) -> dict[str, str]:
        """Return one table-shaped description for a rule."""
        rule_logic = self._format_group(rule.root_group)
        if not rule.active_flag:
            rule_logic = f"[inactive rule] {rule_logic}"
        return {
            "rule_id": rule.rule_id,
            "rule_name": rule.rule_name,
            "rule_logic": rule_logic,
            "match_payload": self._format_assignments(rule.assignments),
        }

    def format_matched_rule_explanation(
        self,
        rule: Rule,
        passed_condition_ids: set[str],
    ) -> str | None:
        """
        Render the passed branches of a matched rule using author-facing syntax.
        """
        passed, explanation = self._format_passed_group(
            rule.root_group,
            passed_condition_ids,
            nested=False,
        )
        if not passed:
            return None
        return explanation

    def _format_group(self, group: ConditionGroup, *, nested: bool = False) -> str:
        """
        Render a condition group as an infix logical expression.
        """
        parts = [
            self._format_condition(condition)
            for condition in group.conditions
        ]
        parts.extend(self._format_group(child_group, nested=True) for child_group in group.groups)
        if not parts:
            expression = "TRUE" if group.logical_operator is LogicalOperator.ALL else "FALSE"
        else:
            joiner = " AND " if group.logical_operator is LogicalOperator.ALL else " OR "
            expression = joiner.join(parts)
        if nested and len(parts) > 1:
            return f"({expression})"
        return expression

    def _format_passed_group(
        self,
        group: ConditionGroup,
        passed_condition_ids: set[str],
        *,
        nested: bool,
    ) -> tuple[bool, str | None]:
        """
        Render only the passed branches of a condition group.
        """
        child_results = [
            (
                condition.condition_id in passed_condition_ids,
                self._format_condition(condition)
                if condition.condition_id in passed_condition_ids
                else None,
            )
            for condition in group.conditions
        ]
        child_results.extend(
            self._format_passed_group(
                child_group,
                passed_condition_ids,
                nested=True,
            )
            for child_group in group.groups
        )

        if not child_results:
            return group.logical_operator is LogicalOperator.ALL, None
        if group.logical_operator is LogicalOperator.ALL:
            group_passed = all(passed for passed, _ in child_results)
            joiner = " AND "
        else:
            group_passed = any(passed for passed, _ in child_results)
            joiner = " OR "

        included = [text for passed, text in child_results if passed and text]
        if not group_passed or not included:
            return group_passed, None
        expression = joiner.join(included)
        if nested and len(included) > 1:
            expression = f"({expression})"
        return group_passed, expression

    def _format_condition(self, condition: Condition) -> str:
        """
        Render one condition expression.
        """
        left = self._format_operand(condition.left)
        operator = self._operator_label(condition.operator)
        if condition.right is None:
            expression = f"{left} {operator}"
        else:
            expression = f"{left} {operator} {self._format_operand(condition.right)}"
        if condition.tolerance_abs != Decimal(0):
            tolerance = self._format_value(condition.tolerance_abs)
            expression = f"{expression} (tolerance_abs={tolerance})"
        if not condition.active_flag:
            expression = f"[inactive] {expression}"
        return expression

    def _format_assignments(self, assignments: tuple[Assignment, ...]) -> str:
        """
        Render the assignment payload emitted when a rule matches.
        """
        return ", ".join(
            self.format_assignment_expression(assignment)
            for assignment in assignments
        )

    def format_assignment_expression(self, assignment: Assignment) -> str:
        """Render one complete assignment expression for audit output."""
        return f"{assignment.target_field} = {self._format_operand(assignment.value)}"

    def _format_operand(self, operand: Operand) -> str:
        """
        Render an operand in author-facing expression syntax.
        """
        if isinstance(operand, FieldOperand):
            expression = operand.field_name
        elif isinstance(operand, AssignedOperand):
            expression = f"assigned({operand.target_field})"
        elif isinstance(operand, LiteralOperand):
            expression = self._format_value(operand.value)
        elif isinstance(operand, CustomFunctionOperand):
            expression = self._format_custom_function(operand)
        else:
            raise TypeError(f"Unsupported operand type: {type(operand).__name__}")
        if operand.default_if_null is not None:
            expression = (
                f"default_if_null({expression}, "
                f"{self._format_value(operand.default_if_null.value)})"
            )
        return expression

    def _format_custom_function(self, operand: CustomFunctionOperand) -> str:
        """
        Render a custom function operand.
        """
        args = ", ".join(
            f"{name}={self._format_arg(value)}"
            for name, value in operand.args.items()
        )
        return f"{operand.function_name}({args})"

    def _format_arg(self, value: Any) -> str:
        """
        Render a custom function argument.
        """
        if isinstance(
            value,
            (AssignedOperand, FieldOperand, LiteralOperand, CustomFunctionOperand),
        ):
            return self._format_operand(value)
        return self._format_value(value)

    def _operator_label(self, operator: ComparisonOperator) -> str:
        """
        Return a compact comparison operator label.
        """
        return {
            ComparisonOperator.EQ: "==",
            ComparisonOperator.NE: "!=",
            ComparisonOperator.GT: ">",
            ComparisonOperator.GE: ">=",
            ComparisonOperator.LT: "<",
            ComparisonOperator.LE: "<=",
            ComparisonOperator.IN: "in",
            ComparisonOperator.NOT_IN: "not in",
            ComparisonOperator.BETWEEN: "between",
            ComparisonOperator.NOT_BETWEEN: "not between",
            ComparisonOperator.LIKE: "like",
            ComparisonOperator.NOT_LIKE: "not like",
            ComparisonOperator.CONTAINS: "contains",
            ComparisonOperator.NOT_CONTAINS: "does not contain",
            ComparisonOperator.STARTS_WITH: "starts with",
            ComparisonOperator.ENDS_WITH: "ends with",
            ComparisonOperator.IS_NULL: "is null",
            ComparisonOperator.IS_NOT_NULL: "is not null",
        }[operator]

    def _format_value(self, value: Any) -> str:
        """
        Render a literal value for a readable rule expression.
        """
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, set):
            return (
                "{"
                + ", ".join(
                    self._format_value(item)
                    for item in sorted(value, key=repr)
                )
                + "}"
            )
        if isinstance(value, tuple):
            return "[" + ", ".join(self._format_value(item) for item in value) + "]"
        if isinstance(value, list):
            return "[" + ", ".join(self._format_value(item) for item in value) + "]"
        if isinstance(value, dict):
            return (
                "{"
                + ", ".join(
                    f"{key}: {self._format_value(item)}"
                    for key, item in value.items()
                )
                + "}"
            )
        return str(value)
