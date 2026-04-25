"""
Ruleset normalizer.

Normalization prepares metadata for publish/runtime use without changing
semantics. It materializes explicit defaults that are allowed by the semantic
contract, most notably ``tolerance_abs=0`` when authoring omitted tolerance.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

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
    RowFilterPredicate,
    Rule,
    Ruleset,
)


class RulesetNormalizer:
    """
    Normalize ruleset metadata into a publish-ready explicit shape.
    """

    def normalize_ruleset(self, ruleset: Ruleset) -> Ruleset:
        """
        Normalize all rules in a ruleset.
        """
        return replace(
            ruleset,
            rules=tuple(self._normalize_rule(rule) for rule in ruleset.rules),
        )

    def _normalize_rule(self, rule: Rule) -> Rule:
        """
        Normalize one rule's condition tree and assignment operands.
        """
        return replace(
            rule,
            root_group=self._normalize_group(rule.root_group),
            assignments=tuple(self._normalize_assignment(item) for item in rule.assignments),
        )

    def _normalize_group(self, group: ConditionGroup) -> ConditionGroup:
        """
        Normalize every condition and nested group in a logical group.
        """
        return replace(
            group,
            conditions=tuple(self._normalize_condition(item) for item in group.conditions),
            groups=tuple(self._normalize_group(item) for item in group.groups),
        )

    def _normalize_condition(self, condition: Condition) -> Condition:
        """
        Normalize a condition's operands and explicit absolute tolerance.
        """
        return replace(
            condition,
            left=self._normalize_operand(condition.left),
            right=self._normalize_operand(condition.right) if condition.right is not None else None,
            tolerance_abs=Decimal(str(condition.tolerance_abs or "0")),
        )

    def _normalize_assignment(self, assignment: Assignment) -> Assignment:
        """
        Normalize the operand used as an assignment value.
        """
        return replace(assignment, value=self._normalize_operand(assignment.value))

    def _normalize_operand(self, operand: Operand) -> Operand:
        """
        Rebuild an operand into a fully materialized immutable shape.

        This is especially important for aggregate args and custom function
        args, which are accepted as mappings but should persist as ordinary
        dictionaries.
        """
        if isinstance(operand, AggregateOperand):
            return AggregateOperand.build(
                function=operand.function,
                field_name=operand.field_name,
                scope=operand.scope,
                by=operand.by,
                args=dict(operand.args),
                filter_=self._normalize_aggregate_filter(operand.filter),
                order_by=operand.order_by,
                null_input_mode=operand.null_input_mode,
                null_result_mode=operand.null_result_mode,
                null_default_value=operand.null_default_value,
            )
        if isinstance(operand, CustomFunctionOperand):
            return CustomFunctionOperand(
                function_name=operand.function_name,
                args=dict(operand.args),
            )
        if isinstance(operand, FieldOperand):
            return operand
        if isinstance(operand, LiteralOperand):
            return operand
        return operand

    def _normalize_aggregate_filter(
        self,
        aggregate_filter: AggregateFilter | None,
    ) -> AggregateFilter | None:
        """
        Normalize all predicates in an aggregate filter.
        """
        if aggregate_filter is None:
            return None
        return AggregateFilter(
            logical_operator=aggregate_filter.logical_operator,
            predicates=tuple(
                RowFilterPredicate(
                    left=self._normalize_operand(predicate.left),
                    operator=predicate.operator,
                    right=(
                        self._normalize_operand(predicate.right)
                        if predicate.right is not None
                        else None
                    ),
                    tolerance_abs=Decimal(str(predicate.tolerance_abs or "0")),
                    null_input_mode=predicate.null_input_mode,
                    null_result_mode=predicate.null_result_mode,
                    null_default_value=predicate.null_default_value,
                )
                for predicate in aggregate_filter.predicates
            ),
        )
