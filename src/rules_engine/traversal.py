"""Shared, deterministic traversal of canonical ruleset metadata.

Literal contents are data boundaries: dictionaries inside a LiteralOperand are
never interpreted as operands. Function argument collections may contain
operands and are traversed recursively. Callers explicitly choose active-only
filtering and whether literal null fallbacks are included.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from rules_engine.models import (
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    Operand,
    Rule,
    Ruleset,
)


def iter_nested_operands(value: Any) -> Iterator[Operand]:
    """Yield immediate operands recursively through argument collections."""
    for leaf in iter_argument_leaves(value):
        if isinstance(leaf, Operand):
            yield leaf


def iter_argument_leaves(value: Any) -> Iterator[Any]:
    """Yield scalar data or whole operands at argument collection boundaries."""
    if isinstance(value, Operand) or not isinstance(value, (Mapping, list, tuple, set)):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from iter_argument_leaves(item)
    elif isinstance(value, (list, tuple, set)):
        items = sorted(value, key=repr) if isinstance(value, set) else value
        for item in items:
            yield from iter_argument_leaves(item)


def iter_rules(
    ruleset: Ruleset, *, active_only: bool = False, ordered: bool = False
) -> Iterator[Rule]:
    """Yield rules in authored or explicit execution order."""
    rules = sorted(ruleset.rules, key=lambda rule: rule.rule_order) if ordered else ruleset.rules
    for rule in rules:
        if not active_only or rule.active_flag is True:
            yield rule


def iter_groups(group: ConditionGroup) -> Iterator[ConditionGroup]:
    """Yield a group and each descendant in canonical preorder."""
    yield group
    for child in group.groups:
        yield from iter_groups(child)


def iter_conditions(group: ConditionGroup, *, active_only: bool = False) -> Iterator[Condition]:
    """Yield conditions in a group tree, optionally omitting inactive ones."""
    for current in iter_groups(group):
        for condition in current.conditions:
            if not active_only or condition.active_flag is True:
                yield condition


def iter_operand_tree(operand: Operand, *, include_defaults: bool = True) -> Iterator[Operand]:
    """Yield an operand, function argument operands, and optional fallbacks."""
    yield operand
    if isinstance(operand, CustomFunctionOperand):
        for nested in iter_nested_operands(operand.args):
            yield from iter_operand_tree(nested, include_defaults=include_defaults)
    if include_defaults and operand.default_if_null is not None:
        yield from iter_operand_tree(operand.default_if_null, include_defaults=True)


def iter_ruleset_operands(
    ruleset: Ruleset, *, active_only: bool = False, include_defaults: bool = True
) -> Iterator[Operand]:
    """Yield all condition and assignment operand trees in one ruleset."""
    for rule in iter_rules(ruleset, active_only=active_only):
        for condition in iter_conditions(rule.root_group, active_only=active_only):
            yield from iter_operand_tree(condition.left, include_defaults=include_defaults)
            if condition.right is not None:
                yield from iter_operand_tree(condition.right, include_defaults=include_defaults)
        for assignment in rule.assignments:
            yield from iter_operand_tree(assignment.value, include_defaults=include_defaults)
