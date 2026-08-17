"""Human-readable semantic ruleset comparison."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import (
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Operand,
    Rule,
    Ruleset,
    RulesetExpectation,
)
from rules_engine.serializer import DeltaRowSerializer


@dataclass(frozen=True)
class SemanticChange:
    """One named before/after semantic change."""

    field: str
    before: Any
    after: Any


@dataclass(frozen=True)
class RuleDiff:
    """Added, removed, or changed rule details."""

    rule_id: str
    rule_name: str
    change_type: str
    changes: tuple[SemanticChange, ...]


@dataclass(frozen=True)
class RulesetDiff:
    """Semantic comparison between two immutable ruleset versions."""

    ruleset_name: str
    baseline_version: str
    candidate_version: str
    baseline_content_hash: str
    candidate_content_hash: str
    metadata_changes: tuple[SemanticChange, ...]
    rule_diffs: tuple[RuleDiff, ...]

    @property
    def has_changes(self) -> bool:
        return bool(self.metadata_changes or self.rule_diffs)

    def to_text(self) -> str:
        """Render a compact review document in author-facing syntax."""
        lines = [
            f"Ruleset diff: {self.ruleset_name} "
            f"{self.baseline_version} -> {self.candidate_version}",
            f"Content hash: {self.baseline_content_hash} -> "
            f"{self.candidate_content_hash}",
        ]
        if not self.has_changes:
            return "\n".join([*lines, "No semantic changes."])
        for change in self.metadata_changes:
            lines.append(
                f"[metadata] {change.field}: "
                f"{change.before!r} -> {change.after!r}"
            )
        for rule_diff in self.rule_diffs:
            lines.append(
                f"[{rule_diff.change_type}] {rule_diff.rule_id} "
                f"({rule_diff.rule_name})"
            )
            for change in rule_diff.changes:
                lines.append(
                    f"  {change.field}: {change.before!r} -> {change.after!r}"
                )
        return "\n".join(lines)


class RulesetDiffer:
    """Compare rulesets by stable rule identifiers and readable semantics."""

    def __init__(self) -> None:
        self._formatter = HumanReadableRulesetFormatter()
        self._serializer = DeltaRowSerializer()

    def diff(self, baseline: Ruleset, candidate: Ruleset) -> RulesetDiff:
        """Return semantic changes from baseline to candidate."""
        metadata_changes = [
            *self._changes(
                {
                    "ruleset_id": baseline.ruleset_id,
                    "ruleset_name": baseline.ruleset_name,
                    "status": baseline.status.value,
                    "description": baseline.description,
                    "owner": baseline.owner,
                    "owner_department": baseline.owner_department,
                },
                {
                    "ruleset_id": candidate.ruleset_id,
                    "ruleset_name": candidate.ruleset_name,
                    "status": candidate.status.value,
                    "description": candidate.description,
                    "owner": candidate.owner,
                    "owner_department": candidate.owner_department,
                },
            ),
            *self._expected_case_changes(baseline, candidate),
        ]
        baseline_rules = {rule.rule_id: rule for rule in baseline.rules}
        candidate_rules = {rule.rule_id: rule for rule in candidate.rules}
        rule_diffs: list[RuleDiff] = []
        ordered_ids = [
            *[rule.rule_id for rule in sorted(baseline.rules, key=self._rule_order)],
            *[
                rule.rule_id
                for rule in sorted(candidate.rules, key=self._rule_order)
                if rule.rule_id not in baseline_rules
            ],
        ]
        for rule_id in ordered_ids:
            before = baseline_rules.get(rule_id)
            after = candidate_rules.get(rule_id)
            if before is None and after is not None:
                rule_diffs.append(
                    RuleDiff(
                        rule_id=rule_id,
                        rule_name=after.rule_name,
                        change_type="added",
                        changes=self._changes({}, self._rule_semantics(after)),
                    )
                )
            elif before is not None and after is None:
                rule_diffs.append(
                    RuleDiff(
                        rule_id=rule_id,
                        rule_name=before.rule_name,
                        change_type="removed",
                        changes=self._changes(self._rule_semantics(before), {}),
                    )
                )
            elif before is not None and after is not None:
                changes = self._changes(
                    self._rule_semantics(before),
                    self._rule_semantics(after),
                )
                if changes:
                    rule_diffs.append(
                        RuleDiff(
                            rule_id=rule_id,
                            rule_name=after.rule_name,
                            change_type="changed",
                            changes=changes,
                        )
                    )
        return RulesetDiff(
            ruleset_name=candidate.ruleset_name,
            baseline_version=baseline.version,
            candidate_version=candidate.version,
            baseline_content_hash=self._serializer.content_hash(baseline),
            candidate_content_hash=self._serializer.content_hash(candidate),
            metadata_changes=tuple(metadata_changes),
            rule_diffs=tuple(rule_diffs),
        )

    def _rule_semantics(self, rule: Rule) -> dict[str, Any]:
        description = self._formatter.describe_rule(rule)
        semantics = {
            "rule_name": rule.rule_name,
            "rule_order": rule.rule_order,
            "active_flag": rule.active_flag,
            "stop_on_match": rule.stop_on_match,
            "description": rule.description,
            "when": description["rule_logic"],
            "assign": description["match_payload"],
        }
        self._add_group_semantics(semantics, rule.root_group)
        for position, assignment in enumerate(rule.assignments, start=1):
            semantics[f"assignment[{assignment.assignment_id}]"] = {
                "position": position,
                "target_field": assignment.target_field,
                "value": self._operand_contract(assignment.value),
            }
        return semantics

    def _add_group_semantics(
        self,
        semantics: dict[str, Any],
        group: ConditionGroup,
        *,
        parent_group_id: str | None = None,
        position: int = 1,
    ) -> None:
        """Flatten a group tree into stable, individually reviewable entries."""
        semantics[f"condition_group[{group.condition_group_id}]"] = {
            "parent_group_id": parent_group_id,
            "position": position,
            "logical_operator": group.logical_operator.value,
            "condition_order": tuple(
                condition.condition_id for condition in group.conditions
            ),
            "child_group_order": tuple(
                child.condition_group_id for child in group.groups
            ),
        }
        for condition_position, condition in enumerate(group.conditions, start=1):
            field_name = f"condition[{condition.condition_id}]"
            semantics[field_name] = self._condition_contract(
                condition,
                group.condition_group_id,
                condition_position,
            )
        for child_position, child in enumerate(group.groups, start=1):
            self._add_group_semantics(
                semantics,
                child,
                parent_group_id=group.condition_group_id,
                position=child_position,
            )

    def _condition_contract(
        self,
        condition: Condition,
        group_id: str,
        position: int,
    ) -> dict[str, Any]:
        """Return every condition field that can affect behavior or identity."""
        return {
            "condition_group_id": group_id,
            "position": position,
            "active_flag": condition.active_flag,
            "left": self._operand_contract(condition.left),
            "operator": condition.operator.value,
            "right": (
                self._operand_contract(condition.right)
                if condition.right is not None
                else None
            ),
            "tolerance_abs": format(condition.tolerance_abs, "f"),
            "null_input_mode": condition.null_input_mode.value,
            "null_result_mode": condition.null_result_mode.value,
            "null_default_value": self._contract_value(
                condition.null_default_value
            ),
        }

    def _operand_contract(self, operand: Operand) -> Any:
        """Return a deterministic representation of an operand tree."""
        if isinstance(operand, FieldOperand):
            return {"kind": operand.kind.value, "field_name": operand.field_name}
        if isinstance(operand, LiteralOperand):
            return {
                "kind": operand.kind.value,
                "value": self._contract_value(operand.value),
                "value_type": operand.value_type,
            }
        if isinstance(operand, CustomFunctionOperand):
            return {
                "kind": operand.kind.value,
                "function_name": operand.function_name,
                "args": tuple(
                    (str(name), self._argument_contract(value))
                    for name, value in sorted(
                        operand.args.items(),
                        key=lambda item: str(item[0]),
                    )
                ),
            }
        raise TypeError(f"Unsupported operand type: {type(operand).__name__}")

    def _argument_contract(self, value: Any) -> Any:
        """Return the contract for an operand-shaped or literal argument."""
        if isinstance(
            value,
            (FieldOperand, LiteralOperand, CustomFunctionOperand),
        ):
            return self._operand_contract(value)
        return self._contract_value(value)

    def _contract_value(self, value: Any) -> Any:
        """Normalize nested values for deterministic semantic comparison."""
        if isinstance(value, Decimal):
            return {"type": "decimal", "value": format(value, "f")}
        if isinstance(value, Enum):
            return {"type": "enum", "value": value.value}
        if isinstance(value, (datetime, date)):
            return {"type": type(value).__name__, "value": value.isoformat()}
        if isinstance(value, Mapping):
            return tuple(
                (str(key), self._contract_value(item))
                for key, item in sorted(value.items(), key=lambda item: str(item[0]))
            )
        if isinstance(value, tuple):
            return {
                "type": "tuple",
                "value": tuple(self._contract_value(item) for item in value),
            }
        if isinstance(value, list):
            return [self._contract_value(item) for item in value]
        if isinstance(value, set):
            return {
                "type": "set",
                "value": tuple(
                    self._contract_value(item)
                    for item in sorted(value, key=repr)
                ),
            }
        return value

    def _expected_case_changes(
        self,
        baseline: Ruleset,
        candidate: Ruleset,
    ) -> tuple[SemanticChange, ...]:
        """Diff expected cases by name instead of one unreadable tuple blob."""
        before = {case.name: case for case in baseline.expect}
        after = {case.name: case for case in candidate.expect}
        changes: list[SemanticChange] = []
        before_order = tuple(case.name for case in baseline.expect)
        after_order = tuple(case.name for case in candidate.expect)
        if before_order != after_order:
            changes.append(
                SemanticChange("expected_case_order", before_order, after_order)
            )
        case_names = [
            *before_order,
            *[name for name in after_order if name not in before],
        ]
        for name in case_names:
            changes.extend(
                self._changes(
                    self._expectation_semantics(before.get(name)),
                    self._expectation_semantics(after.get(name)),
                )
            )
        return tuple(changes)

    def _expectation_semantics(
        self,
        expectation: RulesetExpectation | None,
    ) -> dict[str, Any]:
        """Return individually diffable fields for one expected case."""
        if expectation is None:
            return {}
        prefix = f"expect[{expectation.name}]"
        return {
            f"{prefix}.given": self._contract_value(dict(expectation.given)),
            f"{prefix}.then": self._contract_value(dict(expectation.then)),
        }

    def _changes(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> tuple[SemanticChange, ...]:
        return tuple(
            SemanticChange(field, before.get(field), after.get(field))
            for field in dict.fromkeys((*before, *after))
            if before.get(field) != after.get(field)
        )

    def _rule_order(self, rule: Rule) -> tuple[int, str]:
        return rule.rule_order, rule.rule_id
