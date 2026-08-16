"""Human-readable semantic ruleset comparison."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import Rule, Ruleset


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
    metadata_changes: tuple[SemanticChange, ...]
    rule_diffs: tuple[RuleDiff, ...]

    @property
    def has_changes(self) -> bool:
        return bool(self.metadata_changes or self.rule_diffs)

    def to_text(self) -> str:
        """Render a compact review document in author-facing syntax."""
        lines = [
            f"Ruleset diff: {self.ruleset_name} "
            f"{self.baseline_version} -> {self.candidate_version}"
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

    def diff(self, baseline: Ruleset, candidate: Ruleset) -> RulesetDiff:
        """Return semantic changes from baseline to candidate."""
        metadata_changes = self._changes(
            {
                "ruleset_id": baseline.ruleset_id,
                "ruleset_name": baseline.ruleset_name,
                "status": baseline.status.value,
                "description": baseline.description,
                "owner": baseline.owner,
                "owner_department": baseline.owner_department,
                "expected_cases": self._expected_cases(baseline),
            },
            {
                "ruleset_id": candidate.ruleset_id,
                "ruleset_name": candidate.ruleset_name,
                "status": candidate.status.value,
                "description": candidate.description,
                "owner": candidate.owner,
                "owner_department": candidate.owner_department,
                "expected_cases": self._expected_cases(candidate),
            },
        )
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
            metadata_changes=metadata_changes,
            rule_diffs=tuple(rule_diffs),
        )

    def _rule_semantics(self, rule: Rule) -> dict[str, Any]:
        description = self._formatter.describe_rule(rule)
        return {
            "rule_name": rule.rule_name,
            "rule_order": rule.rule_order,
            "active_flag": rule.active_flag,
            "stop_on_match": rule.stop_on_match,
            "description": rule.description,
            "when": description["rule_logic"],
            "assign": description["match_payload"],
        }

    def _expected_cases(self, ruleset: Ruleset) -> tuple[Any, ...]:
        return tuple(
            (case.name, dict(case.given), dict(case.then))
            for case in ruleset.expect
        )

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
