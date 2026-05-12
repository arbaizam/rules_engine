"""
Serialization between canonical models and persisted ruleset-version rows.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.models import (
    AggregateOperand,
    ConditionGroup,
    CustomFunctionOperand,
    Operand,
    Ruleset,
    RulesetVersionRow,
)


class DeltaRowSerializer:
    """
    Convert canonical ruleset models to and from persisted row objects.
    """

    DEFAULT_EFFECTIVE_END_DATE = "2999-12-31"

    def serialize_ruleset_version(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> RulesetVersionRow:
        """
        Serialize one ruleset version to the authoritative Delta row shape.

        The payload intentionally excludes lifecycle status and effective
        dates. The table row metadata is authoritative, which lets publish and
        retire update lifecycle fields without rewriting rule content.
        """
        payload_json = self._payload_json(ruleset)
        return RulesetVersionRow(
            ruleset_id=ruleset.ruleset_id,
            ruleset_name=ruleset.ruleset_name,
            version=ruleset.version,
            status=ruleset.status.value,
            effective_start_date=effective_start_date
            or self._date_from_timestamp(published_at),
            effective_end_date=effective_end_date or self.DEFAULT_EFFECTIVE_END_DATE,
            description=ruleset.description,
            payload_json=payload_json,
            content_hash=self.content_hash_from_payload_json(payload_json),
            rule_count=len(ruleset.rules),
            condition_count=self._count_conditions(ruleset),
            assignment_count=sum(len(rule.assignments) for rule in ruleset.rules),
            aggregate_count=self._count_operands(ruleset, AggregateOperand),
            custom_function_count=self._count_operands(ruleset, CustomFunctionOperand),
            owner=ruleset.owner,
            owner_department=ruleset.owner_department,
            published_by=published_by,
            published_at=published_at,
            retired_by=retired_by,
            retired_at=retired_at,
        )

    def deserialize_ruleset_version(self, row: RulesetVersionRow) -> Ruleset:
        """
        Reconstruct a canonical ruleset from one authoritative version row.
        """
        payload = json.loads(row.payload_json)
        payload["status"] = row.status
        return YamlRulesetCompiler().compile_payload(payload)

    def content_hash(self, ruleset: Ruleset) -> str:
        """
        Return a deterministic SHA-256 hash of canonical ruleset content.

        Lifecycle provenance and status are intentionally excluded. The hash
        changes when rule semantics or authoring metadata change, not when a
        ruleset is published by a different operator.
        """
        return self.content_hash_from_payload_json(self._payload_json(ruleset))

    def content_hash_from_payload_json(self, payload_json: str) -> str:
        """
        Return the SHA-256 hash of the persisted payload JSON bytes.

        This makes ``content_hash`` independently reproducible from the
        ``payload_json`` column stored in Delta.
        """
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def _count_conditions(self, ruleset: Ruleset) -> int:
        """
        Count all conditions in all rule condition trees.
        """
        return sum(self._count_group_conditions(rule.root_group) for rule in ruleset.rules)

    def _count_group_conditions(self, group: ConditionGroup) -> int:
        """
        Count conditions in one group and its nested child groups.
        """
        return len(group.conditions) + sum(
            self._count_group_conditions(child) for child in group.groups
        )

    def _count_operands(self, ruleset: Ruleset, operand_type: type) -> int:
        """
        Count operands of a target type across conditions and assignments.
        """
        count = 0
        for rule in ruleset.rules:
            count += self._count_group_operands(rule.root_group, operand_type)
            for assignment in rule.assignments:
                count += self._count_operand_tree(assignment.value, operand_type)
        return count

    def _count_group_operands(self, group: ConditionGroup, operand_type: type) -> int:
        """
        Count operands of a target type within one condition group tree.
        """
        count = 0
        for condition in group.conditions:
            count += self._count_operand_tree(condition.left, operand_type)
            if condition.right is not None:
                count += self._count_operand_tree(condition.right, operand_type)
        for child in group.groups:
            count += self._count_group_operands(child, operand_type)
        return count

    def _count_operand_tree(self, operand: Operand, operand_type: type) -> int:
        """
        Count one operand and any operands nested inside aggregate filters.
        """
        count = 1 if isinstance(operand, operand_type) else 0
        if isinstance(operand, AggregateOperand) and operand.filter is not None:
            for predicate in operand.filter.predicates:
                count += self._count_operand_tree(predicate.left, operand_type)
                if predicate.right is not None:
                    count += self._count_operand_tree(predicate.right, operand_type)
        return count

    def _payload_json(self, ruleset: Ruleset) -> str:
        """
        Serialize the canonical authoring payload used for persistence.
        """
        payload = YamlRulesetExporter().export_payload(ruleset)
        payload.pop("status", None)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _date_from_timestamp(self, value: str | None) -> str:
        """
        Return a non-null effective date from a timestamp-like string.
        """
        if value:
            return value[:10]
        return date.today().isoformat()
