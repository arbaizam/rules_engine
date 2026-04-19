"""
Serialization between canonical models and persisted ruleset-version rows.
"""

from __future__ import annotations

import hashlib
import json

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.models import (
    AggregateOperand,
    ConditionGroup,
    CustomFunctionOperand,
    FunctionRegistryRow,
    Operand,
    Ruleset,
    RulesetVersionRow,
)
from rules_engine.registry import CustomFunctionSpec


class DeltaRowSerializer:
    """
    Convert canonical ruleset models to and from persisted row objects.
    """

    def serialize_ruleset_version(
        self,
        ruleset: Ruleset,
        *,
        created_by: str = "system",
        created_at: str = "unknown",
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> RulesetVersionRow:
        """
        Serialize one ruleset version to the authoritative Delta row shape.

        The payload intentionally excludes lifecycle status. The table row
        status is authoritative, which lets publish/retire update lifecycle
        metadata without rewriting rule content.
        """
        payload_json = self._payload_json(ruleset)
        return RulesetVersionRow(
            ruleset_id=ruleset.ruleset_id,
            ruleset_name=ruleset.ruleset_name,
            version=ruleset.version,
            status=ruleset.status.value,
            description=ruleset.description,
            payload_json=payload_json,
            content_hash=self.content_hash_from_payload_json(payload_json),
            rule_count=len(ruleset.rules),
            condition_count=self._count_conditions(ruleset),
            assignment_count=sum(len(rule.assignments) for rule in ruleset.rules),
            aggregate_count=self._count_operands(ruleset, AggregateOperand),
            custom_function_count=self._count_operands(ruleset, CustomFunctionOperand),
            created_by=created_by,
            created_at=created_at,
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
        draft is saved or published by a different operator.
        """
        return self.content_hash_from_payload_json(self._payload_json(ruleset))

    def content_hash_from_payload_json(self, payload_json: str) -> str:
        """
        Return the SHA-256 hash of the persisted payload JSON bytes.

        This makes ``content_hash`` independently reproducible from the
        ``payload_json`` column stored in Delta.
        """
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def serialize_function_spec(self, spec: CustomFunctionSpec) -> FunctionRegistryRow:
        """
        Serialize a custom function spec to a registry row.
        """
        return spec.to_row()

    def deserialize_function_spec(self, row: FunctionRegistryRow) -> CustomFunctionSpec:
        """
        Reconstruct a custom function spec from a registry row.
        """
        return CustomFunctionSpec.from_row(row)

    def _count_conditions(self, ruleset: Ruleset) -> int:
        return sum(self._count_group_conditions(rule.root_group) for rule in ruleset.rules)

    def _count_group_conditions(self, group: ConditionGroup) -> int:
        return len(group.conditions) + sum(
            self._count_group_conditions(child) for child in group.groups
        )

    def _count_operands(self, ruleset: Ruleset, operand_type: type) -> int:
        count = 0
        for rule in ruleset.rules:
            count += self._count_group_operands(rule.root_group, operand_type)
            for assignment in rule.assignments:
                count += self._count_operand_tree(assignment.value, operand_type)
        return count

    def _count_group_operands(self, group: ConditionGroup, operand_type: type) -> int:
        count = 0
        for condition in group.conditions:
            count += self._count_operand_tree(condition.left, operand_type)
            if condition.right is not None:
                count += self._count_operand_tree(condition.right, operand_type)
        for child in group.groups:
            count += self._count_group_operands(child, operand_type)
        return count

    def _count_operand_tree(self, operand: Operand, operand_type: type) -> int:
        count = 1 if isinstance(operand, operand_type) else 0
        if isinstance(operand, AggregateOperand) and operand.filter is not None:
            for predicate in operand.filter.predicates:
                count += self._count_operand_tree(predicate.left, operand_type)
                if predicate.right is not None:
                    count += self._count_operand_tree(predicate.right, operand_type)
        return count

    def _payload_json(self, ruleset: Ruleset) -> str:
        payload = YamlRulesetExporter().export_payload(ruleset)
        payload.pop("status", None)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))
