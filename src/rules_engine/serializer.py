"""
Serialization between canonical models and persisted ruleset-version rows.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import RulesetStatus
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.models import (
    ConditionGroup,
    CustomFunctionOperand,
    Operand,
    Ruleset,
    RulesetVersionRow,
    iter_nested_operands,
)

_JSON_TYPE_KEY = "$rules_engine_type"
_JSON_VALUE_KEY = "value"


class DeltaRowSerializer:
    """
    Convert canonical ruleset models to and from persisted row objects.
    """

    def serialize_ruleset_version(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
        published_at: str | None = None,
    ) -> RulesetVersionRow:
        """
        Serialize one ruleset version to the authoritative Delta row shape.

        Lifecycle status belongs to the table row rather than authored rule
        content, so retirement never rewrites the canonical payload.
        """
        payload_json = self._payload_json(ruleset)
        return RulesetVersionRow(
            ruleset_id=ruleset.ruleset_id,
            ruleset_name=ruleset.ruleset_name,
            version=ruleset.version,
            status=RulesetStatus.PUBLISHED.value,
            description=ruleset.description,
            payload_json=payload_json,
            content_hash=self.content_hash_from_payload_json(payload_json),
            rule_count=len(ruleset.rules),
            condition_count=self._count_conditions(ruleset),
            assignment_count=sum(len(rule.assignments) for rule in ruleset.rules),
            custom_function_count=self._count_operands(ruleset, CustomFunctionOperand),
            owner=ruleset.owner,
            owner_department=ruleset.owner_department,
            published_by=published_by,
            published_at=published_at,
            retired_by=None,
            retired_at=None,
        )

    def deserialize_ruleset_version(self, row: RulesetVersionRow) -> Ruleset:
        """
        Reconstruct a canonical ruleset from one authoritative version row.
        """
        payload = _decode_json_types(json.loads(row.payload_json, parse_float=Decimal))
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
        Count one operand and any operands nested inside custom function args.
        """
        count = 1 if isinstance(operand, operand_type) else 0
        if isinstance(operand, CustomFunctionOperand):
            for arg_value in operand.args.values():
                for nested_operand in iter_nested_operands(arg_value):
                    count += self._count_operand_tree(nested_operand, operand_type)
        return count

    def _payload_json(self, ruleset: Ruleset) -> str:
        """
        Serialize the canonical authoring payload used for persistence.
        """
        payload = YamlRulesetExporter().export_payload(ruleset)
        return _canonical_json_dumps(payload)


def _canonical_json_dumps(value: Any) -> str:
    """Encode deterministic JSON while preserving supported Python types.

    Decimal values are emitted as JSON numbers rather than strings. Integral
    Decimals retain a fractional marker so ``parse_float=Decimal`` restores
    their numeric kind during deserialization. Types absent from JSON use
    reserved, collision-safe envelopes decoded by :func:`_decode_json_types`.
    """
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Persisted Decimal values must be finite.")
        text = str(value)
        # ``e0`` forces json.loads to route an integral Decimal through
        # parse_float=Decimal instead of silently restoring it as an int.
        return text if "." in text or "e" in text.lower() else f"{text}e0"
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Persisted float values must be finite.")
    if isinstance(value, datetime):
        return _tagged_json("datetime", json.dumps(value.isoformat()))
    if isinstance(value, date):
        return _tagged_json("date", json.dumps(value.isoformat()))
    if isinstance(value, tuple):
        items = ",".join(_canonical_json_dumps(item) for item in value)
        return _tagged_json("tuple", f"[{items}]")
    if isinstance(value, set):
        items = sorted(_canonical_json_dumps(item) for item in value)
        return _tagged_json("set", "[" + ",".join(items) + "]")
    if isinstance(value, dict):
        if _JSON_TYPE_KEY in value:
            return _tagged_json("mapping", _canonical_mapping_dumps(value))
        return _canonical_mapping_dumps(value)
    if isinstance(value, list):
        return "[" + ",".join(_canonical_json_dumps(item) for item in value) + "]"
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_mapping_dumps(value: dict[Any, Any]) -> str:
    """Encode mapping contents without interpreting reserved persistence keys."""
    encoded_items = (
        f"{json.dumps(key, separators=(',', ':'))}:{_canonical_json_dumps(item)}"
        for key, item in sorted(value.items())
    )
    return "{" + ",".join(encoded_items) + "}"


def _tagged_json(type_name: str, encoded_value: str) -> str:
    """Return one deterministic extended-JSON value envelope."""
    return (
        "{"
        f"{json.dumps(_JSON_TYPE_KEY)}:{json.dumps(type_name)},"
        f"{json.dumps(_JSON_VALUE_KEY)}:{encoded_value}"
        "}"
    )


def _decode_json_types(value: Any) -> Any:
    """Restore Python-only literal types from persisted extended JSON."""
    if isinstance(value, list):
        return [_decode_json_types(item) for item in value]
    if not isinstance(value, dict):
        return value
    if set(value) != {_JSON_TYPE_KEY, _JSON_VALUE_KEY}:
        return {key: _decode_json_types(item) for key, item in value.items()}

    type_name = value[_JSON_TYPE_KEY]
    encoded_value = value[_JSON_VALUE_KEY]
    if type_name == "date":
        if not isinstance(encoded_value, str):
            raise _invalid_envelope(type_name, "value must be an ISO string")
        try:
            return date.fromisoformat(encoded_value)
        except ValueError as exc:
            raise _invalid_envelope(type_name, "value must be a valid ISO date") from exc
    if type_name == "datetime":
        if not isinstance(encoded_value, str):
            raise _invalid_envelope(type_name, "value must be an ISO string")
        try:
            return datetime.fromisoformat(encoded_value)
        except ValueError as exc:
            raise _invalid_envelope(
                type_name,
                "value must be a valid ISO datetime",
            ) from exc
    if type_name == "tuple":
        if not isinstance(encoded_value, list):
            raise _invalid_envelope(type_name, "value must be an array")
        return tuple(_decode_json_types(item) for item in encoded_value)
    if type_name == "set":
        if not isinstance(encoded_value, list):
            raise _invalid_envelope(type_name, "value must be an array")
        try:
            return {_decode_json_types(item) for item in encoded_value}
        except TypeError as exc:
            raise _invalid_envelope(
                type_name,
                "decoded items must be hashable",
            ) from exc
    if type_name == "mapping":
        if not isinstance(encoded_value, dict):
            raise _invalid_envelope(type_name, "value must be an object")
        # Decode the mapping's values only. Recursing on the inner mapping as a
        # whole would reinterpret an escaped user-owned $rules_engine_type key.
        encoded_items = ((key, _decode_json_types(item)) for key, item in encoded_value.items())
        return dict(encoded_items)
    raise ValueError(f"Unsupported persisted literal type envelope: {type_name!r}.")


def _invalid_envelope(type_name: Any, reason: str) -> ValueError:
    """Return one uniform corruption error for an extended-JSON envelope."""
    return ValueError(f"Invalid persisted {type_name!r} envelope: {reason}.")
