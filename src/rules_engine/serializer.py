"""
Serialization between canonical models and persisted ruleset-version rows.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from decimal import Decimal
from typing import Any

from rules_engine.canonical_values import canonical_json_dumps as _canonical_json_dumps
from rules_engine.canonical_values import decode_json_types as _decode_json_types
from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.model_codec import decode_ruleset, encode_ruleset
from rules_engine.models import (
    CustomFunctionOperand,
    Ruleset,
    RulesetVersionRow,
)
from rules_engine.traversal import iter_conditions, iter_ruleset_operands


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
            condition_count=sum(
                1 for rule in ruleset.rules for _ in iter_conditions(rule.root_group)
            ),
            assignment_count=sum(len(rule.assignments) for rule in ruleset.rules),
            custom_function_count=sum(
                isinstance(operand, CustomFunctionOperand)
                for operand in iter_ruleset_operands(ruleset)
            ),
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
        try:
            if not isinstance(row.payload_json, str):
                raise ValueError("Persisted payload_json must be a string.")
            expected_hash = self.content_hash_from_payload_json(row.payload_json)
            if not isinstance(row.content_hash, str) or not hmac.compare_digest(
                expected_hash, row.content_hash
            ):
                raise ValueError("Persisted content_hash does not match payload_json.")
            payload = _decode_json_types(
                json.loads(
                    row.payload_json,
                    parse_float=Decimal,
                    parse_constant=_reject_json_constant,
                    object_pairs_hook=_unique_json_mapping,
                )
            )
            ruleset = decode_ruleset(payload)
            for field in ("ruleset_id", "ruleset_name", "version"):
                if getattr(ruleset, field) != getattr(row, field):
                    raise ValueError(f"Persisted {field} disagrees with payload identity.")
            if self._payload_json(ruleset) != row.payload_json:
                raise ValueError("Persisted payload is not canonical for its declared format.")
            return ruleset
        except (ValueError, TypeError, KeyError, ArithmeticError, RecursionError) as exc:
            raise RepositoryError(f"Cannot load persisted ruleset: {exc}") from exc

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

    def _payload_json(self, ruleset: Ruleset) -> str:
        """
        Serialize the explicitly versioned canonical persistence document.
        """
        payload = encode_ruleset(ruleset)
        return _canonical_json_dumps(payload)


def _reject_json_constant(value: str) -> Any:
    """Reject JSON extensions that permit non-finite numbers."""
    raise ValueError(f"Non-finite JSON numeric token is forbidden: {value}.")


def _unique_json_mapping(items: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate keys before decoding the persisted document."""
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError(f"Duplicate persisted JSON key: {key!r}.")
        result[key] = value
    return result
