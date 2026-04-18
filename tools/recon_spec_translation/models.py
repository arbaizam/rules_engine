"""
Models for reconciliation spec translation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceCriterion:
    """
    One source reconciliation criterion row.
    """

    match_rule_name: str
    criteria_sequence: int
    field_name: str
    value_operator: str
    value: str
    join_type: str | None
    group_sequence: int = 1
    group_join_operator: str | None = None


@dataclass
class TranslationAuditRecord:
    """
    Audit record for one source rule translation.
    """

    source_rule_name: str
    source_row_count: int
    translated_rule_name: str | None = None
    translated_condition_count: int = 0
    grouping_pattern_detected: str | None = None
    warnings: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


@dataclass
class TranslationResult:
    """
    Translation result containing YAML payload and audit records.
    """

    payload: dict[str, Any]
    audit_records: list[TranslationAuditRecord]
