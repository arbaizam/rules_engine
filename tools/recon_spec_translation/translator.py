"""
Translate external reconciliation CSV specs into rules engine YAML payloads.

The translator is intentionally strict. ``JoinType`` connects the current
criterion row to the next criterion row, and ``GroupJoinOperator`` connects
the current group to the next group. Both chains are folded left-to-right.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from tools.recon_spec_translation.models import (
    SourceCriterion,
    TranslationAuditRecord,
    TranslationResult,
)
from tools.recon_spec_translation.normalizer import group_by_match_rule


OPERATOR_MAPPING = {
    "TextEquals": "eq",
    "TextNotEquals": "ne",
    "TextContains": "contains",
    "TextNotContains": "not_contains",
    "TextInList": "in",
    "NumericLessThan": "lt",
    "NumericGreaterThan": "gt",
}


class ReconciliationSpecTranslator:
    """
    Translate reconciliation source rows into canonical authoring YAML payloads.
    """

    def __init__(
        self,
        assignment_target_field: str = "translated_match_rule_name",
    ) -> None:
        self.assignment_target_field = assignment_target_field

    def translate(
        self,
        rows: list[SourceCriterion],
        *,
        ruleset_id: str = "translated_reconciliation_rules",
        ruleset_name: str = "Translated Reconciliation Rules",
        version: str = "1.0.0",
    ) -> TranslationResult:
        """
        Translate source criteria into a rules engine YAML payload.
        """
        rules: list[dict[str, Any]] = []
        audit_records: list[TranslationAuditRecord] = []
        grouped = group_by_match_rule(rows)
        for index, (rule_name, criteria) in enumerate(grouped.items(), start=1):
            audit = TranslationAuditRecord(
                source_rule_name=rule_name,
                source_row_count=len(criteria),
            )
            try:
                when_payload, condition_count, pattern = self._translate_rule_tree(criteria)
                rule_id = self._slug(rule_name)
                rules.append(
                    {
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "rule_order": index,
                        "active_flag": True,
                        "when": when_payload,
                        "assign": {
                            self.assignment_target_field: rule_name,
                        },
                    }
                )
                audit.translated_rule_name = rule_name
                audit.translated_condition_count = condition_count
                audit.grouping_pattern_detected = pattern
            except ValueError as exc:
                audit.failures.append(str(exc))
            audit_records.append(audit)

        if any(record.failures for record in audit_records):
            return TranslationResult(payload={}, audit_records=audit_records)

        return TranslationResult(
            payload={
                "ruleset_id": ruleset_id,
                "ruleset_name": ruleset_name,
                "version": version,
                "status": "draft",
                "rules": rules,
            },
            audit_records=audit_records,
        )

    def _translate_rule_tree(
        self,
        criteria: list[SourceCriterion],
    ) -> tuple[dict[str, Any], int, str]:
        if not criteria:
            raise ValueError("Rule has no criteria.")
        grouped = self._group_criteria(criteria)
        group_expressions: list[dict[str, Any]] = []
        group_connectors: list[str | None] = []
        condition_count = 0
        group_patterns: list[str] = []

        for group_sequence in sorted(grouped):
            group_rows = sorted(grouped[group_sequence], key=lambda row: row.criteria_sequence)
            group_expression, group_count, group_pattern = self._translate_group_tree(
                group_sequence,
                group_rows,
            )
            group_expressions.append(group_expression)
            group_connectors.append(self._group_join_operator(group_sequence, group_rows))
            condition_count += group_count
            group_patterns.append(group_pattern)

        if group_connectors[-1] is not None:
            raise ValueError("Last source group must not define GroupJoinOperator.")
        if any(connector is None for connector in group_connectors[:-1]):
            raise ValueError("Non-final source group must define GroupJoinOperator.")

        expression = self._fold_left_to_right(group_expressions, group_connectors[:-1])
        pattern = self._format_pattern(group_patterns, group_connectors[:-1])
        return expression, condition_count, pattern

    def _group_criteria(
        self,
        criteria: list[SourceCriterion],
    ) -> dict[int, list[SourceCriterion]]:
        grouped: dict[int, list[SourceCriterion]] = defaultdict(list)
        for row in criteria:
            if row.group_sequence < 1:
                raise ValueError("GroupSequence must be a positive integer.")
            grouped[row.group_sequence].append(row)
        return grouped

    def _translate_group_tree(
        self,
        group_sequence: int,
        criteria: list[SourceCriterion],
    ) -> tuple[dict[str, Any], int, str]:
        connectors = [self._normalize_join(row.join_type) for row in criteria]
        if connectors[-1] is not None:
            raise ValueError(
                f"Last source criterion in GroupSequence {group_sequence} must not define JoinType."
            )
        if any(connector is None for connector in connectors[:-1]):
            raise ValueError(
                f"Non-final source criterion in GroupSequence {group_sequence} must define JoinType."
            )
        conditions = [self._translate_condition(row) for row in criteria]
        expression = self._fold_left_to_right(conditions, connectors[:-1])
        pattern = self._format_connector_pattern(connectors[:-1])
        return expression, len(conditions), pattern

    def _group_join_operator(
        self,
        group_sequence: int,
        criteria: list[SourceCriterion],
    ) -> str | None:
        connectors = {
            self._normalize_group_join(row.group_join_operator)
            for row in criteria
            if row.group_join_operator is not None
        }
        if len(connectors) > 1:
            raise ValueError(
                f"Conflicting GroupJoinOperator values in GroupSequence {group_sequence}."
            )
        return next(iter(connectors), None)

    def _fold_left_to_right(
        self,
        expressions: list[dict[str, Any]],
        connectors: list[str | None],
    ) -> dict[str, Any]:
        if not expressions:
            raise ValueError("Cannot build an empty rule tree.")
        if len(expressions) != len(connectors) + 1:
            raise ValueError("Connector count does not match expression count.")

        current = expressions[0]
        for connector, expression in zip(connectors, expressions[1:]):
            if connector is None:
                raise ValueError("Left-to-right chain ended before the final expression.")
            current = self._combine(current, connector, expression)
        if len(expressions) == 1 and "all" not in current and "any" not in current:
            return {"all": [current]}
        return current

    def _combine(
        self,
        left: dict[str, Any],
        connector: str,
        right: dict[str, Any],
    ) -> dict[str, Any]:
        key = "all" if connector == "and" else "any"
        items = list(left[key]) if set(left) == {key} else [left]
        if set(right) == {key}:
            items.extend(right[key])
        else:
            items.append(right)
        return {key: items}

    def _format_pattern(
        self,
        group_patterns: list[str],
        group_connectors: list[str | None],
    ) -> str:
        if not group_connectors:
            return group_patterns[0]
        return (
            f"group_left_to_right({self._format_connector_pattern(group_connectors)}):"
            f"{','.join(group_patterns)}"
        )

    def _format_connector_pattern(self, connectors: list[str | None]) -> str:
        if not connectors:
            return "single"
        return "left_to_right:" + ",".join(str(connector) for connector in connectors)

    def _translate_condition(self, row: SourceCriterion) -> dict[str, Any]:
        try:
            operator = OPERATOR_MAPPING[row.value_operator]
        except KeyError as exc:
            raise ValueError(f"Unsupported ValueOperator: {row.value_operator}") from exc
        return {
            "left": {"field": row.field_name},
            "operator": operator,
            "right": {
                "literal": self._parse_value(row.value_operator, row.value),
                "value_type": self._value_type(row.value_operator),
            },
            "tolerance_abs": "0",
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }

    def _parse_value(self, operator: str, value: str) -> Any:
        if operator == "TextInList":
            values = [item.strip() for item in value.split(",") if item.strip()]
            if not values:
                raise ValueError("TextInList requires a non-empty comma-delimited Value.")
            return values
        if operator in {"NumericLessThan", "NumericGreaterThan"}:
            try:
                numeric = float(value)
            except ValueError as exc:
                raise ValueError(f"Numeric operator requires numeric Value: {value}") from exc
            return int(numeric) if numeric.is_integer() else numeric
        return value

    def _value_type(self, operator: str) -> str:
        if operator in {"NumericLessThan", "NumericGreaterThan"}:
            return "number"
        if operator == "TextInList":
            return "list"
        return "string"

    def _normalize_join(self, join_type: str | None) -> str | None:
        if join_type is None:
            return None
        normalized = join_type.strip().lower()
        if normalized not in {"and", "or"}:
            raise ValueError(f"Unsupported JoinType: {join_type}")
        return normalized

    def _normalize_group_join(self, join_type: str | None) -> str | None:
        if join_type is None:
            return None
        normalized = join_type.strip().lower()
        if normalized not in {"and", "or"}:
            raise ValueError(f"Unsupported GroupJoinOperator: {join_type}")
        return normalized

    def _slug(self, value: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
        return slug or "translated_rule"
