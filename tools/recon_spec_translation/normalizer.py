"""
Normalization helpers for reconciliation source specs.
"""

from __future__ import annotations

from collections import defaultdict

from tools.recon_spec_translation.models import SourceCriterion


def group_by_match_rule(rows: list[SourceCriterion]) -> dict[str, list[SourceCriterion]]:
    """
    Group source rows by MatchRuleName and sort by group then criterion order.
    """
    grouped: dict[str, list[SourceCriterion]] = defaultdict(list)
    for row in rows:
        grouped[row.match_rule_name].append(row)
    return {
        rule_name: sorted(
            rule_rows,
            key=lambda item: (item.group_sequence, item.criteria_sequence),
        )
        for rule_name, rule_rows in grouped.items()
    }
