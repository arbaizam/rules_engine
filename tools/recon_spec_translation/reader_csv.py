"""
CSV reader for reconciliation source specifications.
"""

from __future__ import annotations

import csv
from pathlib import Path

from tools.recon_spec_translation.models import SourceCriterion

REQUIRED_COLUMNS = {
    "MatchRuleName",
    "GroupSequence",
    "GroupJoinOperator",
    "CriteriaSequence",
    "FieldName",
    "ValueOperator",
    "Value",
    "JoinType",
}


def read_reconciliation_csv(path: str | Path) -> list[SourceCriterion]:
    """
    Read source reconciliation criteria from CSV.
    """
    path_obj = Path(path)
    with path_obj.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(missing)}")
        rows: list[SourceCriterion] = []
        for row in reader:
            rows.append(
                SourceCriterion(
                    match_rule_name=str(row["MatchRuleName"]).strip(),
                    criteria_sequence=int(row["CriteriaSequence"]),
                    field_name=str(row["FieldName"]).strip(),
                    value_operator=str(row["ValueOperator"]).strip(),
                    value=str(row["Value"]).strip(),
                    join_type=_normalize_blank(row.get("JoinType")),
                    group_sequence=int(row["GroupSequence"]),
                    group_join_operator=_normalize_blank(row.get("GroupJoinOperator")),
                )
            )
    return rows


def _normalize_blank(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None
