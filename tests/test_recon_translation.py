import json
from pathlib import Path

import pytest

pytest.importorskip("tools.recon_spec_translation")

from tools.recon_spec_translation.audit import write_audit
from tools.recon_spec_translation.models import SourceCriterion
from tools.recon_spec_translation.translator import ReconciliationSpecTranslator
from tools.recon_spec_translation.writer_yaml import to_yaml


def source_row(
    rule_name,
    sequence,
    field_name,
    operator,
    value,
    join_type,
    *,
    group_sequence=1,
    group_join_operator=None,
):
    return SourceCriterion(
        match_rule_name=rule_name,
        criteria_sequence=sequence,
        field_name=field_name,
        value_operator=operator,
        value=value,
        join_type=join_type,
        group_sequence=group_sequence,
        group_join_operator=group_join_operator,
    )


def test_all_and_source_rule_translates_correctly():
    """
    What: Translates a flat all-AND reconciliation rule into canonical YAML.
    Why: All-AND chains are the baseline source pattern for recon migration.
    Fails when: JoinType handling, assignment output, or audit grouping regresses.
    """
    result = ReconciliationSpecTranslator("match_rule").translate(
        [
            source_row("Rule A", 1, "BalanceType", "TextEquals", "ParAmount", "And"),
            source_row("Rule A", 2, "PortfolioType", "TextEquals", "TRAD", None),
        ]
    )

    rule = result.payload["rules"][0]
    assert "all" in rule["when"]
    assert rule["assign"] == {"match_rule": "Rule A"}
    assert rule["stop_on_match"] is True
    assert result.audit_records[0].grouping_pattern_detected == "left_to_right:and"


def test_translator_can_disable_stop_on_match():
    """
    What: Verifies translator-level stop_on_match can be disabled.
    Why: Some callers need metadata to return all matching rules instead of first match.
    Fails when: The translator ignores the configured multi-match policy.
    """
    result = ReconciliationSpecTranslator(stop_on_match=False).translate(
        [source_row("Rule A", 1, "Field", "TextEquals", "x", None)]
    )

    assert result.payload["rules"][0]["stop_on_match"] is False


def test_mixed_join_chain_translates_left_to_right():
    """
    What: Translates a mixed AND/OR chain using confirmed left-to-right semantics.
    Why: Manual PySpark JoinType logic chains operators from each row to the next row.
    Fails when: The translator reintroduces precedence assumptions or wrong grouping.
    """
    result = ReconciliationSpecTranslator().translate(
        [
            source_row("Rule B", 1, "BalanceType", "TextEquals", "Unrealized", "And"),
            source_row("Rule B", 2, "PortfolioType", "TextEquals", "TRAD", "And"),
            source_row("Rule B", 3, "Holding", "TextContains", "Agency", "Or"),
            source_row("Rule B", 4, "Holding", "TextContains", "Agencies", None),
        ]
    )

    items = result.payload["rules"][0]["when"]["any"]
    assert len(items) == 2
    assert len(items[0]["all"]) == 3
    assert items[1]["operator"] == "contains"
    assert result.audit_records[0].grouping_pattern_detected == "left_to_right:and,and,or"


def test_group_sequence_translates_with_group_join_operator():
    """
    What: Translates grouped source criteria using GroupSequence and GroupJoinOperator.
    Why: Group metadata resolves source grouping that cannot be inferred from JoinType alone.
    Fails when: Group ordering, group joins, or audit pattern detection regresses.
    """
    result = ReconciliationSpecTranslator().translate(
        [
            source_row(
                "Rule C",
                1,
                "BalanceType",
                "TextEquals",
                "Par",
                None,
                group_sequence=1,
                group_join_operator="Or",
            ),
            source_row("Rule C", 1, "BalanceType", "TextEquals", "UGL", "And", group_sequence=2),
            source_row("Rule C", 2, "PortfolioType", "TextEquals", "AFS", None, group_sequence=2),
        ]
    )

    groups = result.payload["rules"][0]["when"]["any"]
    assert len(groups) == 2
    assert groups[0]["all"][0]["right"]["literal"] == "Par"
    assert len(groups[1]["all"]) == 2
    assert result.audit_records[0].grouping_pattern_detected.startswith("group_left_to_right")


def test_operator_mapping_for_all_supported_source_operators():
    """
    What: Maps every supported source ValueOperator to its canonical operator.
    Why: The translation utility must preserve source semantics without aliases.
    Fails when: A supported operator maps incorrectly or stops translating.
    """
    operators = [
        ("TextEquals", "eq", "x"),
        ("NumericLessThan", "lt", "1"),
        ("NumericGreaterThan", "gt", "1"),
        ("TextNotEquals", "ne", "x"),
        ("TextInList", "in", "a,b"),
        ("TextContains", "contains", "x"),
        ("TextNotContains", "not_contains", "x"),
    ]
    rows = [
        source_row(f"Rule {index}", 1, "Field", source, value, None)
        for index, (source, _, value) in enumerate(operators, start=1)
    ]

    result = ReconciliationSpecTranslator().translate(rows)

    actual = [
        rule["when"]["all"][0]["operator"]
        for rule in result.payload["rules"]
    ]
    assert actual == [expected for _, expected, _ in operators]


def test_slug_collision_emits_unique_rule_ids_and_audit_warning():
    """
    What: Deduplicates translated rule IDs when source rule names slug-collide.
    Why: Rule IDs drive audit output and must remain unique after normalization.
    Fails when: Collision handling creates duplicate IDs or omits audit warnings.
    """
    result = ReconciliationSpecTranslator().translate(
        [
            source_row("US Account", 1, "Field", "TextEquals", "x", None),
            source_row("US-Account", 1, "Field", "TextEquals", "y", None),
        ]
    )

    rule_ids = [rule["rule_id"] for rule in result.payload["rules"]]

    assert rule_ids == ["us_account", "us_account_2"]
    assert len(rule_ids) == len(set(rule_ids))
    assert result.audit_records[1].warnings


def test_invalid_source_operator_fails_translation():
    """
    What: Rejects an unsupported source ValueOperator.
    Why: The migration utility must fail explicitly instead of guessing semantics.
    Fails when: Unsupported operators are silently converted or omitted.
    """
    result = ReconciliationSpecTranslator().translate(
        [source_row("Rule A", 1, "Field", "Unsupported", "x", None)]
    )

    assert result.payload == {}
    assert result.audit_records[0].failures


def test_null_join_before_final_criterion_fails_translation():
    """
    What: Rejects a non-final source row with a null JoinType.
    Why: Null JoinType terminates the source chain and later criteria are ambiguous.
    Fails when: The translator accepts disconnected criteria without audit failure.
    """
    result = ReconciliationSpecTranslator().translate(
        [
            source_row("Rule A", 1, "A", "TextEquals", "x", None),
            source_row("Rule A", 2, "B", "TextEquals", "x", None),
        ]
    )

    assert result.payload == {}
    assert "Non-final source criterion" in result.audit_records[0].failures[0]


def test_last_group_join_operator_fails_translation():
    """
    What: Rejects a final source group that still declares a GroupJoinOperator.
    Why: A final group has no next group, so the join operator cannot be applied.
    Fails when: Dangling group joins are ignored instead of reported.
    """
    result = ReconciliationSpecTranslator().translate(
        [
            source_row(
                "Rule A",
                1,
                "A",
                "TextEquals",
                "x",
                None,
                group_sequence=1,
                group_join_operator="And",
            ),
        ]
    )

    assert result.payload == {}
    assert "Last source group" in result.audit_records[0].failures[0]


def test_output_yaml_aligns_to_engine_authoring_format():
    """
    What: Emits YAML text using the rules engine authoring vocabulary.
    Why: Translated specs must compile as ordinary canonical authoring YAML.
    Fails when: Writer output drifts from compiler-supported YAML shape.
    """
    result = ReconciliationSpecTranslator().translate(
        [source_row("Rule A", 1, "Field", "TextEquals", "x", None)]
    )

    yaml_text = to_yaml(result.payload)

    assert "ruleset_id:" in yaml_text
    assert "operator: eq" in yaml_text
    assert "null_input_mode: propagate" in yaml_text


def test_audit_artifact_contains_expected_fields():
    """
    What: Writes translation audit records with required governance fields.
    Why: One-time migrations need a durable explanation of warnings and failures.
    Fails when: Audit serialization drops source identity, counts, warnings, or failures.
    """
    result = ReconciliationSpecTranslator().translate(
        [source_row("Rule A", 1, "Field", "TextEquals", "x", None)]
    )
    audit_path = Path("audit_test_output.json")

    try:
        write_audit(result.audit_records, audit_path)

        rows = json.loads(audit_path.read_text(encoding="utf-8"))
        assert rows[0]["source_rule_name"] == "Rule A"
        assert rows[0]["source_row_count"] == 1
        assert "warnings" in rows[0]
        assert "failures" in rows[0]
    finally:
        audit_path.unlink(missing_ok=True)
