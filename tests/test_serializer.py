from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.serializer import DeltaRowSerializer


def test_serializer_produces_explicit_scope_tolerance_and_null_fields():
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "aggregate": {
                                        "function": "sum",
                                        "field": "amount",
                                        "scope": "dataset",
                                        "null_input_mode": "ignore",
                                        "null_result_mode": "null",
                                    }
                                },
                                "operator": "gt",
                                "right": {"literal": 1},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    rows = DeltaRowSerializer().serialize_ruleset(ruleset)
    condition_row = rows.condition_rows[0]

    assert condition_row.aggregate_scope == "dataset"
    assert condition_row.tolerance_abs == "0"
    assert condition_row.null_input_mode == "propagate"
    assert condition_row.null_result_mode == "null"
    assert condition_row.left_operand_payload["scope"] == "dataset"
    assert condition_row.left_operand_payload["by"] == []


def test_serializer_stamps_provenance_and_content_hash():
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    rows = DeltaRowSerializer().serialize_ruleset(
        ruleset,
        created_by="tester",
        created_at="2026-04-18T00:00:00+00:00",
    )

    assert rows.ruleset_row.created_by == "tester"
    assert rows.ruleset_row.created_at == "2026-04-18T00:00:00+00:00"
    assert rows.ruleset_row.published_by is None
    assert len(rows.ruleset_row.content_hash) == 64
    assert rows.rule_rows[0].created_by == "tester"
    assert rows.condition_group_rows[0].created_by == "tester"
    assert rows.condition_rows[0].created_by == "tester"
    assert rows.assignment_rows[0].created_by == "tester"


def test_deserializer_reconstructs_canonical_models():
    compiler = YamlRulesetCompiler()
    original = compiler.compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )
    serializer = DeltaRowSerializer()
    reconstructed = serializer.deserialize_ruleset(serializer.serialize_ruleset(original))

    assert reconstructed == original
