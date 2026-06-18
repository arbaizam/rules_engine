from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator


def test_spark_validator_allows_condition_null_result_error_for_udf_row_path():
    """
    What: Allows condition-level null_result_mode=error for ordinary row UDF checks.
    Why: Non-filter row conditions are evaluated inside the Spark Python row runtime.
    Fails when: Spark compatibility validation blocks a supported row-level ruleset.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "OPEN"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "error",
                            }
                        ]
                    },
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset)

    assert result.passed
    assert not result.has_errors()
