from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator


def _validate_aggregate(aggregate_payload):
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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
                                "left": {"aggregate": aggregate_payload},
                                "operator": "gt",
                                "right": {"literal": 0},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )
    return SparkRulesetCompatibilityValidator().validate(ruleset)


def test_spark_validator_rejects_median_and_quantile():
    """
    What: Rejects exact median and quantile aggregates for Spark publication.
    Why: Spark v1 runtime does not provide exact percentile parity with Python.
    Fails when: Approximate percentile paths become publishable for Spark rulesets.
    """
    median_result = _validate_aggregate(
        {
            "function": "median",
            "field": "amount",
            "scope": "dataset",
            "null_input_mode": "ignore",
            "null_result_mode": "null",
        }
    )
    quantile_result = _validate_aggregate(
        {
            "function": "quantile",
            "field": "amount",
            "scope": "dataset",
            "args": {"q": 0.5},
            "null_input_mode": "ignore",
            "null_result_mode": "null",
        }
    )

    assert "SPARK_EXACT_PERCENTILE_UNSUPPORTED" in {
        issue.check_name for issue in median_result.issues
    }
    assert "SPARK_EXACT_PERCENTILE_UNSUPPORTED" in {
        issue.check_name for issue in quantile_result.issues
    }


def test_spark_validator_rejects_unsupported_aggregate_null_modes():
    """
    What: Rejects aggregate null modes that Spark precomputation cannot honor.
    Why: Spark aggregate columns are computed before row UDF evaluation.
    Fails when: Runtime-only aggregate null errors can be published.
    """
    input_result = _validate_aggregate(
        {
            "function": "sum",
            "field": "amount",
            "scope": "dataset",
            "null_input_mode": "error",
            "null_result_mode": "null",
        }
    )
    result_result = _validate_aggregate(
        {
            "function": "sum",
            "field": "amount",
            "scope": "dataset",
            "null_input_mode": "ignore",
            "null_result_mode": "error",
        }
    )

    assert "SPARK_AGGREGATE_NULL_INPUT_ERROR_UNSUPPORTED" in {
        issue.check_name for issue in input_result.issues
    }
    assert "SPARK_AGGREGATE_NULL_RESULT_ERROR_UNSUPPORTED" in {
        issue.check_name for issue in result_result.issues
    }


def test_spark_validator_rejects_first_last_propagate():
    """
    What: Rejects FIRST/LAST aggregates with propagate null input mode.
    Why: Spark order-sensitive aggregate precompute does not support propagate semantics.
    Fails when: Spark-incompatible FIRST/LAST null behavior passes validation.
    """
    result = _validate_aggregate(
        {
            "function": "first",
            "field": "event",
            "scope": "dataset",
            "order_by": [{"field": "sequence", "direction": "asc"}],
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert "SPARK_FIRST_LAST_PROPAGATE_UNSUPPORTED" in {
        issue.check_name for issue in result.issues
    }


def test_spark_validator_rejects_unsupported_filter_null_modes():
    """
    What: Rejects aggregate filter predicates using Spark-unsupported null error modes.
    Why: Filter predicates run in Spark expressions, not the Python row error path.
    Fails when: Unsupported filter null modes are silently coerced in Spark.
    """
    result = _validate_aggregate(
        {
            "function": "sum",
            "field": "amount",
            "scope": "dataset",
            "filter": {
                "all": [
                    {
                        "left": {"field": "status"},
                        "operator": "eq",
                        "right": {"literal": "OPEN"},
                        "null_input_mode": "error",
                        "null_result_mode": "error",
                    }
                ]
            },
            "null_input_mode": "ignore",
            "null_result_mode": "null",
        }
    )

    assert {
        "SPARK_FILTER_NULL_INPUT_ERROR_UNSUPPORTED",
        "SPARK_FILTER_NULL_RESULT_ERROR_UNSUPPORTED",
    } <= {issue.check_name for issue in result.issues}


def test_spark_validator_allows_condition_null_result_error_for_udf_row_path():
    """
    What: Allows condition-level null_result_mode=error for ordinary row UDF checks.
    Why: Non-filter row conditions are evaluated inside the Spark Python row runtime.
    Fails when: Spark compatibility validation blocks supported row-level error modes.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "draft",
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

    assert "SPARK_CONDITION_NULL_RESULT_ERROR_UNSUPPORTED" not in {
        issue.check_name for issue in result.issues
    }
