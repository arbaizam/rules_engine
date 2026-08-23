import pytest
from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_runtime import (
    SparkRulesEngineRuntime,
    _result_struct,
    result_field_names,
)


class NoOpRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class FakeSparkRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=True):
        return self.values


def _payload(*, version="1"):
    return {
        "ruleset_id": "loan_cleaning",
        "ruleset_name": "Loan Cleaning",
        "version": version,
        "owner": "Data Quality",
        "owner_department": "Lending",
        "rules": [
            {
                "rule_id": "prime",
                "rule_name": "Prime loans",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "condition_id": "fico-prime",
                            "left": {"field": "fico"},
                            "operator": "ge",
                            "right": {"literal": 720},
                        }
                    ]
                },
                "assign": {"bucket": "prime", "rate": 0.0425},
            },
            {
                "rule_id": "near-prime",
                "rule_name": "Near-prime loans",
                "rule_order": 2,
                "when": {
                    "all": [
                        {
                            "condition_id": "fico-near-prime",
                            "left": {"field": "fico"},
                            "operator": "ge",
                            "right": {"literal": 680},
                        }
                    ]
                },
                "assign": {"review": True},
            },
        ],
    }


# Audit contracts and Python/Spark differential behavior


def test_full_audit_controls_detailed_schema_and_payload():
    ruleset = YamlRulesetCompiler().compile_payload(_payload())
    runtime = SparkRulesEngineRuntime(NoOpRepository(), FunctionRegistry())
    assign_fields = ["bucket", "rate", "review"]
    assign_types = {
        "bucket": T.StringType(),
        "rate": T.DecimalType(10, 4),
        "review": T.BooleanType(),
    }

    payloads = {}
    for full_audit in (False, True):
        evaluator = runtime._build_row_evaluator(
            ruleset,
            assign_fields,
            assign_types,
            full_audit=full_audit,
        )
        payloads[full_audit] = evaluator(FakeSparkRow({"fico": 740}))
        assert tuple(payloads[full_audit]) == result_field_names(full_audit=full_audit)
        assert tuple(payloads[full_audit]) == tuple(
            _result_struct(
                T.StructType(),
                full_audit=full_audit,
            ).fieldNames()
        )

    assert tuple(payloads[False]) == (
        "error",
        "matched",
        "matched_rule_ids",
        "assign",
    )
    assert "assignment_results" not in payloads[False]
    assert "matched_rules" not in payloads[False]
    assert payloads[True]["matched_rules"][0]["conditions"]
    assert "assignment_results" in payloads[True]
    assert "matched_rules" in payloads[True]


def test_non_boolean_full_audit_fails_before_spark_execution():
    with pytest.raises(TypeError, match="full_audit must be a bool"):
        result_field_names(full_audit="true")


def test_python_evaluator_and_spark_worker_share_rule_ordering_semantics():
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "differential",
            "ruleset_name": "Differential",
            "version": "1",
            "rules": [
                {
                    "rule_id": "inactive",
                    "rule_name": "Inactive",
                    "rule_order": 1,
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"first": "inactive"},
                },
                {
                    "rule_id": "merge-a",
                    "rule_name": "Merge A",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"first": "A", "shared": "early"},
                },
                {
                    "rule_id": "stop",
                    "rule_name": "Stop",
                    "rule_order": 3,
                    "stop_on_match": True,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "stop"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"shared": "late"},
                },
                {
                    "rule_id": "after-stop",
                    "rule_name": "After stop",
                    "rule_order": 4,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"after": "evaluated"},
                },
            ],
        }
    )
    row_evaluator = SparkRowEvaluator.without_repository(FunctionRegistry())
    runtime = SparkRulesEngineRuntime(NoOpRepository(), FunctionRegistry())
    spark_evaluator = runtime._build_row_evaluator(
        ruleset,
        ["first", "shared", "after"],
        {
            "first": T.StringType(),
            "shared": T.StringType(),
            "after": T.StringType(),
        },
    )

    for row in (
        {"eligible": True, "stop": True},
        {"eligible": True, "stop": False},
        {"eligible": False, "stop": False},
    ):
        expected = row_evaluator.evaluate_row(ruleset, row)
        actual = spark_evaluator(FakeSparkRow(row))

        assert {key: actual[key] for key in ("matched", "matched_rule_ids", "assign")} == expected
