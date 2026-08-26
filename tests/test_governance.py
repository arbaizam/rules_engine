import pytest
from pyspark.sql import types as T

from rules_engine import RulesetCoverageAnalyzer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_runtime import (
    SparkRulesEngineRuntime,
    result_field_names,
)
from rules_engine.standard_functions import STANDARD_FUNCTION_VERSION
from rules_engine.version import __version__


class NoOpRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class FakeSparkRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=True):
        return self.values


# Audit contracts and Python/Spark differential behavior


def test_public_coverage_export_and_metadata_versions_stay_aligned():
    """The documented analyzer imports publicly and registry metadata uses engine version."""
    assert RulesetCoverageAnalyzer.__name__ == "RulesetCoverageAnalyzer"
    assert STANDARD_FUNCTION_VERSION == __version__


def test_non_boolean_full_audit_fails_before_spark_execution():
    """String truthiness cannot silently enable the expensive audit contract."""
    with pytest.raises(TypeError, match="full_audit must be a bool"):
        result_field_names(full_audit="true")


def test_python_evaluator_and_spark_worker_share_rule_ordering_semantics():
    """The compact Python and Spark-worker paths share ordering and stop semantics."""
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
