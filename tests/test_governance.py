from dataclasses import replace

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


@pytest.mark.parametrize("full_audit", [False, True], ids=["compact", "full-audit"])
def test_python_evaluator_and_spark_worker_share_rule_ordering_semantics(full_audit):
    """Shuffled metadata yields independently specified merge and stop outcomes."""
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
    rules_by_id = {rule.rule_id: rule for rule in ruleset.rules}
    # Shuffle the compiled model so compiler ordering cannot conceal a runtime
    # that mistakenly executes the authored sequence instead of rule_order.
    ruleset = replace(
        ruleset,
        rules=tuple(
            rules_by_id[rule_id] for rule_id in ("after-stop", "stop", "inactive", "merge-a")
        ),
    )
    row_evaluator = SparkRowEvaluator(FunctionRegistry())
    runtime = SparkRulesEngineRuntime(NoOpRepository(), FunctionRegistry())
    spark_evaluator = runtime._build_row_evaluator(
        ruleset,
        ["first", "shared", "after"],
        {
            "first": T.StringType(),
            "shared": T.StringType(),
            "after": T.StringType(),
        },
        full_audit=full_audit,
    )

    for row, matched_rule_ids, assignments in (
        (
            {"eligible": True, "stop": True},
            ["merge-a", "stop"],
            {
                "first": {"applied": True, "value": "A"},
                "shared": {"applied": True, "value": "late"},
                "after": {"applied": False, "value": None},
            },
        ),
        (
            {"eligible": True, "stop": False},
            ["merge-a", "after-stop"],
            {
                "first": {"applied": True, "value": "A"},
                "shared": {"applied": True, "value": "early"},
                "after": {"applied": True, "value": "evaluated"},
            },
        ),
        (
            {"eligible": False, "stop": False},
            [],
            {
                "first": {"applied": False, "value": None},
                "shared": {"applied": False, "value": None},
                "after": {"applied": False, "value": None},
            },
        ),
        (
            {"eligible": False, "stop": True},
            ["stop"],
            {
                "first": {"applied": False, "value": None},
                "shared": {"applied": True, "value": "late"},
                "after": {"applied": False, "value": None},
            },
        ),
    ):
        expected = {
            "matched": bool(matched_rule_ids),
            "matched_rule_ids": matched_rule_ids,
            "assign": assignments,
        }
        python_result = row_evaluator.evaluate_row(ruleset, row)
        actual = spark_evaluator(FakeSparkRow(row))

        assert python_result == expected
        assert actual["error"] is None
        assert {key: actual[key] for key in ("matched", "matched_rule_ids", "assign")} == expected
        if full_audit:
            assert [trace["rule_id"] for trace in actual["matched_rules"]] == matched_rule_ids
