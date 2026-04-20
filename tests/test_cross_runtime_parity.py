import os

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import RulesEngineRuntime
from rules_engine.spark_runtime import SparkRulesEngineRuntime


pytest.importorskip("pyspark")

pytestmark = pytest.mark.skipif(
    os.environ.get("RULES_ENGINE_RUN_SPARK_TESTS") != "1",
    reason="Set RULES_ENGINE_RUN_SPARK_TESTS=1 to run Spark parity tests.",
)


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        yield active_session
        return

    builder = (
        SparkSession.builder
        .appName("rules-engine-cross-runtime-parity-tests")
        .config("spark.ui.enabled", "false")
    )
    connect_or_databricks = any(
        os.environ.get(name)
        for name in (
            "SPARK_REMOTE",
            "SPARK_CONNECT_MODE_ENABLED",
            "DATABRICKS_RUNTIME_VERSION",
            "DATABRICKS_CLUSTER_ID",
        )
    )
    if not connect_or_databricks:
        builder = builder.master("local[1]")

    session = builder.getOrCreate()
    yield session
    if not connect_or_databricks:
        session.stop()


def _compile(condition):
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {"all": [condition]},
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )


def _assert_parity(spark, rows, condition):
    ruleset = _compile(condition)
    registry = FunctionRegistry()
    python_output, _ = RulesEngineRuntime(DummyRepository(), registry).evaluate(rows, ruleset)
    spark_rows = (
        SparkRulesEngineRuntime(DummyRepository(), registry)
        .evaluate_dataframe(spark.createDataFrame(rows), ruleset, fail_on_error=True)
        .orderBy("row_id")
        .collect()
    )

    assert [row["matched"] for row in python_output] == [
        row["rules_engine_matched"] for row in spark_rows
    ]


def test_cross_runtime_parity_row_comparison(spark):
    """
    What: Compares Python and Spark results for a simple row comparison.
    Why: Spark row UDF behavior should preserve reference runtime semantics.
    Fails when: Basic equality matching diverges between runtimes.
    """
    _assert_parity(
        spark,
        [{"row_id": 1, "account": "A"}, {"row_id": 2, "account": "B"}],
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )


def test_cross_runtime_parity_string_operator(spark):
    """
    What: Compares Python and Spark results for SQL LIKE wildcard matching.
    Why: LIKE was a prior semantic divergence risk and needs parity coverage.
    Fails when: Python regex LIKE and Spark Column.like disagree.
    """
    _assert_parity(
        spark,
        [{"row_id": 1, "name": "abcde"}, {"row_id": 2, "name": "xyz"}],
        {
            "left": {"field": "name"},
            "operator": "like",
            "right": {"literal": "abc%"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )


def test_cross_runtime_parity_dataset_aggregate(spark):
    """
    What: Compares Python and Spark results for dataset aggregate evaluation.
    Why: Aggregate precompute must preserve reference runtime results.
    Fails when: Spark dataset aggregate values differ from Python aggregate values.
    """
    _assert_parity(
        spark,
        [{"row_id": 1, "amount": 10}, {"row_id": 2, "amount": 20}],
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
            "operator": "eq",
            "right": {"literal": 30},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )


def test_cross_runtime_parity_group_aggregate(spark):
    """
    What: Compares Python and Spark results for group aggregate evaluation.
    Why: Explicit group scope must behave identically across runtimes.
    Fails when: Spark grouping/joining diverges from Python group-key resolution.
    """
    _assert_parity(
        spark,
        [
            {"row_id": 1, "account": "A", "amount": 10},
            {"row_id": 2, "account": "A", "amount": 20},
            {"row_id": 3, "account": "B", "amount": 5},
        ],
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "group",
                    "by": ["account"],
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "gt",
            "right": {"literal": 15},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )


def test_cross_runtime_parity_filtered_aggregate(spark):
    """
    What: Compares Python and Spark results for filtered aggregate evaluation.
    Why: Filtered aggregate semantics must not depend on runtime implementation.
    Fails when: Spark filter precompute and Python filter evaluation diverge.
    """
    _assert_parity(
        spark,
        [
            {"row_id": 1, "status": "OPEN", "amount": 10},
            {"row_id": 2, "status": "CLOSED", "amount": 50},
            {"row_id": 3, "status": "OPEN", "amount": 20},
        ],
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "dataset",
                    "filter": {
                        "all": [
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "OPEN"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "eq",
            "right": {"literal": 30},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )


def test_cross_runtime_parity_first_with_desc_null_ordering(spark):
    """
    What: Compares Python and Spark results for FIRST desc ordering with nulls.
    Why: Descending null ordering was a prior cross-runtime divergence risk.
    Fails when: Either runtime changes null placement for order-sensitive aggregates.
    """
    _assert_parity(
        spark,
        [
            {"row_id": 1, "sequence": None, "event": "null-sequence"},
            {"row_id": 2, "sequence": 2, "event": "largest"},
            {"row_id": 3, "sequence": 1, "event": "smallest"},
        ],
        {
            "left": {
                "aggregate": {
                    "function": "first",
                    "field": "event",
                    "scope": "dataset",
                    "order_by": [{"field": "sequence", "direction": "desc"}],
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "eq",
            "right": {"literal": "largest"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    )
