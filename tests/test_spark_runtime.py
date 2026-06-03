import json
import os

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime


pytest.importorskip("pyspark")

pytestmark = pytest.mark.skipif(
    os.environ.get("RULES_ENGINE_RUN_SPARK_TESTS") != "1",
    reason="Set RULES_ENGINE_RUN_SPARK_TESTS=1 to run local Spark runtime tests.",
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
        .appName("rules-engine-spark-runtime-tests")
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


def _spark_runtime():
    return SparkRulesEngineRuntime(DummyRepository(), FunctionRegistry())


def _compile(condition, assign=None):
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
                    "assign": assign or {"bucket": "matched"},
                }
            ],
        }
    )


def test_spark_runtime_evaluates_row_rule(spark):
    """
    What: Evaluates a row-level rule through Spark DataFrame runtime.
    Why: Spark output columns must reflect the same matching/assignment contract.
    Fails when: UDF result struct, output columns, or assignment JSON regress.
    """
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"account": "A"}, {"account": "B"}])

    rows = _spark_runtime().evaluate_dataframe(df, ruleset).orderBy("account").collect()

    assert rows[0]["rules_engine_matched"] is True
    assert json.loads(rows[0]["rules_engine_assign"]) == {"bucket": "matched"}
    rule_results = json.loads(rows[0]["rules_engine_rule_results"])
    assert rule_results[0]["rule_id"] == "r1"
    assert rule_results[0]["conditions"][0]["columns"] == ["account"]
    assert rule_results[0]["conditions"][0]["left"]["column"] == "account"
    assert rule_results[0]["conditions"][0]["left"]["value"] == "A"
    assert rows[1]["rules_engine_matched"] is False


def test_spark_runtime_evaluates_dataset_aggregate(spark):
    """
    What: Evaluates a dataset aggregate through Spark precompute plus UDF.
    Why: Spark must join dataset aggregate results back to every input row.
    Fails when: Dataset aggregate precompute or cross join behavior changes.
    """
    ruleset = _compile(
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
        }
    )
    df = spark.createDataFrame([{"amount": 10}, {"amount": 20}])

    rows = _spark_runtime().evaluate_dataframe(df, ruleset).collect()

    assert [row["rules_engine_matched"] for row in rows] == [True, True]


def test_spark_runtime_evaluates_group_aggregate(spark):
    """
    What: Evaluates a group aggregate through Spark precompute plus UDF.
    Why: Spark must compute and join aggregate values by explicit group keys.
    Fails when: Group aggregation, join keys, or per-row lookup regresses.
    """
    ruleset = _compile(
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
        }
    )
    df = spark.createDataFrame(
        [
            {"account": "A", "amount": 10},
            {"account": "A", "amount": 20},
            {"account": "B", "amount": 5},
        ]
    )

    rows = _spark_runtime().evaluate_dataframe(df, ruleset).orderBy("account", "amount").collect()

    assert [row["rules_engine_matched"] for row in rows] == [True, True, False]


def test_spark_runtime_evaluates_filtered_aggregate(spark):
    """
    What: Evaluates a Spark dataset aggregate with an aggregate filter.
    Why: Spark precompute must filter aggregate inputs without filtering output rows.
    Fails when: Filter expression generation or aggregate input selection regresses.
    """
    ruleset = _compile(
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
        }
    )
    df = spark.createDataFrame(
        [
            {"status": "OPEN", "amount": 10},
            {"status": "CLOSED", "amount": 50},
            {"status": "OPEN", "amount": 20},
        ]
    )

    rows = _spark_runtime().evaluate_dataframe(df, ruleset).collect()

    assert [row["rules_engine_matched"] for row in rows] == [True, True, True]
