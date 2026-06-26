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
    Fails when: UDF result struct, output columns, or assignment struct regress.
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
    assert rows[0]["rules_engine_assign"]["bucket"] == "matched"
    winning_rule = rows[0]["rules_engine_winning_rule"]
    assert rows[0]["rules_engine_winning_rule_id"] == "r1"
    assert rows[0]["rules_engine_winning_rule_name"] == "Rule 1"
    assert rows[0]["rules_engine_winning_rule_explanation"] == "account == 'A'"
    assert winning_rule["rule_id"] == "r1"
    assert winning_rule["conditions"][0]["columns"] == ["account"]
    assert winning_rule["conditions"][0]["left"]["column"] == "account"
    assert winning_rule["conditions"][0]["left"]["value"] == "A"
    assert "rules_engine_rule_results" not in rows[0].asDict()
    assert rows[1]["rules_engine_matched"] is False
    assert rows[1]["rules_engine_winning_rule"] is None
    assert rows[1]["rules_engine_winning_rule_explanation"] is None


def test_spark_runtime_evaluates_precomputed_aggregate_field(spark):
    """
    What: Evaluates a rule using an upstream aggregate column.
    Why: Spark jobs should precompute cross-row facts before invoking the rules engine.
    Fails when: Precomputed aggregate fields stop behaving like ordinary row fields.
    """
    ruleset = _compile(
        {
            "left": {"field": "dataset_amount_sum"},
            "operator": "eq",
            "right": {"literal": 30},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame(
        [
            {"amount": 10, "dataset_amount_sum": 30},
            {"amount": 20, "dataset_amount_sum": 30},
        ]
    )

    rows = _spark_runtime().evaluate_dataframe(df, ruleset).collect()

    assert [row["rules_engine_matched"] for row in rows] == [True, True]


def test_spark_runtime_preserves_mapping_literal_assignment_as_struct(spark):
    """
    What: Emits mapping literal assignments as nested Spark structs.
    Why: Downstream jobs must select fields such as rules_engine_assign.non_modeled.market_value directly.
    Fails when: Mapping literals are inferred as strings or returned as formatted trace text.
    """
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={
            "leaf_key": "10110",
            "non_modeled": {
                "literal": {
                    "market_value": True,
                    "book_value": False,
                }
            },
        },
    )
    df = spark.createDataFrame([{"account": "A"}])

    output = _spark_runtime().evaluate_dataframe(df, ruleset)
    assign_type = output.schema["rules_engine_assign"].dataType
    row = output.collect()[0]

    assert assign_type["non_modeled"].dataType.simpleString() == (
        "struct<market_value:boolean,book_value:boolean>"
    )
    assert row["rules_engine_assign"]["leaf_key"] == "10110"
    assert row["rules_engine_assign"]["non_modeled"]["market_value"] is True
    assert row["rules_engine_assign"]["non_modeled"]["book_value"] is False
