from contextlib import redirect_stdout
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
import os

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime


pytest.importorskip("pyspark")

pytestmark = pytest.mark.skipif(
    os.environ.get("RULES_ENGINE_RUN_SPARK_TESTS") == "0",
    reason="RULES_ENGINE_RUN_SPARK_TESTS=0 disables Spark runtime tests.",
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

    try:
        session = builder.getOrCreate()
    except Exception as exc:
        if not connect_or_databricks and "JAVA_GATEWAY_EXITED" in str(exc):
            pytest.skip("Local Spark tests require Java and JAVA_HOME.")
        raise
    yield session
    if not connect_or_databricks:
        session.stop()


def _spark_runtime():
    return SparkRulesEngineRuntime(DummyRepository(), FunctionRegistry())


def _plan_text(df, *, mode="extended"):
    output = StringIO()
    with redirect_stdout(output):
        df.explain(mode=mode)
    return output.getvalue().lower()


def _score(**kwargs):
    return kwargs["x"] + kwargs["y"]


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
    What: Evaluates a field/literal rule through a native Spark plan.
    Why: Native output columns must preserve the matching, assignment, and trace contract.
    Fails when: Python evaluation returns, output wiring changes, or native structs regress.
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

    output = _spark_runtime().evaluate_dataframe(df, ruleset)
    plan = _plan_text(output)
    assert "pythonudf" not in plan
    assert "batchevalpython" not in plan
    assert "arrowevalpython" not in plan
    pruned_plan = _plan_text(
        output.select("rules_engine_assign.bucket"),
        mode="formatted",
    )
    assert "winning_rule" not in pruned_plan
    rows = output.orderBy("account").collect()

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
    assert winning_rule["conditions"][0]["tolerance_abs"] is None
    assert winning_rule["conditions"][0]["null_input_mode"] is None
    assert winning_rule["conditions"][0]["null_result_mode"] is None
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
    non_modeled_type = assign_type["non_modeled"].dataType
    row = output.collect()[0]

    assert {
        field.name: field.dataType.simpleString()
        for field in non_modeled_type.fields
    } == {
        "market_value": "boolean",
        "book_value": "boolean",
    }
    assert row["rules_engine_assign"]["leaf_key"] == "10110"
    assert row["rules_engine_assign"]["non_modeled"]["market_value"] is True
    assert row["rules_engine_assign"]["non_modeled"]["book_value"] is False


def test_spark_runtime_native_explanation_uses_only_passed_any_branches(spark):
    """
    What: Compiles an any-group explanation from only the conditions that passed.
    Why: Native execution must preserve the winning-path explanation contract.
    Fails when: Native expressions include failed OR branches or use the wrong joiner.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
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
                    "when": {
                        "any": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "open"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                        ]
                    },
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )
    df = spark.createDataFrame(
        [
            {"account": "A", "status": "closed"},
            {"account": "A", "status": "open"},
        ]
    )

    explanations = [
        row["rules_engine_winning_rule_explanation"]
        for row in _spark_runtime().evaluate_dataframe(df, ruleset).orderBy("status").collect()
    ]

    assert explanations == [
        "account == 'A'",
        "account == 'A' OR status == 'open'",
    ]


@pytest.mark.parametrize(
    ("condition", "input_rows"),
    [
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "A"},
                {"record_id": "2", "value": "B"},
            ],
            id="eq-string",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"field": "expected"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "A", "expected": "A"},
                {"record_id": "2", "value": "A", "expected": "B"},
            ],
            id="eq-field-to-field",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"field": "expected"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {
                    "record_id": "1",
                    "value": Decimal("10.50"),
                    "expected": Decimal("10.50"),
                },
                {
                    "record_id": "2",
                    "value": Decimal("10.50"),
                    "expected": Decimal("11.00"),
                },
            ],
            id="eq-decimal-field-to-field",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": date(2026, 7, 12)},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": date(2026, 7, 12)},
                {"record_id": "2", "value": date(2026, 7, 13)},
            ],
            id="eq-date",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": True},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": True},
                {"record_id": "2", "value": False},
            ],
            id="eq-boolean",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": datetime(2026, 7, 12, 8, 30)},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": datetime(2026, 7, 12, 8, 30)},
                {"record_id": "2", "value": datetime(2026, 7, 12, 9, 30)},
            ],
            id="eq-timestamp",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "gt",
                "right": {"literal": 0.0},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 1.2e20},
                {"record_id": "2", "value": -1.0},
            ],
            id="gt-large-double",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": float("nan")},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": float("nan")},
                {"record_id": "2", "value": 1.0},
            ],
            id="eq-nan",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "between",
                "right": {"literal": [10.0, 20.0]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": float("nan")},
                {"record_id": "2", "value": 15.0},
            ],
            id="between-nan",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_between",
                "right": {"literal": [10.0, 20.0]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": float("nan")},
                {"record_id": "2", "value": 15.0},
            ],
            id="not-between-nan",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "in",
                "right": {"literal": [float("nan")]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": float("nan")},
                {"record_id": "2", "value": 1.0},
            ],
            id="in-nan",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_in",
                "right": {"literal": [float("nan")]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": float("nan")},
                {"record_id": "2", "value": 1.0},
            ],
            id="not-in-nan",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "in",
                "right": {"literal": ["A", "B"]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "A"},
                {"record_id": "2", "value": "C"},
            ],
            id="in",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "ne",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "B"},
                {"record_id": "2", "value": "A"},
            ],
            id="ne",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "ge",
                "right": {"literal": 10},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 10},
                {"record_id": "2", "value": 9},
            ],
            id="ge",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "lt",
                "right": {"literal": 10},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 9},
                {"record_id": "2", "value": 10},
            ],
            id="lt",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "le",
                "right": {"literal": 10},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 10},
                {"record_id": "2", "value": 11},
            ],
            id="le",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_in",
                "right": {"literal": ["A", "B"]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "C"},
                {"record_id": "2", "value": "A"},
            ],
            id="not-in",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "between",
                "right": {"literal": [10, 20]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 15},
                {"record_id": "2", "value": 25},
            ],
            id="between",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_between",
                "right": {"literal": [10, 20]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": 25},
                {"record_id": "2", "value": 15},
            ],
            id="not-between",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "contains",
                "right": {"literal": "BC"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "ABCD"},
                {"record_id": "2", "value": "AXYD"},
            ],
            id="contains",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_contains",
                "right": {"literal": "BC"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "AXYD"},
                {"record_id": "2", "value": "ABCD"},
            ],
            id="not-contains",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "starts_with",
                "right": {"literal": "AB"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "ABCD"},
                {"record_id": "2", "value": "XABC"},
            ],
            id="starts-with",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "ends_with",
                "right": {"literal": "CD"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "ABCD"},
                {"record_id": "2", "value": "CDAB"},
            ],
            id="ends-with",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "like",
                "right": {"literal": "AB%"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "ABCD"},
                {"record_id": "2", "value": "AXYD"},
            ],
            id="like",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "not_like",
                "right": {"literal": "AB%"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": "AXYD"},
                {"record_id": "2", "value": "ABCD"},
            ],
            id="not-like",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "is_null",
            },
            [
                {"record_id": "1", "value": None},
                {"record_id": "2", "value": "A"},
            ],
            id="is-null",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "is_not_null",
            },
            [
                {"record_id": "1", "value": "A"},
                {"record_id": "2", "value": None},
            ],
            id="is-not-null",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": 0},
                "null_input_mode": "zero",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": None},
                {"record_id": "2", "value": 1},
            ],
            id="zero-null-input",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "default",
                "null_default_value": True,
            },
            [
                {"record_id": "1", "value": None},
                {"record_id": "2", "value": "B"},
            ],
            id="default-null-result",
        ),
        pytest.param(
            {
                "left": {"field": "value"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "ignore",
                "null_result_mode": "null",
            },
            [
                {"record_id": "1", "value": None},
                {"record_id": "2", "value": "A"},
            ],
            id="ignore-null-input",
        ),
    ],
)
def test_spark_runtime_native_and_udf_paths_have_matching_core_results(
    spark,
    condition,
    input_rows,
):
    """
    What: Compares native and Python compatibility results for supported semantics.
    Why: The two implementations must not change business outcomes or result types.
    Fails when: Operator, null, assignment, winner, or schema behavior drifts by path.
    """
    ruleset = _compile(condition)
    df = spark.createDataFrame(input_rows)
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, df.schema)

    native = runtime._evaluate_natively(df, ruleset, assign_schema, "native")
    udf = runtime._evaluate_with_udf(df, ruleset, assign_schema, "udf")
    suffixes = (
        "matched",
        "matched_rule_ids",
        "assign",
        "winning_rule_id",
        "winning_rule_name",
        "winning_rule_explanation",
        "error",
    )
    schema_suffixes = (*suffixes, "winning_rule")
    native_rows = native.orderBy("record_id").collect()
    udf_rows = udf.orderBy("record_id").collect()

    for suffix in schema_suffixes:
        assert native.schema[f"native_{suffix}"].dataType == udf.schema[
            f"udf_{suffix}"
        ].dataType
    for native_row, udf_row in zip(native_rows, udf_rows, strict=True):
        for suffix in suffixes:
            assert native_row[f"native_{suffix}"] == udf_row[f"udf_{suffix}"]
        if not any(
            isinstance(item.get("value"), (float, date, datetime))
            for item in input_rows
        ):
            assert native_row["native_winning_rule"] == udf_row["udf_winning_rule"]


def test_spark_runtime_native_plan_growth_is_not_exponential(spark):
    """
    What: Builds and explains native plans with ten and twenty ordered rules.
    Why: Winning-output expressions must remain tractable on Spark Connect.
    Fails when: A fold self-references prior CASE expressions and doubles plan size per rule.
    """
    def ruleset_with_count(rule_count):
        return YamlRulesetCompiler().compile_payload(
            {
                "ruleset_id": f"rs_{rule_count}",
                "ruleset_name": f"Ruleset {rule_count}",
                "version": "1",
                "status": "published",
                "rules": [
                    {
                        "rule_id": f"r{index}",
                        "rule_name": f"Rule {index}",
                        "rule_order": index,
                        "stop_on_match": True,
                        "when": {
                            "all": [
                                {
                                    "left": {"field": "code"},
                                    "operator": "eq",
                                    "right": {"literal": f"C{index}"},
                                    "null_input_mode": "propagate",
                                    "null_result_mode": "null",
                                }
                            ]
                        },
                        "assign": {"bucket": f"B{index}"},
                    }
                    for index in range(1, rule_count + 1)
                ],
            }
        )

    df = spark.createDataFrame([{"code": "C20"}])
    plan_10 = _plan_text(_spark_runtime().evaluate_dataframe(df, ruleset_with_count(10)))
    plan_20 = _plan_text(_spark_runtime().evaluate_dataframe(df, ruleset_with_count(20)))

    assert len(plan_20) < len(plan_10) * 6


def test_spark_runtime_native_and_udf_paths_match_ordered_multi_match(spark):
    """
    What: Compares ordered multi-match, winner, assignment merge, and stop behavior.
    Why: Flattened CASE expressions must preserve sequential rule semantics.
    Fails when: Later assignments, matched IDs, or first-winner selection drift by path.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "ordered",
            "ruleset_name": "Ordered Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "first",
                    "rule_name": "First",
                    "rule_order": 1,
                    "stop_on_match": False,
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
                    "assign": {"bucket": "first"},
                },
                {
                    "rule_id": "second",
                    "rule_name": "Second",
                    "rule_order": 2,
                    "stop_on_match": True,
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
                    "assign": {"bucket": "second", "risk": "high"},
                },
                {
                    "rule_id": "third",
                    "rule_name": "Third",
                    "rule_order": 3,
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
                    "assign": {"bucket": "third"},
                },
            ],
        }
    )
    df = spark.createDataFrame([{"account": "A"}])
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, df.schema)

    native = runtime._evaluate_natively(df, ruleset, assign_schema, "native").collect()[0]
    udf = runtime._evaluate_with_udf(df, ruleset, assign_schema, "udf").collect()[0]

    assert native["native_matched_rule_ids"] == udf["udf_matched_rule_ids"] == [
        "first",
        "second",
    ]
    assert native["native_assign"].asDict() == udf["udf_assign"].asDict() == {
        "bucket": "second",
        "risk": "high",
    }
    assert native["native_winning_rule_id"] == udf["udf_winning_rule_id"] == "first"


def test_spark_runtime_native_and_udf_paths_match_nested_and_empty_groups(spark):
    """
    What: Compares nested ANY-in-ALL logic and an empty ALL group across paths.
    Why: Group identity values, parentheses, and passed-branch explanations must agree.
    Fails when: Native recursive group compilation drifts from Python all/any semantics.
    """
    nested_ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "nested",
            "ruleset_name": "Nested",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "nested_rule",
                    "rule_name": "Nested Rule",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "record_type"},
                                "operator": "eq",
                                "right": {"literal": "asset"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "any": [
                                    {
                                        "left": {"field": "market_value"},
                                        "operator": "eq",
                                        "right": {"literal": True},
                                        "null_input_mode": "propagate",
                                        "null_result_mode": "null",
                                    },
                                    {
                                        "left": {"field": "book_value"},
                                        "operator": "eq",
                                        "right": {"literal": True},
                                        "null_input_mode": "propagate",
                                        "null_result_mode": "null",
                                    },
                                ]
                            },
                        ]
                    },
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )
    empty_ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "empty",
            "ruleset_name": "Empty",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "empty_rule",
                    "rule_name": "Empty Rule",
                    "rule_order": 1,
                    "when": {"all": []},
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )
    fixtures = [
        (
            nested_ruleset,
            spark.createDataFrame(
                [
                    {
                        "record_type": "asset",
                        "market_value": True,
                        "book_value": False,
                    }
                ]
            ),
        ),
        (empty_ruleset, spark.createDataFrame([{"record_id": "1"}])),
    ]

    for ruleset, df in fixtures:
        runtime = _spark_runtime()
        assign_schema = runtime._assignment_schema(ruleset, df.schema)
        native = runtime._evaluate_natively(
            df,
            ruleset,
            assign_schema,
            "native",
        ).collect()[0]
        udf = runtime._evaluate_with_udf(
            df,
            ruleset,
            assign_schema,
            "udf",
        ).collect()[0]

        assert native["native_matched"] == udf["udf_matched"] is True
        assert native["native_assign"].asDict() == udf["udf_assign"].asDict()
        assert (
            native["native_winning_rule_explanation"]
            == udf["udf_winning_rule_explanation"]
        )


def test_spark_runtime_native_path_is_ansi_safe_for_large_double(spark):
    """
    What: Executes a large-double native comparison with ANSI mode enabled.
    Why: Native comparisons must not reintroduce overflow-prone decimal casts.
    Fails when: ANSI execution raises or changes the large-double result.
    """
    previous = spark.conf.get("spark.sql.ansi.enabled")
    ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "gt",
            "right": {"literal": 0.0},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"amount": 1.2e20}])

    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        row = _spark_runtime().evaluate_dataframe(
            df,
            ruleset,
            require_native=True,
        ).collect()[0]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)

    assert row["rules_engine_matched"] is True


def test_spark_runtime_resolves_literal_column_names_containing_dots(spark):
    """
    What: Evaluates a field whose literal Spark column name contains a dot.
    Why: Rule fields refer to top-level input names, not implicit nested access.
    Fails when: Spark interprets the field name as a struct traversal.
    """
    ruleset = _compile(
        {
            "left": {"field": "risk.score"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([("A",)], ["risk.score"])

    row = _spark_runtime().evaluate_dataframe(df, ruleset).collect()[0]

    assert row["rules_engine_matched"] is True
    assert row["rules_engine_winning_rule"]["conditions"][0]["left"]["value"] == "A"


def test_spark_runtime_custom_function_uses_observable_udf_fallback(spark, caplog):
    """
    What: Executes a custom-function rule through the Python compatibility path.
    Why: Fallback wiring and plan observability must remain intact for supported functions.
    Fails when: Custom functions are compiled natively or UDF result columns are miswired.
    """
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="tests.test_spark_runtime._score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        implementation=_score,
    )
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {
                        "x": {"field": "risk.score"},
                        "y": {"literal": 3},
                    },
                }
            },
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([(2,)], ["risk.score"])

    with caplog.at_level("WARNING", logger="rules_engine.spark_runtime"):
        output = SparkRulesEngineRuntime(DummyRepository(), registry).evaluate_dataframe(
            df,
            ruleset,
            fail_on_error=False,
        )
    plan = _plan_text(output)
    row = output.collect()[0]

    assert any(
        node in plan
        for node in ("pythonudf", "batchevalpython", "arrowevalpython")
    )
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_winning_rule_id"] == "r1"
    assert row["rules_engine_winning_rule"]["conditions"][0]["left"][
        "source_columns"
    ] == ["risk.score"]
    assert row["rules_engine_error"] is None
    assert "Using Python UDF compatibility path" in caplog.text
