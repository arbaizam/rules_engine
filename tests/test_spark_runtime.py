import os
from contextlib import redirect_stdout
from datetime import date, datetime
from decimal import Decimal
from io import StringIO

import pytest
from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime, required_source_columns
from rules_engine.standard_functions import register_standard_functions

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


def _error_on_bad_value(*, value):
    if value == "bad":
        raise ValueError("bad test value")
    return True


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

    output = _spark_runtime().evaluate_dataframe(df, ruleset)
    matched_rules_type = output.schema["rules_engine_matched_rules"].dataType
    assignment_results_type = output.schema[
        "rules_engine_assignment_results"
    ].dataType
    assert matched_rules_type.containsNull is False
    assert matched_rules_type.elementType["assigned_fields"].nullable is False
    assert matched_rules_type.elementType["assigned_fields"].dataType.containsNull is False
    assert assignment_results_type.containsNull is False
    assert assignment_results_type.elementType["authored_expression"].nullable is False
    assert assignment_results_type.elementType["changed"].nullable is False
    assert assignment_results_type.elementType["effective"].nullable is False

    rows = output.orderBy("account").collect()

    assert rows[0]["rules_engine_matched"] is True
    assert rows[0]["rules_engine_assign"]["bucket"] == "matched"
    winning_rule = rows[0]["rules_engine_winning_rule"]
    assert rows[0]["rules_engine_winning_rule_id"] == "r1"
    assert rows[0]["rules_engine_winning_rule_name"] == "Rule 1"
    assert rows[0]["rules_engine_winning_rule_explanation"] == "account == 'A'"
    assert rows[0]["rules_engine_first_matched_rule"] == winning_rule
    assert rows[0]["rules_engine_first_matched_rule_id"] == "r1"
    assert rows[0]["rules_engine_first_matched_rule_name"] == "Rule 1"
    assert rows[0]["rules_engine_first_matched_rule_explanation"] == "account == 'A'"
    assert [item["rule_id"] for item in rows[0]["rules_engine_matched_rules"]] == [
        "r1"
    ]
    assert rows[0]["rules_engine_last_matched_rule"]["rule_id"] == "r1"
    assert rows[0]["rules_engine_assignment_results"][0]["effective"] is True
    assert rows[0]["rules_engine_assignment_results"][0]["authored_expression"] == (
        "bucket = 'matched'"
    )
    assert winning_rule["rule_id"] == "r1"
    assert winning_rule["conditions"][0]["columns"] == ["account"]
    assert winning_rule["conditions"][0]["left"]["column"] == "account"
    assert winning_rule["conditions"][0]["left"]["value"] == "A"
    assert "rules_engine_rule_results" not in rows[0].asDict()
    assert rows[1]["rules_engine_matched"] is False
    assert rows[1]["rules_engine_winning_rule"] is None
    assert rows[1]["rules_engine_winning_rule_explanation"] is None
    assert rows[1]["rules_engine_first_matched_rule"] is None
    assert rows[1]["rules_engine_matched_rules"] == []
    assert rows[1]["rules_engine_last_matched_rule"] is None
    assert rows[1]["rules_engine_assignment_results"] == []


def test_spark_runtime_uses_one_python_evaluation_node(spark):
    """All result fields project from one Python UDF evaluation in the physical plan."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    output = _spark_runtime().evaluate_dataframe(
        spark.createDataFrame([{"account": "A"}]),
        ruleset,
    )
    plan_output = StringIO()

    with redirect_stdout(plan_output):
        output.explain(mode="simple")

    assert plan_output.getvalue().count("BatchEvalPython") == 1


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


def test_spark_runtime_evaluates_and_assigns_standard_date_functions(spark):
    """Date arithmetic remains typed through the real Spark UDF boundary."""
    registry = register_standard_functions(FunctionRegistry())
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "date_add_months",
                    "args": {
                        "value": {"field": "funded_date"},
                        "months": 1,
                    },
                }
            },
            "operator": "ge",
            "right": {"field": "maturity_date"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={
            "review_date": {
                "custom_function": {
                    "name": "date_add_years",
                    "args": {
                        "value": {"field": "funded_date"},
                        "years": 1,
                    },
                }
            }
        },
    )
    df = spark.createDataFrame(
        [(date(2024, 1, 31), date(2024, 2, 29))],
        ["funded_date", "maturity_date"],
    )
    runtime = SparkRulesEngineRuntime(DummyRepository(), registry)

    output = runtime.evaluate_dataframe(df, ruleset)
    row = output.collect()[0]

    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"]["review_date"] == date(2025, 1, 31)
    assert output.schema["rules_engine_assign"].dataType["review_date"].dataType.typeName() == "date"


def test_spark_runtime_serializes_only_required_literal_source_columns(spark):
    """
    What: Evaluates a dotted source field while retaining an unrelated input column.
    Why: The UDF should receive only required fields without changing output columns.
    Fails when: Literal column names are misread or source projection drops input data.
    """
    ruleset = _compile(
        {
            "left": {"field": "risk.score"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"copied": {"field": "source_value"}},
    )
    df = spark.createDataFrame(
        [("A", "kept", "assigned")],
        ["risk.score", "unused_payload", "source_value"],
    )

    assert required_source_columns(ruleset) == ("risk.score", "source_value")

    row = _spark_runtime().evaluate_dataframe(df, ruleset).collect()[0]

    assert row["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"]["copied"] == "assigned"
    assert row["rules_engine_winning_rule"]["conditions"][0]["left"]["value"] == "A"


def test_spark_runtime_evaluates_literal_only_rule_without_source_dependencies(spark):
    """
    What: Evaluates a literal-only rule with an empty dependency set.
    Why: The optimized UDF input struct must support rules requiring no source fields.
    Fails when: Empty source projection produces an invalid Spark struct or row payload.
    """
    ruleset = _compile(
        {
            "left": {"literal": "A"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"unused_payload": "kept"}])

    assert required_source_columns(ruleset) == ()

    row = _spark_runtime().evaluate_dataframe(df, ruleset).collect()[0]

    assert row["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True


def test_spark_runtime_applies_column_prefix_to_all_new_outputs(spark):
    """Every additive audit output respects the configured column prefix."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"account": "A"}])

    output = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        column_prefix="audit",
    )

    assert {
        "audit_first_matched_rule",
        "audit_first_matched_rule_id",
        "audit_first_matched_rule_name",
        "audit_first_matched_rule_explanation",
        "audit_matched_rules",
        "audit_last_matched_rule",
        "audit_assignment_results",
    } <= set(output.columns)
    assert "rules_engine_first_matched_rule" not in output.columns


def test_spark_runtime_validates_schema_before_building_udf(spark):
    """An incompatible existing target fails before row evaluation."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"target": 10},
    )
    df = spark.createDataFrame([("A", "existing")], ["account", "target"])

    with pytest.raises(
        ValidationFailedError,
        match="SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE",
    ):
        _spark_runtime().evaluate_dataframe(df, ruleset)


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


def test_spark_runtime_preserves_decimal_and_array_assignments(spark):
    """Financial values stay exact across the real Python UDF boundary."""
    registry = register_standard_functions(FunctionRegistry())
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={
            "existing_rate": 0.0425,
            "parsed_balance": {
                "custom_function": {
                    "name": "to_number",
                    "args": {"value": {"field": "balance_text"}},
                }
            },
            "factors": [0.1, 0.25],
        },
    )
    schema = T.StructType(
        [
            T.StructField(
                "status",
                T.StringType(),
                True,
            ),
            T.StructField(
                "balance_text",
                T.StringType(),
                True,
            ),
            T.StructField(
                "existing_rate",
                T.DecimalType(10, 4),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(
        [("OPEN", "1234.56", Decimal("0.0300"))],
        schema,
    )

    row = SparkRulesEngineRuntime(DummyRepository(), registry).evaluate_dataframe(
        df,
        ruleset,
        fail_on_error=False,
    ).collect()[0]

    assert row["rules_engine_assign"]["existing_rate"] == Decimal("0.0425")
    assert row["rules_engine_assign"]["parsed_balance"] == Decimal("1234.56")
    assert row["rules_engine_assign"]["factors"] == [
        Decimal("0.10"),
        Decimal("0.25"),
    ]


def test_spark_runtime_quarantines_errors_without_failing_job(spark):
    """Production quarantine mode retains good rows and marks bad rows."""
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="error_on_bad_value",
            implementation_reference="tests.error_on_bad_value",
            arg_names=("value",),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
            return_type_hint="boolean",
        ),
        _error_on_bad_value,
    )
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "error_on_bad_value",
                    "args": {"value": {"field": "value"}},
                }
            },
            "operator": "eq",
            "right": {"literal": True},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"value": "good"}, {"value": "bad"}])

    rows = SparkRulesEngineRuntime(DummyRepository(), registry).evaluate_dataframe(
        df,
        ruleset,
        fail_on_error=False,
    ).collect()

    by_value = {row["value"]: row for row in rows}
    assert by_value["good"]["rules_engine_error"] is None
    assert by_value["bad"]["rules_engine_error"].startswith(
        "ValueError: bad test value"
    )
    assert "Traceback" not in by_value["bad"]["rules_engine_error"]

    fail_fast_output = SparkRulesEngineRuntime(
        DummyRepository(),
        registry,
    ).evaluate_dataframe(df, ruleset, fail_on_error=True)
    with pytest.raises(Exception, match="bad test value"):
        fail_fast_output.collect()


def test_fail_on_error_remains_lazy_until_callers_action(spark):
    """Building output does not hide a separate full-data validation action."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    df = spark.createDataFrame([{"account": "A"}])

    output = _spark_runtime().evaluate_dataframe(df, ruleset)

    assert not output.storageLevel.useMemory
    assert not output.storageLevel.useDisk
    assert output.collect()[0]["rules_engine_matched"] is True


def test_spark_runtime_preserves_timestamp_assignment_type(spark):
    """Timestamp assignment values survive the real worker serialization path."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"copied_timestamp": {"field": "source_timestamp"}},
    )
    expected = datetime(2025, 1, 15, 10, 30)  # noqa: DTZ001 - Spark TimestampType is naive.
    df = spark.createDataFrame([("OPEN", expected)], ["status", "source_timestamp"])

    row = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        fail_on_error=False,
    ).collect()[0]

    assert row["rules_engine_assign"]["copied_timestamp"] == expected


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_spark_runtime_preserves_timestamp_ntz_assignment_type(spark):
    """TimestampNTZ survives schema inference and live worker serialization."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
        },
        assign={"copied_timestamp": {"field": "source_timestamp"}},
    )
    expected = datetime(2025, 1, 15, 10, 30)  # noqa: DTZ001
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), False),
            T.StructField("source_timestamp", T.TimestampNTZType(), False),
        ]
    )
    df = spark.createDataFrame([("OPEN", expected)], schema)

    output = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        fail_on_error=False,
    )
    row = output.collect()[0]

    assert isinstance(
        output.schema["rules_engine_assign"].dataType["copied_timestamp"].dataType,
        T.TimestampNTZType,
    )
    assert row["rules_engine_assign"]["copied_timestamp"] == expected
