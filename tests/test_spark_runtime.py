import os
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

import rules_engine.spark_runtime as spark_runtime_module
from rules_engine.analytics import RulesetCoverageAnalyzer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.registry import (
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    FunctionRegistry,
)
from rules_engine.serializer import DeltaRowSerializer
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

    builder = SparkSession.builder.appName("rules-engine-spark-runtime-tests").config(
        "spark.ui.enabled", "false"
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


def _evaluation(
    df,
    ruleset,
    *,
    runtime=None,
    key_columns=None,
    **kwargs,
):
    """Build one keyed evaluation with terse test defaults."""
    return (runtime or _spark_runtime()).evaluate_dataframe(
        df,
        ruleset,
        key_columns=key_columns or [df.columns[0]],
        **kwargs,
    )


def _compile(condition, assign=None):
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
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
        }
    )
    df = spark.createDataFrame(
        [("A", "existing"), ("B", "unchanged")],
        ["account", "bucket"],
    )

    evaluation = _evaluation(df, ruleset, full_audit=True)
    output = evaluation.results_df
    matched_rules_type = output.schema["rules_engine_matched_rules"].dataType
    assignment_results_type = output.schema["rules_engine_assignment_results"].dataType
    condition_type = matched_rules_type.elementType["conditions"].dataType.elementType
    assert matched_rules_type.containsNull is False
    assert matched_rules_type.elementType["assignments_applied"].dataType.containsNull is False
    assert matched_rules_type.elementType["conditions"].dataType.containsNull is False
    assert condition_type["condition_id"].nullable is False
    assert condition_type["condition_group_id"].nullable is False
    assert condition_type["condition_group_operator"].nullable is False
    assert condition_type["active_flag"].nullable is False
    assert assignment_results_type.containsNull is False
    assert assignment_results_type.elementType["authored_expression"].nullable is False
    assert assignment_results_type.elementType["changed"].nullable is False
    assert assignment_results_type.elementType["effective"].nullable is False

    rows = output.orderBy("account").collect()

    assert rows[0]["rules_engine_matched"] is True
    assert rows[0]["rules_engine_assign"]["bucket"].asDict() == {
        "applied": True,
        "value": "matched",
    }
    match_trace = rows[0]["rules_engine_matched_rules"][0]
    assert [item["rule_id"] for item in rows[0]["rules_engine_matched_rules"]] == ["r1"]
    assert rows[0]["rules_engine_assignment_results"][0]["effective"] is True
    assert rows[0]["rules_engine_assignment_results"][0]["authored_expression"] == (
        "bucket = 'matched'"
    )
    assert rows[0]["rules_engine_assignment_results"][0]["old_value"] == "existing"
    assert rows[0]["rules_engine_assignment_results"][0]["proposed_value"] == "matched"
    assert rows[0]["rules_engine_assignment_results"][0]["changed"] is True
    assert match_trace["rule_id"] == "r1"
    assert match_trace["rule_name"] == "Rule 1"
    assert match_trace["rule_order"] == 1
    assert match_trace["explanation"] == "account == 'A'"
    assert match_trace["conditions"][0]["condition_id"] == "cg:r1:root:c1"
    assert match_trace["conditions"][0]["condition_group_id"] == "cg:r1:root"
    assert match_trace["conditions"][0]["condition_group_operator"] == "all"
    assert match_trace["conditions"][0]["active_flag"] is True
    assert match_trace["conditions"][0]["columns"] == ["account"]
    assert match_trace["conditions"][0]["left"]["column"] == "account"
    assert match_trace["conditions"][0]["left"]["value"] == "A"
    assert "rules_engine_rule_results" not in rows[0].asDict()
    assert rows[1]["rules_engine_matched"] is False
    assert rows[1]["rules_engine_assign"]["bucket"].asDict() == {
        "applied": False,
        "value": None,
    }
    assert rows[1]["rules_engine_matched_rules"] == []
    assert rows[1]["rules_engine_assignment_results"] == []
    applied_rows = {
        row["account"]: row.asDict(recursive=True)
        for row in evaluation.apply_assignments().collect()
    }
    assert applied_rows["A"]["bucket"] == "matched"
    assert applied_rows["B"]["bucket"] == "unchanged"

    multi_match_ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "multi",
            "ruleset_name": "Multi-match",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "rule_order": rule_order,
                    "stop_on_match": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": assignments,
                }
                for rule_id, rule_name, rule_order, assignments in (
                    (
                        "first",
                        "First",
                        1,
                        [
                            {
                                "assignment_id": "first_bucket",
                                "target_field": "bucket",
                                "value": {"literal": "first"},
                            }
                        ],
                    ),
                    (
                        "second",
                        "Second",
                        2,
                        [
                            {
                                "assignment_id": "second_bucket",
                                "target_field": "bucket",
                                "value": {"literal": "second"},
                            },
                            {
                                "assignment_id": "second_review",
                                "target_field": "review",
                                "value": {"literal": True},
                            },
                        ],
                    ),
                )
            ],
        }
    )
    multi_evaluation = _evaluation(
        spark.createDataFrame([("A", "original")], ["account", "bucket"]),
        multi_match_ruleset,
        full_audit=True,
    )
    multi_row = multi_evaluation.results_df.collect()[0]
    multi_events = {
        event["assignment_id"]: event for event in multi_row["rules_engine_assignment_results"]
    }

    assert multi_row["rules_engine_matched_rule_ids"] == ["first", "second"]
    assert [trace["rule_id"] for trace in multi_row["rules_engine_matched_rules"]] == [
        "first",
        "second",
    ]
    assert all(trace["conditions"] for trace in multi_row["rules_engine_matched_rules"])
    assert multi_row["rules_engine_assign"].asDict(recursive=True) == {
        "bucket": {"applied": True, "value": "second"},
        "review": {"applied": True, "value": True},
    }
    assert multi_evaluation.apply_assignments().collect()[0].asDict() == {
        "account": "A",
        "bucket": "second",
        "review": True,
    }
    assert multi_events["first_bucket"]["effective"] is False
    assert multi_events["first_bucket"]["overridden_by_rule_id"] == "second"
    assert multi_events["first_bucket"]["overridden_by_assignment_id"] == ("second_bucket")
    assert multi_events["second_bucket"]["old_value"] == "first"
    assert multi_events["second_bucket"]["changed"] is True


def test_dataframe_evaluation_separates_results_and_applies_atomic_values(spark):
    """Keyed results stay separate while scalar, null, and struct values apply atomically."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "apply_assignments",
            "ruleset_name": "Apply assignments",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "clear_values",
                    "rule_name": "Clear values",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "action"},
                                "operator": "eq",
                                "right": {"literal": "clear"},
                            }
                        ]
                    },
                    "assign": {
                        "status": {"literal": None, "value_type": "string"},
                        "details": {"literal": None},
                        "new_note": {"literal": None, "value_type": "string"},
                    },
                },
                {
                    "rule_id": "replace_values",
                    "rule_name": "Replace values",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "action"},
                                "operator": "eq",
                                "right": {"literal": "replace"},
                            }
                        ]
                    },
                    "assign": {
                        "status": "updated",
                        "new_flag": True,
                        "details": {
                            "literal": {
                                "book_value": True,
                                "market_value": False,
                            }
                        },
                    },
                },
            ],
        }
    )
    details_type = T.StructType(
        [
            T.StructField("book_value", T.BooleanType(), True),
            T.StructField("market_value", T.BooleanType(), True),
        ]
    )
    input_schema = T.StructType(
        [
            T.StructField("row_id", T.StringType(), False),
            T.StructField("action", T.StringType(), False),
            T.StructField("status", T.StringType(), True),
            T.StructField("details", details_type, True),
        ]
    )
    original_details = {"book_value": False, "market_value": True}
    input_df = spark.createDataFrame(
        [
            ("clear", "clear", "original", original_details),
            ("replace", "replace", "original", original_details),
            ("keep", "none", "original", original_details),
        ],
        input_schema,
    )

    evaluation = _evaluation(
        input_df,
        ruleset,
        key_columns=["row_id", "action"],
        full_audit=True,
    )

    assert evaluation.key_columns == ("row_id", "action")
    assert evaluation.results_df.columns == [
        "row_id",
        "action",
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_matched_rules",
        "rules_engine_assignment_results",
        "rules_engine_ruleset",
        "rules_engine_engine_version",
    ]
    result_rows = {
        row["row_id"]: row.asDict(recursive=True) for row in evaluation.results_df.collect()
    }
    assert result_rows["clear"]["rules_engine_assign"]["status"] == {
        "applied": True,
        "value": None,
    }
    assert result_rows["clear"]["rules_engine_assign"]["details"] == {
        "applied": True,
        "value": None,
    }
    assert result_rows["keep"]["rules_engine_assign"]["status"] == {
        "applied": False,
        "value": None,
    }
    assert result_rows["replace"]["rules_engine_assign"]["details"] == {
        "applied": True,
        "value": {"book_value": True, "market_value": False},
    }

    applied = evaluation.apply_assignments()
    assert applied.columns == [
        "row_id",
        "action",
        "status",
        "details",
        "new_note",
        "new_flag",
    ]
    applied_rows = {row["row_id"]: row.asDict(recursive=True) for row in applied.collect()}
    assert applied_rows["clear"]["status"] is None
    assert applied_rows["clear"]["details"] is None
    assert applied_rows["clear"]["new_note"] is None
    assert applied_rows["replace"]["status"] == "updated"
    assert applied_rows["replace"]["details"] == {
        "book_value": True,
        "market_value": False,
    }
    assert applied_rows["replace"]["new_flag"] is True
    assert applied_rows["keep"]["status"] == "original"
    assert applied_rows["keep"]["details"] == original_details
    assert applied_rows["keep"]["new_note"] is None
    assert applied_rows["keep"]["new_flag"] is None


def test_dataframe_evaluation_persists_one_shared_plan(spark):
    """Explicit persistence is owned by the internal source-plus-results plan."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    evaluation = _evaluation(
        spark.createDataFrame([("one", "A")], ["row_id", "account"]),
        ruleset,
        key_columns=["row_id"],
    )

    assert evaluation.persist() is evaluation
    assert evaluation._evaluated_df.storageLevel.useMemory
    assert evaluation.unpersist(blocking=True) is evaluation
    assert not evaluation._evaluated_df.storageLevel.useMemory


def test_persisted_projections_evaluate_custom_assignment_once_per_row(spark):
    """Both public projections reuse one cached worker evaluation."""
    invocation_count = spark.sparkContext.accumulator(0)

    def count_assignment(*, value):
        invocation_count.add(1)
        return value

    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="count_assignment",
            implementation_reference="tests.count_assignment",
            arguments=(CustomFunctionArgSpec("value"),),
            allowed_in_condition_flag=False,
            allowed_in_assignment_flag=True,
            return_type_hint="string",
        ),
        count_assignment,
    )
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "is_not_null",
        },
        assign={
            "copied": {
                "custom_function": {
                    "name": "count_assignment",
                    "args": {"value": {"field": "account"}},
                }
            }
        },
    )
    evaluation = _evaluation(
        spark.createDataFrame(
            [("1", "A"), ("2", "B"), ("3", "C")],
            ["row_id", "account"],
        ).coalesce(1),
        ruleset,
        runtime=SparkRulesEngineRuntime(DummyRepository(), registry),
        key_columns=["row_id"],
    ).persist()

    try:
        evaluation.results_df.collect()
        evaluation.apply_assignments().collect()
        assert invocation_count.value == 3
    finally:
        evaluation.unpersist(blocking=True)


def test_spark_runtime_preserves_input_column_named_like_old_temp_result(spark):
    """Internal UDF plumbing cannot reserve or overwrite a caller column."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    df = spark.createDataFrame(
        [("A", "keep-me")],
        ["account", "rules_engine_result"],
    )

    evaluation = _evaluation(df, ruleset)
    row = evaluation.apply_assignments().collect()[0]

    assert row["rules_engine_result"] == "keep-me"


def test_spark_runtime_applies_typed_operand_defaults_before_comparison(spark):
    """Numeric and string fallbacks are applied before the real Spark UDF compares."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "null_defaults",
            "ruleset_name": "Null defaults",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "numeric_default",
                    "rule_name": "Numeric default",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "amount", "default_if_null": 0},
                                "operator": "eq",
                                "right": {"literal": 0},
                            }
                        ]
                    },
                    "assign": {"numeric_result": "matched"},
                },
                {
                    "rule_id": "string_default",
                    "rule_name": "String default",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "field": "status",
                                    "default_if_null": "UNKNOWN",
                                },
                                "operator": "eq",
                                "right": {"literal": "UNKNOWN"},
                            }
                        ]
                    },
                    "assign": {"string_result": "matched"},
                },
            ],
        }
    )
    frame = spark.createDataFrame(
        [("one", None, None)],
        "row_id string, amount double, status string",
    )

    row = _evaluation(
        frame,
        ruleset,
        full_audit=True,
    ).results_df.collect()[0]

    assert row["rules_engine_matched_rule_ids"] == [
        "numeric_default",
        "string_default",
    ]
    traces = {trace["rule_id"]: trace for trace in row["rules_engine_matched_rules"]}
    numeric_left = traces["numeric_default"]["conditions"][0]["left"]
    string_left = traces["string_default"]["conditions"][0]["left"]
    assert numeric_left["original_value"] is None
    assert numeric_left["value"] == "0"
    assert numeric_left["default_applied"] is True
    assert string_left["original_value"] is None
    assert string_left["value"] == "UNKNOWN"
    assert string_left["default_applied"] is True


def test_spark_runtime_quarantines_error_on_null(spark):
    """A remaining null becomes a compact row error when explicitly required."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
            "error_on_null": True,
        }
    )
    frame = spark.createDataFrame([("one", None)], "row_id string, status string")

    row = _evaluation(
        frame,
        ruleset,
        fail_on_error=False,
        full_audit=True,
    ).results_df.collect()[0]

    assert row["rules_engine_matched"] is False
    assert row["rules_engine_matched_rules"] == []
    assert "error_on_null=true" in row["rules_engine_error"]


def test_spark_runtime_builds_one_python_udf(spark, monkeypatch):
    """The runtime creates one UDF whose result struct feeds every output field."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    udf_factory_calls = []
    original_udf = spark_runtime_module.F.udf

    def tracked_udf(*args, **kwargs):
        udf_factory_calls.append((args, kwargs))
        return original_udf(*args, **kwargs)

    monkeypatch.setattr(spark_runtime_module.F, "udf", tracked_udf)
    evaluation = _evaluation(
        spark.createDataFrame([{"account": "A"}]),
        ruleset,
    )

    assert len(udf_factory_calls) == 1
    assert evaluation.results_df.collect()[0]["rules_engine_matched"] is True


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

    evaluation = _evaluation(df, ruleset, runtime=runtime)
    output = evaluation.results_df
    row = output.collect()[0]

    assert row["rules_engine_matched"] is True
    review_date = row["rules_engine_assign"]["review_date"]
    assert review_date["applied"] is True
    assert review_date["value"] == date(2025, 1, 31)
    review_outcome_type = output.schema["rules_engine_assign"].dataType["review_date"].dataType
    assert review_outcome_type["value"].dataType.typeName() == "date"


def test_spark_runtime_executes_nested_array_and_optional_standard_arguments(spark):
    """Recursive arguments and optional defaults survive the real worker boundary."""
    registry = register_standard_functions(FunctionRegistry())
    ruleset = _compile(
        {
            "left": {"literal": True},
            "operator": "eq",
            "right": {"literal": True},
        },
        assign={
            "selected": {
                "custom_function": {
                    "name": "coalesce",
                    "args": {
                        "values": [
                            {"field": "primary_code"},
                            {"field": "secondary_code"},
                        ]
                    },
                }
            },
            "suffix": {
                "custom_function": {
                    "name": "substring",
                    "args": {
                        "value": {"field": "secondary_code"},
                        "start": 2,
                    },
                }
            },
            "has_tags": {
                "custom_function": {
                    "name": "array_contains_all",
                    "args": {
                        "values": {"field": "tags"},
                        "candidates": ["review", "active"],
                    },
                }
            },
            "tags_text": {
                "custom_function": {
                    "name": "array_join",
                    "args": {
                        "values": {"field": "tags"},
                        "separator": "|",
                    },
                }
            },
        },
    )
    df = spark.createDataFrame(
        [("1", None, "ABC", ["review", "active"])],
        "row_id string, primary_code string, secondary_code string, tags array<string>",
    )
    runtime = SparkRulesEngineRuntime(DummyRepository(), registry)

    evaluation = _evaluation(df, ruleset, runtime=runtime, key_columns=["row_id"])
    row = evaluation.apply_assignments().collect()[0]

    assert row["selected"] == "ABC"
    assert row["suffix"] == "BC"
    assert row["has_tags"] is True
    assert row["tags_text"] == "review|active"


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
        },
        assign={"copied": {"field": "source_value"}},
    )
    df = spark.createDataFrame(
        [("A", "kept", "assigned")],
        ["risk.score", "unused_payload", "source_value"],
    )

    assert required_source_columns(ruleset) == ("risk.score", "source_value")

    evaluation = _evaluation(
        df,
        ruleset,
        full_audit=True,
        key_columns=["risk.score"],
    )
    row = evaluation.results_df.collect()[0]
    applied = evaluation.apply_assignments().collect()[0]

    assert applied["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"]["copied"]["value"] == "assigned"
    assert applied["copied"] == "assigned"
    assert row["rules_engine_matched_rules"][0]["conditions"][0]["left"]["value"] == "A"


def test_spark_runtime_evaluates_literal_only_rule_without_source_dependencies(spark):
    """
    What: Evaluates a literal-only rule with an empty dependency set.
    Why: The optimized UDF input struct must support rules requiring no source fields.
    Fails when: Empty source projection produces an invalid Spark struct or row payload.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "literal_only",
            "ruleset_name": "Literal only",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "literal_match",
                    "rule_name": "Literal match",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"literal": "A"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"matched": True},
                }
            ],
        }
    )
    df = spark.createDataFrame([{"unused_payload": "kept"}])

    assert required_source_columns(ruleset) == ()

    evaluation = _evaluation(df, ruleset)
    output = evaluation.results_df
    row = output.collect()[0]

    assert row["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"]["matched"].asDict() == {
        "applied": True,
        "value": True,
    }
    assert output.schema["rules_engine_assign"].dataType.fieldNames() == ["matched"]


def test_spark_runtime_carries_assigned_values_across_real_worker_boundary(spark):
    """Committed assignment state and provenance survive Spark UDF serialization."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "assigned_worker",
            "ruleset_name": "Assigned worker",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "producer",
                    "rule_name": "Producer",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": [
                        {
                            "assignment_id": "produce_bucket",
                            "target_field": "bucket",
                            "value": {"literal": "A"},
                        }
                    ],
                },
                {
                    "rule_id": "consumer",
                    "rule_name": "Consumer",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"assigned": "bucket"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"copied_bucket": {"assigned": "bucket"}},
                },
            ],
        }
    )

    row = _evaluation(
        spark.createDataFrame([(True,)], ["eligible"]),
        ruleset,
        full_audit=True,
    ).results_df.collect()[0]
    operand = row["rules_engine_matched_rules"][1]["conditions"][0]["left"]

    assert row["rules_engine_matched_rule_ids"] == ["producer", "consumer"]
    assert row["rules_engine_assign"]["copied_bucket"]["value"] == "A"
    assert operand["kind"] == "assigned"
    assert operand["produced_by_rule_id"] == "producer"
    assert operand["produced_by_assignment_id"] == "produce_bucket"


def test_spark_runtime_applies_column_prefix_to_all_new_outputs(spark):
    """Custom prefixes preserve exact compact and full-audit column contracts."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    df = spark.createDataFrame([{"account": "A"}])

    compact = _evaluation(
        df,
        ruleset,
        column_prefix="audit",
    ).results_df
    full = _evaluation(
        df,
        ruleset,
        column_prefix="audit",
        full_audit=True,
    ).results_df

    assert compact.columns == [
        "account",
        "audit_error",
        "audit_matched",
        "audit_matched_rule_ids",
        "audit_assign",
        "audit_ruleset",
        "audit_engine_version",
    ]
    assert full.columns == [
        "account",
        "audit_error",
        "audit_matched",
        "audit_matched_rule_ids",
        "audit_assign",
        "audit_matched_rules",
        "audit_assignment_results",
        "audit_ruleset",
        "audit_engine_version",
    ]


def test_spark_runtime_validates_schema_before_building_udf(spark):
    """An incompatible existing target fails before row evaluation."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        },
        assign={"target": 10},
    )
    df = spark.createDataFrame([("A", "existing")], ["account", "target"])

    with pytest.raises(
        ValidationFailedError,
        match="SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE",
    ):
        _evaluation(df, ruleset)


def test_spark_runtime_rejects_a_ruleset_with_no_active_rules(spark):
    """Evaluation cannot expose an unwritable zero-field assignment struct."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "inactive_ruleset",
            "ruleset_name": "Inactive Ruleset",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "inactive_rule",
                    "rule_name": "Inactive Rule",
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"bucket": "inactive"},
                }
            ],
        }
    )

    with pytest.raises(ValidationFailedError, match="At least one active rule"):
        _evaluation(spark.createDataFrame([("A",)], ["account"]), ruleset)


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

    evaluation = _evaluation(df, ruleset)
    output = evaluation.results_df
    assign_type = output.schema["rules_engine_assign"].dataType
    non_modeled_type = assign_type["non_modeled"].dataType["value"].dataType
    row = output.collect()[0]

    assert {field.name: field.dataType.simpleString() for field in non_modeled_type.fields} == {
        "market_value": "boolean",
        "book_value": "boolean",
    }
    assert row["rules_engine_assign"]["leaf_key"]["value"] == "10110"
    assert row["rules_engine_assign"]["non_modeled"]["applied"] is True
    assert row["rules_engine_assign"]["non_modeled"]["value"]["market_value"] is True
    assert row["rules_engine_assign"]["non_modeled"]["value"]["book_value"] is False
    applied = evaluation.apply_assignments().collect()[0]
    assert applied["non_modeled"].asDict() == {
        "market_value": True,
        "book_value": False,
    }


def test_spark_runtime_preserves_decimal_and_array_assignments(spark):
    """Financial values stay exact across the real Python UDF boundary."""
    registry = register_standard_functions(FunctionRegistry())
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
        },
        assign={
            "existing_rate": 0.0425,
            "parsed_balance": {
                "custom_function": {
                    "name": "to_decimal",
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

    evaluation = _evaluation(
        df,
        ruleset,
        runtime=SparkRulesEngineRuntime(DummyRepository(), registry),
        fail_on_error=False,
    )
    row = evaluation.results_df.collect()[0]

    assert row["rules_engine_assign"]["existing_rate"]["value"] == Decimal("0.0425")
    assert row["rules_engine_assign"]["parsed_balance"]["value"] == Decimal("1234.56")
    assert row["rules_engine_assign"]["factors"]["value"] == [
        Decimal("0.10"),
        Decimal("0.25"),
    ]
    applied = evaluation.apply_assignments().collect()[0]
    assert applied["existing_rate"] == Decimal("0.0425")
    assert applied["parsed_balance"] == Decimal("1234.56")


def test_spark_runtime_quarantines_errors_without_failing_job(spark):
    """Production quarantine mode retains good rows and marks bad rows."""

    def error_on_bad_value(*, value):
        if value == "bad":
            raise ValueError("bad test value")
        return True

    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="error_on_bad_value",
            implementation_reference="tests.error_on_bad_value",
            arguments=(CustomFunctionArgSpec("value"),),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
            return_type_hint="boolean",
        ),
        error_on_bad_value,
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
        },
        assign={"bucket": "matched", "new_note": "created"},
    )
    df = spark.createDataFrame(
        [("good", "original"), ("bad", "original")],
        ["value", "bucket"],
    )

    evaluation = _evaluation(
        df,
        ruleset,
        runtime=SparkRulesEngineRuntime(DummyRepository(), registry),
        fail_on_error=False,
    )
    rows = evaluation.results_df.collect()

    by_value = {row["value"]: row for row in rows}
    assert by_value["good"]["rules_engine_error"] is None
    assert by_value["bad"]["rules_engine_error"].startswith("ValueError: bad test value")
    assert "Traceback" not in by_value["bad"]["rules_engine_error"]
    applied = {
        row["value"]: row.asDict(recursive=True) for row in evaluation.apply_assignments().collect()
    }
    assert applied["good"]["bucket"] == "matched"
    assert applied["good"]["new_note"] == "created"
    assert applied["bad"]["bucket"] == "original"
    assert applied["bad"]["new_note"] is None

    fail_fast_output = _evaluation(
        df,
        ruleset,
        runtime=SparkRulesEngineRuntime(DummyRepository(), registry),
        fail_on_error=True,
    ).results_df
    with pytest.raises(Exception, match="bad test value"):
        fail_fast_output.collect()


def test_fail_on_error_remains_lazy_until_callers_action(spark):
    """Building output does not hide a separate full-data validation action."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    df = spark.createDataFrame([{"account": "A"}])

    group_id = "rules-engine-lazy-plan-test"
    tracker = spark.sparkContext.statusTracker()
    spark.sparkContext.setJobGroup(group_id, "rules engine lazy plan test")
    try:
        evaluation = _evaluation(df, ruleset)
        output = evaluation.results_df
        _ = evaluation.apply_assignments()

        assert tracker.getJobIdsForGroup(group_id) == []
        assert output.collect()[0]["rules_engine_matched"] is True
        assert tracker.getJobIdsForGroup(group_id)
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)


def test_spark_runtime_preserves_timestamp_assignment_type(spark):
    """Timestamp assignment values survive the real worker serialization path."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
        },
        assign={"copied_timestamp": {"field": "source_timestamp"}},
    )
    expected = datetime(2025, 1, 15, 10, 30)  # noqa: DTZ001 - Spark TimestampType is naive.
    df = spark.createDataFrame([("OPEN", expected)], ["status", "source_timestamp"])

    evaluation = _evaluation(
        df,
        ruleset,
        fail_on_error=False,
    )
    row = evaluation.results_df.collect()[0]

    assert row["rules_engine_assign"]["copied_timestamp"]["value"] == expected
    assert evaluation.apply_assignments().collect()[0]["copied_timestamp"] == expected


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

    evaluation = _evaluation(
        df,
        ruleset,
        fail_on_error=False,
    )
    output = evaluation.results_df
    row = output.collect()[0]

    assert isinstance(
        output.schema["rules_engine_assign"]
        .dataType["copied_timestamp"]
        .dataType["value"]
        .dataType,
        T.TimestampNTZType,
    )
    assert row["rules_engine_assign"]["copied_timestamp"]["value"] == expected


def test_full_audit_emits_ordered_optional_detail_and_identity(spark):
    """The default stays compact while full audit adds ordered diagnostics."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    df = spark.createDataFrame(
        [(7, "A", "retained")],
        ["row_id", "account", "source_note"],
    )

    compact_evaluation = _evaluation(df, ruleset, key_columns=["row_id"])
    full_evaluation = _evaluation(
        df,
        ruleset,
        key_columns=["row_id"],
        full_audit=True,
    )
    compact = compact_evaluation.results_df
    full = full_evaluation.results_df
    compact_row = compact.collect()[0]

    assert compact.columns == [
        "row_id",
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_ruleset",
        "rules_engine_engine_version",
    ]
    assert full.columns == [
        "row_id",
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_matched_rules",
        "rules_engine_assignment_results",
        "rules_engine_ruleset",
        "rules_engine_engine_version",
    ]
    assert "rules_engine_matched_rules" not in compact.columns
    assert "rules_engine_assignment_results" not in compact.columns
    assert compact_row["rules_engine_ruleset"].asDict() == {
        "id": ruleset.ruleset_id,
        "version": ruleset.version,
        "content_hash": DeltaRowSerializer().content_hash(ruleset),
    }
    assert compact_row["rules_engine_engine_version"]
    assert compact_evaluation.apply_assignments().columns == [
        "row_id",
        "account",
        "source_note",
        "bucket",
    ]


def test_coverage_report_finds_dead_broad_and_clean_no_match_rows(spark):
    """Coverage aggregates matches and returns clean no-match rows."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "coverage",
            "ruleset_name": "Coverage",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "prime",
                    "rule_name": "Prime",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "condition_id": "prime-fico",
                                "left": {"field": "fico"},
                                "operator": "ge",
                                "right": {"literal": 720},
                            }
                        ]
                    },
                    "assign": {"bucket": "prime"},
                },
                {
                    "rule_id": "near",
                    "rule_name": "Near prime",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "condition_id": "near-fico",
                                "left": {"field": "fico"},
                                "operator": "ge",
                                "right": {"literal": 680},
                            }
                        ]
                    },
                    "assign": {"review": True},
                },
                {
                    "rule_id": "impossible",
                    "rule_name": "Impossible",
                    "rule_order": 3,
                    "when": {
                        "all": [
                            {
                                "condition_id": "impossible-fico",
                                "left": {"field": "fico"},
                                "operator": "gt",
                                "right": {"literal": 900},
                            }
                        ]
                    },
                    "assign": {"invalid": True},
                },
            ],
        }
    )
    registry = FunctionRegistry()
    runtime = SparkRulesEngineRuntime(DummyRepository(), registry)
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        report = RulesetCoverageAnalyzer(runtime).analyze(
            spark.createDataFrame(
                [(1, 740), (2, 690), (3, 600)],
                ["loan_id", "fico"],
            ),
            ruleset,
            broad_match_threshold=0.60,
        )
        no_match = report.no_match_rows.collect()[0]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)

    assert report.total_row_count == 3
    assert report.no_match_count == 1
    assert report.error_count == 0
    assert report.dead_rule_ids == ("impossible",)
    assert report.suspiciously_broad_rule_ids == ("near",)
    assert no_match["loan_id"] == 3
