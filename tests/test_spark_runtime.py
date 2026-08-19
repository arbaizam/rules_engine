import os
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

import rules_engine.spark_runtime as spark_runtime_module
from rules_engine.analytics import RulesetCoverageAnalyzer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
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
    df = spark.createDataFrame(
        [("A", "existing"), ("B", "unchanged")],
        ["account", "bucket"],
    )

    output = _spark_runtime().evaluate_dataframe(df, ruleset, full_audit=True)
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
    match_trace = rows[0]["rules_engine_first_matched_rule_trace"]
    assert [item["rule_id"] for item in rows[0]["rules_engine_matched_rules"]] == [
        "r1"
    ]
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
    assert match_trace["conditions"][0]["columns"] == ["account"]
    assert match_trace["conditions"][0]["left"]["column"] == "account"
    assert match_trace["conditions"][0]["left"]["value"] == "A"
    assert "rules_engine_rule_results" not in rows[0].asDict()
    assert rows[1]["rules_engine_matched"] is False
    assert rows[1]["rules_engine_first_matched_rule_trace"] is None
    assert rows[1]["rules_engine_matched_rules"] == []
    assert rows[1]["rules_engine_assignment_results"] == []

    multi_match_ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "multi",
            "ruleset_name": "Multi-match",
            "version": "1",
            "status": "published",
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
    multi_row = _spark_runtime().evaluate_dataframe(
        spark.createDataFrame([("A", "original")], ["account", "bucket"]),
        multi_match_ruleset,
        full_audit=True,
    ).collect()[0]
    multi_events = {
        event["assignment_id"]: event
        for event in multi_row["rules_engine_assignment_results"]
    }

    assert multi_row["rules_engine_matched_rule_ids"] == ["first", "second"]
    assert multi_row["rules_engine_assign"].asDict() == {
        "bucket": "second",
        "review": True,
    }
    assert multi_events["first_bucket"]["effective"] is False
    assert multi_events["first_bucket"]["overridden_by_rule_id"] == "second"
    assert multi_events["first_bucket"]["overridden_by_assignment_id"] == (
        "second_bucket"
    )
    assert multi_events["second_bucket"]["old_value"] == "original"


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
    output = _spark_runtime().evaluate_dataframe(
        spark.createDataFrame([{"account": "A"}]),
        ruleset,
    )

    assert len(udf_factory_calls) == 1
    assert output.collect()[0]["rules_engine_matched"] is True


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

    row = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        full_audit=True,
    ).collect()[0]

    assert row["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"]["copied"] == "assigned"
    assert row["rules_engine_first_matched_rule_trace"]["conditions"][0]["left"][
        "value"
    ] == "A"


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
            "status": "published",
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
                    "assign": [],
                }
            ],
        }
    )
    df = spark.createDataFrame([{"unused_payload": "kept"}])

    assert required_source_columns(ruleset) == ()

    output = _spark_runtime().evaluate_dataframe(df, ruleset)
    row = output.collect()[0]

    assert row["unused_payload"] == "kept"
    assert row["rules_engine_matched"] is True
    assert row["rules_engine_assign"] is None
    assert output.schema["rules_engine_assign"].dataType.fieldNames() == ["__empty"]


def test_spark_runtime_applies_column_prefix_to_all_new_outputs(spark):
    """Custom prefixes preserve exact compact and full-audit column contracts."""
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

    compact = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        column_prefix="audit",
    )
    full = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        column_prefix="audit",
        full_audit=True,
    )

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
        "audit_first_matched_rule_trace",
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
    def error_on_bad_value(*, value):
        if value == "bad":
            raise ValueError("bad test value")
        return True

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

    compact = _spark_runtime().evaluate_dataframe(df, ruleset)
    full = _spark_runtime().evaluate_dataframe(
        df,
        ruleset,
        full_audit=True,
    )
    compact_row = compact.collect()[0]

    assert compact.columns == [
        "row_id",
        "account",
        "source_note",
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_ruleset",
        "rules_engine_engine_version",
    ]
    assert full.columns == [
        "row_id",
        "account",
        "source_note",
        "rules_engine_error",
        "rules_engine_matched",
        "rules_engine_matched_rule_ids",
        "rules_engine_assign",
        "rules_engine_matched_rules",
        "rules_engine_first_matched_rule_trace",
        "rules_engine_assignment_results",
        "rules_engine_ruleset",
        "rules_engine_engine_version",
    ]
    assert "rules_engine_first_matched_rule_trace" not in compact.columns
    assert "rules_engine_assignment_results" not in compact.columns
    assert compact_row["rules_engine_ruleset"].asDict() == {
        "id": ruleset.ruleset_id,
        "version": ruleset.version,
        "content_hash": DeltaRowSerializer().content_hash(ruleset),
    }
    assert compact_row["rules_engine_engine_version"]


def test_coverage_report_finds_dead_broad_and_closest_rules(spark):
    """Coverage aggregates matches and diagnoses clean no-match rows."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "coverage",
            "ruleset_name": "Coverage",
            "version": "1",
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
        report = RulesetCoverageAnalyzer(runtime, registry).analyze(
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
    assert no_match["rules_engine_coverage_closest_rule_id"] == "prime"
    assert no_match["rules_engine_coverage_failed_condition_ids"] == ["prime-fico"]
