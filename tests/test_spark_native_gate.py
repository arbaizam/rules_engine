import pytest

from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


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


def _native_issue(ruleset, source_schema):
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, source_schema)
    return runtime._native_compatibility_issue(
        ruleset,
        source_schema,
        assign_schema,
    )


def test_spark_runtime_selects_native_execution_for_field_literal_rules():
    """
    What: Marks ordinary field/literal rules as native-Spark compatible.
    Why: High-volume evaluation must avoid a Python worker for the common path.
    Fails when: Native capability detection unnecessarily routes simple rules to the UDF.
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
    schema = T.StructType([T.StructField("account", T.StringType(), True)])

    assert _native_issue(ruleset, schema) is None


@pytest.mark.parametrize(
    ("literal", "value_type", "field_type"),
    [
        (5, "number", T.LongType()),
        (5, "double", T.DoubleType()),
    ],
)
def test_spark_runtime_keeps_valid_numeric_hints_on_native_path(
    literal,
    value_type,
    field_type,
):
    """
    What: Accepts broad and explicit numeric literal hints on compatible fields.
    Why: A valid hint must not unnecessarily route an ordinary comparison to Python.
    Fails when: Hint interpretation disagrees between schema gating and compilation.
    """
    ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "eq",
            "right": {"literal": literal, "value_type": value_type},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    schema = T.StructType([T.StructField("amount", field_type, True)])

    assert _native_issue(ruleset, schema) is None


def test_spark_runtime_routes_custom_functions_to_python_compatibility_path():
    """
    What: Identifies Python custom functions as incompatible with native columns.
    Why: Unsupported rules must retain their existing behavior through an explicit fallback.
    Fails when: Capability detection attempts to compile Python callables as Spark expressions.
    """
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"x": {"field": "amount"}},
                }
            },
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    schema = T.StructType([T.StructField("amount", T.LongType(), True)])

    issue = _native_issue(ruleset, schema)

    assert issue is not None
    assert "custom function 'score' requires Python" in issue


def test_spark_runtime_require_native_rejects_udf_fallback_during_planning():
    """
    What: Raises before Spark execution when strict native mode finds a custom function.
    Why: Production callers need a fail-fast guard against accidental Python UDF plans.
    Fails when: require_native silently permits the high-cost compatibility path.
    """
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"x": {"field": "amount"}},
                }
            },
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    class SchemaOnlyDataFrame:
        schema = T.StructType([T.StructField("amount", T.LongType(), True)])

    with pytest.raises(ValueError, match="cannot use native Spark execution"):
        _spark_runtime().evaluate_dataframe(
            SchemaOnlyDataFrame(),
            ruleset,
            require_native=True,
        )


@pytest.mark.parametrize(
    ("condition", "schema", "message"),
    [
        (
            {
                "left": {"field": "missing"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "source field 'missing' is missing",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "error",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "null_input_mode=error",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "error",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "null_result_mode=error",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "eq",
                "right": {"literal": "A"},
                "null_input_mode": "zero",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "null_input_mode=zero",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "gt",
                "right": {"literal": "A"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "requires numeric Spark operands",
        ),
        (
            {
                "left": {"field": "amount"},
                "operator": "eq",
                "right": {"literal": 1},
                "tolerance_abs": "0.01",
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("amount", T.LongType(), True)]),
            "nonzero tolerance requires Python decimal semantics",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "eq",
                "right": {"literal": True},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "incompatible Spark operand types",
        ),
        (
            {
                "left": {"field": "amount"},
                "operator": "eq",
                "right": {"literal": 9007199254740993},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("amount", T.DoubleType(), True)]),
            "incompatible Spark operand types",
        ),
        (
            {
                "left": {"field": "amount"},
                "operator": "eq",
                "right": {"literal": 1.5},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("amount", T.DecimalType(18, 4), True)]),
            "incompatible Spark operand types",
        ),
        (
            {
                "left": {"field": "amount"},
                "operator": "eq",
                "right": {
                    "literal": "not-a-number",
                    "value_type": "number",
                },
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("amount", T.DoubleType(), True)]),
            "does not match declared value_type",
        ),
        (
            {
                "left": {"field": "as_of_date"},
                "operator": "eq",
                "right": {"field": "as_of_timestamp"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType(
                [
                    T.StructField("as_of_date", T.DateType(), True),
                    T.StructField("as_of_timestamp", T.TimestampType(), True),
                ]
            ),
            "incompatible Spark operand types",
        ),
        (
            {
                "left": {"field": "attributes"},
                "operator": "eq",
                "right": {"field": "expected_attributes"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType(
                [
                    T.StructField(
                        "attributes",
                        T.MapType(T.StringType(), T.StringType()),
                        True,
                    ),
                    T.StructField(
                        "expected_attributes",
                        T.MapType(T.StringType(), T.StringType()),
                        True,
                    ),
                ]
            ),
            "incompatible Spark operand types",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "eq",
                "right": {"literal": {"code": "A"}},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "mapping comparisons require Python semantics",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "in",
                "right": {"field": "allowed_accounts"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType(
                [
                    T.StructField("account", T.StringType(), True),
                    T.StructField(
                        "allowed_accounts",
                        T.ArrayType(T.StringType()),
                        True,
                    ),
                ]
            ),
            "requires a literal collection",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "in",
                "right": {"literal": ["A", None]},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "collections containing null",
        ),
        (
            {
                "left": {"field": "amount"},
                "operator": "contains",
                "right": {"literal": "1"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("amount", T.LongType(), True)]),
            "requires string Spark operands",
        ),
        (
            {
                "left": {"field": "account"},
                "operator": "like",
                "right": {"literal": "A\\_%"},
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            },
            T.StructType([T.StructField("account", T.StringType(), True)]),
            "backslashes require Python semantics",
        ),
    ],
)
def test_spark_runtime_native_gate_routes_unsupported_semantics_to_udf(
    condition,
    schema,
    message,
):
    """
    What: Routes behavior that cannot be preserved by Spark expressions to the UDF.
    Why: Native capability checks are the contract boundary between two evaluators.
    Fails when: Unsupported null, type, collection, tolerance, or LIKE behavior leaks natively.
    """
    issue = _native_issue(_compile(condition), schema)

    assert issue is not None
    assert message in issue


def test_spark_runtime_native_gate_validates_inactive_condition_trace_operands():
    """
    What: Keeps an inactive custom-function condition on the UDF path.
    Why: Inactive operands are still represented in the winning trace.
    Fails when: Gate ordering lets an unsupported trace operand reach native compilation.
    """
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"x": {"field": "amount"}},
                }
            },
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
            "active_flag": False,
        }
    )
    schema = T.StructType([T.StructField("amount", T.LongType(), True)])

    issue = _native_issue(ruleset, schema)

    assert issue is not None
    assert "custom function 'score' requires Python" in issue


def test_spark_runtime_native_gate_ignores_inactive_rules():
    """
    What: Excludes inactive rules before native capability checking.
    Why: Inactive custom functions cannot affect the live Spark plan or trace.
    Fails when: Dead rule metadata unnecessarily forces the complete ruleset to the UDF.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "inactive_gate",
            "ruleset_name": "Inactive Gate",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "active",
                    "rule_name": "Active",
                    "rule_order": 1,
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
                    "assign": {"bucket": "active"},
                },
                {
                    "rule_id": "inactive",
                    "rule_name": "Inactive",
                    "rule_order": 2,
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "score",
                                        "args": {"x": {"field": "amount"}},
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": 5},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "inactive"},
                },
            ],
        }
    )
    schema = T.StructType(
        [
            T.StructField("account", T.StringType(), True),
            T.StructField("amount", T.LongType(), True),
        ]
    )

    assert _native_issue(ruleset, schema) is None


def test_spark_runtime_native_gate_routes_mixed_field_assignment_types_to_udf():
    """
    What: Routes incompatible field-backed assignment types to Python formatting.
    Why: Spark string casts do not share the compatibility formatter's output contract.
    Fails when: A mixed target changes formatting only on the native path.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "mixed_assign",
            "ruleset_name": "Mixed Assign",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "text",
                    "rule_name": "Text",
                    "rule_order": 1,
                    "when": {"all": []},
                    "assign": {"result": {"field": "text_value"}},
                },
                {
                    "rule_id": "number",
                    "rule_name": "Number",
                    "rule_order": 2,
                    "when": {"all": []},
                    "assign": {"result": {"field": "number_value"}},
                },
            ],
        }
    )
    schema = T.StructType(
        [
            T.StructField("text_value", T.StringType(), True),
            T.StructField("number_value", T.LongType(), True),
        ]
    )

    issue = _native_issue(ruleset, schema)

    assert issue is not None
    assert "mixed field types require Python string formatting" in issue
