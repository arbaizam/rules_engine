import json

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import RulesEngineRuntime
from rules_engine.spark_runtime import SparkRulesEngineRuntime

import pytest


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


def _runtime(registry=None):
    return RulesEngineRuntime(DummyRepository(), registry or FunctionRegistry())


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


class FakeSparkRow:
    def __init__(self, data):
        self._data = data

    def asDict(self, recursive=True):
        return self._data


def test_runtime_evaluates_simple_row_rule_and_assignment():
    """
    What: Evaluates a basic row-level rule and assignment in Python runtime.
    Why: This is the reference behavior used by tests and Spark row UDFs.
    Fails when: Matching, assignment output, or rule trace generation regresses.
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

    output, traces = _runtime().evaluate(
        [{"account": "A"}, {"account": "B"}],
        ruleset,
    )

    assert output[0]["matched"] is True
    assert output[0]["assign"] == {"bucket": "matched"}
    assert output[1]["matched"] is False
    assert traces[0].matched is True
    assert traces[1].matched is False


def test_runtime_rule_results_include_condition_trace_values():
    """
    What: Emits resolved operand values and columns in rule_results.
    Why: Runtime metadata must be traceable back to each evaluated condition.
    Fails when: rule_results falls back to compact rule-level pass/fail output.
    """
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A", "value_type": "string"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    output, traces = _runtime().evaluate([{"account": "A"}], ruleset)

    rule_result = output[0]["rule_results"][0]
    condition_result = rule_result["conditions"][0]
    assert rule_result["rule_id"] == "r1"
    assert rule_result["rule_name"] == "Rule 1"
    assert rule_result["matched"] is True
    assert rule_result["assignments_applied"] == ["bucket"]
    assert traces[0].condition_traces[0].condition_id is not None
    assert "condition_id" not in condition_result
    assert "condition_group_id" not in condition_result
    assert "condition_group_operator" not in condition_result
    assert "active_flag" not in condition_result
    assert "evaluated" not in condition_result["left"]
    assert "rule_order" not in rule_result
    assert condition_result["columns"] == ["account"]
    assert condition_result["operator"] == "eq"
    assert condition_result["left"] == {
        "kind": "field",
        "column": "account",
        "value": "A",
    }
    assert condition_result["right"] == {
        "kind": "literal",
        "value": "A",
        "value_type": "string",
    }
    assert condition_result["comparison_result"] is True
    assert condition_result["passed"] is True
    json.dumps(output[0]["rule_results"])


def test_runtime_rule_results_include_aggregate_trace_values():
    """
    What: Emits aggregate operand columns, group key, and resolved aggregate value.
    Why: Aggregate-backed conditions must explain the value compared for a row.
    Fails when: aggregate traces omit source columns or evaluated values.
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

    output, _ = _runtime().evaluate(
        [
            {"account": "A", "amount": 10},
            {"account": "A", "amount": 20},
            {"account": "B", "amount": 5},
        ],
        ruleset,
    )

    aggregate_trace = output[0]["rule_results"][0]["conditions"][0]["left"]
    assert aggregate_trace["kind"] == "aggregate"
    assert aggregate_trace["source_columns"] == ["amount", "account"]
    assert aggregate_trace["function"] == "sum"
    assert aggregate_trace["scope"] == "group"
    assert aggregate_trace["group_key"] == {"account": "A"}
    assert aggregate_trace["value"] == 30


def test_runtime_rule_results_include_custom_function_arg_trace_values():
    """
    What: Emits custom-function argument traces and resolved function values.
    Why: Custom-function conditions need traceability for nested operand inputs.
    Fails when: function traces hide the row values passed into the callable.
    """
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="tests.score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        implementation=lambda **kwargs: kwargs["x"] + kwargs["y"],
    )
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"x": {"field": "amount"}, "y": {"literal": 3}},
                }
            },
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    output, _ = _runtime(registry).evaluate([{"amount": 2}], ruleset)

    function_trace = output[0]["rule_results"][0]["conditions"][0]["left"]
    assert function_trace["kind"] == "custom_function"
    assert function_trace["function_name"] == "score"
    assert function_trace["source_columns"] == ["amount"]
    assert function_trace["value"] == 5
    assert function_trace["args"]["x"]["value"] == 2
    assert function_trace["args"]["x"]["column"] == "amount"
    assert function_trace["args"]["y"]["value"] == 3


def test_spark_row_evaluator_serializes_enriched_rule_results():
    """
    What: Serializes the enriched rule-result payload through the Spark row UDF.
    Why: Spark's JSON column must expose the same trace metadata as Python.
    Fails when: Spark keeps emitting only rule-level pass/fail metadata.
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
    evaluator = SparkRulesEngineRuntime(
        DummyRepository(),
        FunctionRegistry(),
    )._build_row_evaluator(ruleset, {}, ["account"])

    result = evaluator(FakeSparkRow({"account": "A"}))
    rule_results = json.loads(result["rule_results"])

    assert result["matched"] is True
    assert rule_results[0]["rule_id"] == "r1"
    assert rule_results[0]["matched"] is True
    assert rule_results[0]["conditions"][0]["columns"] == ["account"]
    assert rule_results[0]["conditions"][0]["left"]["column"] == "account"
    assert rule_results[0]["conditions"][0]["left"]["value"] == "A"


def test_runtime_supports_canonical_string_operators():
    """
    What: Evaluates contains/not_contains/starts_with/ends_with in runtime.
    Why: Canonical string operators must behave consistently after compilation.
    Fails when: Runtime string operator dispatch or matching semantics change.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_id": operator,
                "rule_name": operator,
                "rule_order": index,
                "when": {
                    "all": [
                        {
                            "left": {"field": "name"},
                            "operator": operator,
                            "right": {"literal": literal},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
                "assign": {operator: True},
            }
            for index, (operator, literal) in enumerate(
                [
                    ("contains", "bc"),
                    ("not_contains", "zz"),
                    ("starts_with", "ab"),
                    ("ends_with", "de"),
                ],
                start=1,
            )
        ],
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    output, _ = _runtime().evaluate([{"name": "abcde"}], ruleset)

    assert output[0]["matched_rule_ids"] == [
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
    ]


def test_runtime_like_uses_sql_wildcard_semantics():
    """
    What: Evaluates SQL LIKE percent wildcard behavior in Python runtime.
    Why: Python and Spark runtimes must agree on LIKE semantics.
    Fails when: LIKE falls back to equality or mishandles SQL wildcards.
    """
    ruleset = _compile(
        {
            "left": {"field": "name"},
            "operator": "like",
            "right": {"literal": "abc%"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    output, _ = _runtime().evaluate([{"name": "abcde"}, {"name": "xyz"}], ruleset)

    assert [row["matched"] for row in output] == [True, False]


def test_runtime_null_result_default_controls_condition_result():
    """
    What: Evaluates null_result_mode=default on a null comparison result.
    Why: Null handling is explicit metadata and controls final condition truth.
    Fails when: Null defaults are ignored or treated as ordinary nulls.
    """
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
            "null_default_value": True,
        }
    )

    output, _ = _runtime().evaluate([{}], ruleset)

    assert output[0]["matched"] is True


def test_runtime_executes_custom_function_operand():
    """
    What: Resolves and executes a registered custom function operand.
    Why: Custom logic is allowed only through the registry contract.
    Fails when: Runtime cannot look up or call registered custom functions.
    """
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="tests.score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        implementation=lambda **kwargs: kwargs["x"] + kwargs["y"],
    )
    ruleset = _compile(
        {
            "left": {"custom_function": {"name": "score", "args": {"x": 2, "y": 3}}},
            "operator": "eq",
            "right": {"literal": 5},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    output, _ = _runtime(registry).evaluate([{}], ruleset)

    assert output[0]["matched"] is True


def test_runtime_evaluates_dataset_aggregate():
    """
    What: Evaluates a dataset-scoped aggregate against all rows.
    Why: Dataset aggregates must apply to the incoming row set exactly as supplied.
    Fails when: Dataset aggregate caching or row application semantics regress.
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

    output, _ = _runtime().evaluate(
        [{"amount": 10}, {"amount": 20}],
        ruleset,
    )

    assert [row["matched"] for row in output] == [True, True]


def test_runtime_evaluates_group_aggregate_against_current_row_group():
    """
    What: Evaluates a group-scoped aggregate using the current row's group key.
    Why: Group aggregates must return different values for different groups.
    Fails when: Group-key resolution or aggregate cache partitioning regresses.
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

    output, _ = _runtime().evaluate(
        [
            {"account": "A", "amount": 10},
            {"account": "A", "amount": 20},
            {"account": "B", "amount": 5},
        ],
        ruleset,
    )

    assert [row["matched"] for row in output] == [True, True, False]


def test_runtime_evaluates_filtered_aggregate():
    """
    What: Evaluates a dataset aggregate with a row-level filter.
    Why: Filtered aggregates should filter only aggregate inputs, not output rows.
    Fails when: Filter predicates are ignored or applied to the outer row set.
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

    output, _ = _runtime().evaluate(
        [
            {"status": "OPEN", "amount": 10},
            {"status": "CLOSED", "amount": 50},
            {"status": "OPEN", "amount": 20},
        ],
        ruleset,
    )

    assert [row["matched"] for row in output] == [True, True, True]


def test_runtime_evaluates_order_sensitive_first_aggregate():
    """
    What: Evaluates FIRST aggregate with explicit ascending order_by.
    Why: Order-sensitive aggregates require deterministic ordering.
    Fails when: FIRST ignores order_by or uses input order accidentally.
    """
    ruleset = _compile(
        {
            "left": {
                "aggregate": {
                    "function": "first",
                    "field": "event",
                    "scope": "dataset",
                    "order_by": [{"field": "sequence", "direction": "asc"}],
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "eq",
            "right": {"literal": "first"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    output, _ = _runtime().evaluate(
        [
            {"sequence": 2, "event": "second"},
            {"sequence": 1, "event": "first"},
        ],
        ruleset,
    )

    assert [row["matched"] for row in output] == [True, True]


def test_runtime_desc_ordering_keeps_nulls_last_for_first_aggregate():
    """
    What: Evaluates FIRST with descending order and null order values.
    Why: Python and Spark runtimes intentionally keep nulls last for ordering.
    Fails when: Descending sort places nulls first or diverges from Spark.
    """
    ruleset = _compile(
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
        }
    )

    output, _ = _runtime().evaluate(
        [
            {"sequence": None, "event": "null-sequence"},
            {"sequence": 2, "event": "largest"},
            {"sequence": 1, "event": "smallest"},
        ],
        ruleset,
    )

    assert [row["matched"] for row in output] == [True, True, True]


def test_spark_runtime_fails_fast_for_exact_median_gap():
    """
    What: Calls Spark aggregate support guard for median.
    Why: Spark exact median/quantile semantics are intentionally unsupported in v1.
    Fails when: Approximate Spark percentile paths become silently reachable.
    """
    ruleset = _compile(
        {
            "left": {
                "aggregate": {
                    "function": "median",
                    "field": "amount",
                    "scope": "dataset",
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "eq",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    aggregate = ruleset.rules[0].root_group.conditions[0].left

    with pytest.raises(ValueError, match="exact median/quantile"):
        SparkRulesEngineRuntime(DummyRepository(), FunctionRegistry())._validate_spark_supported_aggregate(
            aggregate
        )
