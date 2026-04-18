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


def test_runtime_evaluates_simple_row_rule_and_assignment():
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


def test_runtime_supports_canonical_string_operators():
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
