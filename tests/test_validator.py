from dataclasses import replace

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.validator import RulesetValidator


def _base_payload(condition):
    return {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "draft",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "rule_order": 1,
                "when": {"all": [condition]},
                "assign": {"bucket": "A"},
            }
        ],
    }


def _validate_condition(condition, registry=None):
    ruleset = YamlRulesetCompiler().compile_payload(_base_payload(condition))
    return RulesetValidator(registry).validate(ruleset)


def test_quantile_missing_q_fails_validation():
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "quantile",
                    "field": "amount",
                    "scope": "dataset",
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "gt",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert result.has_errors()
    assert any(issue.check_name == "QUANTILE_Q_REQUIRED" for issue in result.issues)


def test_group_scope_without_by_fails_validation():
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "group",
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "gt",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert any(issue.check_name == "AGGREGATE_GROUP_BY_REQUIRED" for issue in result.issues)


def test_dataset_scope_with_by_fails_validation():
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "dataset",
                    "by": ["account"],
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "gt",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert any(issue.check_name == "AGGREGATE_DATASET_BY_FORBIDDEN" for issue in result.issues)


def test_order_sensitive_aggregate_without_order_by_fails_validation():
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "first",
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

    assert any(issue.check_name == "AGGREGATE_ORDER_BY_REQUIRED" for issue in result.issues)


def test_nested_aggregate_in_filter_fails_validation():
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "dataset",
                    "filter": {
                        "all": [
                            {
                                "left": {
                                    "aggregate": {
                                        "function": "count",
                                        "field": "amount",
                                        "scope": "dataset",
                                        "null_input_mode": "ignore",
                                        "null_result_mode": "null",
                                    }
                                },
                                "operator": "gt",
                                "right": {"literal": 1},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "null_input_mode": "ignore",
                    "null_result_mode": "null",
                }
            },
            "operator": "gt",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert any(issue.check_name == "NESTED_AGGREGATE_FORBIDDEN" for issue in result.issues)


def test_custom_function_args_mismatch_fails_validation():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="pkg.score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        )
    )
    result = _validate_condition(
        {
            "left": {"custom_function": {"name": "score", "args": {"x": 1}}},
            "operator": "gt",
            "right": {"literal": 1},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        registry=registry,
    )

    assert any(issue.check_name == "CUSTOM_FUNCTION_ARGS_MISMATCH" for issue in result.issues)


def test_null_result_mode_default_without_default_fails_validation():
    result = _validate_condition(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
        }
    )

    assert any(issue.check_name == "NULL_DEFAULT_REQUIRED" for issue in result.issues)


def test_valid_string_operators_validate():
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "contains",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    assert not RulesetValidator().validate(ruleset).has_errors()

