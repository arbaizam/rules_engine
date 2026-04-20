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
        "owner": "Rules Team",
        "owner_department": "ALM Engineering",
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
    """
    What: Validates that quantile aggregates require args.q.
    Why: Quantile semantics are undefined without an explicit requested quantile.
    Fails when: The validator allows quantile metadata with missing or implicit q.
    """
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
    """
    What: Validates that group-scoped aggregates require by fields.
    Why: Group scope is explicit and cannot infer grouping keys from context.
    Fails when: The validator permits implicit grouping for scope=group.
    """
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


def test_missing_owner_metadata_fails_validation():
    """
    What: Validates that owner and owner_department are required.
    Why: Rulesets need business ownership metadata for governance and audit.
    Fails when: Rulesets can validate without required ownership fields.
    """
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    payload.pop("owner")
    payload.pop("owner_department")
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    result = RulesetValidator().validate(ruleset)

    assert {
        "RULESET_OWNER_REQUIRED",
        "RULESET_OWNER_DEPARTMENT_REQUIRED",
    } <= {issue.check_name for issue in result.issues}


def test_dataset_scope_with_by_fails_validation():
    """
    What: Validates that dataset-scoped aggregates reject by fields.
    Why: Dataset scope means the entire incoming row set, not grouped subsets.
    Fails when: Dataset aggregates can carry contradictory group-by metadata.
    """
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
    """
    What: Validates that first/last aggregates require order_by.
    Why: Order-sensitive aggregates must not rely on implicit row ordering.
    Fails when: The validator permits nondeterministic first/last aggregates.
    """
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
    """
    What: Validates that aggregate filters cannot contain aggregate operands.
    Why: Nested aggregates are outside the v1 semantic contract.
    Fails when: Filter predicates can recursively depend on aggregate results.
    """
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
    """
    What: Validates custom function argument names against the registry contract.
    Why: Runtime function calls must be deterministic and fully specified.
    Fails when: Missing, extra, or misspelled custom function args are accepted.
    """
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
    """
    What: Validates that null_result_mode=default requires null_default_value.
    Why: Default null behavior must be explicit in published metadata.
    Fails when: Conditions can silently default null results without a value.
    """
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
    assert result.passed is False


def test_valid_string_operators_validate():
    """
    What: Validates a condition using a canonical string operator.
    Why: Supported string operators must pass semantic validation consistently.
    Fails when: String operator support is removed or misclassified.
    """
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


def test_empty_aggregate_filter_fails_validation():
    """
    What: Validates that aggregate filters contain at least one predicate.
    Why: Empty filters diverge across runtimes and make aggregate semantics unclear.
    Fails when: filter all/any with an empty list can be published.
    """
    result = _validate_condition(
        {
            "left": {
                "aggregate": {
                    "function": "sum",
                    "field": "amount",
                    "scope": "dataset",
                    "filter": {"all": []},
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

    assert any(issue.check_name == "AGGREGATE_FILTER_EMPTY" for issue in result.issues)


def test_between_with_nonzero_tolerance_fails_validation():
    """
    What: Validates that between/not_between cannot use non-zero tolerance.
    Why: v1 defines tolerance for scalar comparisons but not range expansion.
    Fails when: Metadata can persist a tolerance that runtime would ignore.
    """
    result = _validate_condition(
        {
            "left": {"field": "amount"},
            "operator": "between",
            "right": {"literal": [10, 20]},
            "tolerance_abs": "0.01",
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert any(issue.check_name == "BETWEEN_TOLERANCE_FORBIDDEN" for issue in result.issues)


def test_duplicate_condition_ids_fail_validation():
    """
    What: Validates that condition_id values are unique within a ruleset.
    Why: Duplicate condition IDs make runtime traces and audit diagnostics ambiguous.
    Fails when: Code or YAML authoring can publish colliding condition IDs.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "draft",
        "owner": "Rules Team",
        "owner_department": "ALM Engineering",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "condition_id": "duplicate_condition",
                            "left": {"field": "account"},
                            "operator": "eq",
                            "right": {"literal": "A"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                        {
                            "condition_id": "duplicate_condition",
                            "left": {"field": "status"},
                            "operator": "eq",
                            "right": {"literal": "OPEN"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                    ]
                },
                "assign": {"bucket": "A"},
            }
        ],
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)
    result = RulesetValidator().validate(ruleset)

    assert any(issue.check_name == "CONDITION_ID_DUPLICATE" for issue in result.issues)
