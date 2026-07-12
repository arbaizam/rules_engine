from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.validator import RulesetValidator


def _base_payload(condition):
    return {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
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


def test_literal_value_type_must_match_runtime_value():
    """
    What: Rejects a literal whose explicit type hint contradicts its runtime value.
    Why: Spark must not rely on ANSI-sensitive implicit casts from inaccurate hints.
    Fails when: A numeric hint can approve a string literal for native execution.
    """
    result = _validate_condition(
        {
            "left": {"field": "amount"},
            "operator": "eq",
            "right": {"literal": "not-a-number", "value_type": "number"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert any(
        issue.check_name == "LITERAL_VALUE_TYPE_INVALID"
        for issue in result.issues
    )


def test_literal_value_type_accepts_compatible_numeric_and_list_values():
    """
    What: Accepts established number and list hints when their values agree.
    Why: Hint validation must preserve canonical YAML already used by the package.
    Fails when: Valid numeric thresholds or collection literals are rejected.
    """
    numeric = _validate_condition(
        {
            "left": {"field": "amount"},
            "operator": "eq",
            "right": {"literal": 5, "value_type": "number"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    collection = _validate_condition(
        {
            "left": {"field": "account"},
            "operator": "in",
            "right": {"literal": ["A", "B"], "value_type": "list"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert not any(
        issue.check_name == "LITERAL_VALUE_TYPE_INVALID"
        for result in (numeric, collection)
        for issue in result.issues
    )


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
        "status": "published",
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


def test_duplicate_condition_group_ids_fail_validation():
    """
    What: Validates that condition_group_id values are unique within a ruleset.
    Why: Duplicate group IDs make audit diagnostics ambiguous for code-authored rulesets.
    Fails when: Nested condition groups can reuse the same identifier.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "owner": "Rules Team",
        "owner_department": "ALM Engineering",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "rule_order": 1,
                "when": {
                    "condition_group_id": "duplicate_group",
                    "all": [
                        {
                            "left": {"field": "account"},
                            "operator": "eq",
                            "right": {"literal": "A"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                        {
                            "condition_group_id": "duplicate_group",
                            "any": [
                                {
                                    "left": {"field": "status"},
                                    "operator": "eq",
                                    "right": {"literal": "OPEN"},
                                    "null_input_mode": "propagate",
                                    "null_result_mode": "null",
                                }
                            ],
                        },
                    ],
                },
                "assign": {"bucket": "A"},
            }
        ],
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)
    result = RulesetValidator().validate(ruleset)

    assert any(issue.check_name == "CONDITION_GROUP_ID_DUPLICATE" for issue in result.issues)
