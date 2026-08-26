import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RegistryError
from rules_engine.registry import (
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    FunctionRegistry,
)
from rules_engine.validator import RulesetValidator


def _base_payload(condition):
    return {
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


def test_ruleset_requires_at_least_one_active_rule():
    """A valid ruleset must have a writable active assignment contract."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    payload["rules"][0]["active_flag"] = False

    result = RulesetValidator().validate(YamlRulesetCompiler().compile_payload(payload))

    assert "RULESET_ACTIVE_RULE_REQUIRED" in {issue.check_name for issue in result.issues}


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
            arguments=(CustomFunctionArgSpec("x"), CustomFunctionArgSpec("y")),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        )
    )
    result = _validate_condition(
        {
            "left": {"custom_function": {"name": "score", "args": {"x": 1}}},
            "operator": "gt",
            "right": {"literal": 1},
        },
        registry=registry,
    )

    assert any(issue.check_name == "CUSTOM_FUNCTION_ARGS_MISMATCH" for issue in result.issues)


def test_custom_function_optional_defaults_and_argument_constraints_are_validated():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="pkg.score",
            arguments=(
                CustomFunctionArgSpec("value", type_hint="number"),
                CustomFunctionArgSpec(
                    "mode",
                    required=False,
                    default="strict",
                    type_hint="string",
                    allowed_values=("strict", "lenient"),
                    literal_only=True,
                ),
            ),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        )
    )

    valid = _validate_condition(
        {
            "left": {"custom_function": {"name": "score", "args": {"value": 1}}},
            "operator": "gt",
            "right": {"literal": 0},
        },
        registry=registry,
    )
    bad_type = _validate_condition(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"value": "not-numeric"},
                }
            },
            "operator": "gt",
            "right": {"literal": 0},
        },
        registry=registry,
    )
    bad_value = _validate_condition(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"value": 1, "mode": "unknown"},
                }
            },
            "operator": "gt",
            "right": {"literal": 0},
        },
        registry=registry,
    )
    dynamic_mode = _validate_condition(
        {
            "left": {
                "custom_function": {
                    "name": "score",
                    "args": {"value": 1, "mode": {"field": "mode"}},
                }
            },
            "operator": "gt",
            "right": {"literal": 0},
        },
        registry=registry,
    )

    assert valid.passed
    assert "CUSTOM_FUNCTION_ARG_TYPE_MISMATCH" in {issue.check_name for issue in bad_type.issues}
    assert "CUSTOM_FUNCTION_ARG_VALUE_INVALID" in {issue.check_name for issue in bad_value.issues}
    assert "CUSTOM_FUNCTION_ARG_LITERAL_REQUIRED" in {
        issue.check_name for issue in dynamic_mode.issues
    }


def test_custom_function_registry_rejects_unenforceable_argument_metadata():
    with pytest.raises(RegistryError, match="literal-only"):
        CustomFunctionSpec(
            function_name="bad_allowed_values",
            implementation_reference="pkg.bad_allowed_values",
            arguments=(
                CustomFunctionArgSpec(
                    "mode",
                    allowed_values=("a", "b"),
                ),
            ),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=True,
        )

    with pytest.raises(RegistryError, match="JSON-compatible"):
        CustomFunctionSpec(
            function_name="bad_default",
            implementation_reference="pkg.bad_default",
            arguments=(
                CustomFunctionArgSpec(
                    "value",
                    required=False,
                    default=object(),
                ),
            ),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=True,
        )


def test_error_on_null_is_rejected_for_unary_null_operators():
    """Unary null checks cannot also demand an error for the value they inspect."""
    result = _validate_condition(
        {
            "left": {"field": "account"},
            "operator": "is_null",
            "error_on_null": True,
        }
    )

    assert "ERROR_ON_NULL_UNARY_FORBIDDEN" in {issue.check_name for issue in result.issues}


def test_assigned_operand_requires_an_active_lower_order_producer():
    """Same-rule, future, and inactive assignments cannot satisfy a reference."""
    payload = _base_payload(
        {
            "left": {"assigned": "bucket"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    result = RulesetValidator().validate(ruleset)

    assert "ASSIGNED_VALUE_PRIOR_PRODUCER_REQUIRED" in {issue.check_name for issue in result.issues}


def test_assigned_operand_accepts_a_potential_prior_producer():
    """The producer must be structurally earlier; it need not match every row."""
    payload = _base_payload(
        {
            "left": {"field": "status"},
            "operator": "eq",
            "right": {"literal": "OPEN"},
        }
    )
    payload["rules"].append(
        {
            "rule_id": "r2",
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
            "assign": {"review": True},
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    result = RulesetValidator().validate(ruleset)

    assert "ASSIGNED_VALUE_PRIOR_PRODUCER_REQUIRED" not in {
        issue.check_name for issue in result.issues
    }


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
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    assert not RulesetValidator().validate(ruleset).has_errors()


def test_between_with_nonzero_tolerance_fails_validation():
    """
    What: Validates that between/not_between cannot use non-zero tolerance.
    Why: The current contract defines tolerance for scalar comparisons, not ranges.
    Fails when: Metadata can persist a tolerance that runtime would ignore.
    """
    result = _validate_condition(
        {
            "left": {"field": "amount"},
            "operator": "between",
            "right": {"literal": [10, 20]},
            "tolerance_abs": "0.01",
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
                        },
                        {
                            "condition_id": "duplicate_condition",
                            "left": {"field": "status"},
                            "operator": "eq",
                            "right": {"literal": "OPEN"},
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
    Why: Duplicate group IDs make audit diagnostics ambiguous.
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
                        },
                        {
                            "condition_group_id": "duplicate_group",
                            "any": [
                                {
                                    "left": {"field": "status"},
                                    "operator": "eq",
                                    "right": {"literal": "OPEN"},
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


def test_duplicate_assignment_target_within_rule_fails_validation():
    """One rule cannot use list order to resolve duplicate target fields."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    payload["rules"][0]["assign"] = [
        {
            "assignment_id": "first",
            "target_field": "bucket",
            "value": {"literal": "A"},
        },
        {
            "assignment_id": "second",
            "target_field": "bucket",
            "value": {"literal": "A"},
        },
    ]

    result = RulesetValidator().validate(YamlRulesetCompiler().compile_payload(payload))
    issue = next(
        item
        for item in result.issues
        if item.check_name == "ASSIGNMENT_TARGET_DUPLICATE_WITHIN_RULE"
    )

    assert issue.details == {
        "rule_id": "r1",
        "target_field": "bucket",
        "assignment_ids": ["first", "second"],
    }


def test_duplicate_assignment_id_across_rules_fails_validation():
    """Assignment identity is unique across one ruleset version."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    first_rule = payload["rules"][0]
    first_rule["assign"] = [
        {
            "assignment_id": "shared",
            "target_field": "bucket",
            "value": {"literal": "A"},
        }
    ]
    payload["rules"].append(
        {
            **first_rule,
            "rule_id": "r2",
            "rule_name": "Rule 2",
            "rule_order": 2,
            "assign": [
                {
                    "assignment_id": "shared",
                    "target_field": "other",
                    "value": {"literal": "B"},
                }
            ],
        }
    )

    result = RulesetValidator().validate(YamlRulesetCompiler().compile_payload(payload))
    issue = next(item for item in result.issues if item.check_name == "ASSIGNMENT_ID_DUPLICATE")

    assert issue.details["ruleset_id"] == "rs1"
    assert issue.details["version"] == "1"
    assert issue.details["rule_ids"] == ["r1", "r2"]


def test_duplicate_assignment_id_within_rule_has_clear_location():
    """A same-rule duplicate does not claim two differently named rules."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    payload["rules"][0]["assign"] = [
        {
            "assignment_id": "shared",
            "target_field": "bucket",
            "value": {"literal": "A"},
        },
        {
            "assignment_id": "shared",
            "target_field": "other",
            "value": {"literal": "B"},
        },
    ]

    result = RulesetValidator().validate(YamlRulesetCompiler().compile_payload(payload))
    issue = next(item for item in result.issues if item.check_name == "ASSIGNMENT_ID_DUPLICATE")

    assert "more than once in rule r1" in issue.message


def test_assignment_id_may_be_reused_when_versions_are_validated_independently():
    """The uniqueness boundary does not leak across ruleset versions."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    payload["rules"][0]["assign"] = [
        {
            "assignment_id": "stable",
            "target_field": "bucket",
            "value": {"literal": "A"},
        }
    ]
    next_version = {**payload, "version": "2"}

    assert RulesetValidator().validate(YamlRulesetCompiler().compile_payload(payload)).passed
    assert RulesetValidator().validate(YamlRulesetCompiler().compile_payload(next_version)).passed
