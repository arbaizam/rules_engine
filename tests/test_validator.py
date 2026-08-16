from dataclasses import replace
from decimal import Decimal

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.models import LiteralOperand
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


def test_null_result_mode_default_rejects_string_boolean():
    """Quoted YAML booleans must not become truthy defaults at runtime."""
    result = _validate_condition(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
            "null_default_value": "false",
        }
    )

    assert "NULL_DEFAULT_BOOLEAN_REQUIRED" in {
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


def test_duplicate_assignment_target_within_rule_fails_validation():
    """One rule cannot use list order to resolve duplicate target fields."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
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

    result = RulesetValidator().validate(
        YamlRulesetCompiler().compile_payload(payload)
    )
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
            "null_input_mode": "propagate",
            "null_result_mode": "null",
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

    result = RulesetValidator().validate(
        YamlRulesetCompiler().compile_payload(payload)
    )
    issue = next(
        item
        for item in result.issues
        if item.check_name == "ASSIGNMENT_ID_DUPLICATE"
    )

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
            "null_input_mode": "propagate",
            "null_result_mode": "null",
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

    result = RulesetValidator().validate(
        YamlRulesetCompiler().compile_payload(payload)
    )
    issue = next(
        item
        for item in result.issues
        if item.check_name == "ASSIGNMENT_ID_DUPLICATE"
    )

    assert "more than once in rule r1" in issue.message


def test_assignment_id_may_be_reused_when_versions_are_validated_independently():
    """The uniqueness boundary does not leak across ruleset versions."""
    payload = _base_payload(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
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

    assert RulesetValidator().validate(
        YamlRulesetCompiler().compile_payload(payload)
    ).passed
    assert RulesetValidator().validate(
        YamlRulesetCompiler().compile_payload(next_version)
    ).passed


def test_code_authored_nonfinite_decimal_literal_fails_validation():
    """Dataclass authoring cannot bypass the compiler's finite-number guard."""
    ruleset = YamlRulesetCompiler().compile_payload(
        _base_payload(
            {
                "left": {"field": "rate"},
                "operator": "eq",
                "right": {"literal": 1},
            }
        )
    )
    rule = ruleset.rules[0]
    group = rule.root_group
    bad_condition = replace(
        group.conditions[0],
        right=LiteralOperand(Decimal("NaN")),
    )
    bad_ruleset = replace(
        ruleset,
        rules=(replace(rule, root_group=replace(group, conditions=(bad_condition,))),),
    )

    result = RulesetValidator().validate(bad_ruleset)

    assert "LITERAL_DECIMAL_FINITE_REQUIRED" in {
        issue.check_name for issue in result.issues
    }


def test_code_authored_nonfinite_tolerance_fails_validation_cleanly():
    """NaN tolerances produce a validation issue instead of Decimal failure."""
    ruleset = YamlRulesetCompiler().compile_payload(
        _base_payload(
            {
                "left": {"field": "rate"},
                "operator": "eq",
                "right": {"literal": 1},
            }
        )
    )
    rule = ruleset.rules[0]
    group = rule.root_group
    bad_condition = replace(group.conditions[0], tolerance_abs=Decimal("NaN"))
    bad_ruleset = replace(
        ruleset,
        rules=(replace(rule, root_group=replace(group, conditions=(bad_condition,))),),
    )

    result = RulesetValidator().validate(bad_ruleset)

    assert "TOLERANCE_FINITE_REQUIRED" in {
        issue.check_name for issue in result.issues
    }


def test_code_authored_nonfinite_float_literal_fails_validation():
    """Dataclass authoring cannot bypass finite floating-point validation."""
    ruleset = YamlRulesetCompiler().compile_payload(
        _base_payload(
            {
                "left": {"field": "rate"},
                "operator": "eq",
                "right": {"literal": 1},
            }
        )
    )
    rule = ruleset.rules[0]
    group = rule.root_group
    bad_condition = replace(
        group.conditions[0],
        right=LiteralOperand(float("inf"), "double"),
    )
    bad_ruleset = replace(
        ruleset,
        rules=(replace(rule, root_group=replace(group, conditions=(bad_condition,))),),
    )

    result = RulesetValidator().validate(bad_ruleset)

    assert "LITERAL_FLOAT_FINITE_REQUIRED" in {
        issue.check_name for issue in result.issues
    }
