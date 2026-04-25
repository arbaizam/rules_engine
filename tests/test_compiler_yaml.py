import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import AggregateFunction, ComparisonOperator, RulesetStatus
from rules_engine.exceptions import CompilationError


def test_valid_simple_row_rule_compiles_and_validates():
    """
    What: Compiles a minimal row-level YAML rule with owner metadata.
    Why: Basic YAML authoring must produce canonical condition metadata.
    Fails when: Required fields, owner fields, default tolerance, or eq parsing regress.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
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
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A", "value_type": "string"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assert ruleset.status is RulesetStatus.PUBLISHED
    assert ruleset.owner == "Rules Team"
    assert ruleset.owner_department == "ALM Engineering"
    assert condition.operator is ComparisonOperator.EQ
    assert str(condition.tolerance_abs) == "0"


def test_valid_group_aggregate_rule_compiles():
    """
    What: Compiles a group-scoped aggregate with an aggregate filter.
    Why: Group aggregates are first-class operands and must preserve by/filter shape.
    Fails when: Aggregate scope, by fields, or filtered aggregate parsing changes.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_name": "Group aggregate",
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "aggregate": {
                                        "function": "sum",
                                        "field": "amount",
                                        "scope": "group",
                                        "by": ["account"],
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
                                "operator": "gt",
                                "right": {"literal": 100, "value_type": "number"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "large"},
                }
            ],
        }
    )

    aggregate = ruleset.rules[0].root_group.conditions[0].left
    assert aggregate.function is AggregateFunction.SUM
    assert aggregate.by == ("account",)
    assert aggregate.filter is not None


def test_valid_dataset_aggregate_rule_compiles():
    """
    What: Compiles a dataset-scoped aggregate.
    Why: Dataset aggregates must be expressible without group-by fields.
    Fails when: Dataset scope is misparsed or implicit group fields leak in.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_name": "Dataset aggregate",
                    "when": {
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
                                "right": {"literal": 0, "value_type": "number"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "nonempty"},
                }
            ],
        }
    )

    aggregate = ruleset.rules[0].root_group.conditions[0].left
    assert aggregate.scope.value == "dataset"
    assert aggregate.by == ()


def test_canonical_string_operators_compile():
    """
    What: Compiles all canonical string operators.
    Why: The engine explicitly supports these operators without aliases.
    Fails when: Operator enum parsing drops or renames a canonical string operator.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_name": "Strings",
                "when": {
                    "all": [
                        {
                            "left": {"field": "name"},
                            "operator": operator,
                            "right": {"literal": "abc"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                        for operator in [
                            "contains",
                            "not_contains",
                            "starts_with",
                            "ends_with",
                        ]
                    ]
                },
                "assign": {"bucket": "match"},
            }
        ],
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)

    assert [item.operator.value for item in ruleset.rules[0].root_group.conditions] == [
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
    ]


def test_value_operand_alias_is_rejected():
    """
    What: Rejects the non-canonical operand key value.
    Why: Authoring must use literal so persisted metadata remains deterministic.
    Fails when: Alias support is accidentally reintroduced in the YAML compiler.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_name": "Alias",
                "when": {
                    "all": [
                        {
                            "left": {"field": "name"},
                            "operator": "eq",
                            "right": {"value": "abc"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
                "assign": {"bucket": "match"},
            }
        ],
    }

    with pytest.raises(CompilationError, match="Unsupported operand key: value"):
        YamlRulesetCompiler().compile_payload(payload)


def test_assignments_rule_alias_is_rejected():
    """
    What: Rejects the non-canonical rule key assignments.
    Why: Authoring must use assign to avoid multiple spellings for the same concept.
    Fails when: The compiler accepts legacy or convenience aliases.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_name": "Alias",
                "when": {
                    "all": [
                        {
                            "left": {"field": "name"},
                            "operator": "eq",
                            "right": {"literal": "abc"},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
                "assignments": {"bucket": "match"},
            }
        ],
    }

    with pytest.raises(CompilationError, match="Unsupported rule key: assignments"):
        YamlRulesetCompiler().compile_payload(payload)


def test_aggregate_field_name_alias_is_rejected():
    """
    What: Rejects the non-canonical aggregate key field_name.
    Why: Aggregate authoring must use field as the single accepted vocabulary.
    Fails when: Aggregate aliases are accepted or canonical key enforcement weakens.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_name": "Alias",
                "when": {
                    "all": [
                        {
                            "left": {
                                "aggregate": {
                                    "function": "sum",
                                    "field_name": "amount",
                                    "scope": "dataset",
                                    "null_input_mode": "ignore",
                                    "null_result_mode": "null",
                                }
                            },
                            "operator": "gt",
                            "right": {"literal": 0},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
                "assign": {"bucket": "match"},
            }
        ],
    }

    with pytest.raises(CompilationError, match="Unsupported aggregate key: field_name"):
        YamlRulesetCompiler().compile_payload(payload)
