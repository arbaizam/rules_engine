from datetime import date
from decimal import Decimal

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import ComparisonOperator, RulesetStatus
from rules_engine.exceptions import CompilationError


def test_compile_text_preserves_untyped_fractional_yaml_as_decimal():
    """Financial YAML literals must not silently become binary floats."""
    ruleset = YamlRulesetCompiler().compile_text(
        """
ruleset_id: rs1
ruleset_name: Decimal authoring
version: '1'
rules:
  - rule_name: Exact rate
    when:
      all:
        - left: {field: rate}
          operator: ge
          right: {literal: 0.0425}
          null_input_mode: propagate
          null_result_mode: 'null'
    assign:
      normalized_rate: 0.0425
      high_precision_rate: 0.123456789012345678901
"""
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assignment = ruleset.rules[0].assignments[0]

    assert condition.right.value == Decimal("0.0425")
    assert assignment.value.value == Decimal("0.0425")
    assert ruleset.rules[0].assignments[1].value.value == Decimal(
        "0.123456789012345678901"
    )


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


def test_condition_null_modes_use_documented_authoring_defaults():
    """Concise YAML materializes explicit null semantics in canonical models."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Defaults",
            "version": "1",
            "rules": [
                {
                    "rule_name": "Default modes",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "OPEN"},
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assert condition.null_input_mode.value == "propagate"
    assert condition.null_result_mode.value == "null"


@pytest.mark.parametrize("literal", [".nan", ".inf", "-.inf"])
def test_nonfinite_yaml_numbers_fail_compilation(literal):
    """NaN and infinities cannot enter comparison or persistence paths."""
    yaml_text = f"""
ruleset_id: rs1
ruleset_name: Nonfinite
version: '1'
rules:
  - rule_name: Invalid number
    when:
      all:
        - left: {{field: rate}}
          operator: eq
          right: {{literal: {literal}}}
    assign:
      bucket: invalid
"""

    with pytest.raises(CompilationError, match="must be finite"):
        YamlRulesetCompiler().compile_text(yaml_text)


@pytest.mark.parametrize(
    ("value", "value_type"),
    [
        (float("inf"), "double"),
        (float("-inf"), "float"),
        (float("nan"), "number"),
        (Decimal("Infinity"), "double"),
    ],
)
def test_nonfinite_explicit_numeric_payloads_fail_compilation(value, value_type):
    """Explicit floating hints cannot bypass the finite-number invariant."""
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Nonfinite payload",
        "version": "1",
        "rules": [
            {
                "rule_name": "Invalid number",
                "when": {
                    "all": [
                        {
                            "left": {"field": "rate"},
                            "operator": "eq",
                            "right": {
                                "literal": value,
                                "value_type": value_type,
                            },
                        }
                    ]
                },
                "assign": {"bucket": "invalid"},
            }
        ],
    }

    with pytest.raises(CompilationError, match="finite"):
        YamlRulesetCompiler().compile_payload(payload)


def test_explicit_decimal_collection_is_normalized_recursively():
    """A collection hint no longer bypasses exact Decimal normalization."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Decimal collection",
            "version": "1",
            "rules": [
                {
                    "rule_name": "Membership",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "rate"},
                                "operator": "in",
                                "right": {
                                    "literal": ["0.0425", "0.05"],
                                    "value_type": "decimal",
                                },
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    values = ruleset.rules[0].root_group.conditions[0].right.value
    assert values == [Decimal("0.0425"), Decimal("0.05")]


def test_explicit_date_literal_normalizes_quoted_iso_text():
    """A date hint turns portable quoted ISO authoring text into a Python date."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Date literal",
            "version": "1",
            "rules": [
                {
                    "rule_name": "Calendar comparison",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "as_of_date"},
                                "operator": "ge",
                                "right": {
                                    "literal": "2024-02-29",
                                    "value_type": "date",
                                },
                            }
                        ]
                    },
                    "assign": {"bucket": "current"},
                }
            ],
        }
    )

    literal = ruleset.rules[0].root_group.conditions[0].right
    assert literal.value == date(2024, 2, 29)


def test_precomputed_aggregate_field_compiles_as_row_field():
    """
    What: Compiles a rule that references an upstream aggregate column as a field.
    Why: Aggregate calculations now belong outside the rules engine runtime.
    Fails when: Field operands stop supporting precomputed aggregate facts.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_name": "Precomputed aggregate",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account_amount_sum"},
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

    condition = ruleset.rules[0].root_group.conditions[0]
    assert condition.left.field_name == "account_amount_sum"


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


def test_aggregate_operand_is_rejected():
    """
    What: Rejects aggregate operands.
    Why: Spark deployments should consume precomputed aggregate fields instead.
    Fails when: Aggregate authoring is accidentally reintroduced.
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

    with pytest.raises(CompilationError, match="Unsupported operand key: aggregate"):
        YamlRulesetCompiler().compile_payload(payload)


def test_aggregate_operand_inside_custom_function_arg_is_rejected():
    """
    What: Rejects aggregate operands nested inside custom-function args.
    Why: Aggregate authoring should not be reachable through nested operand shapes.
    Fails when: Custom-function argument compilation accepts aggregate payloads.
    """
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "status": "published",
        "rules": [
            {
                "rule_name": "Nested aggregate",
                "when": {
                    "all": [
                        {
                            "left": {
                                "custom_function": {
                                    "name": "score",
                                    "args": {
                                        "x": {
                                            "aggregate": {
                                                "function": "sum",
                                                "field": "amount",
                                                "scope": "dataset",
                                                "null_input_mode": "ignore",
                                                "null_result_mode": "null",
                                            }
                                        }
                                    },
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

    with pytest.raises(CompilationError, match="Unsupported operand key: aggregate"):
        YamlRulesetCompiler().compile_payload(payload)


def test_generated_assignment_ids_are_stable_by_rule_and_target():
    """Generated IDs do not change when assignment order changes."""
    base = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "when": {
                    "all": [
                        {
                            "left": {"literal": True},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
            }
        ],
    }
    first = {**base, "rules": [{**base["rules"][0], "assign": {"a": 1, "b": 2}}]}
    reordered = {
        **base,
        "rules": [{**base["rules"][0], "assign": {"b": 2, "a": 1}}],
    }

    first_ruleset = YamlRulesetCompiler().compile_payload(first)
    reordered_ruleset = YamlRulesetCompiler().compile_payload(reordered)

    assert {
        item.target_field: item.assignment_id
        for item in first_ruleset.rules[0].assignments
    } == {
        "a": "assignment:r1:a",
        "b": "assignment:r1:b",
    }
    assert {
        item.target_field: item.assignment_id
        for item in reordered_ruleset.rules[0].assignments
    } == {
        "a": "assignment:r1:a",
        "b": "assignment:r1:b",
    }


def test_explicit_assignment_list_uses_stable_generated_id_when_omitted():
    """List-form assignments use the same rule-and-target ID contract."""
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "when": {
                    "all": [
                        {
                            "left": {"literal": True},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        }
                    ]
                },
                "assign": [
                    {"target_field": "bucket", "value": {"literal": "A"}}
                ],
            }
        ],
    }

    assignment = YamlRulesetCompiler().compile_payload(payload).rules[0].assignments[0]

    assert assignment.assignment_id == "assignment:r1:bucket"


def test_duplicate_yaml_mapping_keys_fail_compilation():
    """Duplicate keys are rejected before the YAML parser drops a value."""
    yaml_text = """
ruleset_id: rs1
ruleset_name: Ruleset
version: '1'
rules:
  - rule_id: r1
    rule_name: Rule 1
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      RateCode: Adjustable
      RateCode: Fixed
"""

    with pytest.raises(CompilationError, match="duplicate key 'RateCode'"):
        YamlRulesetCompiler().compile_text(yaml_text)


def test_yaml_merge_allows_explicit_key_override():
    """A legal YAML merge may be overridden by an explicit mapping key."""
    yaml_text = """
assignment_defaults: &assignment_defaults
  bucket: inherited
ruleset_id: rs1
ruleset_name: Ruleset
version: '1'
rules:
  - rule_id: r1
    rule_name: Rule 1
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      <<: *assignment_defaults
      bucket: explicit
"""

    ruleset = YamlRulesetCompiler().compile_text(yaml_text)

    assert ruleset.rules[0].assignments[0].value.value == "explicit"


def test_yaml_loader_preserves_recursive_alias_construction():
    """Two-phase mapping construction supports legal recursive YAML aliases."""
    yaml_text = """
ignored_recursive_metadata: &self
  self: *self
ruleset_id: rs1
ruleset_name: Ruleset
version: '1'
rules:
  - rule_id: r1
    rule_name: Rule 1
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: explicit
"""

    ruleset = YamlRulesetCompiler().compile_text(yaml_text)

    assert ruleset.ruleset_id == "rs1"
