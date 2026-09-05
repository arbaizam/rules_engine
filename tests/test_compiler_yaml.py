from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import ComparisonOperator
from rules_engine.exceptions import CompilationError
from rules_engine.models import AssignedOperand, FieldOperand


def _minimal_payload():
    """Return one complete ruleset payload for compiler shape tests."""
    return {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "rules": [
            {
                "rule_id": "r1",
                "rule_name": "Rule 1",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "condition_id": "c1",
                            "left": {"field": "account"},
                            "operator": "eq",
                            "right": {"literal": "A"},
                        }
                    ]
                },
                "assign": {"bucket": "A"},
            }
        ],
    }


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
    assign:
      normalized_rate: 0.0425
      high_precision_rate: 0.123456789012345678901
"""
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assignment = ruleset.rules[0].assignments[0]

    assert condition.right.value == Decimal("0.0425")
    assert assignment.value.value == Decimal("0.0425")
    assert ruleset.rules[0].assignments[1].value.value == Decimal("0.123456789012345678901")


def test_explicit_binary_float_yaml_tag_preserves_kind_without_changing_type_hints():
    """Canonical float tags preserve kind while ordinary fractions remain Decimal."""
    ruleset = YamlRulesetCompiler().compile_text(
        """ruleset_id: tagged
ruleset_name: Tagged numbers
version: '1'
rules:
- rule_name: Types
  when: {all: [{left: {literal: true}, operator: eq, right: {literal: true}}]}
  assign:
    binary: !rules_engine/float '1.5'
    decimal: 1.5
    typed_decimal: {literal: !rules_engine/float '1.5', value_type: decimal}
    typed_double: {literal: !rules_engine/float '1.5', value_type: double}
"""
    )
    operands = {assignment.target_field: assignment.value for assignment in ruleset.rules[0].assignments}
    assert type(operands["binary"].value) is float
    assert operands["binary"].value_type is None
    assert type(operands["decimal"].value) is Decimal
    assert operands["decimal"].value_type is None
    assert operands["typed_decimal"].value == Decimal("1.5")
    assert type(operands["typed_decimal"].value) is Decimal
    assert operands["typed_decimal"].value_type == "decimal"
    assert type(operands["typed_double"].value) is float
    assert operands["typed_double"].value_type == "double"

    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {"literal": 1.5}
    ordinary_value = YamlRulesetCompiler().compile_payload(payload).rules[0].root_group.conditions[0].right
    assert type(ordinary_value.value) is Decimal
    assert ordinary_value.value_type is None


@pytest.mark.parametrize(
    ("tagged_value", "message"),
    [
        ("NaN", "finite"),
        ("inf", "finite"),
        ("-inf", "finite"),
        ("1e309", "finite"),
        ("not-a-number", "numeric"),
        ("[1.5]", "expected a scalar node"),
        ("{value: 1.5}", "expected a scalar node"),
    ],
)
def test_explicit_binary_float_yaml_tag_rejects_invalid_values(tagged_value, message):
    """The safe float tag cannot bypass the finite literal contract."""
    text = f"""ruleset_id: tagged
ruleset_name: Tagged numbers
version: '1'
rules:
- rule_name: Invalid number
  when: {{all: [{{left: {{literal: true}}, operator: eq, right: {{literal: true}}}}]}}
  assign: {{result: !rules_engine/float {tagged_value}}}
"""
    with pytest.raises(CompilationError, match=message):
        YamlRulesetCompiler().compile_text(text)


def test_valid_simple_row_rule_compiles_with_owner_and_default_metadata():
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
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assert ruleset.owner == "Rules Team"
    assert ruleset.owner_department == "ALM Engineering"
    assert condition.operator is ComparisonOperator.EQ
    assert str(condition.tolerance_abs) == "0"


def test_condition_uses_no_match_as_the_default_null_semantics():
    """Concise YAML needs no null configuration for the common behavior."""
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
    assert condition.error_on_null is False
    assert condition.left.default_if_null is None
    assert condition.right.default_if_null is None


def test_operand_default_if_null_supports_scalar_and_typed_literals():
    """Each operand can replace null before comparison with an explicit literal."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Defaults",
            "version": "1",
            "rules": [
                {
                    "rule_name": "Operand defaults",
                    "when": {
                        "all": [
                            {
                                "left": {"field": "amount", "default_if_null": 0},
                                "operator": "gt",
                                "right": {
                                    "field": "floor",
                                    "default_if_null": {
                                        "literal": "1.25",
                                        "value_type": "decimal",
                                    },
                                },
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                }
            ],
        }
    )

    condition = ruleset.rules[0].root_group.conditions[0]
    assert condition.left.default_if_null.value == 0
    assert condition.right.default_if_null.value == Decimal("1.25")
    assert condition.right.default_if_null.value_type == "decimal"


def test_assigned_operand_compiles_as_a_custom_function_argument():
    """Custom functions may explicitly consume a prior committed value."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Assigned values",
            "version": "1",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Producer",
                    "rule_order": 1,
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
                },
                {
                    "rule_id": "r2",
                    "rule_name": "Consumer",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "identity",
                                        "args": {
                                            "value": {
                                                "assigned": "bucket",
                                                "default_if_null": "UNKNOWN",
                                            }
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"review": True},
                },
            ],
        }
    )

    operand = ruleset.rules[1].root_group.conditions[0].left.args["value"]

    assert isinstance(operand, AssignedOperand)
    assert operand.target_field == "bucket"
    assert operand.default_if_null.value == "UNKNOWN"


def test_custom_function_arguments_compile_operands_inside_collections():
    payload = _minimal_payload()
    payload["rules"][0]["assign"] = {
        "selected": {
            "custom_function": {
                "name": "coalesce",
                "args": {
                    "values": [
                        {"field": "primary"},
                        {"field": "secondary"},
                    ]
                },
            }
        }
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)
    values = ruleset.rules[0].assignments[0].value.args["values"]

    assert [type(value) for value in values] == [FieldOperand, FieldOperand]
    assert [value.field_name for value in values] == ["primary", "secondary"]


def test_custom_function_mapping_escape_preserves_data_keys_and_nested_operands():
    payload = _minimal_payload()
    payload["rules"][0]["assign"] = {
        "result": {
            "custom_function": {
                "name": "lookup",
                "args": {
                    "mapping": {
                        "$rules_engine_mapping": {
                            "field": "legacy_name",
                            "dynamic": {"field": "source"},
                            "$rules_engine_mapping": "ordinary data",
                        }
                    }
                },
            }
        }
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)

    assert ruleset.rules[0].assignments[0].value.args["mapping"] == {
        "field": "legacy_name",
        "dynamic": FieldOperand("source"),
        "$rules_engine_mapping": "ordinary data",
    }


@pytest.mark.parametrize(
    ("escaped", "message"),
    [
        ({"$rules_engine_mapping": None}, "must be a mapping"),
        ({"$rules_engine_mapping": []}, "must be a mapping"),
        ({"$rules_engine_mapping": {}, "field": "x"}, "unsupported keys"),
        ({"$rules_engine_mapping": {}, "other": True}, "unsupported keys"),
    ],
)
def test_custom_function_mapping_escape_rejects_malformed_shapes(escaped, message):
    payload = _minimal_payload()
    payload["rules"][0]["assign"] = {
        "result": {"custom_function": {"name": "lookup", "args": {"mapping": escaped}}}
    }

    with pytest.raises(CompilationError, match=message):
        YamlRulesetCompiler().compile_payload(payload)


def test_assigned_operand_compiles_with_a_null_fallback():
    """YAML can explicitly read the latest value from an earlier matched rule."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Assigned values",
            "version": "1",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Producer",
                    "rule_order": 1,
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
                },
                {
                    "rule_id": "r2",
                    "rule_name": "Consumer",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "assigned": "bucket",
                                    "default_if_null": "UNKNOWN",
                                },
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"review": True},
                },
            ],
        }
    )

    operand = ruleset.rules[1].root_group.conditions[0].left

    assert isinstance(operand, AssignedOperand)
    assert operand.target_field == "bucket"
    assert operand.default_if_null.value == "UNKNOWN"


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


@pytest.mark.parametrize(
    ("authored", "expected"),
    [
        (["0.0425", "0.05"], [Decimal("0.0425"), Decimal("0.05")]),
        (
            {"nested": [("0.0425", {"0.05"})]},
            {"nested": [(Decimal("0.0425"), {Decimal("0.05")})]},
        ),
    ],
)
def test_explicit_decimal_collection_is_normalized_recursively(authored, expected):
    """A collection hint applies exact Decimal normalization recursively."""
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
                                    "literal": authored,
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
    assert values == expected

    def assert_decimal_leaves(value):
        if isinstance(value, dict):
            for item in value.values():
                assert_decimal_leaves(item)
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                assert_decimal_leaves(item)
        else:
            assert type(value) is Decimal

    assert_decimal_leaves(values)


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


@pytest.mark.parametrize(
    ("value_type", "authored", "expected"),
    [
        (
            "timestamp",
            "2024-02-29T05:00:00+05:00",
            datetime(2024, 2, 29, tzinfo=timezone.utc),
        ),
        (
            "timestamp_ntz",
            "2024-02-29T05:00:00",
            datetime(2024, 2, 29, 5, 0),  # noqa: DTZ001 - expected NTZ value.
        ),
    ],
)
def test_explicit_timestamp_literals_normalize_to_datetime(value_type, authored, expected):
    """Timestamp hints compile strings into the runtime representation they declare."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": value_type,
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)

    value = ruleset.rules[0].root_group.conditions[0].right.value
    assert value == expected
    assert value.tzinfo is (timezone.utc if value_type == "timestamp" else None)
    assert value.replace(tzinfo=None) == expected.replace(tzinfo=None)


@pytest.mark.parametrize(
    ("value_type", "authored"),
    [
        ("timestamp", "2024-02-29T05:00:00"),
        ("timestamp_ntz", "2024-02-29T05:00:00Z"),
    ],
)
def test_explicit_timestamp_literals_enforce_offset_representation(value_type, authored):
    """Timestamp and timestamp_ntz hints reject the opposite offset representation."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": value_type,
    }

    with pytest.raises(CompilationError, match=f"{value_type} literal"):
        YamlRulesetCompiler().compile_payload(payload)


@pytest.mark.parametrize("value_type", ["boolean", "bool"])
def test_boolean_literal_hint_requires_an_actual_boolean(value_type):
    """Quoted boolean-looking text fails compilation instead of creating a dead rule."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": "true",
        "value_type": value_type,
    }

    with pytest.raises(CompilationError, match="actual boolean"):
        YamlRulesetCompiler().compile_payload(payload)


@pytest.mark.parametrize(
    ("value_type", "authored", "expected"),
    [
        ("string", "A", "A"),
        ("str", "A", "A"),
        ("integer", 1, 1),
        ("int", 1.0, 1),
        ("long", Decimal(1), 1),
        ("number", 1, 1.0),
        ("float", Decimal("1.25"), 1.25),
        ("double", 1.25, 1.25),
    ],
)
def test_known_scalar_literal_hints_normalize_to_their_runtime_type(
    value_type,
    authored,
    expected,
):
    """Known scalar hints must produce values compatible with their declared type."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": value_type,
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)

    literal = ruleset.rules[0].root_group.conditions[0].right
    assert literal.value == expected
    assert type(literal.value) is type(expected)


@pytest.mark.parametrize(
    ("value_type", "authored", "error_text"),
    [
        ("string", 1, "must be a string"),
        ("str", True, "must be a string"),
        ("integer", "1", "must be numeric"),
        ("int", True, "must be numeric"),
        ("long", 1.5, "fractional component"),
        ("integer", 2**63, "signed 64-bit"),
        ("double", "1.5", "must be numeric"),
        ("float", True, "must be numeric"),
    ],
)
def test_known_scalar_literal_hints_reject_incompatible_values(
    value_type,
    authored,
    error_text,
):
    """Known hints must not let incompatible Python values reach schema validation."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": value_type,
    }

    with pytest.raises(CompilationError, match=error_text):
        YamlRulesetCompiler().compile_payload(payload)


@pytest.mark.parametrize(
    "authored",
    [
        [1, "2"],
        [1, {"nested": (2, ["3"])}],
        {"nested": ({1, "2"},)},
    ],
)
def test_known_scalar_literal_hints_validate_collection_items_recursively(authored):
    """One invalid typed collection item must fail compilation at the source."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": "integer",
    }

    with pytest.raises(CompilationError, match="must be numeric"):
        YamlRulesetCompiler().compile_payload(payload)


def test_unknown_literal_hint_is_preserved_as_extension_metadata():
    """Unknown hints remain metadata until schema compatibility needs them."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": "550e8400-e29b-41d4-a716-446655440000",
        "value_type": "uuid",
    }

    ruleset = YamlRulesetCompiler().compile_payload(payload)

    literal = ruleset.rules[0].root_group.conditions[0].right
    assert literal.value == "550e8400-e29b-41d4-a716-446655440000"
    assert literal.value_type == "uuid"


@pytest.mark.parametrize("authored", ["20240229", "2024-W09-4"])
def test_explicit_date_literal_rejects_noncanonical_iso_forms(authored):
    """Date hints accept only the documented YYYY-MM-DD spelling."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": authored,
        "value_type": "date",
    }

    with pytest.raises(CompilationError, match="ISO YYYY-MM-DD"):
        YamlRulesetCompiler().compile_payload(payload)


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
        "rules": [
            {
                "rule_name": "Strings",
                "when": {
                    "all": [
                        {
                            "left": {"field": "name"},
                            "operator": operator,
                            "right": {"literal": "abc"},
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


@pytest.mark.parametrize(
    "location",
    (
        "ruleset",
        "rule",
        "group",
        "condition",
        "assignment",
        "function",
        "field_operand",
        "assigned_operand",
        "literal_operand",
        "custom_function_operand",
    ),
)
def test_unknown_mapping_keys_are_rejected_at_every_contract_level(location):
    """Every structured authoring mapping has an explicit closed key set."""
    payload = _minimal_payload()
    if location == "ruleset":
        target = payload
    elif location == "rule":
        target = payload["rules"][0]
    elif location == "group":
        target = payload["rules"][0]["when"]
    elif location == "condition":
        target = payload["rules"][0]["when"]["all"][0]
    elif location == "assignment":
        payload["rules"][0]["assign"] = [
            {
                "assignment_id": "a1",
                "target_field": "bucket",
                "value": {"literal": "A"},
            }
        ]
        target = payload["rules"][0]["assign"][0]
    elif location in {"field_operand", "assigned_operand", "literal_operand"}:
        kind = location.removesuffix("_operand")
        target = {kind: "account"}
        payload["rules"][0]["when"]["all"][0]["left"] = target
    else:
        function = {
            "name": "identity",
            "args": {"value": {"field": "account"}},
        }
        operand = {"custom_function": function}
        payload["rules"][0]["when"]["all"][0]["left"] = operand
        target = operand if location == "custom_function_operand" else function
    target["unexpected"] = True

    with pytest.raises(CompilationError, match="unsupported keys"):
        YamlRulesetCompiler().compile_payload(payload)


def test_mixed_type_unsupported_keys_raise_compilation_error():
    """Malformed YAML keys must not escape as a Python sorting error."""
    payload = _minimal_payload()
    payload[1] = "unsupported"
    payload["unexpected"] = "also unsupported"

    with pytest.raises(CompilationError, match="unsupported keys") as raised:
        YamlRulesetCompiler().compile_payload(payload)
    assert "'unexpected'" in str(raised.value)
    assert "1" in str(raised.value)


def test_required_authoring_text_rejects_whitespace_only_values():
    """Whitespace is not a usable persisted identifier, name, or field."""
    paths = (
        ("ruleset_id",),
        ("ruleset_name",),
        ("rules", 0, "rule_id"),
        ("rules", 0, "rule_name"),
        ("rules", 0, "when", "condition_group_id"),
        ("rules", 0, "when", "all", 0, "condition_id"),
        ("rules", 0, "when", "all", 0, "left", "field"),
    )
    for path in paths:
        payload = _minimal_payload()
        target = payload
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = "   "

        with pytest.raises(CompilationError, match="non-empty string"):
            YamlRulesetCompiler().compile_payload(payload)

    payload = _minimal_payload()
    payload["rules"][0]["assign"] = {"   ": "A"}
    with pytest.raises(CompilationError, match="non-empty strings"):
        YamlRulesetCompiler().compile_payload(payload)


def test_literal_mapping_keys_cannot_collide_after_string_normalization():
    """Persistence must not silently merge distinct authored mapping keys."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {"literal": {1: "integer", "1": "string"}}

    with pytest.raises(CompilationError, match="both normalize to '1'"):
        YamlRulesetCompiler().compile_payload(payload)


def test_nested_function_argument_mapping_keys_cannot_collapse():
    """Nested custom-function configuration uses the same canonical key rule."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["left"] = {
        "custom_function": {
            "name": "identity",
            "args": {"config": {"nested": {1: "integer", "1": "string"}}},
        }
    }

    with pytest.raises(CompilationError, match="both normalize to '1'"):
        YamlRulesetCompiler().compile_payload(payload)


def test_root_wrapper_is_not_part_of_the_ruleset_contract():
    """The compiler accepts one document shape: the ruleset mapping itself."""
    with pytest.raises(CompilationError, match="unsupported keys"):
        YamlRulesetCompiler().compile_payload({"ruleset": _minimal_payload()})


def test_unknown_operand_kind_is_rejected_generically():
    """Operand parsing is defined only by the four current operand kinds."""
    payload = _minimal_payload()
    payload["rules"][0]["when"]["all"][0]["right"] = {"unexpected": "A"}

    with pytest.raises(CompilationError, match="exactly one operand kind"):
        YamlRulesetCompiler().compile_payload(payload)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        ("rule_order", "1", "rule_order must be an integer"),
        ("active_flag", "false", "active_flag must be a boolean"),
        ("stop_on_match", 1, "stop_on_match must be a boolean"),
    ),
)
def test_rule_scalar_types_are_not_coerced(field_name, value, message):
    """Rule metadata must use its declared YAML scalar types."""
    payload = _minimal_payload()
    payload["rules"][0][field_name] = value

    with pytest.raises(CompilationError, match=message):
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
        item.target_field: item.assignment_id for item in first_ruleset.rules[0].assignments
    } == {
        "a": "assignment:r1:a",
        "b": "assignment:r1:b",
    }
    assert {
        item.target_field: item.assignment_id for item in reordered_ruleset.rules[0].assignments
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
                        }
                    ]
                },
                "assign": [{"target_field": "bucket", "value": {"literal": "A"}}],
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
    assign:
      RateCode: Adjustable
      RateCode: Fixed
"""

    with pytest.raises(CompilationError, match="duplicate key 'RateCode'"):
        YamlRulesetCompiler().compile_text(yaml_text)


def test_yaml_merge_allows_explicit_key_override():
    """A legal YAML merge may be overridden by an explicit mapping key."""
    yaml_text = """
ruleset_id: rs1
ruleset_name: Ruleset
version: '1'
rules:
  - rule_id: defaults
    rule_name: Defaults
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
    assign: &assignment_defaults
      bucket: inherited
  - rule_id: r1
    rule_name: Rule 1
    when:
      all:
        - left: {literal: true}
          operator: eq
          right: {literal: true}
    assign:
      <<: *assignment_defaults
      bucket: explicit
"""

    ruleset = YamlRulesetCompiler().compile_text(yaml_text)

    assert ruleset.rules[1].assignments[0].value.value == "explicit"
