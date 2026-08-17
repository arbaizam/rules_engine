from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from rules_engine import required_source_columns as public_required_source_columns
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_runtime import (
    EMPTY_ASSIGN_STRUCT,
    RESULT_FIELD_NAMES,
    SparkRulesEngineRuntime,
    _result_struct,
    _SparkRowUdfEvaluator,
    required_source_columns,
)


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class FakeSparkRow:
    def __init__(self, data):
        self._data = data

    def asDict(self, recursive=True):
        return self._data


def _spark_runtime(registry=None):
    return SparkRulesEngineRuntime(DummyRepository(), registry or FunctionRegistry())


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


def _compile_when(when, assign=None):
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
                    "when": when,
                    "assign": assign or {"bucket": "matched"},
                }
            ],
        }
    )


def _evaluate_worker(
    ruleset,
    row,
    registry=None,
    assign_fields=None,
    *,
    raise_on_error=False,
    include_error_traceback=False,
):
    runtime = _spark_runtime(registry)
    assign_schema = runtime._assignment_schema(ruleset, T.StructType())
    inferred_types = {field.name: field.dataType for field in assign_schema.fields}
    assign_field_names = assign_fields or [field.name for field in assign_schema.fields]
    assign_field_types = {
        field_name: inferred_types.get(field_name, T.StringType())
        for field_name in assign_field_names
    }
    evaluator = runtime._build_row_evaluator(
        ruleset,
        assign_field_names,
        assign_field_types,
        raise_on_error=raise_on_error,
        include_error_traceback=include_error_traceback,
    )
    return evaluator(FakeSparkRow(row))


@pytest.mark.parametrize(
    ("operator", "expected"),
    [("in", True), ("not_in", False)],
)
def test_numeric_membership_uses_decimal_equality(operator, expected):
    """Worker floats match exact Decimal collection literals consistently."""
    ruleset = YamlRulesetCompiler().compile_text(
        f"""
ruleset_id: rs1
ruleset_name: Membership
version: '1'
rules:
  - rule_id: r1
    rule_name: Numeric membership
    when:
      all:
        - left: {{field: rate}}
          operator: {operator}
          right: {{literal: [0.0425, 0.05]}}
    assign:
      bucket: matched
"""
    )

    result = _evaluate_worker(ruleset, {"rate": 0.0425})

    assert result["error"] is None
    assert result["matched"] is expected


def test_string_membership_semantics_are_unchanged():
    """Membership still applies exact equality to ordinary strings."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "in",
            "right": {"literal": ["OPEN", "CURRENT"]},
        }
    )

    assert _evaluate_worker(ruleset, {"status": "OPEN"})["matched"] is True
    assert _evaluate_worker(ruleset, {"status": "open"})["matched"] is False


def test_scalar_string_membership_fails_with_contains_guidance():
    """IN rejects scalar strings instead of silently applying character matching."""
    ruleset = _compile(
        {
            "left": {"field": "flag"},
            "operator": "in",
            "right": {"field": "flags_string"},
        }
    )

    result = _evaluate_worker(
        ruleset,
        {"flag": "AB", "flags_string": "ABC"},
    )

    assert result["matched"] is False
    assert "Use contains/not_contains" in result["error"]


def test_result_payload_keys_are_derived_from_the_declared_schema():
    """Success and error payloads cannot drift from the Spark result schema."""
    evaluator = object.__new__(_SparkRowUdfEvaluator)

    success = evaluator._success_payload(
        matched_rule_ids=[],
        matched_rules=[],
        assign_payload=None,
        first_matched_rule=None,
        first_matched_rule_explanation=None,
        assignment_results=[],
    )
    error = evaluator._error_payload(ValueError("bad"), include_traceback=False)

    assert RESULT_FIELD_NAMES == tuple(_result_struct(EMPTY_ASSIGN_STRUCT).fieldNames())
    assert tuple(success) == RESULT_FIELD_NAMES
    assert tuple(error) == RESULT_FIELD_NAMES


def test_assignment_provenance_uses_stable_event_positions():
    """Last-assignment precedence does not depend on dictionary identity."""
    evaluator = object.__new__(_SparkRowUdfEvaluator)
    events = [
        {
            "assignment_id": "a1",
            "rule_id": "r1",
            "rule_name": "Rule 1",
            "rule_order": 1,
            "target_field": "bucket",
            "authored_expression": "bucket = 'first'",
            "old_value": "old",
            "proposed_value": "first",
        },
        {
            "assignment_id": "a2",
            "rule_id": "r2",
            "rule_name": "Rule 2",
            "rule_order": 2,
            "target_field": "bucket",
            "authored_expression": "bucket = 'last'",
            "old_value": "old",
            "proposed_value": "last",
        },
    ]

    results = evaluator._assignment_results([dict(event) for event in events])

    assert results[0]["effective"] is False
    assert results[0]["overridden_by_assignment_id"] == "a2"
    assert results[1]["effective"] is True


def test_set_trace_text_is_deterministic():
    """Unordered values produce stable audit text across worker processes."""
    evaluator = object.__new__(_SparkRowUdfEvaluator)

    assert evaluator._trace_text({"beta", "alpha"}) == "[alpha, beta]"


def test_human_readable_values_sort_sets_and_use_iso_temporal_text():
    """Authored audit expressions are deterministic across Python workers."""
    ruleset = _compile(
        {
            "left": {"field": "status"},
            "operator": "in",
            "right": {"literal": {"beta", "alpha"}},
        },
        assign={
            "tags": {"beta", "alpha"},
            "processed_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        },
    )

    description = HumanReadableRulesetFormatter().describe_rule(ruleset.rules[0])

    assert description["rule_logic"] == "status in {'alpha', 'beta'}"
    assert description["match_payload"] == (
        "tags = {'alpha', 'beta'}, processed_at = 2026-01-02T03:04:05+00:00"
    )


@pytest.mark.parametrize(
    ("operator", "right", "expected"),
    [
        ("gt", date(2024, 1, 31), True),
        ("ge", date(2024, 2, 29), True),
        ("lt", date(2024, 3, 1), True),
        ("le", date(2024, 2, 29), True),
        ("gt", date(2024, 3, 1), False),
    ],
)
def test_spark_row_evaluator_orders_date_operands(operator, right, expected):
    ruleset = _compile(
        {
            "left": {"field": "as_of_date"},
            "operator": operator,
            "right": {"literal": right},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(ruleset, {"as_of_date": date(2024, 2, 29)})

    assert result["error"] is None
    assert result["matched"] is expected


@pytest.mark.parametrize("operator", ["between", "not_between"])
def test_spark_row_evaluator_orders_date_ranges(operator):
    ruleset = _compile(
        {
            "left": {"field": "as_of_date"},
            "operator": operator,
            "right": {
                "literal": [date(2024, 2, 1), date(2024, 2, 29)]
            },
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(ruleset, {"as_of_date": date(2024, 2, 15)})

    assert result["error"] is None
    assert result["matched"] is (operator == "between")


def test_spark_row_evaluator_orders_timezone_aware_timestamps():
    ruleset = _compile(
        {
            "left": {"field": "received_at"},
            "operator": "gt",
            "right": {
                "literal": datetime(2024, 2, 29, 12, 0, tzinfo=timezone.utc)
            },
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(
        ruleset,
        {"received_at": datetime(2024, 2, 29, 13, 0, tzinfo=timezone.utc)},
    )

    assert result["error"] is None
    assert result["matched"] is True


@pytest.mark.parametrize(
    ("left", "right", "error_text"),
    [
        (
            date(2024, 2, 29),
            datetime(2024, 2, 29, 0, 0, tzinfo=timezone.utc),
            "require two dates",
        ),
        (
            datetime(2024, 2, 29, 0, 0),  # noqa: DTZ001 - exercises naive input
            datetime(2024, 2, 29, 0, 0, tzinfo=timezone.utc),
            "timezone-aware and naive",
        ),
    ],
)
def test_spark_row_evaluator_rejects_ambiguous_temporal_comparisons(
    left,
    right,
    error_text,
):
    ruleset = _compile(
        {
            "left": {"field": "temporal_value"},
            "operator": "ge",
            "right": {"literal": right},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(ruleset, {"temporal_value": left})

    assert error_text in result["error"]


def test_spark_row_evaluator_rejects_numeric_tolerance_for_dates():
    ruleset = _compile(
        {
            "left": {"field": "as_of_date"},
            "operator": "ge",
            "right": {"literal": date(2024, 2, 29)},
            "tolerance_abs": "1",
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(ruleset, {"as_of_date": date(2024, 2, 29)})

    assert "require tolerance_abs=0" in result["error"]


def test_required_source_columns_returns_only_active_runtime_dependencies():
    """
    What: Reports active condition, custom-function, and assignment source fields.
    Why: Spark should serialize only input values the row evaluator can resolve.
    Fails when: Inactive, duplicate, nested, or assignment dependencies are mishandled.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "dependencies",
            "ruleset_name": "Dependencies",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "later",
                    "rule_name": "Later",
                    "rule_order": 20,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "later_field"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"copy": {"field": "later_assignment"}},
                },
                {
                    "rule_id": "first",
                    "rule_name": "First",
                    "rule_order": 10,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "score",
                                        "args": {
                                            "value": {"field": "risk.score"},
                                            "duplicate": {"field": "account"},
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": 1},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "active_flag": False,
                                "left": {"field": "inactive_condition"},
                                "operator": "eq",
                                "right": {"literal": "ignored"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                        ]
                    },
                    "assign": {"source_copy": {"field": "assignment_source"}},
                },
                {
                    "rule_id": "inactive",
                    "rule_name": "Inactive",
                    "rule_order": 1,
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "inactive_rule"},
                                "operator": "eq",
                                "right": {"literal": "ignored"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"hidden": {"field": "inactive_assignment"}},
                },
            ],
        }
    )

    assert required_source_columns(ruleset) == (
        "account",
        "risk.score",
        "assignment_source",
        "later_field",
        "later_assignment",
    )
    assert public_required_source_columns is required_source_columns


def test_required_source_columns_can_return_no_dependencies():
    """
    What: Returns an empty tuple for literal-only rules and assignments.
    Why: Literal rules must evaluate without serializing an unrelated source value.
    Fails when: Dependency discovery invents a field or requires a nonempty projection.
    """
    ruleset = _compile(
        {
            "left": {"literal": "A"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"bucket": "matched"},
    )

    assert required_source_columns(ruleset) == ()


def test_spark_row_evaluator_returns_native_winning_rule_trace():
    """
    What: Returns assignment and winning-rule trace payloads through the Spark row UDF.
    Why: Spark output should avoid full JSON rule-results payloads.
    Fails when: Spark reintroduces all-rule trace output or stringifies winning traces.
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

    result = _evaluate_worker(ruleset, {"account": "A"})
    winning_rule = result["winning_rule"]

    assert result["matched"] is True
    assert result["assign"] == {"bucket": "matched"}
    assert "rule_results" not in result
    assert result["winning_rule_id"] == "r1"
    assert result["winning_rule_name"] == "Rule 1"
    assert result["winning_rule_explanation"] == "account == 'A'"
    assert winning_rule["rule_id"] == "r1"
    assert winning_rule["matched"] is True
    assert winning_rule["conditions"][0]["columns"] == ["account"]
    assert winning_rule["conditions"][0]["left"]["column"] == "account"
    assert winning_rule["conditions"][0]["left"]["value"] == "A"


def test_spark_row_evaluator_winning_rule_trace_keeps_default_options_null():
    """
    What: Leaves default condition options null in the winning-rule Spark trace.
    Why: The Spark trace struct intentionally omits default-valued metadata.
    Fails when: Trace simplification starts emitting default values as strings.
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

    result = _evaluate_worker(ruleset, {"account": "A"})
    condition = result["winning_rule"]["conditions"][0]

    assert condition["tolerance_abs"] is None
    assert condition["null_input_mode"] is None
    assert condition["null_result_mode"] is None
    assert condition["null_default_value"] is None


def test_spark_row_evaluator_assignment_struct_includes_unassigned_fields_as_null():
    """
    What: Returns all assignment struct fields with nulls for fields not assigned.
    Why: Spark output uses a stable ruleset-derived struct schema.
    Fails when: Assignment output becomes a sparse map or drops nullable fields.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "a_rule",
                    "rule_name": "A Rule",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "A"},
                },
                {
                    "rule_id": "b_rule",
                    "rule_name": "B Rule",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "B"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"secondary_bucket": "B"},
                },
            ],
        }
    )

    result = _evaluate_worker(
        ruleset,
        {"account": "A"},
        assign_fields=["bucket", "secondary_bucket"],
    )

    assert result["assign"] == {"bucket": "A", "secondary_bucket": None}


def test_spark_row_evaluator_winning_rule_explanation_uses_any_joiner():
    """
    What: Uses OR when a winning root any group has multiple passed conditions.
    Why: The readable explanation should preserve the winning rule's boolean logic.
    Fails when: Passed conditions are flattened and always joined with AND.
    """
    ruleset = _compile_when(
        {
            "any": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "left": {"field": "status"},
                    "operator": "eq",
                    "right": {"literal": "open"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
            ]
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A", "status": "open"})

    assert result["winning_rule_explanation"] == "account == 'A' OR status == 'open'"


def test_spark_row_evaluator_winning_rule_explanation_drops_failing_any_branches():
    """
    What: Omits failed OR branches from the winning-rule explanation.
    Why: The explanation should describe the passed path, not every authored branch.
    Fails when: Failed sibling conditions appear in the winning-rule explanation.
    """
    ruleset = _compile_when(
        {
            "any": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "left": {"field": "status"},
                    "operator": "eq",
                    "right": {"literal": "open"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
            ]
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A", "status": "closed"})

    assert result["winning_rule_explanation"] == "account == 'A'"


def test_spark_row_evaluator_winning_rule_explanation_preserves_nested_groups():
    """
    What: Preserves parentheses and OR joiners for nested winning groups.
    Why: Explanations should not misstate nested boolean rule logic.
    Fails when: Nested group conditions are flattened into a single AND list.
    """
    ruleset = _compile_when(
        {
            "all": [
                {
                    "left": {"field": "record_type"},
                    "operator": "eq",
                    "right": {"literal": "asset"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "any": [
                        {
                            "left": {"field": "market_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                        {
                            "left": {"field": "book_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                    ]
                },
            ]
        }
    )

    result = _evaluate_worker(
        ruleset,
        {
            "record_type": "asset",
            "market_value": True,
            "book_value": True,
        },
    )

    assert result["winning_rule_explanation"] == (
        "record_type == 'asset' AND "
        "(market_value == true OR book_value == true)"
    )


def test_spark_row_evaluator_winning_rule_explanation_drops_failing_nested_or_arm():
    """
    What: Omits a failed nested OR arm while preserving the passed nested path.
    Why: Nested explanations should stay concise without misrepresenting the winning logic.
    Fails when: Failed nested conditions leak into the winning-rule explanation.
    """
    ruleset = _compile_when(
        {
            "all": [
                {
                    "left": {"field": "record_type"},
                    "operator": "eq",
                    "right": {"literal": "asset"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "any": [
                        {
                            "left": {"field": "market_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                        {
                            "left": {"field": "book_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                            "null_input_mode": "propagate",
                            "null_result_mode": "null",
                        },
                    ]
                },
            ]
        }
    )

    result = _evaluate_worker(
        ruleset,
        {
            "record_type": "asset",
            "market_value": True,
            "book_value": False,
        },
    )

    assert result["winning_rule_explanation"] == (
        "record_type == 'asset' AND market_value == true"
    )


def test_spark_row_evaluator_winning_rule_explanation_matches_service_formatter():
    """
    What: Uses the same author-facing syntax as the service helper when all branches pass.
    Why: Runtime explanations and service rule descriptions should not diverge.
    Fails when: Runtime falls back to trace-value formatting.
    """
    ruleset = _compile_when(
        {
            "all": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "left": {"field": "amount"},
                    "operator": "gt",
                    "right": {"literal": 100},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
            ]
        }
    )
    result = _evaluate_worker(ruleset, {"account": "A", "amount": 150})
    service_logic = HumanReadableRulesetFormatter().describe_rules(ruleset)[0]["rule_logic"]

    assert result["winning_rule_explanation"] == service_logic


def test_spark_row_evaluator_preserves_mapping_literal_assignment_as_struct():
    """
    What: Preserves a mapping literal assignment as a nested struct payload.
    Why: Business outputs such as non_modeled flags should be selectable as Spark struct fields.
    Fails when: Mapping literals are inferred as strings and formatted as trace text.
    """
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={
            "leaf_key": "10110",
            "non_modeled": {
                "literal": {
                    "market_value": True,
                    "book_value": False,
                }
            },
        },
    )

    schema = _spark_runtime()._assignment_schema(ruleset, T.StructType())
    field_types = {field.name: field.dataType for field in schema.fields}
    non_modeled_type = field_types["non_modeled"]
    result = _evaluate_worker(
        ruleset,
        {"account": "A"},
        assign_fields=["leaf_key", "non_modeled"],
    )

    assert isinstance(non_modeled_type, T.StructType)
    assert {field.name: field.dataType for field in non_modeled_type.fields} == {
        "market_value": T.BooleanType(),
        "book_value": T.BooleanType(),
    }
    assert result["assign"] == {
        "leaf_key": "10110",
        "non_modeled": {
            "market_value": True,
            "book_value": False,
        },
    }


def test_spark_assignment_schema_ignores_inactive_rules():
    """
    What: Infers assignment schema from active rules only.
    Why: Inactive lifecycle rules must not alter live Spark output types or fields.
    Fails when: Inactive assignments force string fallback or add null-only fields.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "active_struct",
                    "rule_name": "Active Struct",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {
                        "non_modeled": {
                            "literal": {
                                "market_value": True,
                                "book_value": False,
                            }
                        }
                    },
                },
                {
                    "rule_id": "inactive_conflict",
                    "rule_name": "Inactive Conflict",
                    "rule_order": 2,
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {
                        "non_modeled": "retired string shape",
                        "inactive_only": "retired only",
                    },
                },
            ],
        }
    )

    schema = _spark_runtime()._assignment_schema(ruleset, T.StructType())
    field_types = {field.name: field.dataType for field in schema.fields}
    result = _evaluate_worker(ruleset, {"account": "A"})

    assert isinstance(field_types["non_modeled"], T.StructType)
    assert "inactive_only" not in field_types
    assert result["assign"] == {
        "non_modeled": {
            "market_value": True,
            "book_value": False,
        },
    }


def test_spark_row_evaluator_merges_assignments_when_stop_on_match_false():
    """
    What: Merges assignments from multiple matching rules when evaluation continues.
    Why: stop_on_match=false should keep all matched IDs and use last-writer-wins assignment values.
    Fails when: Later matches are skipped, matched IDs are incomplete, or first assignments win.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "first_match",
                    "rule_name": "First Match",
                    "rule_order": 1,
                    "stop_on_match": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": [
                        {
                            "assignment_id": "first_bucket",
                            "target_field": "bucket",
                            "value": {"literal": "first"},
                        }
                    ],
                },
                {
                    "rule_id": "second_match",
                    "rule_name": "Second Match",
                    "rule_order": 2,
                    "stop_on_match": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": [
                        {
                            "assignment_id": "second_bucket",
                            "target_field": "bucket",
                            "value": {"literal": "second"},
                        },
                        {
                            "assignment_id": "second_risk",
                            "target_field": "risk",
                            "value": {"literal": "high"},
                        },
                        {
                            "assignment_id": "clear_value",
                            "target_field": "cleared",
                            "value": {"literal": None, "value_type": "string"},
                        },
                    ],
                },
            ],
        }
    )

    result = _evaluate_worker(
        ruleset,
        {"account": "A", "bucket": "original", "risk": "high", "cleared": None},
    )

    assert result["matched_rule_ids"] == ["first_match", "second_match"]
    assert result["assign"] == {
        "bucket": "second",
        "risk": "high",
        "cleared": None,
    }
    assert [item["rule_order"] for item in result["matched_rules"]] == [1, 2]
    assert result["matched_rules"][0]["human_readable_condition"] == "account == 'A'"
    assert result["matched_rules"][1]["assigned_fields"] == [
        "bucket",
        "risk",
        "cleared",
    ]
    assert result["last_matched_rule"]["rule_id"] == "second_match"
    assert result["first_matched_rule"] == result["winning_rule"]
    assert result["first_matched_rule_id"] == result["winning_rule_id"]
    assert result["first_matched_rule_name"] == result["winning_rule_name"]
    assert (
        result["first_matched_rule_explanation"]
        == result["winning_rule_explanation"]
    )
    assert result["winning_rule_id"] == "first_match"
    assert result["winning_rule_explanation"] == "account == 'A'"
    assignment_results = {
        item["assignment_id"]: item
        for item in result["assignment_results"]
    }
    assert assignment_results["first_bucket"]["effective"] is False
    assert assignment_results["first_bucket"]["overridden_by_rule_id"] == "second_match"
    assert (
        assignment_results["first_bucket"]["overridden_by_assignment_id"]
        == "second_bucket"
    )
    assert assignment_results["second_bucket"]["effective"] is True
    assert assignment_results["second_bucket"]["authored_expression"] == (
        "bucket = 'second'"
    )
    assert assignment_results["second_risk"]["effective"] is True
    assert assignment_results["second_risk"]["changed"] is False
    assert assignment_results["clear_value"]["proposed_value"] is None
    assert assignment_results["clear_value"]["changed"] is False


def test_spark_row_evaluator_no_match_returns_empty_audit_arrays():
    """No-match rows use empty summary/provenance arrays and null rule structs."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    result = _evaluate_worker(ruleset, {"account": "B"})

    assert result["matched"] is False
    assert result["matched_rules"] == []
    assert result["last_matched_rule"] is None
    assert result["assignment_results"] == []
    assert result["first_matched_rule"] is None
    assert result["winning_rule"] is None


def test_rule_summaries_are_precomputed_once_per_row_evaluator(monkeypatch):
    """Static human-readable rule descriptions are not rebuilt per row."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    calls = 0
    original = HumanReadableRulesetFormatter.describe_rule

    def record_description(self, rule):
        nonlocal calls
        calls += 1
        return original(self, rule)

    monkeypatch.setattr(
        HumanReadableRulesetFormatter,
        "describe_rule",
        record_description,
    )
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, T.StructType())
    evaluator = runtime._build_row_evaluator(
        ruleset,
        [field.name for field in assign_schema.fields],
        {field.name: field.dataType for field in assign_schema.fields},
    )

    evaluator(FakeSparkRow({"account": "A"}))
    evaluator(FakeSparkRow({"account": "B"}))

    assert calls == 1


@pytest.mark.parametrize(
    ("value", "value_type", "error_text"),
    [
        (3.7, "integer", "fractional component"),
        ("no", "boolean", "not a boolean"),
        (Decimal("0.1234567890123456789"), "decimal", "without rounding"),
    ],
)
def test_spark_row_evaluator_rejects_lossy_assignment_coercion(
    value,
    value_type,
    error_text,
):
    """Typed assignment values fail instead of being silently truncated."""
    ruleset = _compile(
        {
            "left": {"literal": True},
            "operator": "eq",
            "right": {"literal": True},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"target": {"literal": value, "value_type": value_type}},
    )

    result = _evaluate_worker(ruleset, {})

    assert error_text in result["error"]


def test_spark_row_evaluator_stop_on_match_excludes_later_summaries_and_assignments():
    """stop_on_match prevents later rules from appearing in either audit array."""
    payload = {
        "ruleset_id": "rs1",
        "ruleset_name": "Ruleset",
        "version": "1",
        "rules": [],
    }
    condition = {
        "left": {"field": "account"},
        "operator": "eq",
        "right": {"literal": "A"},
        "null_input_mode": "propagate",
        "null_result_mode": "null",
    }
    for order, stop in ((1, True), (2, False)):
        payload["rules"].append(
            {
                "rule_id": f"r{order}",
                "rule_name": f"Rule {order}",
                "rule_order": order,
                "stop_on_match": stop,
                "when": {"all": [condition]},
                "assign": {f"field_{order}": order},
            }
        )
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    result = _evaluate_worker(ruleset, {"account": "A"})

    assert result["matched_rule_ids"] == ["r1"]
    assert [item["rule_id"] for item in result["matched_rules"]] == ["r1"]
    assert [item["rule_id"] for item in result["assignment_results"]] == ["r1"]


def test_spark_row_evaluator_builds_condition_traces_only_for_winner(monkeypatch):
    """
    What: Builds condition trace objects only for the first matching rule.
    Why: Losing-rule trace allocation is discarded output and a major row-level cost.
    Fails when: Match-only evaluation regresses to tracing every tested rule.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "trace_efficiency",
            "ruleset_name": "Trace Efficiency",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "loser",
                    "rule_name": "Loser",
                    "rule_order": 1,
                    "stop_on_match": True,
                    "when": {
                        "all": [
                            {
                                "condition_id": "loser_first",
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "B"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "condition_id": "loser_second",
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "open"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                        ]
                    },
                    "assign": {"bucket": "loser"},
                },
                {
                    "rule_id": "winner",
                    "rule_name": "Winner",
                    "rule_order": 2,
                    "stop_on_match": True,
                    "when": {
                        "all": [
                            {
                                "condition_id": "winner_condition",
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"bucket": "winner"},
                },
            ],
        }
    )
    traced_condition_ids = []
    original = SparkRowEvaluator._condition_trace

    def record_trace(self, **kwargs):
        traced_condition_ids.append(kwargs["condition"].condition_id)
        return original(self, **kwargs)

    monkeypatch.setattr(SparkRowEvaluator, "_condition_trace", record_trace)

    result = _evaluate_worker(ruleset, {"account": "A", "status": "open"})

    assert result["winning_rule_id"] == "winner"
    assert traced_condition_ids == ["winner_condition"]


def test_match_only_losing_rule_preserves_later_condition_errors():
    """
    What: Evaluates every condition in a losing group when a later one errors.
    Why: Optimization must not hide row errors that the traced evaluator surfaced.
    Fails when: Match-only evaluation short-circuits after the first false condition.
    """
    ruleset = _compile_when(
        {
            "all": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "B"},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
                {
                    "left": {"field": "amount"},
                    "operator": "gt",
                    "right": {"literal": 10},
                    "null_input_mode": "propagate",
                    "null_result_mode": "null",
                },
            ]
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A", "amount": "invalid"})

    assert result["matched"] is False
    assert "decimal" in result["error"].lower()


def test_match_only_and_traced_paths_agree_on_inactive_condition_groups():
    """
    What: Pins inactive conditions as false in both ALL and ANY groups.
    Why: Match-only optimization must preserve the traced evaluator's semantics.
    Fails when: Inactive conditions are skipped or the two evaluation paths diverge.
    """
    condition_items = [
        {
            "condition_id": "active_true",
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        {
            "condition_id": "inactive_false",
            "active_flag": False,
            "left": {"field": "inactive_source"},
            "operator": "eq",
            "right": {"literal": "ignored"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
    ]
    evaluator = SparkRowEvaluator(DummyRepository(), FunctionRegistry())
    row = {"account": "A", "inactive_source": "ignored"}

    for operator, expected in (("all", False), ("any", True)):
        rule = _compile_when({operator: condition_items}).rules[0]

        assert evaluator._rule_matches(rule, row) is expected
        assert evaluator._evaluate_rule(rule, row)[0] is expected


def test_spark_assignment_schema_rejects_incompatible_same_target_assignments():
    """
    What: Rejects incompatible active assignments to one target.
    Why: Spark type conflicts must fail validation instead of falling back to strings.
    Fails when: Incompatible values silently become a StringType field.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "status": "published",
            "rules": [
                {
                    "rule_id": "string_shape",
                    "rule_name": "String Shape",
                    "rule_order": 1,
                    "stop_on_match": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {"review_result": "manual"},
                },
                {
                    "rule_id": "struct_shape",
                    "rule_name": "Struct Shape",
                    "rule_order": 2,
                    "stop_on_match": False,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {
                        "review_result": {
                            "literal": {
                                "market_value": True,
                                "book_value": False,
                            }
                        }
                    },
                },
            ],
        }
    )

    with pytest.raises(ValueError, match="SPARK_ASSIGNMENT_TYPE_CONFLICT"):
        _spark_runtime()._assignment_schema(ruleset, T.StructType())


def test_spark_row_evaluator_winning_rule_trace_includes_precomputed_aggregate_field():
    """
    What: Emits precomputed aggregate columns like ordinary field operands.
    Why: Aggregate calculations now live upstream while winning-rule trace remains useful.
    Fails when: Precomputed field values disappear from the winning-rule trace.
    """
    ruleset = _compile(
        {
            "left": {"field": "account_amount_sum"},
            "operator": "gt",
            "right": {"literal": 15},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    result = _evaluate_worker(ruleset, {"account": "A", "account_amount_sum": 30})

    left = result["winning_rule"]["conditions"][0]["left"]
    assert left["kind"] == "field"
    assert left["column"] == "account_amount_sum"
    assert left["source_columns"] == ["account_amount_sum"]
    assert left["value"] == "30"
    assert result["winning_rule_explanation"] == "account_amount_sum > 15"


def test_spark_row_evaluator_winning_rule_trace_includes_custom_function_args():
    """
    What: Emits custom-function argument summaries in the winning-rule trace.
    Why: Function-backed winning rules must remain explainable after dropping all-rule traces.
    Fails when: Custom function source columns or resolved argument values disappear.
    """
    registry = FunctionRegistry()
    calls = []

    def score(**kwargs):
        calls.append(dict(kwargs))
        return kwargs["x"] + kwargs["y"]

    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="tests.score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        implementation=score,
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

    result = _evaluate_worker(ruleset, {"amount": 2}, registry=registry)

    left = result["winning_rule"]["conditions"][0]["left"]
    assert left["kind"] == "custom_function"
    assert left["function_name"] == "score"
    assert left["source_columns"] == ["amount"]
    assert left["value"] == "5"
    assert left["arguments"] == {"x": "amount=2", "y": "3"}
    assert result["winning_rule_explanation"] == "score(x=amount, y=3) == 5"
    assert calls == [{"x": 2, "y": 3}]


def test_trace_value_returns_common_scalars_without_json_serialization(monkeypatch):
    """
    What: Returns primitive trace values without invoking the JSON encoder.
    Why: Scalar operands dominate row evaluation and are already Spark-safe values.
    Fails when: Common trace values regain repeated JSON serialization overhead.
    """
    evaluator = SparkRowEvaluator(DummyRepository(), FunctionRegistry())

    def unexpected_json_serialization(value):
        raise AssertionError(f"Unexpected JSON serialization for {value!r}")

    monkeypatch.setattr(
        "rules_engine.runtime.json.dumps",
        unexpected_json_serialization,
    )

    assert evaluator._trace_value(None) is None
    assert evaluator._trace_value("A") == "A"
    assert evaluator._trace_value(10) == 10
    assert evaluator._trace_value(1.5) == 1.5
    assert evaluator._trace_value(True) is True


def test_spark_row_evaluator_like_uses_sql_wildcard_semantics():
    """
    What: Evaluates SQL LIKE percent wildcard behavior in the Spark row evaluator.
    Why: UDF row semantics must match Spark LIKE wildcard behavior for supported patterns.
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

    assert _evaluate_worker(ruleset, {"name": "abcde"})["matched"] is True
    assert _evaluate_worker(ruleset, {"name": "xyz"})["matched"] is False


def test_spark_row_evaluator_null_result_default_controls_condition_result():
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

    assert _evaluate_worker(ruleset, {})["matched"] is True


def test_spark_row_evaluator_rejects_non_boolean_null_default_at_runtime():
    """Direct callers cannot bypass the validator into Python truthiness."""
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
            "null_default_value": "false",
        }
    )

    result = _evaluate_worker(ruleset, {})

    assert result["error"].startswith("TypeError: null_default_value must be")
    assert "Traceback" not in result["error"]


def test_spark_row_evaluator_can_include_debug_traceback():
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
            "null_default_value": "false",
        }
    )

    result = _evaluate_worker(ruleset, {}, include_error_traceback=True)

    assert "Traceback" in result["error"]


def test_spark_row_evaluator_can_raise_during_materializing_action():
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "default",
            "null_default_value": "false",
        }
    )

    with pytest.raises(RuntimeError, match="Rules engine row evaluation failed"):
        _evaluate_worker(ruleset, {}, raise_on_error=True)


def test_assignment_changed_compares_spark_normalized_values():
    """Date audit changes reflect the stored value, not Python input classes."""
    ruleset = _compile(
        {
            "left": {"literal": True},
            "operator": "eq",
            "right": {"literal": True},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        },
        assign={"target": date(2025, 1, 15)},
    )

    result = _evaluate_worker(
        ruleset,
        {"target": datetime(2025, 1, 15, 23, 59, tzinfo=timezone.utc)},
    )

    assert result["assignment_results"][0]["changed"] is False


class _UnserializableFunction:
    def __call__(self, **kwargs):
        return kwargs["value"]

    def __getstate__(self):
        raise TypeError("test callable cannot be pickled")


def test_spark_runtime_preflights_custom_function_serialization():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="unserializable",
            implementation_reference="tests.unserializable",
            arg_names=("value",),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        _UnserializableFunction(),
    )
    ruleset = _compile(
        {
            "left": {
                "custom_function": {
                    "name": "unserializable",
                    "args": {"value": {"field": "value"}},
                }
            },
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    runtime = _spark_runtime(registry)
    evaluator = runtime._build_row_evaluator(
        ruleset,
        ["bucket"],
        {"bucket": T.StringType()},
    )

    with pytest.raises(ValidationFailedError, match="Spark-worker-serializable"):
        runtime.validate_worker_serializable(evaluator)


def test_spark_runtime_accepts_serializable_worker_evaluator():
    ruleset = _compile(
        {
            "left": {"field": "value"},
            "operator": "eq",
            "right": {"literal": "A"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    runtime = _spark_runtime()
    evaluator = runtime._build_row_evaluator(
        ruleset,
        ["bucket"],
        {"bucket": T.StringType()},
    )

    runtime.validate_worker_serializable(evaluator)
