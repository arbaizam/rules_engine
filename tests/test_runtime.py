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
    COMPACT_RESULT_FIELD_NAMES,
    FULL_AUDIT_ONLY_RESULT_FIELD_NAMES,
    FULL_AUDIT_RESULT_FIELD_NAMES,
    SparkRulesEngineRuntime,
    _result_struct,
    _SparkRowUdfEvaluator,
    required_source_columns,
    result_field_names,
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
    full_audit=True,
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
        full_audit=full_audit,
    )
    return evaluator(FakeSparkRow(row))


def _assigned_chain_ruleset():
    """Return a chain that exercises original-row and committed-value reads."""
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "assigned-chain",
            "ruleset_name": "Assigned chain",
            "version": "1",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "producer",
                    "rule_name": "Producer",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": [
                        {
                            "assignment_id": "produce_bucket",
                            "target_field": "bucket",
                            "value": {"literal": "A"},
                        },
                        {
                            "assignment_id": "produce_score",
                            "target_field": "score",
                            "value": {"literal": 10},
                        },
                    ],
                },
                {
                    "rule_id": "consumer",
                    "rule_name": "Consumer",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "condition_id": "assigned_bucket_is_a",
                                "left": {"assigned": "bucket"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            },
                            {
                                "condition_id": "original_bucket_unchanged",
                                "left": {"field": "bucket"},
                                "operator": "eq",
                                "right": {"literal": "ORIGINAL"},
                            },
                        ]
                    },
                    "assign": [
                        {
                            "assignment_id": "replace_score",
                            "target_field": "score",
                            "value": {"literal": 20},
                        },
                        {
                            "assignment_id": "copy_prior_score",
                            "target_field": "copied_score",
                            "value": {"assigned": "score"},
                        },
                    ],
                },
            ],
        }
    )


def test_assigned_values_are_visible_to_later_rules_and_atomic_within_a_rule():
    """Later rules see commits, while sibling assignments share one snapshot."""
    result = SparkRowEvaluator.for_embedded_ruleset(
        FunctionRegistry()
    ).evaluate_row(
        _assigned_chain_ruleset(),
        {"eligible": True, "bucket": "ORIGINAL"},
    )

    assert result == {
        "matched": True,
        "matched_rule_ids": ["producer", "consumer"],
        "assign": {
            "bucket": {"applied": True, "value": "A"},
            "score": {"applied": True, "value": 20},
            "copied_score": {"applied": True, "value": 10},
        },
    }


def test_missing_prior_commit_is_null_and_can_use_default_if_null():
    """A potential producer need not match; an absent commit resolves as null."""
    payload = {
        "ruleset_id": "assigned-default",
        "ruleset_name": "Assigned default",
        "version": "1",
        "rules": [
            {
                "rule_id": "producer",
                "rule_name": "Producer",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "left": {"field": "eligible"},
                            "operator": "eq",
                            "right": {"literal": True},
                        }
                    ]
                },
                "assign": {"bucket": "A"},
            },
            {
                "rule_id": "fallback",
                "rule_name": "Fallback",
                "rule_order": 2,
                "when": {
                    "all": [
                        {
                            "left": {
                                "assigned": "bucket",
                                "default_if_null": "MISSING",
                            },
                            "operator": "eq",
                            "right": {"literal": "MISSING"},
                        }
                    ]
                },
                "assign": {"review": True},
            },
        ],
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    result = SparkRowEvaluator.for_embedded_ruleset(
        FunctionRegistry()
    ).evaluate_row(ruleset, {"eligible": False})

    assert result["matched_rule_ids"] == ["fallback"]
    assert result["assign"] == {
        "bucket": {"applied": False, "value": None},
        "review": {"applied": True, "value": True},
    }


def test_full_audit_identifies_the_assignment_that_produced_an_operand():
    """Assigned traces explain both the consumed target and its producer."""
    result = _evaluate_worker(
        _assigned_chain_ruleset(),
        {"eligible": True, "bucket": "ORIGINAL"},
        full_audit=True,
    )

    trace = result["matched_rules"][1]["conditions"][0]["left"]

    assert trace["kind"] == "assigned"
    assert trace["target_field"] == "bucket"
    assert trace["value"] == "A"
    assert trace["produced_by_rule_id"] == "producer"
    assert trace["produced_by_assignment_id"] == "produce_bucket"


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


@pytest.mark.parametrize(
    ("full_audit", "expected_field_names"),
    [
        (False, COMPACT_RESULT_FIELD_NAMES),
        (True, FULL_AUDIT_RESULT_FIELD_NAMES),
    ],
)
def test_result_payload_keys_match_the_declared_schema(
    full_audit,
    expected_field_names,
):
    """Compact and full payloads cannot drift from their Spark result schemas."""
    evaluator = object.__new__(_SparkRowUdfEvaluator)
    base_payload_template = evaluator._base_payload(
        ["bucket"],
        full_audit=full_audit,
    )

    success = evaluator._success_payload(
        matched_rule_ids=[],
        matched_rules=[],
        assign_payload={"bucket": {"applied": False, "value": None}},
        assignment_results=[],
        base_payload_template=base_payload_template,
        full_audit=full_audit,
    )
    error = evaluator._error_payload(
        ValueError("bad"),
        include_traceback=False,
        base_payload_template=base_payload_template,
        full_audit=full_audit,
    )
    another_error = evaluator._error_payload(
        ValueError("also bad"),
        include_traceback=False,
        base_payload_template=base_payload_template,
        full_audit=full_audit,
    )

    assert expected_field_names == tuple(
        _result_struct(
            T.StructType(),
            full_audit=full_audit,
        ).fieldNames()
    )
    assert tuple(success) == expected_field_names
    assert tuple(error) == expected_field_names
    assert error["assign"] == {"bucket": {"applied": False, "value": None}}
    assert error["matched_rule_ids"] is not another_error["matched_rule_ids"]
    if full_audit:
        assert error["matched_rules"] is not another_error["matched_rules"]
        assert error["assignment_results"] is not another_error["assignment_results"]


@pytest.mark.parametrize(
    "field_name",
    (
        "error",
        "matched",
        "matched_rule_ids",
        "assign",
        "ruleset",
        "engine_version",
    ),
)
def test_dataframe_evaluation_reserves_every_output_name_in_compact_mode(
    field_name,
):
    """Compact evaluation cannot accept a name that another mode may overwrite."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    column_name = f"rules_engine_{field_name}"
    input_frame = type("InputFrame", (), {"columns": ["row_id", column_name]})()

    with pytest.raises(ValueError, match=column_name):
        _spark_runtime().evaluate_dataframe(
            input_frame,
            ruleset,
            key_columns=["row_id"],
        )


def test_compact_evaluation_reserves_full_audit_only_names():
    """Switching audit detail later cannot silently overwrite an input column."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )

    for field_name in (
        "matched_rules",
        "assignment_results",
    ):
        column_name = f"rules_engine_{field_name}"
        input_frame = type("InputFrame", (), {"columns": ["row_id", column_name]})()
        with pytest.raises(ValueError, match=column_name):
            _spark_runtime().evaluate_dataframe(
                input_frame,
                ruleset,
                key_columns=["row_id"],
            )


def test_dataframe_evaluation_rejects_an_empty_column_prefix():
    """The output namespace must be explicit and non-empty."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    input_frame = type("InputFrame", (), {"columns": ["row_id"]})()

    with pytest.raises(ValueError, match="column_prefix must be non-empty"):
        _spark_runtime().evaluate_dataframe(
            input_frame,
            ruleset,
            key_columns=["row_id"],
            column_prefix="",
        )


@pytest.mark.parametrize(
    ("key_columns", "message"),
    [
        ("row_id", "not a string"),
        ([], "at least one"),
        (["row_id", "row_id"], "duplicate"),
        (["missing"], "missing from"),
        ([1], "non-empty strings"),
    ],
)
def test_dataframe_evaluation_rejects_invalid_key_metadata(key_columns, message):
    """Key shape is validated without scanning key values or starting Spark."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    input_frame = type("InputFrame", (), {"columns": ["row_id", "account"]})()

    with pytest.raises((TypeError, ValueError), match=message):
        _spark_runtime().evaluate_dataframe(
            input_frame,
            ruleset,
            key_columns=key_columns,
        )


def test_dataframe_evaluation_rejects_an_ambiguous_key_column():
    """Duplicate source names cannot provide deterministic row identity."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    input_frame = type(
        "InputFrame",
        (),
        {"columns": ["row_id", "row_id", "account"]},
    )()

    with pytest.raises(ValueError, match="ambiguous"):
        _spark_runtime().evaluate_dataframe(
            input_frame,
            ruleset,
            key_columns=["row_id"],
        )


def test_dataframe_evaluation_rejects_assignment_to_an_immutable_key():
    """Application cannot change the columns used to correlate separate results."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        },
        assign={"row_id": "replacement"},
    )
    input_frame = type("InputFrame", (), {"columns": ["row_id", "account"]})()

    with pytest.raises(ValueError, match="cannot modify immutable key"):
        _spark_runtime().evaluate_dataframe(
            input_frame,
            ruleset,
            key_columns=["row_id"],
        )


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
                            },
                            {
                                "active_flag": False,
                                "left": {"field": "inactive_condition"},
                                "operator": "eq",
                                "right": {"literal": "ignored"},
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
        },
        assign={"bucket": "matched"},
    )

    assert required_source_columns(ruleset) == ()


def test_required_source_columns_does_not_treat_assigned_targets_as_input_fields():
    """Assigned state is internal and must not enlarge the serialized row projection."""
    ruleset = _assigned_chain_ruleset()

    assert required_source_columns(ruleset) == ("eligible", "bucket")


def test_spark_row_evaluator_returns_native_matched_rule_trace():
    """
    What: Returns assignment and matched-rule trace payloads through the Spark row UDF.
    Why: Spark output should avoid full JSON rule-results payloads.
    Fails when: Spark stringifies match traces or separates their condition detail.
    """
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A"})
    match_trace = result["matched_rules"][0]

    assert result["matched"] is True
    assert result["assign"] == {
        "bucket": {"applied": True, "value": "matched"}
    }
    assert "rule_results" not in result
    assert match_trace["rule_id"] == "r1"
    assert match_trace["rule_name"] == "Rule 1"
    assert match_trace["rule_order"] == 1
    assert match_trace["explanation"] == "account == 'A'"
    assert match_trace["conditions"][0]["condition_id"] == "cg:r1:root:c1"
    assert match_trace["conditions"][0]["condition_group_id"] == "cg:r1:root"
    assert match_trace["conditions"][0]["condition_group_operator"] == "all"
    assert match_trace["conditions"][0]["active_flag"] is True
    assert match_trace["conditions"][0]["columns"] == ["account"]
    assert match_trace["conditions"][0]["left"]["column"] == "account"
    assert match_trace["conditions"][0]["left"]["value"] == "A"


def test_full_audit_distinguishes_inactive_conditions():
    """Trace identity and activity explain why an inactive branch did not pass."""
    ruleset = _compile_when(
        {
            "condition_group_id": "eligibility",
            "any": [
                {
                    "condition_id": "inactive_branch",
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                    "active_flag": False,
                },
                {
                    "condition_id": "active_branch",
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                },
            ],
        }
    )

    conditions = _evaluate_worker(ruleset, {"account": "A"})[
        "matched_rules"
    ][0]["conditions"]

    assert [condition["condition_id"] for condition in conditions] == [
        "inactive_branch",
        "active_branch",
    ]
    assert conditions[0]["active_flag"] is False
    assert conditions[0]["comparison_result"] is None
    assert conditions[1]["active_flag"] is True
    assert conditions[1]["comparison_result"] is True
    assert all(
        condition["condition_group_id"] == "eligibility"
        and condition["condition_group_operator"] == "any"
        for condition in conditions
    )


def test_spark_row_evaluator_trace_shows_operand_default_application():
    """
    What: Shows the original null, configured fallback, and effective value.
    Why: Full audit must explain when a fallback changed a comparison operand.
    Fails when: Null substitution is invisible or reported as the source value.
    """
    ruleset = _compile(
        {
            "left": {"field": "account", "default_if_null": "UNKNOWN"},
            "operator": "eq",
            "right": {"literal": "UNKNOWN"},
        }
    )

    result = _evaluate_worker(ruleset, {"account": None})
    condition = result["matched_rules"][0]["conditions"][0]

    assert condition["tolerance_abs"] is None
    assert condition["left"]["original_value"] is None
    assert condition["left"]["value"] == "UNKNOWN"
    assert condition["left"]["default_if_null"] == "UNKNOWN"
    assert condition["left"]["default_applied"] is True
    assert condition["right"]["default_applied"] is False


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

    assert result["assign"] == {
        "bucket": {"applied": True, "value": "A"},
        "secondary_bucket": {"applied": False, "value": None},
    }


def test_spark_row_evaluator_match_trace_explanation_uses_any_joiner():
    """
    What: Uses OR when a matched root any group has multiple passed conditions.
    Why: The readable explanation should preserve the matched rule's boolean logic.
    Fails when: Passed conditions are flattened and always joined with AND.
    """
    ruleset = _compile_when(
        {
            "any": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                },
                {
                    "left": {"field": "status"},
                    "operator": "eq",
                    "right": {"literal": "open"},
                },
            ]
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A", "status": "open"})

    assert result["matched_rules"][0]["explanation"] == (
        "account == 'A' OR status == 'open'"
    )


def test_spark_row_evaluator_match_trace_explanation_drops_failing_any_branches():
    """
    What: Omits failed OR branches from the matched-rule explanation.
    Why: The explanation should describe the passed path, not every authored branch.
    Fails when: Failed sibling conditions appear in the matched-rule explanation.
    """
    ruleset = _compile_when(
        {
            "any": [
                {
                    "left": {"field": "account"},
                    "operator": "eq",
                    "right": {"literal": "A"},
                },
                {
                    "left": {"field": "status"},
                    "operator": "eq",
                    "right": {"literal": "open"},
                },
            ]
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A", "status": "closed"})

    assert result["matched_rules"][0]["explanation"] == "account == 'A'"


def test_spark_row_evaluator_match_trace_explanation_preserves_nested_groups():
    """
    What: Preserves parentheses and OR joiners for nested matched groups.
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
                },
                {
                    "any": [
                        {
                            "left": {"field": "market_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                        },
                        {
                            "left": {"field": "book_value"},
                            "operator": "eq",
                            "right": {"literal": True},
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

    assert result["matched_rules"][0]["explanation"] == (
        "record_type == 'asset' AND "
        "(market_value == true OR book_value == true)"
    )


def test_spark_row_evaluator_match_trace_explanation_drops_failing_nested_or_arm():
    """
    What: Omits a failed nested OR arm while preserving the passed nested path.
    Why: Nested explanations should stay concise without misrepresenting matched logic.
    Fails when: Failed nested conditions leak into the matched-rule explanation.
    """
    ruleset = _compile_when(
        {
            "all": [
                {
                    "left": {"field": "record_type"},
                    "operator": "eq",
                    "right": {"literal": "asset"},
                },
                {
                    "any": [
                        {
                            "left": {"field": "market_value"},
                            "operator": "eq",
                            "right": {"literal": True},
                        },
                        {
                            "left": {"field": "book_value"},
                            "operator": "eq",
                            "right": {"literal": True},
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

    assert result["matched_rules"][0]["explanation"] == (
        "record_type == 'asset' AND market_value == true"
    )


def test_spark_row_evaluator_match_trace_explanation_matches_service_formatter():
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
                },
                {
                    "left": {"field": "amount"},
                    "operator": "gt",
                    "right": {"literal": 100},
                },
            ]
        }
    )
    result = _evaluate_worker(ruleset, {"account": "A", "amount": 150})
    service_logic = HumanReadableRulesetFormatter().describe_rules(ruleset)[0]["rule_logic"]

    assert result["matched_rules"][0]["explanation"] == service_logic


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
        "leaf_key": {"applied": True, "value": "10110"},
        "non_modeled": {
            "applied": True,
            "value": {
                "market_value": True,
                "book_value": False,
            },
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
            "applied": True,
            "value": {
                "market_value": True,
                "book_value": False,
            },
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
    compact_result = _evaluate_worker(
        ruleset,
        {"account": "A", "bucket": "original", "risk": "high", "cleared": None},
        full_audit=False,
    )
    another_result = _evaluate_worker(
        ruleset,
        {"account": "A", "bucket": "original", "risk": "high", "cleared": None},
    )

    assert result["matched_rule_ids"] == ["first_match", "second_match"]
    assert result["assign"] == {
        "bucket": {"applied": True, "value": "second"},
        "risk": {"applied": True, "value": "high"},
        "cleared": {"applied": True, "value": None},
    }
    assert compact_result["matched_rule_ids"] == result["matched_rule_ids"]
    assert compact_result["assign"] == result["assign"]
    assert "assignment_results" not in compact_result
    assert [item["rule_order"] for item in result["matched_rules"]] == [1, 2]
    assert result["matched_rules"][0]["explanation"] == "account == 'A'"
    assert result["matched_rules"][1]["assignments_applied"] == [
        "bucket",
        "risk",
        "cleared",
    ]
    assert result["matched_rules"][-1]["rule_id"] == "second_match"
    assert result["matched_rules"][0] is not another_result["matched_rules"][0]
    assert (
        result["matched_rules"][0]["conditions"]
        is not another_result["matched_rules"][0]["conditions"]
    )
    assert [item["rule_id"] for item in result["matched_rules"]] == [
        "first_match",
        "second_match",
    ]
    assert all(item["conditions"] for item in result["matched_rules"])
    assert result["matched_rules"][0]["rule_id"] == "first_match"
    assert result["matched_rules"][0]["explanation"] == "account == 'A'"
    assignment_results = {
        item["assignment_id"]: item
        for item in result["assignment_results"]
    }
    assert assignment_results["first_bucket"]["effective"] is False
    assert assignment_results["first_bucket"]["old_value"] == "original"
    assert assignment_results["first_bucket"]["overridden_by_rule_id"] == "second_match"
    assert (
        assignment_results["first_bucket"]["overridden_by_assignment_id"]
        == "second_bucket"
    )
    assert assignment_results["second_bucket"]["effective"] is True
    assert assignment_results["second_bucket"]["old_value"] == "first"
    assert assignment_results["second_bucket"]["changed"] is True
    assert assignment_results["second_bucket"]["authored_expression"] == (
        "bucket = 'second'"
    )
    assert assignment_results["second_risk"]["effective"] is True
    assert assignment_results["second_risk"]["changed"] is False
    assert assignment_results["clear_value"]["proposed_value"] is None
    assert assignment_results["clear_value"]["changed"] is False


def test_spark_row_evaluator_no_match_returns_empty_audit_arrays():
    """No-match rows use empty trace and provenance arrays."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )

    result = _evaluate_worker(ruleset, {"account": "B"})

    assert result["matched"] is False
    assert result["matched_rules"] == []
    assert result["assignment_results"] == []


def test_compact_and_full_audit_payloads_have_core_result_parity():
    """Audit detail changes observability, not success, no-match, or error decisions."""
    ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "gt",
            "right": {"literal": 10},
        }
    )
    cases = {
        "success": (
            {"amount": 20},
            True,
            ["r1"],
            {"bucket": {"applied": True, "value": "matched"}},
        ),
        "no_match": (
            {"amount": 5},
            False,
            [],
            {"bucket": {"applied": False, "value": None}},
        ),
        "error": (
            {"amount": "invalid"},
            False,
            [],
            {"bucket": {"applied": False, "value": None}},
        ),
    }

    for case_name, (row, matched, matched_rule_ids, assign) in cases.items():
        compact = _evaluate_worker(ruleset, row, full_audit=False)
        full = _evaluate_worker(ruleset, row, full_audit=True)

        assert {
            field_name: compact[field_name]
            for field_name in COMPACT_RESULT_FIELD_NAMES
        } == {
            field_name: full[field_name]
            for field_name in COMPACT_RESULT_FIELD_NAMES
        }, case_name
        assert compact["matched"] is matched
        assert compact["matched_rule_ids"] == matched_rule_ids
        assert compact["assign"] == assign
        assert not any(
            field_name in compact
            for field_name in FULL_AUDIT_ONLY_RESULT_FIELD_NAMES
        )

        if case_name == "success":
            assert full["error"] is None
            assert [item["rule_id"] for item in full["matched_rules"]] == ["r1"]
            assert full["matched_rules"][0]["rule_id"] == "r1"
            assert len(full["assignment_results"]) == 1
        elif case_name == "no_match":
            assert full["error"] is None
            assert full["matched_rules"] == []
            assert full["assignment_results"] == []
        else:
            assert full["error"] is not None
            assert full["matched_rules"] == []
            assert full["assignment_results"] == []


def test_full_audit_builds_explanations_only_for_matched_rules(monkeypatch):
    """Losing rules are evaluated once but do not build discarded explanations."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    calls = 0
    original = HumanReadableRulesetFormatter.format_matched_rule_explanation

    def record_explanation(self, rule, passed_condition_ids):
        nonlocal calls
        calls += 1
        return original(self, rule, passed_condition_ids)

    monkeypatch.setattr(
        HumanReadableRulesetFormatter,
        "format_matched_rule_explanation",
        record_explanation,
    )
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, T.StructType())
    evaluator = runtime._build_row_evaluator(
        ruleset,
        [field.name for field in assign_schema.fields],
        {field.name: field.dataType for field in assign_schema.fields},
        full_audit=True,
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
        },
        assign={"target": {"literal": value, "value_type": value_type}},
    )

    result = _evaluate_worker(ruleset, {})

    assert error_text in result["error"]


def test_spark_row_evaluator_stop_on_match_excludes_later_traces_and_assignments():
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


def test_full_audit_evaluates_each_condition_once_and_emits_only_matched_traces(monkeypatch):
    """
    What: Evaluates every condition once and emits detailed traces only for matches.
    Why: Full audit must not re-invoke custom logic to construct matched-rule detail.
    Fails when: Conditions are skipped, repeated, or losing rules enter matched_rules.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "trace_efficiency",
            "ruleset_name": "Trace Efficiency",
            "version": "1",
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
                            },
                            {
                                "condition_id": "loser_second",
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "open"},
                            },
                        ]
                    },
                    "assign": {"bucket": "loser"},
                },
                {
                    "rule_id": "first_match",
                    "rule_name": "First Match",
                    "rule_order": 2,
                    "stop_on_match": True,
                    "when": {
                        "all": [
                            {
                                "condition_id": "first_match_condition",
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"bucket": "first"},
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

    assert result["matched_rules"][0]["rule_id"] == "first_match"
    assert traced_condition_ids == [
        "loser_first",
        "loser_second",
        "first_match_condition",
    ]


def test_losing_custom_condition_is_invoked_once_during_full_audit():
    """A losing custom condition is not repeated while searching for the first match."""
    calls = []

    def never_matches(**kwargs):
        calls.append(dict(kwargs))
        return False

    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="never_matches",
            implementation_reference="tests.never_matches",
            arg_names=("value",),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
            return_type_hint="boolean",
        ),
        implementation=never_matches,
    )
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "custom_call_count",
            "ruleset_name": "Custom Call Count",
            "version": "1",
            "rules": [
                {
                    "rule_id": "custom_loser",
                    "rule_name": "Custom Loser",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "never_matches",
                                        "args": {"value": {"field": "account"}},
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"bucket": "loser"},
                },
                {
                    "rule_id": "plain_match",
                    "rule_name": "Plain Match",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "account"},
                                "operator": "eq",
                                "right": {"literal": "A"},
                            }
                        ]
                    },
                    "assign": {"bucket": "matched"},
                },
            ],
        }
    )

    result = _evaluate_worker(
        ruleset,
        {"account": "A"},
        registry=registry,
        full_audit=True,
    )

    assert result["matched_rule_ids"] == ["plain_match"]
    assert calls == [{"value": "A"}]


def test_base_payload_field_construction_is_hoisted_out_of_row_evaluation(
    monkeypatch,
):
    """Stable payload field construction runs once when the worker closure is built."""
    ruleset = _compile(
        {
            "left": {"field": "account"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )
    calls = []
    original = result_field_names

    def tracked_result_field_names(*, full_audit=False):
        calls.append(full_audit)
        return original(full_audit=full_audit)

    monkeypatch.setattr(
        "rules_engine.spark_runtime.result_field_names",
        tracked_result_field_names,
    )
    runtime = _spark_runtime()
    assign_schema = runtime._assignment_schema(ruleset, T.StructType())
    evaluator = runtime._build_row_evaluator(
        ruleset,
        [field.name for field in assign_schema.fields],
        {field.name: field.dataType for field in assign_schema.fields},
        full_audit=True,
    )

    evaluator(FakeSparkRow({"account": "A"}))
    evaluator(FakeSparkRow({"account": "B"}))

    assert calls == [True]


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
                },
                {
                    "left": {"field": "amount"},
                    "operator": "gt",
                    "right": {"literal": 10},
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
        },
        {
            "condition_id": "inactive_false",
            "active_flag": False,
            "left": {"field": "inactive_source"},
            "operator": "eq",
            "right": {"literal": "ignored"},
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


def test_spark_row_evaluator_matched_rule_trace_includes_custom_function_args():
    """
    What: Emits custom-function argument summaries in a matched-rule trace.
    Why: Function-backed matched rules must remain explainable in full audit.
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
        }
    )

    result = _evaluate_worker(ruleset, {"amount": 2}, registry=registry)

    left = result["matched_rules"][0]["conditions"][0]["left"]
    assert left["kind"] == "custom_function"
    assert left["function_name"] == "score"
    assert left["source_columns"] == ["amount"]
    assert left["value"] == "5"
    assert left["arguments"] == {"x": "amount=2", "y": "3"}
    assert result["matched_rules"][0]["explanation"] == (
        "score(x=amount, y=3) == 5"
    )
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
        "rules_engine.runtime.json_dumps",
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
        }
    )

    assert _evaluate_worker(ruleset, {"name": "abcde"})["matched"] is True
    assert _evaluate_worker(ruleset, {"name": "xyz"})["matched"] is False


def test_spark_row_evaluator_default_if_null_controls_condition_result():
    """
    What: Replaces a null operand before comparison.
    Why: Operand-level fallbacks are the explicit way to make missing data match.
    Fails when: The fallback is applied after comparison or ignored.
    """
    ruleset = _compile(
        {
            "left": {"field": "missing", "default_if_null": "A"},
            "operator": "eq",
            "right": {"literal": "A"},
        }
    )

    assert _evaluate_worker(ruleset, {})["matched"] is True


def test_spark_row_evaluator_default_if_null_applies_to_custom_function_result_once():
    """A null custom-function result receives its fallback without reevaluation."""
    registry = FunctionRegistry()
    calls = []

    def missing_value():
        calls.append(True)
        return None

    registry.register(
        CustomFunctionSpec(
            function_name="missing_value",
            implementation_reference="tests.missing_value",
            arg_names=(),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
            return_type_hint="string",
        ),
        implementation=missing_value,
    )
    ruleset = _compile(
        {
            "left": {
                "custom_function": {"name": "missing_value", "args": {}},
                "default_if_null": "UNKNOWN",
            },
            "operator": "eq",
            "right": {"literal": "UNKNOWN"},
        }
    )

    result = _evaluate_worker(ruleset, {}, registry=registry)

    left_trace = result["matched_rules"][0]["conditions"][0]["left"]
    assert result["matched"] is True
    assert left_trace["original_value"] is None
    assert left_trace["value"] == "UNKNOWN"
    assert left_trace["default_applied"] is True
    assert calls == [True]


def test_spark_row_evaluator_default_if_null_applies_to_assignment_operand():
    """Assignment operands replace null before producing their typed value."""
    ruleset = _compile(
        {
            "left": {"literal": True},
            "operator": "eq",
            "right": {"literal": True},
        },
        assign={
            "bucket": {
                "literal": None,
                "default_if_null": "UNKNOWN",
            }
        },
    )

    result = _evaluate_worker(ruleset, {})

    assert result["assign"] == {
        "bucket": {"applied": True, "value": "UNKNOWN"}
    }


def test_spark_row_evaluator_error_on_null_returns_compact_error():
    """error_on_null turns an unresolved null into an explicit row error."""
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "error_on_null": True,
        }
    )

    result = _evaluate_worker(ruleset, {})

    assert result["error"].startswith("ValueError: Null operand encountered")
    assert "Traceback" not in result["error"]


def test_spark_row_evaluator_can_include_debug_traceback():
    ruleset = _compile(
        {
            "left": {"field": "missing"},
            "operator": "eq",
            "right": {"literal": "A"},
            "error_on_null": True,
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
            "error_on_null": True,
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
        }
    )
    runtime = _spark_runtime()
    evaluator = runtime._build_row_evaluator(
        ruleset,
        ["bucket"],
        {"bucket": T.StringType()},
    )

    runtime.validate_worker_serializable(evaluator)
