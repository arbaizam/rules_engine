from rules_engine import required_source_columns as public_required_source_columns
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.spark_runtime import SparkRulesEngineRuntime, required_source_columns
from pyspark.sql import types as T


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


def _evaluate_worker(ruleset, row, registry=None, assign_fields=None):
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
    )
    return evaluator(FakeSparkRow(row))


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
                    "assign": {"bucket": "first"},
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
                    "assign": {"bucket": "second", "risk": "high"},
                },
            ],
        }
    )

    result = _evaluate_worker(ruleset, {"account": "A"})

    assert result["matched_rule_ids"] == ["first_match", "second_match"]
    assert result["assign"] == {"bucket": "second", "risk": "high"}
    assert result["winning_rule_id"] == "first_match"
    assert result["winning_rule_explanation"] == "account == 'A'"


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


def test_spark_row_evaluator_stringifies_incompatible_same_target_assignments():
    """
    What: Falls back to string output for incompatible active assignments to one target.
    Why: Spark requires one declared type per assignment struct field.
    Fails when: Incompatible assignment values break UDF serialization or silently change schema.
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

    schema = _spark_runtime()._assignment_schema(ruleset, T.StructType())
    field_types = {field.name: field.dataType for field in schema.fields}
    result = _evaluate_worker(ruleset, {"account": "A"})

    assert isinstance(field_types["review_result"], T.StringType)
    assert result["matched_rule_ids"] == ["string_shape", "struct_shape"]
    assert "market_value=True" in result["assign"]["review_result"]
    assert "book_value=False" in result["assign"]["review_result"]


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
