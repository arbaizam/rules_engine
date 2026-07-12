from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime
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
    registry.register(
        CustomFunctionSpec(
            function_name="score",
            implementation_reference="tests.score",
            arg_names=("x", "y"),
            allowed_in_condition_flag=True,
            allowed_in_assignment_flag=False,
        ),
        implementation=lambda **kwargs: kwargs["x"] + kwargs["y"],
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


def test_spark_row_evaluator_uses_strict_string_equality():
    """
    What: Compares two strings as strings even when both contain numeric text.
    Why: Spark string equality and the Python compatibility path must agree.
    Fails when: Numeric-looking strings are silently coerced only by the UDF.
    """
    ruleset = _compile(
        {
            "left": {"field": "code"},
            "operator": "eq",
            "right": {"literal": "7"},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert _evaluate_worker(ruleset, {"code": "7.0"})["matched"] is False
    assert _evaluate_worker(ruleset, {"code": "7"})["matched"] is True


def test_spark_row_evaluator_matches_spark_nan_comparisons():
    """
    What: Treats NaN as equal to itself and greater than finite numeric values.
    Why: Spark has explicit NaN comparison semantics used by the native path.
    Fails when: The Python compatibility evaluator errors or disagrees for NaN rows.
    """
    equals_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "eq",
            "right": {"literal": float("nan")},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    greater_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "gt",
            "right": {"literal": 1.0},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    between_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "between",
            "right": {"literal": [10.0, 20.0]},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    not_between_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "not_between",
            "right": {"literal": [10.0, 20.0]},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    in_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "in",
            "right": {"literal": [float("nan")]},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )
    not_in_ruleset = _compile(
        {
            "left": {"field": "amount"},
            "operator": "not_in",
            "right": {"literal": [float("nan")]},
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }
    )

    assert _evaluate_worker(equals_ruleset, {"amount": float("nan")})["matched"] is True
    assert _evaluate_worker(greater_ruleset, {"amount": float("nan")})["matched"] is True
    assert _evaluate_worker(between_ruleset, {"amount": float("nan")})["matched"] is False
    assert _evaluate_worker(not_between_ruleset, {"amount": float("nan")})["matched"] is True
    assert _evaluate_worker(in_ruleset, {"amount": float("nan")})["matched"] is True
    assert _evaluate_worker(not_in_ruleset, {"amount": float("nan")})["matched"] is False
