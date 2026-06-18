from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime


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


def _evaluate_worker(ruleset, row, registry=None, assign_fields=None):
    assign_field_names = assign_fields or ["bucket"]
    evaluator = _spark_runtime(registry)._build_row_evaluator(
        ruleset,
        assign_field_names,
        set(assign_field_names),
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
    assert result["winning_rule_explanation"] == "account=A == A"
    assert winning_rule["rule_id"] == "r1"
    assert winning_rule["matched"] is True
    assert winning_rule["conditions"][0]["columns"] == ["account"]
    assert winning_rule["conditions"][0]["left"]["column"] == "account"
    assert winning_rule["conditions"][0]["left"]["value"] == "A"


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
    assert result["winning_rule_explanation"] == "account_amount_sum=30 > 15"


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
    assert result["winning_rule_explanation"] == "score(x=amount=2, y=3)=5 == 5"


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
