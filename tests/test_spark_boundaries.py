"""Worker-boundary regressions runnable without a JVM or Spark session."""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T

import rules_engine.spark_validator as spark_validator_module
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime, _result_struct
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.validator import RulesetValidator


def _rules(assign, condition=None):
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "boundary",
            "ruleset_name": "Boundary checks",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Rule 1",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            condition
                            or {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": assign,
                }
            ],
        }
    )


def _worker(rules, schema, *, registry=None, full_audit=False, raise_on_error=False):
    registry = registry or FunctionRegistry()
    runtime = SparkRulesEngineRuntime(None, registry)
    prepared = SparkRulesetCompatibilityValidator(registry).prepare(rules, schema)
    assert prepared.validation.passed, prepared.validation.to_text()
    assignment_types = {field.name: field.dataType for field in prepared.assignment_schema.fields}
    evaluator = runtime._build_row_evaluator(
        rules,
        list(assignment_types),
        assignment_types,
        source_schema=schema,
        full_audit=full_audit,
        raise_on_error=raise_on_error,
    )
    runtime.validate_worker_serializable(evaluator)
    return evaluator, _result_struct(prepared.assignment_schema, full_audit=full_audit)


@pytest.mark.parametrize("operator", ["eq", "ge", "in", "between"])
def test_timestamp_field_compares_with_offset_literal_after_real_spark_decode(operator):
    """Spark's naive local datetime retains its instant when compared with UTC literals."""
    instant = datetime(2026, 2, 1, tzinfo=timezone.utc)
    right = "2026-02-01T01:00:00+01:00"
    if operator == "in":
        right = [right]
    elif operator == "between":
        right = [right, "2026-02-02T00:00:00+00:00"]
    rules = _rules(
        {"copied": {"field": "event_at"}},
        {
            "left": {"field": "event_at"},
            "operator": operator,
            "right": {"literal": right, "value_type": "timestamp"},
        },
    )
    schema = T.StructType([T.StructField("event_at", T.TimestampType())])
    worker_value = T.TimestampType().fromInternal(T.TimestampType().toInternal(instant))
    assert worker_value.tzinfo is None
    evaluate, result_schema = _worker(rules, schema, full_audit=True)

    result = evaluate(Row(event_at=worker_value))

    assert result["error"] is None
    assert result["matched"] is True
    assert result["assign"]["copied"]["value"] == instant
    result_schema.toInternal(result)


def test_timestamp_normalization_recurses_and_preserves_ntz():
    """Nested structs, arrays and maps keep instants separate from wall-clock times."""
    instant = datetime(2026, 2, 1, tzinfo=timezone.utc)
    decoded = T.TimestampType().fromInternal(T.TimestampType().toInternal(instant))
    wall_clock = datetime(2026, 2, 1, 12, 30)  # noqa: DTZ001 - deliberate NTZ value.
    nested_type = T.StructType(
        [
            T.StructField("instants", T.ArrayType(T.TimestampType())),
            T.StructField("by_name", T.MapType(T.StringType(), T.TimestampType())),
            T.StructField("wall_clock", T.TimestampNTZType()),
        ]
    )
    schema = T.StructType([T.StructField("nested", nested_type)])
    rules = _rules({"copied": {"field": "nested"}})
    evaluate, result_schema = _worker(rules, schema)

    result = evaluate(
        Row(nested=Row(instants=[decoded], by_name={"a": decoded}, wall_clock=wall_clock))
    )

    assert result["error"] is None
    copied = result["assign"]["copied"]["value"]
    assert copied == {"instants": [instant], "by_name": {"a": instant}, "wall_clock": wall_clock}
    assert copied["wall_clock"].tzinfo is None
    result_schema.toInternal(result)


@pytest.mark.parametrize("original", [float("nan"), float("inf"), float("-inf")])
def test_full_audit_does_not_revalidate_original_nonfinite_value(original):
    rules = _rules({"target": 1})
    schema = T.StructType([T.StructField("target", T.DoubleType())])
    compact, _ = _worker(rules, schema)
    audited, result_schema = _worker(rules, schema, full_audit=True)

    compact_result = compact(Row(target=original))
    result = audited(Row(target=original))

    assert result["error"] is None
    assert {key: result[key] for key in compact_result} == compact_result
    assert result["assignment_results"][0]["changed"] is True
    result_schema.toInternal(result)


@pytest.mark.parametrize(
    ("data_type", "bad_value"),
    [
        (T.TimestampType(), 123),
        (T.TimestampType(), datetime(2026, 1, 1)),  # noqa: DTZ001 - invalid instant.
        (T.TimestampType(), "2026-01-01T00:00:00"),
        (T.DateType(), 123),
        (T.TimestampNTZType(), datetime(2026, 1, 1, tzinfo=timezone.utc)),
        (T.ArrayType(T.LongType()), "123"),
        (T.ArrayType(T.LongType(), False), [None]),
        (T.StructType([T.StructField("score", T.LongType(), False)]), {"score": None}),
        (T.StructType([T.StructField("score", T.LongType())]), {"typo": 1}),
        (T.MapType(T.StringType(), T.TimestampType()), {"a": 123}),
        (T.MapType(T.StringType(), T.LongType()), {None: 1}),
        (T.MapType(T.StringType(), T.LongType(), False), {"a": None}),
        (T.BinaryType(), "text"),
        (T.FloatType(), 1e40),
        (T.DayTimeIntervalType(), timedelta.max),
    ],
)
def test_bad_custom_returns_are_captured_before_spark_output_conversion(data_type, bad_value):
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec("bad", "tests.bad", (), True, True, return_type_hint="any"),
        lambda: bad_value,
    )
    rules = _rules({"target": {"custom_function": {"name": "bad", "args": {}}}})
    schema = T.StructType([T.StructField("target", data_type)])
    evaluate, result_schema = _worker(rules, schema, registry=registry)

    result = evaluate(Row(target=None))

    assert result["error"] is not None
    assert result["matched"] is False
    assert result["assign"]["target"] == {"applied": False, "value": None}
    # The same conversion used by PySpark's worker must remain safe in error-row mode.
    result_schema.toInternal(result)
    strict, _ = _worker(rules, schema, registry=registry, raise_on_error=True)
    with pytest.raises(RuntimeError, match="Rules engine row evaluation failed"):
        strict(Row(target=None))


def test_prepared_spark_schema_validates_and_resolves_assignment_types_once(monkeypatch):
    rules = _rules({"target": {"field": "source"}})
    schema = T.StructType([T.StructField("source", T.LongType())])
    validator = SparkRulesetCompatibilityValidator()
    calls = {"base": 0, "types": 0}
    original_validate = RulesetValidator.validate
    original_resolve = validator._resolve_assignment_types

    def validate(self, ruleset):
        calls["base"] += 1
        return original_validate(self, ruleset)

    def resolve(*args):
        calls["types"] += 1
        return original_resolve(*args)

    monkeypatch.setattr(RulesetValidator, "validate", validate)
    monkeypatch.setattr(validator, "_resolve_assignment_types", resolve)

    prepared = validator.prepare(rules, schema)

    assert prepared.validation.passed
    assert calls == {"base": 1, "types": 1}
    assert prepared.required_source_columns == ("source",)
    assert prepared.assignment_schema["target"].dataType == T.LongType()


def test_naive_literal_infers_ntz_and_rejects_timestamp_field_comparison():
    wall_clock = datetime(2026, 1, 1, 12, 30)  # noqa: DTZ001 - deliberate NTZ value.
    rules = _rules(
        {"target": {"literal": wall_clock}},
        {
            "left": {"field": "event_at"},
            "operator": "eq",
            "right": {"literal": wall_clock},
        },
    )
    schema = T.StructType([T.StructField("event_at", T.TimestampNTZType())])
    evaluate, result_schema = _worker(rules, schema)

    result = evaluate(Row(event_at=wall_clock))

    assert result["error"] is None
    assert result["matched"] is True
    assert result["assign"]["target"]["value"] == wall_clock
    result_schema.toInternal(result)
    instant_schema = T.StructType([T.StructField("event_at", T.TimestampType())])
    validation = SparkRulesetCompatibilityValidator().validate(rules, instant_schema)
    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" in {issue.check_name for issue in validation.issues}


def test_naive_literal_without_ntz_support_has_actionable_preflight_issue(monkeypatch):
    rules = _rules({"target": datetime(2026, 1, 1)})  # noqa: DTZ001 - deliberate NTZ.
    monkeypatch.setattr(spark_validator_module, "TIMESTAMP_NTZ_TYPE", None)

    validation = SparkRulesetCompatibilityValidator().validate(rules, T.StructType())

    issue = next(
        issue
        for issue in validation.issues
        if issue.check_name == "SPARK_TIMESTAMP_NTZ_UNAVAILABLE"
    )
    assert "value_type='timestamp_ntz'" in issue.message
    assert "UTC offset" in issue.message


def test_struct_map_keys_stay_hashable_after_recursive_timestamp_normalization():
    instant = datetime(2026, 2, 1, tzinfo=timezone.utc)
    decoded = T.TimestampType().fromInternal(T.TimestampType().toInternal(instant))
    key_type = T.StructType(
        [
            T.StructField("code", T.StringType()),
            T.StructField("at", T.TimestampType()),
            T.StructField("nested", T.StructType([T.StructField("label", T.StringType())])),
        ]
    )
    schema = T.StructType([T.StructField("source", T.MapType(key_type, T.LongType()))])
    rules = _rules({"copied": {"field": "source"}})
    evaluate, result_schema = _worker(rules, schema)

    result = evaluate(Row(source={Row(code="A", at=decoded, nested=Row(label="n")): 5}))

    assert result["error"] is None
    mapping = result["assign"]["copied"]["value"]
    assert mapping == {Row(code="A", at=instant, nested=Row(label="n")): 5}
    assert next(iter(mapping)).code == "A"
    result_schema.toInternal(result)


@pytest.mark.parametrize(
    ("key_type", "key"),
    [
        (T.ArrayType(T.LongType()), (1, 2)),
        (T.BinaryType(), b"abc"),
        (
            T.StructType(
                [
                    T.StructField("values", T.ArrayType(T.LongType())),
                    T.StructField("token", T.BinaryType()),
                ]
            ),
            Row(values=(1, 2), token=b"abc"),
        ),
    ],
)
def test_composite_map_output_keys_remain_hashable_and_spark_convertible(key_type, key):
    rules = _rules({"copied": {"field": "source"}})
    schema = T.StructType([T.StructField("source", T.MapType(key_type, T.LongType()))])
    evaluate, result_schema = _worker(rules, schema)

    result = evaluate(Row(source={key: 5}))

    assert result["error"] is None
    assert result["assign"]["copied"]["value"] == {key: 5}
    result_schema.toInternal(result)


@pytest.mark.parametrize("option", ["fail_on_error", "include_error_traceback", "full_audit"])
@pytest.mark.parametrize("invalid", ["false", 0, None])
def test_public_boolean_options_reject_truthy_non_booleans_before_plan_access(option, invalid):
    runtime = SparkRulesEngineRuntime(None, FunctionRegistry())
    with pytest.raises(TypeError, match=f"{option} must be a bool"):
        runtime.evaluate_attached_dataframe(None, _rules({"target": 1}), **{option: invalid})


def test_assignment_audit_captures_values_before_later_custom_code_mutates_them():
    """Audit evidence is fixed at assignment time, including the changed decision."""
    original = bytearray(b"A")
    proposed = bytearray(b"A")

    def mutate_later():
        original.extend(b"X")
        proposed.extend(b"Y")
        return 1

    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec("seed", "tests.seed", (), True, True, return_type_hint="any"),
        lambda: proposed,
    )
    registry.register(
        CustomFunctionSpec("mutate", "tests.mutate", (), True, True, return_type_hint="integer"),
        mutate_later,
    )
    condition = {"left": {"literal": True}, "operator": "eq", "right": {"literal": True}}
    rules = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "audit",
            "ruleset_name": "Audit snapshots",
            "version": "1",
            "owner": "Engineering",
            "owner_department": "Technology",
            "rules": [
                {
                    "rule_name": "Seed",
                    "when": {"all": [condition]},
                    "assign": {"target": {"custom_function": {"name": "seed", "args": {}}}},
                },
                {
                    "rule_name": "Mutate",
                    "when": {"all": [condition]},
                    "assign": {"count": {"custom_function": {"name": "mutate", "args": {}}}},
                },
            ],
        }
    )
    schema = T.StructType([T.StructField("target", T.BinaryType())])
    evaluate, result_schema = _worker(rules, schema, registry=registry, full_audit=True)

    result = evaluate(Row(target=original))

    assert result["error"] is None
    assert original == bytearray(b"AX")
    assert proposed == bytearray(b"AY")
    event = result["assignment_results"][0]
    assert event["old_value"] == "bytearray(b'A')"
    assert event["proposed_value"] == "bytearray(b'A')"
    assert event["changed"] is False
    result_schema.toInternal(result)
