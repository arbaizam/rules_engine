from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.standard_functions import register_standard_functions


def test_spark_validator_allows_error_on_null_for_udf_row_path():
    """
    What: Allows condition-level error_on_null for ordinary row UDF checks.
    Why: Non-filter row conditions are evaluated inside the Spark Python row runtime.
    Fails when: Spark compatibility validation blocks a supported row-level ruleset.
    """
    ruleset = YamlRulesetCompiler().compile_payload(
        {
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
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "OPEN"},
                                "error_on_null": True,
                            }
                        ]
                    },
                    "assign": {"bucket": "matched"},
                }
            ],
        }
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset)

    assert result.passed
    assert not result.has_errors()


def _payload(assign, *, condition_field="status"):
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
                "when": {
                    "all": [
                        {
                            "left": {"field": condition_field},
                            "operator": "eq",
                            "right": {"literal": "OPEN"},
                        }
                    ]
                },
                "assign": assign,
            }
        ],
    }


def _checks(result):
    return {issue.check_name for issue in result.issues}


def test_spark_validator_rejects_incompatible_operand_default():
    """A numeric fallback cannot be applied to a Spark string operand."""
    payload = _payload({"bucket": "A"})
    payload["rules"][0]["when"]["all"][0]["left"]["default_if_null"] = 0
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_DEFAULT_IF_NULL_TYPE_INCOMPATIBLE" in _checks(result)


def test_spark_validator_allows_numeric_operand_default():
    """An integral zero fallback is compatible with a Spark double operand."""
    payload = _payload({"bucket": "A"}, condition_field="amount")
    condition = payload["rules"][0]["when"]["all"][0]
    condition["left"]["default_if_null"] = 0
    condition["right"] = {"literal": 10.0, "value_type": "double"}
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("amount", T.DoubleType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_DEFAULT_IF_NULL_TYPE_INCOMPATIBLE" not in _checks(result)


def test_spark_validator_rejects_missing_condition_field():
    ruleset = YamlRulesetCompiler().compile_payload(_payload({"bucket": "A"}))

    result = SparkRulesetCompatibilityValidator().validate(
        ruleset,
        T.StructType(),
    )

    assert "SPARK_CONDITION_FIELD_MISSING" in _checks(result)


def test_spark_validator_rejects_missing_assignment_source_field():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"bucket": {"field": "missing_source"}})
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_SOURCE_FIELD_MISSING" in _checks(result)


def test_spark_validator_rejects_incompatible_existing_target_type():
    ruleset = YamlRulesetCompiler().compile_payload(_payload({"target": 10}))
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("target", T.StringType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE" in _checks(result)


def test_spark_validator_rejects_incompatible_new_target_assignments():
    payload = _payload({"target": "text"})
    payload["rules"].append(
        {
            **payload["rules"][0],
            "rule_id": "r2",
            "rule_name": "Rule 2",
            "rule_order": 2,
            "assign": {"target": 10},
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_TYPE_CONFLICT" in _checks(result)


def test_spark_validator_infers_new_target_type_from_prior_assignment():
    """An assigned operand carries its producer's Spark type downstream."""
    payload = _payload({"score": 10})
    payload["rules"].append(
        {
            **payload["rules"][0],
            "rule_id": "r2",
            "rule_name": "Rule 2",
            "rule_order": 2,
            "assign": {"copied_score": {"assigned": "score"}},
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("status", T.StringType(), True)])
    validator = SparkRulesetCompatibilityValidator()

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert isinstance(assignment_schema["score"].dataType, T.LongType)
    assert isinstance(assignment_schema["copied_score"].dataType, T.LongType)


def test_spark_validator_existing_target_supplies_null_literal_type():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"target": {"literal": None}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("target", T.LongType(), True),
        ]
    )
    validator = SparkRulesetCompatibilityValidator()

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert isinstance(assignment_schema["target"].dataType, T.LongType)


def test_spark_validator_new_null_literal_requires_value_type():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"new_target": {"literal": None}})
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_NULL_TYPE_REQUIRED" in _checks(result)


def test_spark_validator_new_field_operand_inherits_source_type():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"new_target": {"field": "source_value"}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_value", T.DateType(), True),
        ]
    )
    validator = SparkRulesetCompatibilityValidator()

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert isinstance(assignment_schema["new_target"].dataType, T.DateType)


def test_spark_validator_new_custom_assignment_requires_return_type_hint():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="untyped",
            implementation_reference="pkg.untyped",
            arg_names=(),
            allowed_in_condition_flag=False,
            allowed_in_assignment_flag=True,
        )
    )
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "new_target": {
                    "custom_function": {"name": "untyped", "args": {}}
                }
            }
        )
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator(registry).validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_RETURN_TYPE_REQUIRED" in _checks(result)


def test_spark_validator_polymorphic_assignment_uses_existing_target_type():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="identity",
            implementation_reference="pkg.identity",
            arg_names=("value",),
            allowed_in_condition_flag=False,
            allowed_in_assignment_flag=True,
            return_type_hint="any",
        )
    )
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "target": {
                    "custom_function": {
                        "name": "identity",
                        "args": {"value": {"field": "source_value"}},
                    }
                }
            }
        )
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_value", T.StringType(), True),
            T.StructField("target", T.StringType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator(registry).validate(ruleset, schema)

    assert result.passed


def test_spark_validator_polymorphic_assignment_requires_new_target_type():
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            function_name="identity",
            implementation_reference="pkg.identity",
            arg_names=("value",),
            allowed_in_condition_flag=False,
            allowed_in_assignment_flag=True,
            return_type_hint="any",
        )
    )
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "new_target": {
                    "custom_function": {
                        "name": "identity",
                        "args": {"value": "x"},
                    }
                }
            }
        )
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator(registry).validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_RETURN_TYPE_REQUIRED" in _checks(result)
    assert "SPARK_ASSIGNMENT_RETURN_TYPE_UNSUPPORTED" not in _checks(result)


def test_spark_validator_does_not_guess_condition_coercion_semantics():
    """Runtime-supported string-to-number comparisons are not rejected early."""
    payload = _payload({"bucket": "A"}, condition_field="amount")
    payload["rules"][0]["when"]["all"][0]["operator"] = "gt"
    payload["rules"][0]["when"]["all"][0]["right"] = {"literal": "10"}
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("amount", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert result.passed


def test_spark_validator_unifies_decimal_literals_without_float_fallback():
    payload = _payload({"target": Decimal("123.45")})
    payload["rules"].append(
        {
            **payload["rules"][0],
            "rule_id": "r2",
            "rule_name": "Rule 2",
            "rule_order": 2,
            "assign": {"target": Decimal("0.1234")},
        }
    )
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("status", T.StringType(), True)])
    validator = SparkRulesetCompatibilityValidator()

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert assignment_schema["target"].dataType == T.DecimalType(7, 4)


def test_spark_validator_rejects_lossy_decimal_scale_narrowing():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"target": {"field": "source_decimal"}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_decimal", T.DecimalType(18, 4), True),
            T.StructField("target", T.DecimalType(18, 2), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE" in _checks(result)


def test_spark_validator_accepts_safe_decimal_widening():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"target": {"field": "source_decimal"}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_decimal", T.DecimalType(10, 2), True),
            T.StructField("target", T.DecimalType(14, 4), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert result.passed


def test_spark_validator_accepts_yaml_fraction_for_existing_decimal_target():
    ruleset = YamlRulesetCompiler().compile_payload(_payload({"target": 0.0425}))
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("target", T.DecimalType(10, 4), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert result.passed


def test_spark_validator_infers_decimal_for_new_yaml_fraction_target():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"new_rate": 0.0425})
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])
    validator = SparkRulesetCompatibilityValidator()

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert assignment_schema["new_rate"].dataType == T.DecimalType(4, 4)


def test_spark_validator_accepts_to_number_for_existing_decimal_target():
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "target": {
                    "custom_function": {
                        "name": "to_number",
                        "args": {"value": {"field": "source_value"}},
                    }
                }
            }
        )
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_value", T.StringType(), True),
            T.StructField("target", T.DecimalType(18, 6), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator(registry).validate(ruleset, schema)

    assert result.passed


def test_spark_validator_infers_decimal_for_new_to_number_target():
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "parsed": {
                    "custom_function": {
                        "name": "to_number",
                        "args": {"value": {"field": "source_value"}},
                    }
                }
            }
        )
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_value", T.StringType(), True),
        ]
    )
    validator = SparkRulesetCompatibilityValidator(registry)

    result = validator.validate(ruleset, schema)
    assignment_schema = validator.assignment_schema(ruleset, schema)

    assert result.passed
    assert assignment_schema["parsed"].dataType == T.DecimalType(38, 18)


def test_spark_validator_rejects_date_compared_with_quoted_string():
    payload = _payload({"bucket": "A"}, condition_field="as_of_date")
    payload["rules"][0]["when"]["all"][0]["operator"] = "ge"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": "2025-01-01"
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("as_of_date", T.DateType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" in _checks(result)


def test_spark_validator_rejects_scalar_string_membership_field():
    """IN/NOT_IN require collection-valued fields at schema preflight."""
    payload = _payload({"bucket": "A"}, condition_field="flag")
    payload["rules"][0]["when"]["all"][0]["operator"] = "in"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "field": "flags_string"
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [
            T.StructField("flag", T.StringType(), True),
            T.StructField("flags_string", T.StringType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_MEMBERSHIP_COLLECTION_REQUIRED" in _checks(result)


def test_spark_validator_allows_array_membership_field():
    """A Spark array remains a valid field-backed IN operand."""
    payload = _payload({"bucket": "A"}, condition_field="flag")
    payload["rules"][0]["when"]["all"][0]["operator"] = "in"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "field": "flags"
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [
            T.StructField("flag", T.StringType(), True),
            T.StructField(
                "flags",
                T.ArrayType(T.StringType(), False),
                True,
            ),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_MEMBERSHIP_COLLECTION_REQUIRED" not in _checks(result)


def test_mapping_literal_schema_order_is_stable_after_persistence():
    """Canonical struct field order does not depend on JSON mapping order."""
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload(
            {
                "details": {
                    "literal": {
                        "z_field": 1,
                        "a_field": "A",
                    }
                }
            }
        )
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])
    validator = SparkRulesetCompatibilityValidator()
    serializer = DeltaRowSerializer()
    reloaded = serializer.deserialize_ruleset_version(
        serializer.serialize_ruleset_version(ruleset)
    )

    direct_type = validator.assignment_schema(ruleset, schema)["details"].dataType
    reloaded_type = validator.assignment_schema(reloaded, schema)["details"].dataType

    assert direct_type.fieldNames() == ["a_field", "z_field"]
    assert reloaded_type == direct_type


def test_spark_validator_reports_unsupported_typed_null_precisely():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"new_target": {"literal": None, "value_type": "uuid"}})
    )
    schema = T.StructType([T.StructField("status", T.StringType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_VALUE_TYPE_UNSUPPORTED" in _checks(result)
    assert "SPARK_ASSIGNMENT_NULL_TYPE_REQUIRED" not in _checks(result)


def test_spark_validator_rejects_untyped_nulltype_assignment_source():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"new_target": {"field": "null_source"}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("null_source", T.NullType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_TYPE_UNRESOLVED" in _checks(result)


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_spark_validator_rejects_timestamp_representation_assignment_change():
    ruleset = YamlRulesetCompiler().compile_payload(
        _payload({"target": {"field": "source_timestamp"}})
    )
    schema = T.StructType(
        [
            T.StructField("status", T.StringType(), True),
            T.StructField("source_timestamp", T.TimestampNTZType(), True),
            T.StructField("target", T.TimestampType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_ASSIGNMENT_TARGET_TYPE_INCOMPATIBLE" in _checks(result)


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_spark_validator_rejects_timestamp_representation_condition_change():
    payload = _payload({"bucket": "A"}, condition_field="left_timestamp")
    payload["rules"][0]["when"]["all"][0]["operator"] = "ge"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "field": "right_timestamp"
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [
            T.StructField("left_timestamp", T.TimestampType(), True),
            T.StructField("right_timestamp", T.TimestampNTZType(), True),
        ]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" in _checks(result)


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_timestamp_ntz_literal_mismatch_explains_value_type_fix():
    """A bare datetime diagnostic names the supported NTZ authoring hint."""
    payload = _payload({"bucket": "A"}, condition_field="event_at")
    payload["rules"][0]["when"]["all"][0]["operator"] = "ge"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": datetime(2026, 1, 1)  # noqa: DTZ001 - TimestampNTZ is naive.
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [T.StructField("event_at", T.TimestampNTZType(), True)]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)
    issue = next(
        item
        for item in result.issues
        if item.check_name == "SPARK_CONDITION_TEMPORAL_MISMATCH"
    )

    assert "timestamp_ntz" in issue.message


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_timestamp_ntz_literal_hint_matches_ntz_field():
    """An explicit NTZ hint resolves the representation mismatch preflight."""
    payload = _payload({"bucket": "A"}, condition_field="event_at")
    payload["rules"][0]["when"]["all"][0]["operator"] = "ge"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": datetime(2026, 1, 1),  # noqa: DTZ001 - TimestampNTZ is naive.
        "value_type": "timestamp_ntz",
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [T.StructField("event_at", T.TimestampNTZType(), True)]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" not in _checks(result)


@pytest.mark.skipif(
    not hasattr(T, "TimestampNTZType"),
    reason="Spark version does not expose TimestampNTZType.",
)
def test_timestamp_ntz_collection_hint_matches_ntz_field():
    """The temporal hint applies to every normalized collection element."""
    payload = _payload({"bucket": "A"}, condition_field="event_at")
    payload["rules"][0]["when"]["all"][0]["operator"] = "in"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": [
            datetime(2026, 1, 1),  # noqa: DTZ001 - TimestampNTZ is naive.
        ],
        "value_type": "timestamp_ntz",
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType(
        [T.StructField("event_at", T.TimestampNTZType(), True)]
    )

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" not in _checks(result)


def test_mixed_temporal_between_bounds_fail_preflight():
    """A date/timestamp bound pair cannot evade temporal validation."""
    payload = _payload({"bucket": "A"}, condition_field="as_of_date")
    payload["rules"][0]["when"]["all"][0]["operator"] = "between"
    payload["rules"][0]["when"]["all"][0]["right"] = {
        "literal": [
            date(2026, 1, 1),
            datetime(2026, 12, 31),  # noqa: DTZ001 - deliberate mixed type.
        ]
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    schema = T.StructType([T.StructField("as_of_date", T.DateType(), True)])

    result = SparkRulesetCompatibilityValidator().validate(ruleset, schema)

    assert "SPARK_CONDITION_TEMPORAL_MISMATCH" in _checks(result)
