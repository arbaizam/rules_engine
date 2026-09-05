import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_UP, Decimal, Inexact, Rounded, localcontext

import pytest
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import types as T

import rules_engine.standard_functions as sf
from rules_engine.canonical_values import decode_json_types
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RegistryError
from rules_engine.registry import CustomFunctionArgSpec, CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_runtime import SparkRulesEngineRuntime, required_source_columns
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.standard_functions import (
    date_add_days,
    date_add_months,
    date_add_years,
    date_diff_days,
    month_end,
    month_start,
    register_standard_functions,
    standard_function_rows,
    substring,
    to_date,
)
from rules_engine.validator import RulesetValidator


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class FakeSparkRow:
    def __init__(self, data):
        self._data = data

    def asDict(self, recursive=True):
        return self._data


def test_substring_uses_sql_style_start_position():
    """
    What: Verifies substring uses a 1-based start index.
    Why: Databricks/Spark authors commonly expect SQL substring semantics.
    Fails when: substring behaves like Python zero-based slicing.
    """
    assert substring("ABCDE", 2, 3) == "BCD"


def test_to_date_accepts_iso_date_values_and_propagates_nulls():
    assert to_date(" 2024-02-29 ") == date(2024, 2, 29)
    timestamp = datetime(2024, 2, 29, 15, 30, tzinfo=timezone.utc)
    assert to_date(timestamp) == date(2024, 2, 29)
    assert to_date(date(2024, 2, 29)) == date(2024, 2, 29)
    assert to_date("") is None
    assert to_date(None) is None


def test_to_date_uses_aware_timestamp_own_calendar_date():
    timestamp = datetime(
        2024,
        3,
        1,
        0,
        30,
        tzinfo=timezone(timedelta(hours=14)),
    )

    assert to_date(timestamp) == date(2024, 3, 1)


def test_to_date_rejects_ambiguous_or_invalid_values():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        to_date("02/29/2024")
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        to_date("20240229")
    with pytest.raises(ValueError, match="Cannot convert value to date"):
        to_date(20240229)


def test_date_add_days_supports_positive_and_negative_offsets():
    assert date_add_days(date(2024, 2, 28), 1) == date(2024, 2, 29)
    assert date_add_days(date(2024, 3, 1), -1) == date(2024, 2, 29)
    assert date_add_days(None, 1) is None


@pytest.mark.parametrize(
    ("value", "days"),
    [
        (date.max, 1),
        (date.min, -1),
        (date(2024, 1, 1), 1_000_000_000),
    ],
)
def test_date_add_days_reports_out_of_range_offsets(value, days):
    with pytest.raises(
        ValueError,
        match=rf"Adding {days} days to {value.isoformat()} exceeds the date range",
    ):
        date_add_days(value, days)


@pytest.mark.parametrize(
    ("value", "months", "expected"),
    [
        (date(2024, 1, 31), 1, date(2024, 2, 29)),
        (date(2023, 1, 31), 1, date(2023, 2, 28)),
        (date(2024, 3, 31), -1, date(2024, 2, 29)),
        (date(2024, 12, 31), 2, date(2025, 2, 28)),
    ],
)
def test_date_add_months_clamps_to_target_month_end(value, months, expected):
    assert date_add_months(value, months) == expected


def test_date_add_years_clamps_leap_day_and_rejects_fractional_offsets():
    assert date_add_years(date(2024, 2, 29), 1) == date(2025, 2, 28)
    assert date_add_years(date(2024, 2, 29), -4) == date(2020, 2, 29)
    with pytest.raises(ValueError, match="years must be an integer"):
        date_add_years(date(2024, 1, 1), 1.5)


def test_date_diff_and_month_boundaries_have_explicit_calendar_semantics():
    assert date_diff_days(date(2024, 1, 31), date(2024, 2, 29)) == 29
    assert date_diff_days(date(2024, 2, 29), date(2024, 1, 31)) == -29
    assert date_diff_days(None, date(2024, 1, 31)) is None
    assert month_start(date(2024, 2, 17)) == date(2024, 2, 1)
    assert month_end(date(2024, 2, 17)) == date(2024, 2, 29)


def test_text_functions_cover_cleaning_parsing_padding_and_patterns():
    assert sf.left("ABCDE", 2) == "AB"
    assert sf.right("ABCDE", 2) == "DE"
    assert sf.ltrim("  A  ") == "A  "
    assert sf.rtrim("  A  ") == "  A"
    assert sf.trim("  A  ") == "A"
    assert sf.upper("Ab") == "AB"
    assert sf.lower("Ab") == "ab"
    assert sf.normalize_whitespace(" A\t B\nC ") == "A B C"
    assert sf.text_length("ABC") == 3
    assert sf.replace("A-B-A", "A", "X") == "X-B-X"
    assert sf.split_part("A|B|C", "|", 2) == "B"
    assert sf.split_part("A|B", "|", 3) is None
    assert sf.pad_left("7", 3, "0") == "007"
    assert sf.pad_right("7", 3, "0") == "700"
    assert sf.concat_ws(["A", None, 2], "|") == "A|2"
    assert sf.concat_ws(["A", None], "|", skip_nulls=False) is None
    assert sf.regex_extract("AB-123", r"(\d+)") == "123"
    assert sf.regex_replace("AB-123", r"\d", "X") == "AB-XXX"
    assert sf.regex_match("AB-123", r"^AB") is True
    assert sf.text_contains_any("alpha beta", ["gamma", "beta"]) is True
    assert sf.text_contains_any("None", [None]) is False
    assert sf.is_blank(" \t") is True
    assert sf.is_blank(0) is False


def test_null_composition_functions_preserve_value_types():
    marker = {"quality": "good"}

    assert sf.null_if("N/A", "N/A") is None
    assert sf.null_if(marker, None) is marker
    assert sf.coalesce([None, marker, "fallback"]) is marker
    assert sf.coalesce([None, None]) is None


def test_converters_are_strict_and_offer_an_explicit_null_failure_policy():
    assert sf.to_string(True) == "true"
    assert sf.to_string(Decimal("1.20")) == "1.20"
    assert sf.to_decimal("1.20") == Decimal("1.20")
    assert sf.to_integer("12.0") == 12
    assert sf.to_boolean("YES") is True
    assert sf.to_boolean("0") is False
    assert sf.to_date("not-a-date", on_error="null") is None
    assert sf.to_decimal("not-a-number", on_error="null") is None
    assert sf.to_integer("1.5", on_error="null") is None
    assert sf.to_boolean("maybe", on_error="null") is None

    with pytest.raises(ValueError, match="Cannot convert value to integer"):
        sf.to_integer("1.5")
    with pytest.raises(ValueError, match="on_error"):
        sf.to_decimal("x", on_error="ignore")
    with pytest.raises(ValueError, match="on_error"):
        sf.to_decimal("1", on_error="ignore")


def test_timestamp_converters_distinguish_instant_and_wall_clock_values():
    instant = sf.to_timestamp("2024-01-01T01:00:00+01:00")
    assert instant == datetime(
        2024,
        1,
        1,
        tzinfo=timezone.utc,
    )
    assert instant.utcoffset() == timedelta(0)
    expected_wall_clock = datetime(
        2024,
        1,
        1,
        1,
        tzinfo=timezone.utc,
    ).replace(tzinfo=None)
    wall_clock = sf.to_timestamp_ntz("2024-01-01T01:00:00")
    assert wall_clock == expected_wall_clock
    assert wall_clock.tzinfo is None
    assert sf.to_timestamp("2024-01-01T01:00:00", on_error="null") is None
    assert (
        sf.to_timestamp_ntz(
            "2024-01-01T01:00:00Z",
            on_error="null",
        )
        is None
    )


def test_decimal_functions_use_decimal_arithmetic_and_explicit_rounding():
    assert sf.decimal_abs("-1.25") == Decimal("1.25")
    assert sf.decimal_add("1.1", "2.2") == Decimal("3.3")
    assert sf.decimal_subtract("5", "1.25") == Decimal("3.75")
    assert sf.decimal_multiply("1.5", "2") == Decimal("3.0")
    assert sf.decimal_divide("1", "3", 2) == Decimal("0.33")
    assert sf.decimal_safe_divide("1", "0") is None
    assert sf.decimal_round("2.345", 2, "half_up") == Decimal("2.35")
    assert sf.decimal_round("2.345", 2, "half_even") == Decimal("2.34")
    assert sf.decimal_add(None, "1") is None

    with pytest.raises(ZeroDivisionError):
        sf.decimal_divide("1", "0")
    with pytest.raises(ValueError, match="minimum"):
        sf.decimal_clamp("5", "10", "0")


@pytest.mark.parametrize(
    ("function_name", "arguments", "expected"),
    [
        pytest.param("decimal_min", ("2", "3"), Decimal(2), id="min-forward"),
        pytest.param("decimal_min", ("3", "2"), Decimal(2), id="min-reverse"),
        pytest.param("decimal_min", ("2", "2"), Decimal(2), id="min-equal"),
        pytest.param("decimal_max", ("2", "3"), Decimal(3), id="max-forward"),
        pytest.param("decimal_max", ("3", "2"), Decimal(3), id="max-reverse"),
        pytest.param("decimal_max", ("2", "2"), Decimal(2), id="max-equal"),
        pytest.param("decimal_clamp", ("12", "0", "10"), Decimal(10), id="clamp-above"),
        pytest.param("decimal_clamp", ("-2", "0", "10"), Decimal(0), id="clamp-below"),
        pytest.param("decimal_clamp", ("2.5", "0", "10"), Decimal("2.5"), id="clamp-interior"),
    ],
)
def test_decimal_selection_functions_compare_values_and_preserve_decimal_type(
    function_name,
    arguments,
    expected,
):
    """Min/max compare either argument order and clamp respects both inclusive bounds."""
    result = getattr(sf, function_name)(*arguments)

    assert type(result) is Decimal
    assert result == expected


def test_calendar_boundary_and_completed_period_functions_are_explicit():
    leap_day = date(2024, 2, 29)

    assert sf.date_diff_months(date(2024, 1, 31), leap_day) == 1
    assert sf.date_diff_months(leap_day, date(2024, 1, 31)) == -1
    assert sf.date_diff_months(leap_day, date(2024, 3, 28)) == 0
    assert sf.date_diff_months(date(2024, 3, 28), leap_day) == 0
    assert sf.date_diff_years(leap_day, date(2025, 2, 28)) == 1
    assert sf.date_diff_years(date(2025, 2, 28), leap_day) == -1
    assert sf.date_diff_years(date(2025, 2, 27), leap_day) == 0
    assert sf.date_part(leap_day, "quarter") == 1
    assert sf.date_part(leap_day, "day_of_week") == 4
    assert sf.quarter_start(date(2024, 5, 15)) == date(2024, 4, 1)
    assert sf.quarter_end(date(2024, 5, 15)) == date(2024, 6, 30)
    assert sf.quarter_end(date(2023, 3, 31)) == date(2023, 3, 31)
    assert sf.quarter_end(date(2024, 9, 30)) == date(2024, 9, 30)
    assert sf.quarter_end(date(9999, 12, 31)) == date(9999, 12, 31)
    assert sf.year_start(leap_day) == date(2024, 1, 1)
    assert sf.year_end(leap_day) == date(2024, 12, 31)


def test_business_month_boundaries_require_explicit_holiday_calendars():
    holidays = ["2024-06-03", "2024-08-30"]

    assert sf.first_business_day_of_month("2024-06-15", holidays) == date(
        2024,
        6,
        4,
    )
    assert sf.last_business_day_of_month("2024-08-15", holidays) == date(
        2024,
        8,
        29,
    )
    assert sf.first_business_day_of_month(
        "2024-06-15",
        [],
        weekend_days=[5, 6],
    ) == date(2024, 6, 2)

    with pytest.raises(ValueError, match="every weekday"):
        sf.first_business_day_of_month("2024-06-15", [], list(range(1, 8)))


def test_array_functions_are_null_aware_and_reject_scalar_inputs():
    assert sf.array_size(["A", None, "B"]) == 3
    assert sf.array_size([]) == 0
    assert sf.array_size(None) is None
    assert sf.array_contains_any(["A", "B"], ["B", "C"]) is True
    assert sf.array_contains_any(["A"], ["B"]) is False
    assert sf.array_contains_any(["A"], []) is False
    assert sf.array_contains_all(["A", "B"], ["B", "A"]) is True
    assert sf.array_contains_all(["A", "B"], ["B", "C"]) is False
    assert sf.array_contains_all(["A"], []) is True
    assert sf.array_join(["A", None, 2], "|") == "A|2"
    assert sf.array_join(["A", None], "|", skip_nulls=False) is None

    for function in (sf.array_contains_any, sf.array_contains_all):
        assert function(None, ["A"]) is None
        assert function(["A"], None) is None

    for function, args in [
        (sf.array_size, ("ABC",)),
        (sf.array_contains_any, ("ABC", ["A"])),
        (sf.array_contains_all, (["A"], "A")),
        (sf.array_join, ("ABC", "|")),
    ]:
        with pytest.raises(TypeError, match="array"):
            function(*args)


def test_standard_functions_can_be_registered_for_runtime_field_args():
    """
    What: Registers standard functions and evaluates substring against row fields.
    Why: Common custom functions must be usable with dynamic row values in rules.
    Fails when: custom_function args remain literal-only metadata.
    """
    registry = register_standard_functions(FunctionRegistry())
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
                    "rule_name": "Substring rule",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "substring",
                                        "args": {
                                            "value": {"field": "account_code"},
                                            "start": 2,
                                            "length": 3,
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": "BCD"},
                            }
                        ]
                    },
                    "assign": {
                        "account_prefix": {
                            "custom_function": {
                                "name": "left",
                                "args": {
                                    "value": {"field": "account_code"},
                                    "length": 2,
                                },
                            }
                        }
                    },
                }
            ],
        }
    )

    validation = RulesetValidator(registry).validate(ruleset)
    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)
    evaluator = SparkRulesEngineRuntime(
        DummyRepository(),
        registry,
    )._build_row_evaluator(
        ruleset,
        ["account_prefix"],
        {"account_prefix": T.StringType()},
    )
    output = evaluator(FakeSparkRow({"account_code": "ABCDE"}))

    assert validation.passed
    assert '"field":"account_code"' in row.payload_json
    assert output["matched"] is True
    assert output["assign"] == {"account_prefix": {"applied": True, "value": "AB"}}


def test_optional_defaults_and_nested_argument_operands_work_end_to_end():
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "composition",
            "ruleset_name": "Composition",
            "version": "1",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "compose",
                    "rule_name": "Compose",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {
                        "selected_code": {
                            "custom_function": {
                                "name": "coalesce",
                                "args": {
                                    "values": [
                                        {"field": "primary_code"},
                                        {"field": "secondary_code"},
                                    ]
                                },
                            }
                        },
                        "code_suffix": {
                            "custom_function": {
                                "name": "substring",
                                "args": {
                                    "value": {"field": "secondary_code"},
                                    "start": 2,
                                },
                            }
                        },
                        "has_review_tag": {
                            "custom_function": {
                                "name": "array_contains_any",
                                "args": {
                                    "values": {"field": "tags"},
                                    "candidates": ["review", "hold"],
                                },
                            }
                        },
                    },
                }
            ],
        }
    )
    source_schema = T.StructType(
        [
            T.StructField("primary_code", T.StringType(), True),
            T.StructField("secondary_code", T.StringType(), True),
            T.StructField("tags", T.ArrayType(T.StringType()), True),
        ]
    )
    validator = SparkRulesetCompatibilityValidator(registry)

    prepared_schema = validator.prepare(ruleset, source_schema)
    validation = prepared_schema.validation
    assignment_schema = prepared_schema.assignment_schema
    evaluator = SparkRulesEngineRuntime(
        DummyRepository(),
        registry,
    )._build_row_evaluator(
        ruleset,
        [field.name for field in assignment_schema.fields],
        {field.name: field.dataType for field in assignment_schema.fields},
    )
    serializer = CloudPickleSerializer()
    evaluator = serializer.loads(serializer.dumps(evaluator))
    output = evaluator(
        FakeSparkRow(
            {
                "primary_code": None,
                "secondary_code": "ABC",
                "tags": ["review"],
            }
        )
    )

    assert validation.passed, validation.to_text()
    assert required_source_columns(ruleset) == prepared_schema.required_source_columns == (
        "primary_code",
        "secondary_code",
        "tags",
    )
    assert assignment_schema["selected_code"].dataType == T.StringType()
    assert output["assign"] == {
        "selected_code": {"applied": True, "value": "ABC"},
        "code_suffix": {"applied": True, "value": "BC"},
        "has_review_tag": {"applied": True, "value": True},
    }


def test_standard_date_functions_work_in_conditions_and_typed_assignments():
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "date_rules",
            "ruleset_name": "Date Rules",
            "version": "1",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "seasoning",
                    "rule_name": "Seasoning Date",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "date_add_months",
                                        "args": {
                                            "value": {"field": "funded_date"},
                                            "months": 1,
                                        },
                                    }
                                },
                                "operator": "ge",
                                "right": {"literal": date(2024, 2, 29)},
                            }
                        ]
                    },
                    "assign": {
                        "review_date": {
                            "custom_function": {
                                "name": "date_add_years",
                                "args": {
                                    "value": {"field": "funded_date"},
                                    "years": 1,
                                },
                            }
                        },
                        "age_days": {
                            "custom_function": {
                                "name": "date_diff_days",
                                "args": {
                                    "start": {"field": "funded_date"},
                                    "end": {"field": "as_of_date"},
                                },
                            }
                        },
                    },
                }
            ],
        }
    )
    source_schema = T.StructType(
        [
            T.StructField("funded_date", T.StringType(), True),
            T.StructField("as_of_date", T.StringType(), True),
        ]
    )
    validator = SparkRulesetCompatibilityValidator(registry)

    validation = validator.validate(ruleset, source_schema)
    assignment_schema = validator.assignment_schema(ruleset, source_schema)
    evaluator = SparkRulesEngineRuntime(
        DummyRepository(),
        registry,
    )._build_row_evaluator(
        ruleset,
        [field.name for field in assignment_schema.fields],
        {field.name: field.dataType for field in assignment_schema.fields},
        full_audit=True,
    )
    output = evaluator(
        FakeSparkRow(
            {
                "funded_date": "2024-01-31",
                "as_of_date": "2024-02-29",
            }
        )
    )

    assert validation.passed
    assert assignment_schema["review_date"].dataType == T.DateType()
    assert assignment_schema["age_days"].dataType == T.LongType()
    assert output["matched"] is True
    assert output["matched_rules"][0]["explanation"] == (
        "date_add_months(value=funded_date, months=1) >= 2024-02-29"
    )
    assert output["assign"] == {
        "review_date": {"applied": True, "value": date(2025, 1, 31)},
        "age_days": {"applied": True, "value": 29},
    }
    authored_expressions = {
        event["target_field"]: event["authored_expression"]
        for event in output["assignment_results"]
    }
    assert authored_expressions == {
        "review_date": "review_date = date_add_years(value=funded_date, years=1)",
        "age_days": "age_days = date_diff_days(start=funded_date, end=as_of_date)",
    }


def test_decimal_functions_preserve_full_precision_in_altered_contexts():
    """Exact functions isolate precision, rounding, and traps from their caller."""
    value = Decimal("12345678901234567890.123456789012345678")
    with localcontext() as context:
        context.prec = 3
        context.rounding = ROUND_UP
        context.traps[Inexact] = True
        context.traps[Rounded] = True
        assert sf.decimal_abs(value.copy_negate()) == value
        assert sf.decimal_add(value, "0.000000000000000001") == Decimal(
            "12345678901234567890.123456789012345679"
        )
        assert sf.decimal_subtract(value, "0.000000000000000001") == Decimal(
            "12345678901234567890.123456789012345677"
        )
        assert sf.decimal_multiply(value, 2) == Decimal("24691357802469135780.246913578024691356")
        assert sf.decimal_divide("1", "3", scale=18) == Decimal("0.333333333333333333")
        assert sf.decimal_round(value, 18) == value


def test_division_keeps_requested_fractional_digits_after_a_wide_integer_part():
    """Intermediate division precision includes both integer width and output scale."""
    expected = Decimal("3" * 100 + "." + "3" * 18)
    with localcontext() as context:
        context.prec = 3
        assert sf.decimal_divide("1E100", 3, scale=18) == expected
        assert sf.decimal_safe_divide("1E100", 3, scale=18) == expected


def test_dependency_manifest_preserves_observable_default_collection_kinds():
    """Different bound Python defaults must produce different execution identities."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "defaults",
            "ruleset_name": "Defaults",
            "version": "1",
            "owner": "team",
            "owner_department": "team",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "r1",
                    "when": {
                        "all": [
                            {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            },
                        ]
                    },
                    "assign": {"kind": {"custom_function": {"name": "kind", "args": {}}}},
                }
            ],
        }
    )
    encoded_manifests = []
    for default in ([1, 2], (1, 2), {1, 2}):
        registry = FunctionRegistry()
        registry.register(
            CustomFunctionSpec(
                "kind",
                "tests.kind",
                (CustomFunctionArgSpec("values", required=False, default=default),),
                True,
                True,
                return_type_hint="string",
            ),
            lambda *, values: type(values).__name__,
        )
        manifest = registry.dependency_manifest(ruleset)
        encoded_manifests.append(json.dumps(manifest, sort_keys=True, separators=(",", ":")))
        restored_default = decode_json_types(manifest)[0]["arguments"][0]["default"]
        assert type(restored_default) is type(default)
        assert restored_default == default
        assert (
            SparkRowEvaluator(registry).evaluate_row(ruleset, {})["assign"]["kind"]["value"]
            == type(default).__name__
        )
    assert len(set(encoded_manifests)) == 3
    assert len({hashlib.sha256(value.encode()).hexdigest() for value in encoded_manifests}) == 3


@pytest.mark.parametrize(
    "argument",
    [
        CustomFunctionArgSpec("value", required=False, default={1: "a"}),
        CustomFunctionArgSpec("value", required=False, default=[{"nested": {1: "a"}}]),
        CustomFunctionArgSpec("value", allowed_values=({1: "a"},), literal_only=True),
    ],
)
def test_registry_rejects_nonstring_keys_before_metadata_normalization(argument):
    """Declared defaults and allowed values already use canonical mapping keys."""
    with pytest.raises(RegistryError, match="mapping keys must be strings"):
        CustomFunctionSpec("mapping", "tests.mapping", (argument,), True, True)


def test_registry_string_key_defaults_are_consistent_in_runtime_and_persistence():
    spec = CustomFunctionSpec(
        "mapping",
        "tests.mapping",
        (CustomFunctionArgSpec("value", required=False, default={"1": ["a"]}),),
        True,
        True,
    )
    assert spec.bind_args({})["value"] == {"1": ["a"]}
    assert spec.to_row().arg_contract_payload["arguments"][0]["default"] == {"1": ["a"]}


@pytest.mark.parametrize(
    "value",
    [
        "0001-01-01T00:00:00+01:00",
        "9999-12-31T23:59:59-01:00",
    ],
)
def test_timestamp_utc_overflow_honors_conversion_error_policy(value):
    assert sf.to_timestamp(value, on_error="null") is None
    with pytest.raises(ValueError, match="Cannot convert value to timestamp"):
        sf.to_timestamp(value)


@pytest.mark.parametrize("function_name", ["coalesce", "concat_ws", "array_join"])
def test_order_sensitive_functions_reject_yaml_sets_before_and_after_persistence(function_name):
    """A published unordered literal cannot select a process-dependent result."""
    separator = "\n            separator: ','" if function_name != "coalesce" else ""
    ruleset = YamlRulesetCompiler().compile_text(f"""
ruleset_id: ordered-array
ruleset_name: Ordered array
version: "1"
owner: team
owner_department: team
rules:
  - rule_id: r1
    rule_name: r1
    when:
      all:
        - left: {{literal: true}}
          operator: eq
          right: {{literal: true}}
    assign:
      selected:
        custom_function:
          name: {function_name}
          args:
            values: !!set {{alpha: null, beta: null, gamma: null}}{separator}
""")
    registry = register_standard_functions(FunctionRegistry())
    serializer = DeltaRowSerializer()
    restored = serializer.deserialize_ruleset_version(serializer.serialize_ruleset_version(ruleset))
    for candidate in [ruleset, restored]:
        result = RulesetValidator(registry).validate(candidate)
        assert not result.passed
        assert "ordered_sequence" in result.to_text()
        with pytest.raises(TypeError, match="ordered array"):
            SparkRowEvaluator(registry).evaluate_row(candidate, {})


def test_dependency_manifest_includes_nested_references_and_is_detached():
    """Execution identity captures referenced contracts without unrelated functions."""
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "manifest",
            "ruleset_name": "Manifest",
            "version": "1",
            "owner": "team",
            "owner_department": "team",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "r1",
                    "when": {
                        "any": [
                            {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            },
                            {
                                "active_flag": False,
                                "left": {
                                    "custom_function": {
                                        "name": "lower",
                                        "args": {"value": "A"},
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": "a"},
                            },
                        ]
                    },
                    "assign": {
                        "value": {
                            "custom_function": {
                                "name": "coalesce",
                                "args": {
                                    "values": [
                                        {
                                            "custom_function": {
                                                "name": "upper",
                                                "args": {"value": "a"},
                                            }
                                        }
                                    ]
                                },
                            }
                        }
                    },
                }
            ],
        }
    )
    manifest = registry.dependency_manifest(ruleset)
    assert [item["function_name"] for item in manifest] == ["coalesce", "upper"]
    assert all(item["version"] == sf.STANDARD_FUNCTION_VERSION for item in manifest)
    assert manifest[0]["implementation_reference"] == "rules_engine.standard_functions.coalesce"
    manifest[0]["arguments"][0]["type_hint"] = "mutated"
    assert (
        registry.dependency_manifest(ruleset)[0]["arguments"][0]["type_hint"] == "ordered_sequence"
    )


def test_standard_function_rows_expose_registry_metadata():
    """
    What: Creates persisted metadata rows for standard functions.
    Why: Production workflows can save function specs without hand-authoring them.
    Fails when: standard function specs cannot be written to the registry table.
    """
    rows = standard_function_rows()
    names = {row.function_name for row in rows}

    assert len(rows) == 58
    assert {
        "text_length",
        "text_contains_any",
        "to_decimal",
        "coalesce",
        "first_business_day_of_month",
        "last_business_day_of_month",
        "array_size",
        "array_contains_any",
        "array_contains_all",
        "array_join",
    } <= names
    assert {"length", "contains_any", "to_number"}.isdisjoint(names)

    hints = {row.function_name: row.return_type_hint for row in rows}
    assert hints["date_add_months"] == "date"
    assert hints["date_diff_days"] == "integer"
    assert hints["null_if"] == "same_as:value"
    assert hints["coalesce"] == "common_type:values"

    substring_row = next(row for row in rows if row.function_name == "substring")
    assert substring_row.arg_contract_payload == {
        "arguments": [
            {
                "name": "value",
                "required": True,
                "type_hint": "any",
                "literal_only": False,
            },
            {
                "name": "start",
                "required": True,
                "type_hint": "integer",
                "literal_only": False,
            },
            {
                "name": "length",
                "required": False,
                "type_hint": "integer",
                "literal_only": False,
                "default": None,
            },
        ]
    }
