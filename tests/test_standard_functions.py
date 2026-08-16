from datetime import date, datetime, timedelta, timezone

import pytest
from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_runtime import SparkRulesEngineRuntime
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
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
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
    assert output["assign"] == {"account_prefix": "AB"}


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
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
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
    assert output["winning_rule_explanation"] == (
        "date_add_months(value=funded_date, months=1) >= 2024-02-29"
    )
    assert output["assign"] == {
        "review_date": date(2025, 1, 31),
        "age_days": 29,
    }
    authored_expressions = {
        event["target_field"]: event["authored_expression"]
        for event in output["assignment_results"]
    }
    assert authored_expressions == {
        "review_date": "review_date = date_add_years(value=funded_date, years=1)",
        "age_days": "age_days = date_diff_days(start=funded_date, end=as_of_date)",
    }


def test_standard_function_rows_expose_registry_metadata():
    """
    What: Creates persisted metadata rows for standard functions.
    Why: Production workflows can save function specs without hand-authoring them.
    Fails when: standard function specs cannot be written to the registry table.
    """
    rows = standard_function_rows()
    names = {row.function_name for row in rows}

    assert "substring" in names
    assert "regex_extract" in names
    assert {
        "to_date",
        "date_add_days",
        "date_add_months",
        "date_add_years",
        "date_diff_days",
        "month_start",
        "month_end",
    } <= names

    hints = {row.function_name: row.return_type_hint for row in rows}
    assert hints["date_add_months"] == "date"
    assert hints["date_diff_days"] == "integer"
