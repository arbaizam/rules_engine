from decimal import Decimal

import pytest
from pyspark.sql import types as T

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.publish import PublishService
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_runtime import (
    SparkRulesEngineRuntime,
    _result_struct,
    result_field_names,
)
from rules_engine.testing import RulesetTester
from rules_engine.validator import RulesetValidator


class NoOpRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class RecordingRepository(NoOpRepository):
    def __init__(self):
        self.saved = None

    def save_published(self, ruleset, **kwargs):
        self.saved = ruleset


class FakeSparkRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=True):
        return self.values


def _payload(*, version="1", expectation_bucket="prime"):
    return {
        "ruleset_id": "loan_cleaning",
        "ruleset_name": "Loan Cleaning",
        "version": version,
        "owner": "Data Quality",
        "owner_department": "Lending",
        "rules": [
            {
                "rule_id": "prime",
                "rule_name": "Prime loans",
                "rule_order": 1,
                "when": {
                    "all": [
                        {
                            "condition_id": "fico-prime",
                            "left": {"field": "fico"},
                            "operator": "ge",
                            "right": {"literal": 720},
                        }
                    ]
                },
                "assign": {"bucket": "prime", "rate": 0.0425},
            },
            {
                "rule_id": "near-prime",
                "rule_name": "Near-prime loans",
                "rule_order": 2,
                "when": {
                    "all": [
                        {
                            "condition_id": "fico-near-prime",
                            "left": {"field": "fico"},
                            "operator": "ge",
                            "right": {"literal": 680},
                        }
                    ]
                },
                "assign": {"review": True},
            },
        ],
        "expect": [
            {
                "name": "prime example",
                "given": {"fico": 740},
                "then": {
                    "matched": True,
                    "matched_rule_ids": ["prime", "near-prime"],
                    "bucket": expectation_bucket,
                    "rate": 0.0425,
                    "review": True,
                },
            },
            {
                "name": "clean no-match example",
                "given": {"fico": 600},
                "then": {"matched": False, "matched_rule_ids": []},
            },
        ],
    }


# Executable expected cases and publish gate


def test_expected_cases_round_trip_and_preserve_exact_decimals():
    ruleset = YamlRulesetCompiler().compile_payload(_payload())

    assert ruleset.expect[0].then["rate"] == Decimal("0.0425")
    reconstructed = YamlRulesetCompiler().compile_text(
        YamlRulesetExporter().export_text(ruleset)
    )
    persisted = DeltaRowSerializer().deserialize_ruleset_version(
        DeltaRowSerializer().serialize_ruleset_version(ruleset)
    )

    assert reconstructed.expect == ruleset.expect
    assert persisted.expect == ruleset.expect
    assert DeltaRowSerializer().content_hash(reconstructed) == (
        DeltaRowSerializer().content_hash(ruleset)
    )


def test_ruleset_tester_supports_assignment_shorthand_and_no_match_cases():
    result = RulesetTester(FunctionRegistry()).test(
        YamlRulesetCompiler().compile_payload(_payload())
    )

    assert result.passed is True
    assert result.failure_count == 0
    assert "2/2" in result.to_text()


def test_expected_case_failure_blocks_publish_before_repository_write():
    repository = RecordingRepository()
    service = PublishService(
        repository,
        RulesetValidator(FunctionRegistry()),
        RulesetTester(FunctionRegistry()),
    )

    with pytest.raises(ValidationFailedError, match="expected cases failed"):
        service.publish(
            YamlRulesetCompiler().compile_payload(
                _payload(expectation_bucket="incorrect")
            )
        )

    assert repository.saved is None


def test_passing_expected_cases_publish_normally():
    repository = RecordingRepository()
    service = PublishService(
        repository,
        RulesetValidator(FunctionRegistry()),
        RulesetTester(FunctionRegistry()),
    )

    service.publish(YamlRulesetCompiler().compile_payload(_payload()))

    assert repository.saved is not None
    assert repository.saved.expect[0].name == "prime example"


def test_expected_assign_shape_is_validated_before_execution():
    payload = _payload()
    payload["expect"][0]["then"] = {"assign": "not-a-mapping"}
    validation = RulesetValidator(FunctionRegistry()).validate(
        YamlRulesetCompiler().compile_payload(payload)
    )

    assert {
        issue.check_name for issue in validation.issues
    } >= {"EXPECTED_CASE_ASSIGN_MAPPING_REQUIRED"}


def test_expected_case_rejects_misspelled_assignment_keys():
    payload = _payload()
    payload["expect"][0]["then"] = {"matchd": True}
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    validation = RulesetValidator(FunctionRegistry()).validate(ruleset)
    test_result = RulesetTester(FunctionRegistry()).test(ruleset)

    assert "EXPECTED_CASE_UNKNOWN_KEY" in {
        issue.check_name for issue in validation.issues
    }
    assert test_result.passed is False
    assert "unknown expected assignment key" in test_result.to_text()

    repository = RecordingRepository()
    publish_service = PublishService(
        repository,
        RulesetValidator(FunctionRegistry()),
        RulesetTester(FunctionRegistry()),
    )
    with pytest.raises(
        ValidationFailedError,
        match="EXPECTED_CASE_UNKNOWN_KEY",
    ):
        publish_service.publish(ruleset)
    assert repository.saved is None


def test_reserved_result_name_can_be_asserted_as_explicit_assignment():
    payload = _payload()
    payload["rules"][0]["assign"]["matched"] = "business-value"
    payload["expect"][0]["then"] = {
        "matched": True,
        "assign": {"matched": "business-value"},
    }
    ruleset = YamlRulesetCompiler().compile_payload(payload)

    assert RulesetValidator(FunctionRegistry()).validate(ruleset).passed
    assert RulesetTester(FunctionRegistry()).test(ruleset).passed


# Audit contracts and production/publish differential behavior


def test_full_audit_controls_detailed_schema_and_payload():
    ruleset = YamlRulesetCompiler().compile_payload(_payload())
    runtime = SparkRulesEngineRuntime(NoOpRepository(), FunctionRegistry())
    assign_fields = ["bucket", "rate", "review"]
    assign_types = {
        "bucket": T.StringType(),
        "rate": T.DecimalType(10, 4),
        "review": T.BooleanType(),
    }

    payloads = {}
    for full_audit in (False, True):
        evaluator = runtime._build_row_evaluator(
            ruleset,
            assign_fields,
            assign_types,
            full_audit=full_audit,
        )
        payloads[full_audit] = evaluator(FakeSparkRow({"fico": 740}))
        assert tuple(payloads[full_audit]) == result_field_names(
            full_audit=full_audit
        )
        assert tuple(payloads[full_audit]) == tuple(
            _result_struct(
                T.StructType(),
                full_audit=full_audit,
            ).fieldNames()
        )

    assert tuple(payloads[False]) == (
        "error",
        "matched",
        "matched_rule_ids",
        "assign",
    )
    assert "assignment_results" not in payloads[False]
    assert "matched_rules" not in payloads[False]
    assert payloads[True]["matched_rules"][0]["conditions"]
    assert "assignment_results" in payloads[True]
    assert "matched_rules" in payloads[True]


def test_non_boolean_full_audit_fails_before_spark_execution():
    with pytest.raises(TypeError, match="full_audit must be a bool"):
        result_field_names(full_audit="true")


def test_publish_evaluator_and_spark_worker_share_rule_ordering_semantics():
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "differential",
            "ruleset_name": "Differential",
            "version": "1",
            "rules": [
                {
                    "rule_id": "inactive",
                    "rule_name": "Inactive",
                    "rule_order": 1,
                    "active_flag": False,
                    "when": {
                        "all": [
                            {
                                "left": {"literal": True},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"first": "inactive"},
                },
                {
                    "rule_id": "merge-a",
                    "rule_name": "Merge A",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"first": "A", "shared": "early"},
                },
                {
                    "rule_id": "stop",
                    "rule_name": "Stop",
                    "rule_order": 3,
                    "stop_on_match": True,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "stop"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"shared": "late"},
                },
                {
                    "rule_id": "after-stop",
                    "rule_name": "After stop",
                    "rule_order": 4,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "eligible"},
                                "operator": "eq",
                                "right": {"literal": True},
                            }
                        ]
                    },
                    "assign": {"after": "evaluated"},
                },
            ],
        }
    )
    row_evaluator = SparkRowEvaluator.for_embedded_ruleset(FunctionRegistry())
    runtime = SparkRulesEngineRuntime(NoOpRepository(), FunctionRegistry())
    spark_evaluator = runtime._build_row_evaluator(
        ruleset,
        ["first", "shared", "after"],
        {
            "first": T.StringType(),
            "shared": T.StringType(),
            "after": T.StringType(),
        },
    )

    for row in (
        {"eligible": True, "stop": True},
        {"eligible": True, "stop": False},
        {"eligible": False, "stop": False},
    ):
        expected = row_evaluator.evaluate_row(ruleset, row)
        actual = spark_evaluator(FakeSparkRow(row))

        assert {
            key: actual[key]
            for key in ("matched", "matched_rule_ids", "assign")
        } == expected


def test_expected_null_assignment_fails_when_the_target_was_not_applied():
    """The publish gate cannot confuse an explicit null with no assignment."""
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "explicit_null_expectation",
            "ruleset_name": "Explicit Null Expectation",
            "version": "1",
            "owner": "Data Quality",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "open_bucket",
                    "rule_name": "Open Bucket",
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
                    "assign": {"bucket": "open"},
                },
                {
                    "rule_id": "closed_note",
                    "rule_name": "Closed Note",
                    "rule_order": 2,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "CLOSED"},
                            }
                        ]
                    },
                    "assign": {
                        "note": {"literal": None, "value_type": "string"}
                    },
                },
            ],
            "expect": [
                {
                    "name": "open row must not claim note was cleared",
                    "given": {"status": "OPEN"},
                    "then": {"bucket": "open", "note": None},
                },
                {
                    "name": "closed row explicitly clears note",
                    "given": {"status": "CLOSED"},
                    "then": {"note": None},
                },
            ],
        }
    )

    result = RulesetTester(FunctionRegistry()).test(ruleset)

    assert result.passed is False
    assert result.failure_count == 1
    assert "expected an applied value None" in result.cases[0].failures[0]
    assert result.cases[1].passed is True
