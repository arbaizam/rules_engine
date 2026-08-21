from decimal import Decimal

import pytest
from pyspark.sql import types as T

from rules_engine.change_control import RulesetDiffer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import ValidationFailedError
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.normalizer import RulesetNormalizer
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
        RulesetNormalizer(),
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
        RulesetNormalizer(),
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
        RulesetNormalizer(),
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


# Coverage diagnostics


def test_closest_rule_diagnostic_reports_failed_condition_ids():
    ruleset = YamlRulesetCompiler().compile_payload(_payload())
    evaluator = SparkRowEvaluator(NoOpRepository(), FunctionRegistry())

    diagnostic = evaluator.closest_rule_diagnostic(ruleset, {"fico": 650})

    assert diagnostic == {
        "closest_rule_id": "prime",
        "closest_rule_name": "Prime loans",
        "closest_rule_score": 0.0,
        "passed_condition_count": 0,
        "condition_count": 1,
        "failed_condition_ids": ["fico-prime"],
    }


# Semantic change control


def test_semantic_diff_highlights_order_logic_and_assignment_changes():
    baseline_payload = _payload()
    candidate_payload = _payload(version="2")
    candidate_payload["rules"][0]["rule_order"] = 2
    candidate_payload["rules"][1]["rule_order"] = 1
    candidate_payload["rules"][0]["when"]["all"][0]["right"] = {"literal": 740}
    candidate_payload["rules"][0]["assign"]["bucket"] = "super-prime"
    baseline = YamlRulesetCompiler().compile_payload(baseline_payload)
    candidate = YamlRulesetCompiler().compile_payload(candidate_payload)

    result = RulesetDiffer().diff(baseline, candidate)
    prime = next(item for item in result.rule_diffs if item.rule_id == "prime")
    fields = {change.field for change in prime.changes}

    assert {"rule_order", "when", "assign"} <= fields
    assert "fico >= 720" in result.to_text()
    assert "fico >= 740" in result.to_text()
    assert "bucket = 'super-prime'" in result.to_text()


def test_semantic_diff_detects_operand_default_and_identity_changes():
    baseline_payload = _payload()
    candidate_payload = _payload(version="2")
    candidate_condition = candidate_payload["rules"][0]["when"]["all"][0]
    candidate_condition["condition_id"] = "fico-prime-v2"
    candidate_condition["left"]["default_if_null"] = 720
    baseline = YamlRulesetCompiler().compile_payload(baseline_payload)
    candidate = YamlRulesetCompiler().compile_payload(candidate_payload)

    result = RulesetDiffer().diff(baseline, candidate)
    prime = next(item for item in result.rule_diffs if item.rule_id == "prime")
    fields = {change.field for change in prime.changes}

    assert "condition[fico-prime]" in fields
    assert "condition[fico-prime-v2]" in fields
    assert result.baseline_content_hash == DeltaRowSerializer().content_hash(
        baseline
    )
    assert result.candidate_content_hash == DeltaRowSerializer().content_hash(
        candidate
    )
    assert "default_if_null" in result.to_text()


def test_semantic_diff_renders_only_changed_condition_leaves():
    baseline_payload = _payload()
    candidate_payload = _payload(version="2")
    candidate_payload["rules"][0]["when"]["all"][0]["left"][
        "default_if_null"
    ] = 0
    baseline = YamlRulesetCompiler().compile_payload(baseline_payload)
    candidate = YamlRulesetCompiler().compile_payload(candidate_payload)

    result = RulesetDiffer().diff(baseline, candidate)
    prime = next(item for item in result.rule_diffs if item.rule_id == "prime")
    condition_changes = [
        change
        for change in prime.changes
        if change.field.startswith("condition[fico-prime]")
    ]

    assert [change.field for change in condition_changes] == [
        "condition[fico-prime].left.default_if_null"
    ]
    assert (
        "condition[fico-prime].left.default_if_null: None ->"
        in result.to_text()
    )


def test_semantic_diff_reports_expected_cases_individually_by_name():
    baseline_payload = _payload()
    candidate_payload = _payload(version="2")
    candidate_payload["expect"][0]["then"]["bucket"] = "changed"
    baseline = YamlRulesetCompiler().compile_payload(baseline_payload)
    candidate = YamlRulesetCompiler().compile_payload(candidate_payload)

    result = RulesetDiffer().diff(baseline, candidate)
    fields = {change.field for change in result.metadata_changes}

    assert "expect[prime example].then" in fields
    assert "expected_cases" not in fields


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
        if expected["assign"] is not None:
            expected["assign"] = {
                field: expected["assign"].get(field)
                for field in ("first", "shared", "after")
            }

        assert {
            key: actual[key]
            for key in ("matched", "matched_rule_ids", "assign")
        } == expected
