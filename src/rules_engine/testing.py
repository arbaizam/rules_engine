"""Pure-Python executable ruleset examples and publish-gate results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from rules_engine.models import Ruleset, RulesetExpectation
from rules_engine.registry import FunctionRegistry
from rules_engine.runtime import SparkRowEvaluator


@dataclass(frozen=True)
class ExpectationCaseResult:
    """Outcome of one embedded expected case."""

    name: str
    passed: bool
    expected: Mapping[str, Any]
    actual: Mapping[str, Any]
    failures: tuple[str, ...] = ()


@dataclass(frozen=True)
class RulesetTestResult:
    """Aggregate outcome of all expected cases in a ruleset."""

    cases: tuple[ExpectationCaseResult, ...]

    @property
    def passed(self) -> bool:
        return all(case.passed for case in self.cases)

    @property
    def failure_count(self) -> int:
        return sum(not case.passed for case in self.cases)

    def to_text(self) -> str:
        if not self.cases:
            return "Ruleset has no expected cases."
        lines = [
            f"Expected cases passed: {self.passed} "
            f"({len(self.cases) - self.failure_count}/{len(self.cases)})"
        ]
        for case in self.cases:
            status = "PASS" if case.passed else "FAIL"
            detail = f": {'; '.join(case.failures)}" if case.failures else ""
            lines.append(f"[{status}] {case.name}{detail}")
        return "\n".join(lines)


class RulesetTester:
    """Execute ruleset metadata examples without requiring Spark."""

    _RESERVED_RESULTS = frozenset({"matched", "matched_rule_ids", "assign"})

    def __init__(self, function_registry: FunctionRegistry) -> None:
        self._evaluator = SparkRowEvaluator.for_embedded_ruleset(
            function_registry
        )

    def test(self, ruleset: Ruleset) -> RulesetTestResult:
        """Run every expected case in declaration order."""
        return RulesetTestResult(
            tuple(self._test_case(ruleset, case) for case in ruleset.expect)
        )

    def _test_case(
        self,
        ruleset: Ruleset,
        case: RulesetExpectation,
    ) -> ExpectationCaseResult:
        key_failures = self._validate_expected_keys(ruleset, case.then)
        if key_failures:
            return ExpectationCaseResult(
                name=case.name,
                passed=False,
                expected=case.then,
                actual={},
                failures=tuple(key_failures),
            )
        try:
            actual = self._evaluator.evaluate_row(ruleset, case.given)
            failures = self._compare(case.then, actual)
        except Exception as exc:  # noqa: BLE001 - user function failures are test results
            actual = {"error": f"{type(exc).__name__}: {exc}"}
            failures = [f"evaluation raised {actual['error']}"]
        return ExpectationCaseResult(
            name=case.name,
            passed=not failures,
            expected=case.then,
            actual=actual,
            failures=tuple(failures),
        )

    def _validate_expected_keys(
        self,
        ruleset: Ruleset,
        expected: Mapping[str, Any],
    ) -> list[str]:
        """Reject misspelled assignment fields before evaluating the case."""
        known_targets = {
            assignment.target_field
            for rule in ruleset.rules
            for assignment in rule.assignments
        }
        shorthand = set(expected) - self._RESERVED_RESULTS
        explicit_assign = expected.get("assign")
        explicit = (
            set(explicit_assign)
            if isinstance(explicit_assign, Mapping)
            else set()
        )
        unknown = sorted(
            (shorthand | explicit) - known_targets,
            key=str,
        )
        if not unknown:
            return []
        return [
            "unknown expected assignment key(s) "
            f"{unknown}; known target fields are {sorted(known_targets)}"
        ]

    def _compare(
        self,
        expected: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> list[str]:
        failures: list[str] = []
        for key in ("matched", "matched_rule_ids"):
            if key in expected and actual.get(key) != expected[key]:
                failures.append(
                    f"{key} expected {expected[key]!r}, got {actual.get(key)!r}"
                )
        expected_assign = expected.get("assign")
        if expected_assign is not None:
            if not isinstance(expected_assign, Mapping):
                failures.append("assign expectation must be a mapping")
            else:
                failures.extend(
                    self._compare_assignment_subset(expected_assign, actual.get("assign"))
                )
        shorthand = {
            key: value
            for key, value in expected.items()
            if key not in self._RESERVED_RESULTS
        }
        failures.extend(
            self._compare_assignment_subset(shorthand, actual.get("assign"))
        )
        return failures

    def _compare_assignment_subset(
        self,
        expected: Mapping[str, Any],
        actual: Any,
    ) -> list[str]:
        if not expected:
            return []
        if not isinstance(actual, Mapping):
            return [f"assign expected fields {sorted(expected)}, got {actual!r}"]
        failures: list[str] = []
        for field_name, expected_value in expected.items():
            outcome = actual.get(field_name)
            if not isinstance(outcome, Mapping) or outcome.get("applied") is not True:
                failures.append(
                    f"assign.{field_name} expected an applied value "
                    f"{expected_value!r}, got {outcome!r}"
                )
            elif outcome.get("value") != expected_value:
                failures.append(
                    f"assign.{field_name} expected {expected_value!r}, "
                    f"got {outcome.get('value')!r}"
                )
        return failures
