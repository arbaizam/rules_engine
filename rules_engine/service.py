"""
Public service facade for common Spark rules engine workflows.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from rules_engine.analytics import CoverageReport, RulesetCoverageAnalyzer
from rules_engine.change_control import RulesetDiff, RulesetDiffer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import FunctionRegistryRow, Ruleset
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.publish import PublishService
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
from rules_engine.spark_runtime import SparkRulesEngineRuntime
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.standard_functions import (
    register_standard_functions,
    standard_function_rows,
)
from rules_engine.testing import RulesetTester, RulesetTestResult


class RulesEngineService:
    """
    Convenience facade for package-owned Spark/Delta rules engine workflows.

    The service wires the standard repository, registry, validator, normalizer,
    publish service, and Spark runtime. It does not own external logging,
    archive/drop-zone orchestration, or implicit table creation.
    """

    def __init__(
        self,
        *,
        repository: SparkDeltaRulesetRepository,
        registry: FunctionRegistry,
        normalizer: RulesetNormalizer | None = None,
        validator: SparkRulesetCompatibilityValidator | None = None,
    ) -> None:
        """
        Create a service from explicitly supplied components.
        """
        self.repository = repository
        self.registry = registry
        self.normalizer = normalizer or RulesetNormalizer()
        self.validator = validator or SparkRulesetCompatibilityValidator(registry)
        self.tester = RulesetTester(registry)
        self.publish_service = PublishService(
            repository=repository,
            validator=self.validator,
            normalizer=self.normalizer,
            tester=self.tester,
        )
        self.runtime = SparkRulesEngineRuntime(
            repository,
            registry,
            compatibility_validator=self.validator,
        )
        self.compiler = YamlRulesetCompiler()
        self.rule_formatter = HumanReadableRulesetFormatter()
        self.ruleset_differ = RulesetDiffer()
        self.coverage_analyzer = RulesetCoverageAnalyzer(self.runtime, registry)

    @classmethod
    def from_schema(
        cls,
        spark: SparkSession,
        schema: str,
        *,
        ruleset_versions_table: str | None = None,
        function_registry_table: str | None = None,
        register_standard: bool = True,
    ) -> RulesEngineService:
        """
        Build a service using metadata tables under a schema.

        By default, table names use the standard package footprint. Callers may
        override either table name when an environment uses custom metadata
        table names.
        """
        default_table_names = RulesEngineTableNames.from_schema(schema)
        table_names = RulesEngineTableNames(
            ruleset_versions=ruleset_versions_table or default_table_names.ruleset_versions,
            function_registry=function_registry_table or default_table_names.function_registry,
        )
        repository = SparkDeltaRulesetRepository(spark, table_names)
        registry = FunctionRegistry()
        if register_standard:
            register_standard_functions(registry)
        return cls(repository=repository, registry=registry)

    @property
    def table_names(self) -> RulesEngineTableNames:
        """
        Return the configured Delta metadata table names.
        """
        return self.repository.table_names

    def create_tables(self, mode: str = "error") -> None:
        """
        Create package-owned metadata tables.
        """
        self.repository.create_base_tables(mode=mode)

    def save_standard_function_registry(self, *, update_existing: bool = True) -> None:
        """
        Save standard function metadata rows to the function registry table.

        Existing package-owned rows are updated by default so their persisted
        contracts stay aligned with the installed implementation version.
        """
        self.repository.save_function_registry_rows(
            standard_function_rows(),
            update_existing=update_existing,
        )

    def save_function_registry_rows(
        self,
        rows: list[FunctionRegistryRow],
        *,
        update_existing: bool = True,
    ) -> None:
        """
        Save supplied function metadata rows to the function registry table.
        """
        self.repository.save_function_registry_rows(
            rows,
            update_existing=update_existing,
        )

    def compile_yaml_text(self, yaml_text: str) -> Ruleset:
        """
        Compile YAML text into a ruleset model.
        """
        return self.compiler.compile_text(yaml_text)

    def compile_yaml_path(self, path: str | Path) -> Ruleset:
        """
        Compile a YAML file into a ruleset model.
        """
        return self.compiler.compile_path(path)

    def publish(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> None:
        """
        Validate, normalize, and persist a published ruleset.
        """
        self.publish_service.publish(
            ruleset,
            published_by=published_by,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
        )

    def test_ruleset(self, ruleset: Ruleset) -> RulesetTestResult:
        """Execute embedded expected cases without starting Spark."""
        return self.tester.test(ruleset)

    def publish_yaml_text(
        self,
        yaml_text: str,
        *,
        published_by: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> Ruleset:
        """
        Compile YAML text, publish it, and return the compiled ruleset.
        """
        ruleset = self.compile_yaml_text(yaml_text)
        self.publish(
            ruleset,
            published_by=published_by,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
        )
        return ruleset

    def publish_yaml_path(
        self,
        path: str | Path,
        *,
        published_by: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> Ruleset:
        """
        Compile a YAML file, publish it, and return the compiled ruleset.
        """
        ruleset = self.compile_yaml_path(path)
        self.publish(
            ruleset,
            published_by=published_by,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
        )
        return ruleset

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        return self.repository.load_published(ruleset_name, version)

    def describe_rules(
        self,
        *,
        ruleset: Ruleset | None = None,
        ruleset_name: str | None = None,
        version: str | None = None,
    ) -> list[dict[str, str]]:
        """
        Return readable rule metadata rows for a supplied or loaded ruleset.
        """
        if ruleset is None:
            if ruleset_name is None:
                raise ValueError("ruleset or ruleset_name is required.")
            ruleset = self.load_published(ruleset_name, version)
        return self.rule_formatter.describe_rules(ruleset)

    def diff_rulesets(
        self,
        baseline: Ruleset,
        candidate: Ruleset,
    ) -> RulesetDiff:
        """Compare two supplied rulesets using author-facing rule syntax."""
        return self.ruleset_differ.diff(baseline, candidate)

    def diff_versions(
        self,
        ruleset_name: str,
        baseline_version: str,
        candidate_version: str,
    ) -> RulesetDiff:
        """Load and compare two published versions of the same ruleset."""
        return self.diff_rulesets(
            self.load_published(ruleset_name, baseline_version),
            self.load_published(ruleset_name, candidate_version),
        )

    def evaluate_dataframe(
        self,
        df: DataFrame,
        *,
        ruleset: Ruleset | None = None,
        ruleset_name: str | None = None,
        version: str | None = None,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> DataFrame:
        """
        Evaluate a Spark DataFrame using a supplied or loaded ruleset.
        """
        if ruleset is None:
            if ruleset_name is None:
                raise ValueError("ruleset or ruleset_name is required.")
            ruleset = self.load_published(ruleset_name, version)
        return self.runtime.evaluate_dataframe(
            df,
            ruleset,
            column_prefix=column_prefix,
            fail_on_error=fail_on_error,
            include_error_traceback=include_error_traceback,
            full_audit=full_audit,
        )

    def coverage_report(
        self,
        df: DataFrame,
        *,
        ruleset: Ruleset | None = None,
        ruleset_name: str | None = None,
        version: str | None = None,
        broad_match_threshold: float = 0.40,
        column_prefix: str = "rules_engine_coverage",
    ) -> CoverageReport:
        """Report coverage and reevaluated closest-rule diagnostics.

        Computing aggregate counts starts one Spark action. The returned
        no-match DataFrame is lazy; materializing it reevaluates the rules and
        invokes custom condition functions again for closest-rule scoring.
        """
        if ruleset is None:
            if ruleset_name is None:
                raise ValueError("ruleset or ruleset_name is required.")
            ruleset = self.load_published(ruleset_name, version)
        return self.coverage_analyzer.analyze(
            df,
            ruleset,
            broad_match_threshold=broad_match_threshold,
            column_prefix=column_prefix,
        )

    def retire(
        self,
        ruleset_id: str,
        version: str,
        *,
        retired_by: str | None = None,
        effective_end_date: str | None = None,
    ) -> None:
        """
        Retire a persisted ruleset version.
        """
        self.repository.retire(
            ruleset_id,
            version,
            retired_by=retired_by,
            effective_end_date=effective_end_date,
        )
