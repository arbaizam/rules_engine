"""
Public service facade for common Spark rules engine workflows.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from rules_engine.analytics import CoverageReport, RulesetCoverageAnalyzer
from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.dataframe_evaluation import DataFrameEvaluation
from rules_engine.human_readable import HumanReadableRulesetFormatter
from rules_engine.models import FunctionRegistryRow, Ruleset
from rules_engine.publish import PublishService
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository
from rules_engine.spark_runtime import SparkRulesEngineRuntime
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.standard_functions import (
    register_standard_functions,
    standard_function_rows,
)


class RulesEngineService:
    """
    Convenience facade for package-owned Spark/Delta rules engine workflows.

    The service wires the standard repository, registry, validator, publish
    service, and Spark runtime. It does not own external logging,
    archive/drop-zone orchestration, or implicit table creation.
    """

    def __init__(
        self,
        *,
        repository: SparkDeltaRulesetRepository,
        registry: FunctionRegistry,
        validator: SparkRulesetCompatibilityValidator | None = None,
    ) -> None:
        """
        Create a service from explicitly supplied components.
        """
        self.repository = repository
        self.registry = registry
        self.validator = validator or SparkRulesetCompatibilityValidator(registry)
        self.publish_service = PublishService(
            repository=repository,
            validator=self.validator,
        )
        self.runtime = SparkRulesEngineRuntime(
            repository,
            registry,
            compatibility_validator=self.validator,
        )
        self.compiler = YamlRulesetCompiler()
        self.rule_formatter = HumanReadableRulesetFormatter()
        self.coverage_analyzer = RulesetCoverageAnalyzer(self.runtime)

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
    ) -> None:
        """
        Validate and persist a published ruleset.
        """
        self.publish_service.publish(
            ruleset,
            published_by=published_by,
        )

    def publish_yaml_text(
        self,
        yaml_text: str,
        *,
        published_by: str | None = None,
    ) -> Ruleset:
        """
        Compile YAML text, publish it, and return the compiled ruleset.
        """
        ruleset = self.compile_yaml_text(yaml_text)
        self.publish(
            ruleset,
            published_by=published_by,
        )
        return ruleset

    def publish_yaml_path(
        self,
        path: str | Path,
        *,
        published_by: str | None = None,
    ) -> Ruleset:
        """
        Compile a YAML file, publish it, and return the compiled ruleset.
        """
        ruleset = self.compile_yaml_path(path)
        self.publish(
            ruleset,
            published_by=published_by,
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
        ruleset = self._resolve_ruleset(ruleset, ruleset_name, version)
        return self.rule_formatter.describe_rules(ruleset)

    def evaluate_dataframe(
        self,
        df: DataFrame,
        *,
        ruleset: Ruleset | None = None,
        ruleset_name: str | None = None,
        version: str | None = None,
        key_columns: Sequence[str] | None = None,
        column_prefix: str = "rules_engine",
        fail_on_error: bool = True,
        include_error_traceback: bool = False,
        full_audit: bool = False,
    ) -> DataFrameEvaluation:
        """
        Evaluate keyed Spark rows using a supplied or loaded ruleset.

        When ``key_columns`` is omitted, every input column is used so the
        result projection retains the complete source record. Pass explicit
        keys when rules overwrite existing columns or a compact result is
        preferred.
        """
        ruleset = self._resolve_ruleset(ruleset, ruleset_name, version)
        return self.runtime.evaluate_dataframe(
            df,
            ruleset,
            key_columns=key_columns,
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
        """Report match coverage and clean no-match rows.

        Computing aggregate counts starts one Spark action. The returned
        no-match DataFrame is a filtered view of that evaluation.
        """
        ruleset = self._resolve_ruleset(ruleset, ruleset_name, version)
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
    ) -> None:
        """
        Retire a persisted ruleset version.
        """
        self.repository.retire(
            ruleset_id,
            version,
            retired_by=retired_by,
        )

    def _resolve_ruleset(
        self,
        ruleset: Ruleset | None,
        ruleset_name: str | None,
        version: str | None,
    ) -> Ruleset:
        """Use a supplied ruleset or load the requested published version."""
        if ruleset is not None:
            return ruleset
        if ruleset_name is None:
            raise ValueError("ruleset or ruleset_name is required.")
        return self.load_published(ruleset_name, version)
