"""
Strict metadata-first rules engine.

The package compiles canonical YAML rulesets into dataclasses, validates the
semantic contract, and persists fully explicit metadata rows for Databricks
Delta tables. Production runtime evaluation is exposed through
``SparkRulesEngineRuntime``.
"""

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exporter_yaml import YamlRulesetExporter
from rules_engine.models import Ruleset
from rules_engine.registry import (
    CustomFunctionArgSpec,
    CustomFunctionSpec,
    FunctionRegistry,
)
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.standard_functions import (
    register_standard_functions,
    standard_function_rows,
)
from rules_engine.validator import RulesetValidator
from rules_engine.version import __version__

_LAZY_EXPORTS = {
    "DataFrameEvaluation": (
        "rules_engine.dataframe_evaluation",
        "DataFrameEvaluation",
    ),
    "PublishService": ("rules_engine.publish", "PublishService"),
    "RulesEngineService": ("rules_engine.service", "RulesEngineService"),
    "SparkRulesEngineRuntime": ("rules_engine.spark_runtime", "SparkRulesEngineRuntime"),
    "required_source_columns": (
        "rules_engine.spark_runtime",
        "required_source_columns",
    ),
    "SparkRulesetCompatibilityValidator": (
        "rules_engine.spark_validator",
        "SparkRulesetCompatibilityValidator",
    ),
    "CoverageReport": ("rules_engine.analytics", "CoverageReport"),
    "RuleCoverage": ("rules_engine.analytics", "RuleCoverage"),
}


def __getattr__(name: str):
    """
    Lazily import Spark-backed exports to keep compile-only import paths light.
    """
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = _LAZY_EXPORTS[name]
    from importlib import import_module

    attribute = getattr(import_module(module_name), attribute_name)
    globals()[name] = attribute
    return attribute


__all__ = [
    "CustomFunctionSpec",
    "CustomFunctionArgSpec",
    "CoverageReport",
    "DataFrameEvaluation",
    "DeltaRowSerializer",
    "FunctionRegistry",
    "PublishService",
    "RulesEngineService",
    "Ruleset",
    "RuleCoverage",
    "RulesetValidator",
    "SparkRulesEngineRuntime",
    "SparkRulesetCompatibilityValidator",
    "YamlRulesetCompiler",
    "YamlRulesetExporter",
    "register_standard_functions",
    "required_source_columns",
    "standard_function_rows",
    "__version__",
]
