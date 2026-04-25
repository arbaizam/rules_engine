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
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import RulesEngineRuntime
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.validator import RulesetValidator

_LAZY_EXPORTS = {
    "PublishService": ("rules_engine.publish", "PublishService"),
    "SparkRulesEngineRuntime": ("rules_engine.spark_runtime", "SparkRulesEngineRuntime"),
    "SparkRulesetCompatibilityValidator": (
        "rules_engine.spark_validator",
        "SparkRulesetCompatibilityValidator",
    ),
}


def __getattr__(name: str):
    """
    Lazily import Spark-backed exports so pure-Python use does not require PySpark.
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
    "DeltaRowSerializer",
    "FunctionRegistry",
    "PublishService",
    "RulesEngineRuntime",
    "Ruleset",
    "RulesetNormalizer",
    "RulesetValidator",
    "SparkRulesEngineRuntime",
    "SparkRulesetCompatibilityValidator",
    "YamlRulesetCompiler",
    "YamlRulesetExporter",
]
