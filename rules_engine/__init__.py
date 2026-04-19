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
from rules_engine.publish import PublishService
from rules_engine.registry import CustomFunctionSpec, FunctionRegistry
from rules_engine.runtime import RulesEngineRuntime
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.spark_runtime import SparkRulesEngineRuntime
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.validator import RulesetValidator

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
