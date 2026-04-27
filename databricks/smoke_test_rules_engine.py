"""
Databricks smoke test for rules_engine.

Run this after installing or copying the package into a Databricks cluster.
It creates/overwrites a small set of smoke-test Delta tables, publishes one
ruleset, evaluates a Spark DataFrame, and verifies retire/load behavior.

Warning
-------
This script calls ``create_base_tables(mode="overwrite")`` and destroys the
target smoke-test metadata tables. Use only disposable database/schema/table
prefixes.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rules_engine import (
    FunctionRegistry,
    PublishService,
    RulesetNormalizer,
    SparkRulesEngineRuntime,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
)
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


DATABASE = os.getenv("RULES_ENGINE_SMOKE_DATABASE", "default")
TABLE_PREFIX = os.getenv(
    "RULES_ENGINE_SMOKE_TABLE_PREFIX",
    "rules_engine_smoke_test_deleteme",
)


def table_name(name: str) -> str:
    """Build a fully qualified smoke-test table name."""
    return f"{DATABASE}.{TABLE_PREFIX}_{name}"


def build_table_names() -> RulesEngineTableNames:
    """Return the Delta table names used by this smoke test."""
    return RulesEngineTableNames(
        ruleset_versions=table_name("ruleset_versions"),
        function_registry=table_name("function_registry"),
    )


def build_ruleset():
    """Create one small Spark-compatible published ruleset."""
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "smoke_ruleset",
            "ruleset_name": "Smoke Ruleset",
            "version": "1",
            "status": "published",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "high_value_open",
                    "rule_name": "High Value Open",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {"field": "status"},
                                "operator": "eq",
                                "right": {"literal": "OPEN", "value_type": "string"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                            {
                                "left": {
                                    "aggregate": {
                                        "function": "sum",
                                        "field": "amount",
                                        "scope": "group",
                                        "by": ["account"],
                                        "null_input_mode": "ignore",
                                        "null_result_mode": "null",
                                    }
                                },
                                "operator": "gt",
                                "right": {"literal": 100, "value_type": "number"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            },
                        ]
                    },
                    "assign": {"review_bucket": "high_value_open"},
                }
            ],
        }
    )


def run_smoke_test(spark_session) -> None:
    """Run the full Databricks smoke workflow."""
    table_names = build_table_names()
    repository = SparkDeltaRulesetRepository(spark_session, table_names)
    repository.create_base_tables(mode="overwrite")

    registry = FunctionRegistry()
    validator = SparkRulesetCompatibilityValidator(registry)
    publish_service = PublishService(
        repository=repository,
        validator=validator,
        normalizer=RulesetNormalizer(),
    )

    ruleset = build_ruleset()
    publish_service.publish(ruleset)
    loaded = repository.load_published("Smoke Ruleset", version="1")

    input_df = spark_session.createDataFrame(
        [
            {"account": "A", "status": "OPEN", "amount": 60},
            {"account": "A", "status": "OPEN", "amount": 50},
            {"account": "B", "status": "OPEN", "amount": 10},
            {"account": "C", "status": "CLOSED", "amount": 500},
        ]
    )
    output_df = SparkRulesEngineRuntime(repository, registry).evaluate_dataframe(
        input_df,
        loaded,
        fail_on_error=True,
    )

    if output_df.where(F.col("rules_engine_error").isNotNull()).count() != 0:
        raise AssertionError("Unexpected rules_engine_error rows.")
    matched_count = output_df.where(F.col("rules_engine_matched")).count()
    if matched_count != 2:
        raise AssertionError(f"Expected 2 matched rows, got {matched_count}.")

    repository.retire("smoke_ruleset", "1", retired_by="smoke_test")
    try:
        repository.load_published("Smoke Ruleset", version="1")
    except RepositoryError:
        return
    raise AssertionError("Retired ruleset was still loadable as published.")


if __name__ == "__main__":
    run_smoke_test(SparkSession.builder.getOrCreate())
