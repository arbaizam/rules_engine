"""
Databricks system test for rules_engine.

This test creates an isolated schema, creates package-owned Delta tables,
registers standard functions, publishes a ruleset, evaluates Spark runtime
output, retires the ruleset, verifies it is no longer loadable, and drops the
schema in a ``finally`` block.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rules_engine import (
    FunctionRegistry,
    PublishService,
    RulesetNormalizer,
    SparkRulesEngineRuntime,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    register_standard_functions,
    standard_function_rows,
)
from rules_engine.exceptions import RepositoryError
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository


CATALOG = os.getenv("RULES_ENGINE_SYSTEM_TEST_CATALOG", "main")
SCHEMA_PREFIX = os.getenv("RULES_ENGINE_SYSTEM_TEST_SCHEMA_PREFIX", "rules_engine_system_test")


def safe_name(value: str) -> str:
    """Return a catalog-safe identifier fragment."""
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()


def unique_schema() -> str:
    """Return a unique schema name for one system test run."""
    stamp = safe_name(datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f"))
    return f"{CATALOG}.{SCHEMA_PREFIX}_{stamp}"


def build_ruleset():
    """Create a ruleset that exercises standard functions and Spark aggregates."""
    return YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "system_test_ruleset",
            "ruleset_name": "System Test Ruleset",
            "version": "1.0.0",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "substring_and_group_total",
                    "rule_name": "Substring And Group Total",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "substring",
                                        "args": {
                                            "value": {"field": "account_code"},
                                            "start": 1,
                                            "length": 3,
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": "ABC"},
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
                    "assign": {
                        "review_bucket": "system_match",
                        "account_prefix": {
                            "custom_function": {
                                "name": "left",
                                "args": {
                                    "value": {"field": "account_code"},
                                    "length": 3,
                                },
                            }
                        },
                    },
                }
            ],
        }
    )


def assert_equal(actual, expected, label: str) -> None:
    """Raise an assertion error with a useful label."""
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def run_system_test(spark: SparkSession) -> None:
    """Run the full Databricks system test workflow."""
    schema = unique_schema()
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        table_names = RulesEngineTableNames.from_schema(schema)
        repository = SparkDeltaRulesetRepository(spark, table_names)
        repository.create_base_tables(mode="error")

        registry = register_standard_functions(FunctionRegistry())
        repository.save_function_registry_rows(standard_function_rows())

        ruleset = RulesetNormalizer().normalize_ruleset(build_ruleset())
        validator = SparkRulesetCompatibilityValidator(registry)
        validation = validator.validate(ruleset)
        if validation.has_errors():
            raise AssertionError(validation.to_text())

        publish_service = PublishService(
            repository=repository,
            validator=validator,
            normalizer=RulesetNormalizer(),
        )
        publish_service.publish(ruleset, published_by="system-test")

        version_rows = spark.table(table_names.ruleset_versions).collect()
        assert_equal(len(version_rows), 1, "published row count")
        assert_equal(version_rows[0]["status"], "published", "published status")
        assert_equal(version_rows[0]["published_by"], "system-test", "published_by")

        loaded = repository.load_published("System Test Ruleset", version="1.0.0")
        input_df = spark.createDataFrame(
            [
                {"account": "A", "account_code": "ABC001", "amount": 60},
                {"account": "A", "account_code": "ABC002", "amount": 50},
                {"account": "B", "account_code": "XYZ001", "amount": 200},
            ]
        )
        output_df = SparkRulesEngineRuntime(repository, registry).evaluate_dataframe(
            input_df,
            loaded,
            fail_on_error=True,
        )

        if output_df.where(F.col("rules_engine_error").isNotNull()).count() != 0:
            raise AssertionError("Unexpected rules_engine_error rows.")
        assert_equal(
            output_df.where(F.col("rules_engine_matched")).count(),
            2,
            "matched row count",
        )
        assigned_prefixes = {
            row["rules_engine_assign"]
            for row in output_df.where(F.col("rules_engine_matched")).collect()
        }
        if not all('"account_prefix": "ABC"' in value for value in assigned_prefixes):
            raise AssertionError(f"Unexpected assignment payloads: {assigned_prefixes}")

        repository.retire("system_test_ruleset", "1.0.0", retired_by="system-test")
        retired = spark.table(table_names.ruleset_versions).collect()[0]
        assert_equal(retired["status"], "retired", "retired status")
        assert_equal(retired["retired_by"], "system-test", "retired_by")

        try:
            repository.load_published("System Test Ruleset", version="1.0.0")
        except RepositoryError:
            pass
        else:
            raise AssertionError("Retired ruleset was still loadable as published.")

        print(f"Rules engine system test passed: schema={schema}")
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


if __name__ == "__main__":
    run_system_test(SparkSession.builder.getOrCreate())
