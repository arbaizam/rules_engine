"""Keep the public projection helper aligned with Spark's prepared input schema."""

from decimal import Decimal

import pytest
from pyspark.sql import types as T

from rules_engine.enums import ComparisonOperator, LogicalOperator
from rules_engine.models import (
    AssignedOperand,
    Assignment,
    Condition,
    ConditionGroup,
    CustomFunctionOperand,
    FieldOperand,
    LiteralOperand,
    Rule,
    Ruleset,
)
from rules_engine.registry import CustomFunctionArgSpec, CustomFunctionSpec, FunctionRegistry
from rules_engine.spark_runtime import required_source_columns
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator


def _condition(identifier, left, right=None, *, active=True):
    return Condition(
        identifier,
        left,
        ComparisonOperator.EQ,
        LiteralOperand("x") if right is None else right,
        Decimal(0),
        active_flag=active,
    )


@pytest.mark.parametrize("reverse_metadata", [False, True])
def test_required_columns_match_prepared_schema_across_active_operand_trees(reverse_metadata):
    """Projection preserves traversal order without including retired or literal data."""
    registry = FunctionRegistry()
    registry.register(
        CustomFunctionSpec(
            "collect",
            "tests.collect",
            (CustomFunctionArgSpec("value"),),
            True,
            True,
            return_type_hint="string",
        )
    )
    nested = CustomFunctionOperand(
        "collect",
        {
            "value": {
                "field": FieldOperand("alpha", LiteralOperand("fallback")),
                "nested": [
                    CustomFunctionOperand(
                        "collect",
                        {
                            "value": (
                                FieldOperand("beta"),
                                AssignedOperand("prior", LiteralOperand("fallback")),
                                LiteralOperand({"field": "literal_data"}),
                            )
                        },
                        default_if_null=LiteralOperand("fallback"),
                    ),
                    FieldOperand("alpha"),
                ],
                "set": {FieldOperand("gamma"), FieldOperand("delta")},
                "fallback": LiteralOperand(
                    None, default_if_null=LiteralOperand({"field": "fallback_data"})
                ),
            }
        },
    )
    producer = Rule(
        "producer",
        "Producer",
        10,
        ConditionGroup(
            "producer_group",
            LogicalOperator.ALL,
            (_condition("producer_condition", FieldOperand("z_source")),),
        ),
        (Assignment("produce", "prior", FieldOperand("seed")),),
    )
    consumer = Rule(
        "consumer",
        "Consumer",
        20,
        ConditionGroup(
            "consumer_group",
            LogicalOperator.ALL,
            (
                _condition("root", FieldOperand("a_root")),
                _condition("assigned", AssignedOperand("prior"), FieldOperand("comparison")),
                _condition("inactive", FieldOperand("inactive_condition"), active=False),
            ),
            (
                ConditionGroup(
                    "nested_group",
                    LogicalOperator.ANY,
                    (_condition("nested", nested, FieldOperand("right_side")),),
                ),
            ),
        ),
        (
            Assignment("copy", "copied", AssignedOperand("prior")),
            Assignment("tail", "output", FieldOperand("tail")),
            Assignment("duplicate", "duplicate", FieldOperand("alpha")),
        ),
    )
    retired = Rule(
        "retired",
        "Retired",
        1,
        ConditionGroup(
            "retired_group",
            LogicalOperator.ALL,
            (_condition("retired", FieldOperand("retired_condition")),),
        ),
        (Assignment("retired_assignment", "retired_output", FieldOperand("retired_source")),),
        active_flag=False,
    )
    rules = (consumer, retired, producer)
    ruleset = Ruleset(
        "projection",
        "Projection",
        "1",
        tuple(reversed(rules)) if reverse_metadata else rules,
        owner="Rules Team",
        owner_department="Engineering",
    )
    expected = (
        "z_source",
        "seed",
        "a_root",
        "comparison",
        "alpha",
        "beta",
        "delta",
        "gamma",
        "right_side",
        "tail",
    )
    # The schema deliberately excludes inactive fields and assignment-only targets.
    schema = T.StructType([T.StructField(name, T.StringType()) for name in reversed(expected)])

    prepared = SparkRulesetCompatibilityValidator(registry).prepare(ruleset, schema)

    assert prepared.validation.passed, prepared.validation.to_text()
    assert required_source_columns(ruleset) == prepared.required_source_columns == expected
