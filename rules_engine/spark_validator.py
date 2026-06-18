"""
Spark compatibility validation for ruleset metadata.

Aggregate operands are no longer part of the supported metadata contract, so
the Spark runtime currently shares the base ruleset validation rules.
"""

from __future__ import annotations

from rules_engine.validator import RulesetValidator


class SparkRulesetCompatibilityValidator(RulesetValidator):
    """Validate a ruleset for the current Spark DataFrame runtime."""
