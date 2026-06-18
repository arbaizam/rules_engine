from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.serializer import DeltaRowSerializer
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_runtime import SparkRulesEngineRuntime
from rules_engine.standard_functions import (
    register_standard_functions,
    standard_function_rows,
    substring,
)
from rules_engine.validator import RulesetValidator


class DummyRepository:
    def load_published(self, ruleset_name, version=None):
        raise NotImplementedError


class FakeSparkRow:
    def __init__(self, data):
        self._data = data

    def asDict(self, recursive=True):
        return self._data


def test_substring_uses_sql_style_start_position():
    """
    What: Verifies substring uses a 1-based start index.
    Why: Databricks/Spark authors commonly expect SQL substring semantics.
    Fails when: substring behaves like Python zero-based slicing.
    """
    assert substring("ABCDE", 2, 3) == "BCD"


def test_standard_functions_can_be_registered_for_runtime_field_args():
    """
    What: Registers standard functions and evaluates substring against row fields.
    Why: Common custom functions must be usable with dynamic row values in rules.
    Fails when: custom_function args remain literal-only metadata.
    """
    registry = register_standard_functions(FunctionRegistry())
    ruleset = YamlRulesetCompiler().compile_payload(
        {
            "ruleset_id": "rs1",
            "ruleset_name": "Ruleset",
            "version": "1",
            "owner": "Rules Team",
            "owner_department": "ALM Engineering",
            "rules": [
                {
                    "rule_id": "r1",
                    "rule_name": "Substring rule",
                    "rule_order": 1,
                    "when": {
                        "all": [
                            {
                                "left": {
                                    "custom_function": {
                                        "name": "substring",
                                        "args": {
                                            "value": {"field": "account_code"},
                                            "start": 2,
                                            "length": 3,
                                        },
                                    }
                                },
                                "operator": "eq",
                                "right": {"literal": "BCD"},
                                "null_input_mode": "propagate",
                                "null_result_mode": "null",
                            }
                        ]
                    },
                    "assign": {
                        "account_prefix": {
                            "custom_function": {
                                "name": "left",
                                "args": {
                                    "value": {"field": "account_code"},
                                    "length": 2,
                                },
                            }
                        }
                    },
                }
            ],
        }
    )

    validation = RulesetValidator(registry).validate(ruleset)
    row = DeltaRowSerializer().serialize_ruleset_version(ruleset)
    evaluator = SparkRulesEngineRuntime(
        DummyRepository(),
        registry,
    )._build_row_evaluator(
        ruleset,
        ["account_prefix"],
        {"account_prefix"},
    )
    output = evaluator(FakeSparkRow({"account_code": "ABCDE"}))

    assert validation.passed
    assert '"field":"account_code"' in row.payload_json
    assert output["matched"] is True
    assert output["assign"] == {"account_prefix": "AB"}


def test_standard_function_rows_expose_registry_metadata():
    """
    What: Creates persisted metadata rows for standard functions.
    Why: Production workflows can save function specs without hand-authoring them.
    Fails when: standard function specs cannot be written to the registry table.
    """
    rows = standard_function_rows()
    names = {row.function_name for row in rows}

    assert "substring" in names
    assert "regex_extract" in names
