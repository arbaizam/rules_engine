"""
Translate legacy rules_engine_old YAML rulesets into canonical v1 YAML.

This utility is intentionally separate from runtime execution. It converts the
old account-key ruleset authoring shape into the current canonical YAML format
so the output can be reviewed and manually refined before publish.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.registry import FunctionRegistry
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.validator import RulesetValidator


OPERATOR_MAP = {
    "=": "eq",
    "!=": "ne",
    "contains": "contains",
    "in": "in",
}

TYPE_MAP = {
    "string": "string",
    "integer": "number",
    "number": "number",
    "float": "number",
    "decimal": "number",
    "boolean": "boolean",
}


class LegacyRulesetTranslationError(ValueError):
    """Raised when a legacy ruleset cannot be translated deterministically."""


class LegacyRulesetTranslator:
    """
    Translate legacy YAML payloads into canonical rules engine YAML payloads.
    """

    def translate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Translate one parsed legacy ruleset payload.
        """
        rules = payload.get("rules")
        if not isinstance(rules, list):
            raise LegacyRulesetTranslationError("Legacy ruleset must contain a rules list.")

        return {
            "ruleset_id": self._required(payload, "rule_set_id"),
            "ruleset_name": self._required(payload, "rule_set_name"),
            "version": str(payload.get("version", "1")),
            "status": "draft",
            "description": payload.get("description"),
            "rules": [
                self._translate_rule(rule, index)
                for index, rule in enumerate(sorted(rules, key=self._rule_order), start=1)
            ],
        }

    def translate_file(self, source_path: str | Path) -> dict[str, Any]:
        """
        Translate one legacy YAML file.
        """
        path = Path(source_path)
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise LegacyRulesetTranslationError(f"Legacy YAML must be a mapping: {path}")
        return self.translate_payload(payload)

    def _translate_rule(self, rule: dict[str, Any], index: int) -> dict[str, Any]:
        rule_id = self._required(rule, "rule_id")
        return {
            "rule_id": rule_id,
            "rule_name": self._required(rule, "rule_name"),
            "rule_order": int(rule.get("priority", index)),
            "active_flag": bool(rule.get("enabled", True)),
            "stop_on_match": True,
            "when": self._translate_group(self._required(rule, "when")),
            "assign": self._translate_assign(rule),
        }

    def _translate_group(self, payload: dict[str, Any]) -> dict[str, Any]:
        logical_keys = [key for key in ("all", "any") if key in payload]
        if len(logical_keys) != 1:
            raise LegacyRulesetTranslationError(
                f"Legacy condition group must define exactly one of all/any: {payload}"
            )
        logical_key = logical_keys[0]
        items = payload[logical_key]
        if not isinstance(items, list) or not items:
            raise LegacyRulesetTranslationError(
                f"Legacy condition group {logical_key} must contain a non-empty list."
            )
        return {
            logical_key: [
                self._translate_group(item) if self._is_group(item) else self._translate_condition(item)
                for item in items
            ]
        }

    def _translate_condition(self, condition: dict[str, Any]) -> dict[str, Any]:
        operator = self._required(condition, "op")
        if operator not in OPERATOR_MAP:
            raise LegacyRulesetTranslationError(f"Unsupported legacy operator: {operator}")

        left = self._required(condition, "left")
        right = self._required(condition, "right")

        if self._is_substring_operand(left):
            return self._translate_substring_condition(left, operator, right)

        return {
            "left": self._translate_operand(left),
            "operator": OPERATOR_MAP[operator],
            "right": self._translate_operand(right),
            "tolerance_abs": "0",
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }

    def _translate_substring_condition(
        self,
        left: dict[str, Any],
        operator: str,
        right: dict[str, Any],
    ) -> dict[str, Any]:
        if operator not in {"=", "!="}:
            raise LegacyRulesetTranslationError(
                f"substring operands only support = or !=, found: {operator}"
            )
        literal = self._translate_operand(right)
        value = literal.get("literal")
        if not isinstance(value, str):
            raise LegacyRulesetTranslationError("substring comparison requires a string literal.")

        field_name, start, length = self._substring_parts(left)
        if start != 1:
            raise LegacyRulesetTranslationError("Only substring start position 1 is supported.")
        if length < len(value):
            raise LegacyRulesetTranslationError(
                f"Cannot compare substring length {length} to longer literal {value!r}."
            )

        if length > len(value):
            return {
                "left": {"field": field_name},
                "operator": "eq" if operator == "=" else "not_like",
                "right": literal,
                "tolerance_abs": "0",
                "null_input_mode": "propagate",
                "null_result_mode": "null",
            }

        return {
            "left": {"field": field_name},
            "operator": "starts_with" if operator == "=" else "not_like",
            "right": literal if operator == "=" else {**literal, "literal": f"{value}%"},
            "tolerance_abs": "0",
            "null_input_mode": "propagate",
            "null_result_mode": "null",
        }

    def _translate_operand(self, operand: dict[str, Any]) -> dict[str, Any]:
        if "field" in operand:
            return {"field": str(operand["field"])}
        if "value" in operand:
            return {
                "literal": operand["value"],
                "value_type": TYPE_MAP.get(str(operand.get("type", "string")).lower(), "string"),
            }
        if "values" in operand:
            values = operand["values"]
            if not isinstance(values, list):
                raise LegacyRulesetTranslationError("Legacy values operand must be a list.")
            return {
                "literal": [self._required(item, "value") for item in values],
                "value_type": "list",
            }
        if self._is_substring_operand(operand):
            raise LegacyRulesetTranslationError(
                "substring operands must be translated at the condition level."
            )
        raise LegacyRulesetTranslationError(f"Unsupported legacy operand: {operand}")

    def _translate_assign(self, rule: dict[str, Any]) -> dict[str, Any]:
        then = self._required(rule, "then")
        assign = self._required(then, "assign")
        if not isinstance(assign, dict) or not assign:
            raise LegacyRulesetTranslationError(f"Legacy assign must be a non-empty mapping: {rule}")
        return dict(assign)

    def _substring_parts(self, operand: dict[str, Any]) -> tuple[str, int, int]:
        func = self._required(operand, "func")
        if func.get("name") != "substring":
            raise LegacyRulesetTranslationError(f"Unsupported legacy function: {func.get('name')}")
        args = func.get("args")
        if not isinstance(args, list) or len(args) != 3:
            raise LegacyRulesetTranslationError("substring requires exactly three args.")
        field_arg, start_arg, length_arg = args
        field_name = self._required(field_arg, "field")
        return (
            str(field_name),
            int(self._required(start_arg, "value")),
            int(self._required(length_arg, "value")),
        )

    def _is_substring_operand(self, operand: Any) -> bool:
        return (
            isinstance(operand, dict)
            and isinstance(operand.get("func"), dict)
            and operand["func"].get("name") == "substring"
        )

    def _is_group(self, payload: Any) -> bool:
        return isinstance(payload, dict) and any(key in payload for key in ("all", "any"))

    def _rule_order(self, rule: dict[str, Any]) -> int:
        return int(rule.get("priority", 0))

    def _required(self, payload: dict[str, Any], key: str) -> Any:
        if not isinstance(payload, dict) or key not in payload:
            raise LegacyRulesetTranslationError(f"Missing required key {key}: {payload}")
        return payload[key]


def translate_directory(source_dir: Path, output_dir: Path) -> list[Path]:
    """
    Translate all legacy YAML files in a directory and validate outputs.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    translator = LegacyRulesetTranslator()
    output_paths: list[Path] = []
    for source_path in sorted(source_dir.glob("*.yaml")):
        payload = translator.translate_file(source_path)
        _validate_translated_payload(payload, source_path)
        output_path = output_dir / source_path.name
        output_path.write_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        output_paths.append(output_path)
    return output_paths


def _validate_translated_payload(payload: dict[str, Any], source_path: Path) -> None:
    ruleset = YamlRulesetCompiler().compile_payload(payload)
    semantic_result = RulesetValidator(FunctionRegistry()).validate(ruleset)
    spark_result = SparkRulesetCompatibilityValidator(FunctionRegistry()).validate(ruleset)
    if semantic_result.has_errors():
        raise LegacyRulesetTranslationError(
            f"Semantic validation failed for {source_path}: {semantic_result.to_text()}"
        )
    if spark_result.has_errors():
        raise LegacyRulesetTranslationError(
            f"Spark validation failed for {source_path}: {spark_result.to_text()}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(r"C:\Users\aarba\pydev\rules_engine_old\rulesets"),
        help="Directory containing legacy rules_engine_old YAML files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("translated_legacy_rulesets"),
        help="Directory to write canonical YAML files.",
    )
    args = parser.parse_args()

    output_paths = translate_directory(args.source_dir, args.output_dir)
    for path in output_paths:
        print(path)


if __name__ == "__main__":
    main()
