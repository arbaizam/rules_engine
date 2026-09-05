"""
Custom function registry.

Custom logic is available only through this registry. Metadata persistence
stores implementation references and argument contracts, not executable code.
Actual callables are registered by the runtime environment.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from rules_engine.canonical_values import canonical_json_value, validate_string_mapping_keys
from rules_engine.exceptions import RegistryError
from rules_engine.models import CustomFunctionOperand, FunctionRegistryRow, Ruleset
from rules_engine.traversal import iter_ruleset_operands

SUPPORTED_ARGUMENT_TYPE_HINTS = frozenset(
    {
        "any",
        "boolean",
        "date",
        "date_sequence",
        "integer",
        "integer_sequence",
        "mapping",
        "number",
        "ordered_sequence",
        "sequence",
        "string",
        "string_sequence",
        "timestamp",
    }
)
SUPPORTED_RETURN_TYPE_HINTS = frozenset(
    {
        "any",
        "bool",
        "boolean",
        "date",
        "decimal",
        "double",
        "float",
        "int",
        "integer",
        "long",
        "number",
        "str",
        "string",
        "timestamp",
        "timestamp_ntz",
    }
)
DYNAMIC_RETURN_TYPE_HINT_TEMPLATES = (
    "same_as:<argument_name>",
    "common_type:<argument_name>",
)
_DYNAMIC_RETURN_TYPE_HINT_PREFIXES = frozenset(
    template.partition(":")[0] for template in DYNAMIC_RETURN_TYPE_HINT_TEMPLATES
)


class CustomFunction(Protocol):
    """Callable protocol for registered custom functions."""

    def __call__(self, **kwargs: Any) -> Any:
        """Execute the custom function with keyword arguments."""


@dataclass(frozen=True)
class CustomFunctionArgSpec:
    """Contract for one named custom-function argument."""

    name: str
    required: bool = True
    default: Any = None
    type_hint: str = "any"
    allowed_values: tuple[Any, ...] | None = None
    literal_only: bool = False

    def __post_init__(self) -> None:
        """Reject malformed or internally contradictory argument metadata."""
        if not isinstance(self.name, str) or not self.name.strip():
            raise RegistryError("Function argument name must be a non-empty string.")
        if not isinstance(self.required, bool):
            raise RegistryError("Function argument required must be a boolean.")
        if not isinstance(self.literal_only, bool):
            raise RegistryError("Function argument literal_only must be a boolean.")
        if (
            not isinstance(self.type_hint, str)
            or self.type_hint not in SUPPORTED_ARGUMENT_TYPE_HINTS
        ):
            raise RegistryError(f"Unsupported function argument type hint: {self.type_hint!r}")
        if self.required and self.default is not None:
            raise RegistryError("Required function arguments cannot define a default.")
        if self.allowed_values is not None and not isinstance(
            self.allowed_values,
            tuple,
        ):
            raise RegistryError("Function argument allowed_values must be a tuple.")

    def to_payload(self) -> dict[str, Any]:
        """Return the JSON-compatible persisted argument contract."""
        payload: dict[str, Any] = {
            "name": self.name,
            "required": self.required,
            "type_hint": self.type_hint,
            "literal_only": self.literal_only,
        }
        if not self.required:
            payload["default"] = self._payload_value(self.default)
        if self.allowed_values is not None:
            payload["allowed_values"] = [
                self._payload_value(value) for value in self.allowed_values
            ]
        return payload

    def _payload_value(self, value: Any) -> Any:
        """Normalize tuples and sets for deterministic JSON persistence."""
        if isinstance(value, (tuple, list)):
            return [self._payload_value(item) for item in value]
        if isinstance(value, set):
            return [self._payload_value(item) for item in sorted(value, key=repr)]
        if isinstance(value, dict):
            return {str(key): self._payload_value(item) for key, item in value.items()}
        return value


@dataclass(frozen=True)
class CustomFunctionSpec:
    """
    Metadata contract for a custom function.

    Parameters
    ----------
    function_name : str
        Canonical function name referenced by ruleset metadata.
    implementation_reference : str
        Environment-specific implementation reference. This is metadata only.
    arguments : tuple[CustomFunctionArgSpec, ...]
        Ordered required and optional keyword-argument contracts.
    allowed_in_condition_flag : bool
        Whether this function may be used in conditions.
    allowed_in_assignment_flag : bool
        Whether this function may be used in assignments.
    active_flag : bool
        Whether the function can be referenced by published metadata.
    """

    function_name: str
    implementation_reference: str
    arguments: tuple[CustomFunctionArgSpec, ...]
    allowed_in_condition_flag: bool
    allowed_in_assignment_flag: bool
    active_flag: bool = True
    return_type_hint: str | None = None
    description: str | None = None
    version: str | None = None

    def __post_init__(self) -> None:
        """Reject ambiguous registry contracts when they are constructed."""
        if not isinstance(self.function_name, str) or not self.function_name.strip():
            raise RegistryError("Function name must be a non-empty string.")
        if (
            not isinstance(self.implementation_reference, str)
            or not self.implementation_reference.strip()
        ):
            raise RegistryError("Implementation reference must be a non-empty string.")
        if not isinstance(self.arguments, tuple) or not all(
            isinstance(argument, CustomFunctionArgSpec) for argument in self.arguments
        ):
            raise RegistryError(
                "Function arguments must be a tuple of CustomFunctionArgSpec values."
            )
        names = [argument.name for argument in self.arguments]
        if not isinstance(self.allowed_in_condition_flag, bool) or not isinstance(
            self.allowed_in_assignment_flag,
            bool,
        ):
            raise RegistryError("Function permission flags must be booleans.")
        if not isinstance(self.active_flag, bool):
            raise RegistryError("Function active_flag must be a boolean.")
        if self.return_type_hint is not None and not isinstance(
            self.return_type_hint,
            str,
        ):
            raise RegistryError("Function return_type_hint must be a string or null.")
        if isinstance(self.return_type_hint, str) and not self.return_type_hint.strip():
            raise RegistryError("Function return_type_hint cannot be blank.")
        if len(names) != len(set(names)):
            raise RegistryError(f"Function argument names must be unique: {self.function_name}")
        if self.return_type_hint is not None:
            normalized_return_hint = self.return_type_hint.lower()
            if normalized_return_hint not in SUPPORTED_RETURN_TYPE_HINTS:
                prefix, separator, argument_name = normalized_return_hint.partition(":")
                if (
                    separator != ":"
                    or prefix not in _DYNAMIC_RETURN_TYPE_HINT_PREFIXES
                    or argument_name not in names
                ):
                    raise RegistryError(
                        f"Unsupported return type hint for {self.function_name}: "
                        f"{self.return_type_hint!r}"
                    )
                if prefix == "common_type":
                    argument = next(item for item in self.arguments if item.name == argument_name)
                    if argument.type_hint not in {
                        "sequence",
                        "ordered_sequence",
                        "string_sequence",
                        "integer_sequence",
                        "date_sequence",
                    }:
                        raise RegistryError(
                            f"common_type return hints require a sequence argument: "
                            f"{self.function_name}.{argument_name}"
                        )
        for argument in self.arguments:
            self._validate_argument_mapping_keys(argument)
            if argument.allowed_values is None:
                continue
            if not argument.allowed_values:
                raise RegistryError(
                    f"Allowed values cannot be empty: {self.function_name}.{argument.name}"
                )
            if not argument.literal_only:
                raise RegistryError(
                    f"Arguments with allowed values must be literal-only: "
                    f"{self.function_name}.{argument.name}"
                )
            if not argument.required and argument.default not in argument.allowed_values:
                raise RegistryError(
                    f"Optional argument default must be allowed: "
                    f"{self.function_name}.{argument.name}"
                )
        try:
            json.dumps(
                [argument.to_payload() for argument in self.arguments],
                sort_keys=True,
            )
        except (TypeError, ValueError) as exc:
            raise RegistryError(
                f"Function argument metadata must be JSON-compatible: {self.function_name}"
            ) from exc

    def _validate_argument_mapping_keys(self, argument: CustomFunctionArgSpec) -> None:
        """Reject keys that would change when argument metadata is persisted."""
        try:
            validate_string_mapping_keys(argument.default)
            validate_string_mapping_keys(argument.allowed_values)
        except ValueError as exc:
            raise RegistryError(
                f"Invalid mapping keys for {self.function_name}.{argument.name}: {exc}"
            ) from exc

    @property
    def argument_names(self) -> tuple[str, ...]:
        """Return argument names in their declared order."""
        return tuple(argument.name for argument in self.arguments)

    def bind_args(self, authored_args: Mapping[str, Any]) -> dict[str, Any]:
        """Validate names and add declared optional defaults."""
        actual = set(authored_args)
        allowed = set(self.argument_names)
        required = {argument.name for argument in self.arguments if argument.required}
        missing = required - actual
        extra = actual - allowed
        if missing or extra:
            raise RegistryError(
                f"Invalid args for {self.function_name}: "
                f"missing={sorted(missing)}, extra={sorted(extra)}"
            )
        return {
            argument.name: (
                authored_args[argument.name]
                if argument.name in authored_args
                else deepcopy(argument.default)
            )
            for argument in self.arguments
        }

    def to_authoring_payload(self) -> dict[str, Any]:
        """Return the JSON-compatible function contract used by authoring tools."""
        return {
            "function_name": self.function_name,
            "arguments": [argument.to_payload() for argument in self.arguments],
            "return_type_hint": self.return_type_hint,
            "allowed_in_condition_flag": self.allowed_in_condition_flag,
            "allowed_in_assignment_flag": self.allowed_in_assignment_flag,
            "active_flag": self.active_flag,
            "description": self.description,
            "version": self.version,
        }

    def to_row(self) -> FunctionRegistryRow:
        """
        Convert the function spec to a persisted metadata row.
        """
        return FunctionRegistryRow(
            function_name=self.function_name,
            implementation_reference=self.implementation_reference,
            arg_contract_payload={
                "arguments": [argument.to_payload() for argument in self.arguments]
            },
            return_type_hint=self.return_type_hint,
            allowed_in_condition_flag=self.allowed_in_condition_flag,
            allowed_in_assignment_flag=self.allowed_in_assignment_flag,
            active_flag=self.active_flag,
            description=self.description,
            version=self.version,
        )


class FunctionRegistry:
    """
    In-memory registry of custom function metadata and implementations.
    """

    def __init__(self) -> None:
        """
        Create an empty in-memory custom function registry.
        """
        self._specs: dict[str, CustomFunctionSpec] = {}
        self._implementations: dict[str, CustomFunction] = {}

    def register(
        self,
        spec: CustomFunctionSpec,
        implementation: CustomFunction | None = None,
    ) -> None:
        """
        Register a custom function spec and optional callable implementation.
        """
        if spec.function_name in self._specs:
            raise RegistryError(f"Function already registered: {spec.function_name}")
        if implementation is not None and not callable(implementation):
            raise RegistryError(f"Function implementation must be callable: {spec.function_name}")
        self._specs[spec.function_name] = spec
        if implementation is not None:
            self._implementations[spec.function_name] = implementation

    def get_spec(self, function_name: str) -> CustomFunctionSpec:
        """
        Return registered function metadata.
        """
        try:
            return self._specs[function_name]
        except KeyError as exc:
            raise RegistryError(f"Unknown custom function: {function_name}") from exc

    def get_implementation(self, function_name: str) -> CustomFunction:
        """
        Return the runtime callable for a registered function.
        """
        try:
            return self._implementations[function_name]
        except KeyError as exc:
            raise RegistryError(
                f"Missing implementation for custom function: {function_name}"
            ) from exc

    def has_spec(self, function_name: str) -> bool:
        """
        Return whether function metadata is registered.
        """
        return function_name in self._specs

    def specs(self) -> tuple[CustomFunctionSpec, ...]:
        """Return registered specifications in canonical function-name order."""
        return tuple(self._specs[name] for name in sorted(self._specs))

    def dependency_manifest(self, ruleset: Ruleset) -> list[dict[str, Any]]:
        """Describe the declared implementations used by active rule execution.

        The manifest records contracts and declared versions/references, not a
        hash of executable Python code. Unreferenced registrations and inactive
        branches do not affect this execution identity.
        """
        names = {
            operand.function_name
            for operand in iter_ruleset_operands(ruleset, active_only=True)
            if isinstance(operand, CustomFunctionOperand)
        }
        manifest = []
        for name in sorted(names):
            spec = self.get_spec(name)
            arguments = []
            for argument in spec.arguments:
                payload = argument.to_payload()
                if not argument.required:
                    payload["default"] = argument.default
                if argument.allowed_values is not None:
                    payload["allowed_values"] = list(argument.allowed_values)
                arguments.append(payload)
            manifest.append(
                {
                    **spec.to_authoring_payload(),
                    "implementation_reference": spec.implementation_reference,
                    "arguments": arguments,
                }
            )
        # Extended JSON preserves observable default kinds (tuple/list/set), and
        # yields detached, ordinary JSON values for the Spark identity column.
        return canonical_json_value(manifest)
