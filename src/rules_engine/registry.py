"""
Custom function registry.

Custom logic is available only through this registry. Metadata persistence
stores implementation references and argument contracts, not executable code.
Actual callables are registered by the runtime environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from rules_engine.exceptions import RegistryError
from rules_engine.models import FunctionRegistryRow


class CustomFunction(Protocol):
    """Callable protocol for registered custom functions."""

    def __call__(self, **kwargs: Any) -> Any:
        """Execute the custom function with keyword arguments."""


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
    arg_names : tuple[str, ...]
        Exact keyword arguments allowed by the function.
    allowed_in_condition_flag : bool
        Whether this function may be used in conditions.
    allowed_in_assignment_flag : bool
        Whether this function may be used in assignments.
    active_flag : bool
        Whether the function can be referenced by published metadata.
    """

    function_name: str
    implementation_reference: str
    arg_names: tuple[str, ...]
    allowed_in_condition_flag: bool
    allowed_in_assignment_flag: bool
    active_flag: bool = True
    return_type_hint: str | None = None
    description: str | None = None
    version: str | None = None

    def to_row(self) -> FunctionRegistryRow:
        """
        Convert the function spec to a persisted metadata row.
        """
        return FunctionRegistryRow(
            function_name=self.function_name,
            implementation_reference=self.implementation_reference,
            arg_contract_payload={"arg_names": list(self.arg_names)},
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
