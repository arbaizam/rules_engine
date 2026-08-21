"""Rules engine exception hierarchy."""


class RulesEngineError(Exception):
    """Base exception for rules engine errors."""


class CompilationError(RulesEngineError):
    """Raised when YAML cannot be compiled into canonical models."""


class ValidationFailedError(RulesEngineError):
    """Raised when publish-time validation fails."""


class RegistryError(RulesEngineError):
    """Raised for custom function registry problems."""


class RepositoryError(RulesEngineError):
    """Raised for repository persistence or loading problems."""

