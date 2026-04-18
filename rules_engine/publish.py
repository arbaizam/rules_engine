"""
Publish service for ruleset metadata.
"""

from __future__ import annotations

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import ValidationFailedError
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.repository import RulesetRepository
from rules_engine.validator import RulesetValidator
from rules_engine.models import Ruleset, ValidationResult


class PublishService:
    """
    Coordinate draft save, validation, normalization, and publication.
    """

    def __init__(
        self,
        repository: RulesetRepository,
        validator: RulesetValidator,
        normalizer: RulesetNormalizer,
    ) -> None:
        self._repository = repository
        self._validator = validator
        self._normalizer = normalizer

    def save_draft(
        self,
        ruleset: Ruleset,
        *,
        created_by: str | None = None,
    ) -> ValidationResult:
        """
        Normalize, validate, and save draft metadata.
        """
        if ruleset.status is not RulesetStatus.DRAFT:
            raise ValidationFailedError("save_draft requires ruleset status=draft.")
        normalized = self._normalizer.normalize_ruleset(ruleset)
        validation = self._validator.validate(normalized)
        self._repository.save_draft(normalized, created_by=created_by)
        self._repository.save_validation_results(
            normalized.ruleset_id,
            normalized.version,
            validation,
        )
        return validation

    def publish(
        self,
        ruleset: Ruleset,
        *,
        created_by: str | None = None,
        published_by: str | None = None,
    ) -> None:
        """
        Validate and publish a ruleset version.
        """
        if ruleset.status is not RulesetStatus.DRAFT:
            raise ValidationFailedError("publish requires ruleset status=draft.")
        normalized = self._normalizer.normalize_ruleset(ruleset)
        validation = self._validator.validate(normalized)
        self._repository.save_validation_results(
            normalized.ruleset_id,
            normalized.version,
            validation,
        )
        if validation.has_errors():
            raise ValidationFailedError(
                f"Publish failed for ruleset={normalized.ruleset_name}, version={normalized.version}"
            )
        self._repository.save_draft(normalized, created_by=created_by)
        self._repository.publish(
            normalized.ruleset_id,
            normalized.version,
            published_by=published_by,
        )
