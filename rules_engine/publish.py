"""
Publish service for ruleset metadata.
"""

from __future__ import annotations

import logging

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import ValidationFailedError
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.repository import RulesetRepository
from rules_engine.validator import RulesetValidator
from rules_engine.models import Ruleset, ValidationResult


logger = logging.getLogger(__name__)


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
        """
        Create a publish service from repository, validator, and normalizer.
        """
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

        Validation errors are returned but do not block draft persistence.
        This lets authors checkpoint incomplete work while keeping ``publish``
        as the hard validation gate.
        """
        if ruleset.status is not RulesetStatus.DRAFT:
            raise ValidationFailedError("save_draft requires ruleset status=draft.")
        logger.info(
            "Saving draft ruleset: ruleset_id=%s ruleset_name=%s version=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
        )
        normalized = self._normalizer.normalize_ruleset(ruleset)
        validation = self._validator.validate(normalized)
        if validation.has_errors():
            logger.warning(
                "Draft validation produced errors before save: ruleset_id=%s version=%s issue_count=%s",
                normalized.ruleset_id,
                normalized.version,
                len(validation.issues),
            )
        self._repository.save_draft(normalized, created_by=created_by)
        logger.info(
            "Draft ruleset saved: ruleset_id=%s version=%s validation_passed=%s",
            normalized.ruleset_id,
            normalized.version,
            validation.passed,
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
        logger.info(
            "Publishing ruleset: ruleset_id=%s ruleset_name=%s version=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
        )
        normalized = self._normalizer.normalize_ruleset(ruleset)
        validation = self._validator.validate(normalized)
        if validation.has_errors():
            logger.error(
                "Publish validation failed: ruleset_id=%s version=%s issue_count=%s",
                normalized.ruleset_id,
                normalized.version,
                len(validation.issues),
            )
            raise ValidationFailedError(
                f"Publish failed for ruleset={normalized.ruleset_name}, version={normalized.version}"
            )
        self._repository.save_draft(normalized, created_by=created_by)
        self._repository.publish(
            normalized.ruleset_id,
            normalized.version,
            published_by=published_by,
        )
        logger.info(
            "Ruleset published: ruleset_id=%s ruleset_name=%s version=%s",
            normalized.ruleset_id,
            normalized.ruleset_name,
            normalized.version,
        )
