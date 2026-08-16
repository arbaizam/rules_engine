"""
Publish service for ruleset metadata.
"""

from __future__ import annotations

import logging

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import ValidationFailedError
from rules_engine.models import Ruleset
from rules_engine.normalizer import RulesetNormalizer
from rules_engine.repository import RulesetRepository
from rules_engine.testing import RulesetTester
from rules_engine.validator import RulesetValidator

logger = logging.getLogger(__name__)


class PublishService:
    """
    Coordinate validation, normalization, and publication.
    """

    def __init__(
        self,
        repository: RulesetRepository,
        validator: RulesetValidator,
        normalizer: RulesetNormalizer,
        tester: RulesetTester | None = None,
    ) -> None:
        """
        Create a publish service from repository, validator, and normalizer.
        """
        self._repository = repository
        self._validator = validator
        self._normalizer = normalizer
        self._tester = tester

    def publish(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
    ) -> None:
        """
        Validate and publish a ruleset version.
        """
        if ruleset.status is not RulesetStatus.PUBLISHED:
            raise ValidationFailedError("publish requires ruleset status=published.")
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
        if normalized.expect:
            if self._tester is None:
                raise ValidationFailedError(
                    "Publish cannot execute expected cases without a RulesetTester."
                )
            test_result = self._tester.test(normalized)
            if not test_result.passed:
                raise ValidationFailedError(
                    "Ruleset expected cases failed; metadata was not published.\n"
                    + test_result.to_text()
                )
        self._repository.save_published(
            normalized,
            published_by=published_by,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
        )
        logger.info(
            "Ruleset published: ruleset_id=%s ruleset_name=%s version=%s",
            normalized.ruleset_id,
            normalized.ruleset_name,
            normalized.version,
        )
