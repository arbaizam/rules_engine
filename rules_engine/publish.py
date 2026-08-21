"""
Publish service for ruleset metadata.
"""

from __future__ import annotations

import logging

from rules_engine.exceptions import ValidationFailedError
from rules_engine.models import Ruleset
from rules_engine.repository import RulesetRepository
from rules_engine.testing import RulesetTester
from rules_engine.validator import RulesetValidator

logger = logging.getLogger(__name__)


class PublishService:
    """
    Coordinate validation and publication.
    """

    def __init__(
        self,
        repository: RulesetRepository,
        validator: RulesetValidator,
        tester: RulesetTester | None = None,
    ) -> None:
        """
        Create a publish service from repository and validator.
        """
        self._repository = repository
        self._validator = validator
        self._tester = tester

    def publish(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
    ) -> None:
        """
        Validate and publish a ruleset version.
        """
        logger.info(
            "Publishing ruleset: ruleset_id=%s ruleset_name=%s version=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
        )
        validation = self._validator.validate(ruleset)
        if validation.has_errors():
            logger.error(
                "Publish validation failed: ruleset_id=%s version=%s issue_count=%s",
                ruleset.ruleset_id,
                ruleset.version,
                len(validation.issues),
            )
            raise ValidationFailedError(
                f"Publish failed for ruleset={ruleset.ruleset_name}, "
                f"version={ruleset.version}.\n{validation.to_text()}"
            )
        if ruleset.expect:
            if self._tester is None:
                raise ValidationFailedError(
                    "Publish cannot execute expected cases without a RulesetTester."
                )
            test_result = self._tester.test(ruleset)
            if not test_result.passed:
                raise ValidationFailedError(
                    "Ruleset expected cases failed; metadata was not published.\n"
                    + test_result.to_text()
                )
        self._repository.save_published(
            ruleset,
            published_by=published_by,
        )
        logger.info(
            "Ruleset published: ruleset_id=%s ruleset_name=%s version=%s",
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
        )
