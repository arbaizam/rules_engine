"""
Translation audit artifact writer.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from tools.recon_spec_translation.models import TranslationAuditRecord


def write_audit(records: list[TranslationAuditRecord], path: str | Path) -> None:
    """
    Write translation audit records as JSON.
    """
    Path(path).write_text(
        json.dumps([asdict(record) for record in records], indent=2, sort_keys=True),
        encoding="utf-8",
    )

