"""Installed package and public full-audit schema versions."""

__version__ = "2.2"

# Increment this independently whenever the full-audit output changes shape or
# meaning. Persisted audit rows use it to distinguish data-contract generations
# even when package release cadence and audit evolution eventually diverge.
AUDIT_SCHEMA_VERSION = "2"
