"""
Spark/Delta repository for rules engine metadata.

The repository treats a ruleset version as one immutable metadata document.
The authoritative runtime table stores one row per ruleset_id/version with the
canonical YAML/JSON payload, summary counts, lifecycle status, provenance, and
content hash. Function registry metadata remains separate because it is
environment-level metadata rather than ruleset-version metadata.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Protocol

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.models import FunctionRegistryRow, Ruleset, RulesetVersionRow
from rules_engine.serializer import DeltaRowSerializer


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RulesEngineTableNames:
    """
    Target table names for rules engine metadata.
    """

    ruleset_versions: str
    function_registry: str
    ruleset_validation_logs: str

    @classmethod
    def from_schema(cls, schema: str) -> "RulesEngineTableNames":
        """
        Build the standard rules engine table names under a catalog.schema path.
        """
        return cls(
            ruleset_versions=f"{schema}.ruleset_versions",
            function_registry=f"{schema}.function_registry",
            ruleset_validation_logs=f"{schema}.ruleset_validation_logs",
        )


class RulesetRepository(Protocol):
    """
    Repository protocol used by publish and runtime services.

    Implementations persist canonical ruleset metadata, expose lifecycle
    transitions, and load only published rulesets for runtime execution.
    """

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """Persist draft metadata."""

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """Mark a persisted ruleset version as published."""

    def retire(self, ruleset_id: str, version: str, *, retired_by: str | None = None) -> None:
        """Mark a persisted ruleset version as retired."""

    def load_draft_for_testing(self, ruleset_id: str, version: str) -> Ruleset:
        """Load draft metadata by exact identity for non-production testing."""

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Load published metadata."""

    def append_ruleset_validation_log(self, row: dict) -> None:
        """Append one validation/publish log row."""


class SparkDeltaRulesetRepository:
    """
    Spark-backed repository for Databricks Delta metadata tables.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_names: RulesEngineTableNames,
        serializer: DeltaRowSerializer | None = None,
    ) -> None:
        """
        Create a Spark/Delta repository for the configured metadata tables.
        """
        self.spark = spark
        self.table_names = table_names
        self.serializer = serializer or DeltaRowSerializer()

    @property
    def ruleset_version_schema(self) -> StructType:
        """Return the authoritative ruleset-version table schema."""
        return StructType(
            [
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_name", StringType(), False),
                StructField("version", StringType(), False),
                StructField("status", StringType(), False),
                StructField("description", StringType(), True),
                StructField("payload_json", StringType(), False),
                StructField("content_hash", StringType(), False),
                StructField(
                    "payload_metadata",
                    StructType(
                        [
                            StructField("rule_count", IntegerType(), False),
                            StructField("condition_count", IntegerType(), False),
                            StructField("assignment_count", IntegerType(), False),
                            StructField("aggregate_count", IntegerType(), False),
                            StructField("custom_function_count", IntegerType(), False),
                        ]
                    ),
                    False,
                ),
                StructField(
                    "user_metadata",
                    StructType(
                        [
                            StructField("owner", StringType(), True),
                            StructField("owner_department", StringType(), True),
                            StructField("created_by", StringType(), False),
                            StructField("created_at", StringType(), False),
                            StructField("published_by", StringType(), True),
                            StructField("published_at", StringType(), True),
                            StructField("retired_by", StringType(), True),
                            StructField("retired_at", StringType(), True),
                        ]
                    ),
                    False,
                ),
            ]
        )

    @property
    def function_registry_schema(self) -> StructType:
        """Return the function registry table schema."""
        return StructType(
            [
                StructField("function_name", StringType(), False),
                StructField("implementation_reference", StringType(), False),
                StructField("arg_contract_payload_json", StringType(), False),
                StructField("return_type_hint", StringType(), True),
                StructField("allowed_in_condition_flag", BooleanType(), False),
                StructField("allowed_in_assignment_flag", BooleanType(), False),
                StructField("active_flag", BooleanType(), False),
                StructField("description", StringType(), True),
                StructField("version", StringType(), True),
            ]
        )

    @property
    def ruleset_validation_log_schema(self) -> StructType:
        """Return the ruleset validation/publish log table schema."""
        return StructType(
            [
                StructField("pipeline_run_id", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("operation", StringType(), False),
                StructField("status", StringType(), False),
                StructField("reason", StringType(), True),
                StructField("ruleset_id", StringType(), True),
                StructField("ruleset_name", StringType(), True),
                StructField("version", StringType(), True),
                StructField("content_hash", StringType(), True),
                StructField("source_yaml_path", StringType(), False),
                StructField("canonical_yaml_path", StringType(), True),
                StructField("original_yaml_archive_path", StringType(), True),
                StructField("created_by", StringType(), False),
                StructField("published_by", StringType(), False),
                StructField("retire_existing_published", BooleanType(), False),
                StructField("require_newer_version", BooleanType(), False),
                StructField("retired_ruleset_id", StringType(), True),
                StructField("retired_version", StringType(), True),
                StructField("validation_issue_count", IntegerType(), True),
                StructField("validation_issues_json", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("error_traceback", StringType(), True),
            ]
        )

    def create_base_tables(self, mode: str = "error") -> None:
        """
        Create empty metadata tables using explicit schemas.
        """
        specs = [
            (self.table_names.ruleset_versions, self.ruleset_version_schema),
            (self.table_names.function_registry, self.function_registry_schema),
            (
                self.table_names.ruleset_validation_logs,
                self.ruleset_validation_log_schema,
            ),
        ]
        for table_name, schema in specs:
            logger.info("Creating rules engine metadata table: table=%s mode=%s", table_name, mode)
            self.spark.createDataFrame([], schema=schema).write.format("delta").mode(
                mode
            ).saveAsTable(table_name)

    def append_ruleset_validation_log(self, row: dict) -> None:
        """
        Append one validation/publish log row to the ruleset validation log table.
        """
        (
            self.spark.createDataFrame([row], schema=self.ruleset_validation_log_schema)
            .write.format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(self.table_names.ruleset_validation_logs)
        )

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """
        Persist a draft ruleset version.

        Existing draft rows for the same ruleset_id/version are replaced to keep
        a development publish loop deterministic. Published and retired rows are
        immutable through this path.
        """
        existing_status = self._existing_ruleset_status(ruleset.ruleset_id, ruleset.version)
        if existing_status is not None and existing_status != RulesetStatus.DRAFT.value:
            logger.error(
                "Rejected draft overwrite for immutable ruleset version: ruleset_id=%s version=%s existing_status=%s",
                ruleset.ruleset_id,
                ruleset.version,
                existing_status,
            )
            raise RepositoryError(
                f"Cannot overwrite ruleset version with status={existing_status}: "
                f"ruleset_id={ruleset.ruleset_id}, version={ruleset.version}"
            )
        logger.info(
            "Persisting draft ruleset version: table=%s ruleset_id=%s ruleset_name=%s version=%s existing_status=%s",
            self.table_names.ruleset_versions,
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
            existing_status,
        )
        row = self.serializer.serialize_ruleset_version(
            ruleset,
            created_by=self._actor_or_system(created_by),
            created_at=self._utc_now(),
        )
        self._delete_ruleset_version(ruleset.ruleset_id, ruleset.version)
        self._write_rows(
            self.table_names.ruleset_versions,
            [asdict(row)],
            self.ruleset_version_schema,
        )
        logger.info(
            "Draft ruleset version persisted: ruleset_id=%s version=%s content_hash=%s",
            ruleset.ruleset_id,
            ruleset.version,
            row.content_hash,
        )

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as published.
        """
        logger.info("Promoting ruleset version to published: ruleset_id=%s version=%s", ruleset_id, version)
        self._assert_publish_allowed(ruleset_id, version)
        self._set_status(
            ruleset_id,
            version,
            RulesetStatus.PUBLISHED,
            published_by=self._actor_or_system(published_by),
            published_at=self._utc_now(),
        )

    def retire(self, ruleset_id: str, version: str, *, retired_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as retired.
        """
        logger.info("Retiring ruleset version: ruleset_id=%s version=%s", ruleset_id, version)
        self._set_status(
            ruleset_id,
            version,
            RulesetStatus.RETIRED,
            retired_by=self._actor_or_system(retired_by),
            retired_at=self._utc_now(),
        )

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        ruleset_filter = (
            (F.col("ruleset_name") == ruleset_name)
            & (F.col("status") == RulesetStatus.PUBLISHED.value)
        )
        if version is not None:
            ruleset_filter = ruleset_filter & (F.col("version") == version)
        logger.info(
            "Loading published ruleset: table=%s ruleset_name=%s version=%s",
            self.table_names.ruleset_versions,
            ruleset_name,
            version,
        )
        rows_df = self.spark.table(self.table_names.ruleset_versions).where(ruleset_filter)
        collected = rows_df.limit(2).collect()
        if not collected:
            logger.error("Published ruleset not found: ruleset_name=%s version=%s", ruleset_name, version)
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        if len(collected) > 1 and version is None:
            logger.error("Multiple published ruleset versions found: ruleset_name=%s", ruleset_name)
            raise RepositoryError(
                f"Multiple published versions found for {ruleset_name}; specify version."
            )
        row = RulesetVersionRow(**collected[0].asDict(recursive=True))
        logger.info(
            "Published ruleset loaded: ruleset_id=%s ruleset_name=%s version=%s content_hash=%s",
            row.ruleset_id,
            row.ruleset_name,
            row.version,
            row.content_hash,
        )
        return self.serializer.deserialize_ruleset_version(row)

    def load_draft_for_testing(self, ruleset_id: str, version: str) -> Ruleset:
        """
        Load a draft ruleset by exact identity for non-production testing.

        Draft loads intentionally require ruleset_id and version. They never
        fall back to published metadata and they are not used by runtime facade
        helpers, which keeps production callers on ``load_published``.
        """
        logger.info(
            "Loading draft ruleset for testing: table=%s ruleset_id=%s version=%s",
            self.table_names.ruleset_versions,
            ruleset_id,
            version,
        )
        row_dict = self._ruleset_row_dict(ruleset_id, version)
        if row_dict is None:
            logger.error(
                "Draft ruleset not found for testing: ruleset_id=%s version=%s",
                ruleset_id,
                version,
            )
            raise RepositoryError(
                f"Draft ruleset not found: ruleset_id={ruleset_id}, version={version}"
            )
        if row_dict["status"] != RulesetStatus.DRAFT.value:
            logger.error(
                "Draft testing load rejected for non-draft ruleset: ruleset_id=%s version=%s status=%s",
                ruleset_id,
                version,
                row_dict["status"],
            )
            raise RepositoryError(
                f"Ruleset version is not draft: ruleset_id={ruleset_id}, "
                f"version={version}, status={row_dict['status']}"
            )
        row = RulesetVersionRow(**row_dict)
        logger.info(
            "Draft ruleset loaded for testing: ruleset_id=%s ruleset_name=%s version=%s content_hash=%s",
            row.ruleset_id,
            row.ruleset_name,
            row.version,
            row.content_hash,
        )
        return self.serializer.deserialize_ruleset_version(row)

    def save_function_registry_rows(self, rows: list[FunctionRegistryRow]) -> None:
        """
        Upsert function registry metadata rows by function_name.
        """
        logger.info("Saving function registry rows: table=%s row_count=%s", self.table_names.function_registry, len(rows))
        for row in rows:
            self._delete_from_table(
                self.table_names.function_registry,
                f"function_name = {self._sql(row.function_name)}",
            )
        self._write_rows(
            self.table_names.function_registry,
            [self._function_to_spark_dict(row) for row in rows],
            self.function_registry_schema,
        )

    def _write_rows(self, table_name: str, rows: list[dict], schema: StructType) -> None:
        """
        Append row dictionaries to a Delta table using the supplied schema.

        Empty row lists are treated as no-ops so callers can pass filtered
        write sets without guarding every call.
        """
        if not rows:
            return
        logger.debug("Appending rows to Delta table: table=%s row_count=%s", table_name, len(rows))
        self.spark.createDataFrame(rows, schema=schema).write.format("delta").mode(
            "append"
        ).saveAsTable(table_name)

    def _delete_ruleset_version(self, ruleset_id: str, version: str) -> None:
        """
        Delete one draft ruleset version row by immutable identity.

        This helper is used only after callers have verified that the existing
        row, if present, is still a draft.
        """
        self._delete_from_table(
            self.table_names.ruleset_versions,
            f"ruleset_id = {self._sql(ruleset_id)} AND version = {self._sql(version)}",
        )

    def _set_status(
        self,
        ruleset_id: str,
        version: str,
        status: RulesetStatus,
        *,
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> None:
        """
        Update lifecycle status and nested user metadata for one version.

        The method rewrites the full ``user_metadata`` struct instead of
        assigning nested fields directly. That keeps the SQL compatible with
        Delta runtimes where partial struct-field updates may differ.
        """
        row = self._ruleset_row_dict(ruleset_id, version)
        if row is None:
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )
        user_metadata = row["user_metadata"]

        assignments = [
            f"status = {self._sql(status.value)}",
        ]
        if status is RulesetStatus.PUBLISHED:
            user_metadata["published_by"] = published_by
            user_metadata["published_at"] = published_at
        if status is RulesetStatus.RETIRED:
            user_metadata["retired_by"] = retired_by
            user_metadata["retired_at"] = retired_at
        assignments.append(f"user_metadata = {self._user_metadata_sql(user_metadata)}")

        self.spark.sql(
            f"""
            UPDATE {self.table_names.ruleset_versions}
            SET {", ".join(assignments)}
            WHERE ruleset_id = {self._sql(ruleset_id)}
              AND version = {self._sql(version)}
            """
        )
        updated = self._ruleset_row_dict(ruleset_id, version)
        if updated is None or updated["status"] != status.value:
            logger.error(
                "Ruleset status update verification failed: ruleset_id=%s version=%s expected_status=%s",
                ruleset_id,
                version,
                status.value,
            )
            raise RepositoryError(
                f"Status update failed: ruleset_id={ruleset_id}, "
                f"version={version}, status={status.value}"
            )
        logger.info(
            "Ruleset status updated: ruleset_id=%s version=%s status=%s",
            ruleset_id,
            version,
            status.value,
        )

    def _assert_publish_allowed(self, ruleset_id: str, version: str) -> None:
        """
        Enforce publish lifecycle invariants before status promotion.

        A version must exist, must still be draft, and no sibling version with
        the same ruleset_name may already be published.
        """
        existing = self._ruleset_row_dict(ruleset_id, version)
        if existing is None:
            logger.error("Publish rejected because ruleset version is missing: ruleset_id=%s version=%s", ruleset_id, version)
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )
        if existing["status"] == RulesetStatus.PUBLISHED.value:
            logger.error("Publish rejected for already-published version: ruleset_id=%s version=%s", ruleset_id, version)
            raise RepositoryError(
                f"Cannot overwrite an already published ruleset version: "
                f"ruleset_id={ruleset_id}, version={version}"
            )
        if existing["status"] != RulesetStatus.DRAFT.value:
            logger.error(
                "Publish rejected because version is not draft: ruleset_id=%s version=%s status=%s",
                ruleset_id,
                version,
                existing["status"],
            )
            raise RepositoryError(
                f"Only draft ruleset versions can be published: "
                f"ruleset_id={ruleset_id}, version={version}, status={existing['status']}"
            )
        published_count = (
            self.spark.table(self.table_names.ruleset_versions)
            .where(
                (F.col("ruleset_name") == existing["ruleset_name"])
                & (F.col("status") == RulesetStatus.PUBLISHED.value)
            )
            .count()
        )
        if published_count:
            logger.error(
                "Publish rejected because another version is already published: ruleset_name=%s published_count=%s",
                existing["ruleset_name"],
                published_count,
            )
            raise RepositoryError(
                f"Cannot publish {existing['ruleset_name']} while another version is published."
            )

    def _existing_ruleset_status(self, ruleset_id: str, version: str) -> str | None:
        """
        Return the persisted status for one version, or None when absent.
        """
        row = self._ruleset_row_dict(ruleset_id, version)
        return row["status"] if row is not None else None

    def _ruleset_row_dict(self, ruleset_id: str, version: str) -> dict | None:
        """
        Load one ruleset version row as a recursive Python dictionary.

        Recursive conversion is required because Spark returns nested structs
        as Row objects by default, while serializer/model construction expects
        ordinary nested dictionaries.
        """
        if not self._table_exists(self.table_names.ruleset_versions):
            return None
        rows = (
            self.spark.table(self.table_names.ruleset_versions)
            .where((F.col("ruleset_id") == ruleset_id) & (F.col("version") == version))
            .limit(1)
            .collect()
        )
        return rows[0].asDict(recursive=True) if rows else None

    def _delete_from_table(self, table_name: str, predicate: str) -> None:
        """
        Delete rows from a table when the table exists.

        Missing tables are ignored so setup/cleanup paths can be idempotent.
        """
        if self._table_exists(table_name):
            self.spark.sql(f"DELETE FROM {table_name} WHERE {predicate}")

    def _table_exists(self, table_name: str) -> bool:
        """
        Return whether Spark catalog metadata contains the target table.
        """
        return bool(self.spark.catalog.tableExists(table_name))

    def _sql(self, value: str) -> str:
        """
        Return a single-quoted SQL string literal with quotes escaped.
        """
        return "'" + value.replace("'", "''") + "'"

    def _sql_nullable(self, value: str | None) -> str:
        """
        Return a SQL literal for optional string metadata values.
        """
        return "NULL" if value is None else self._sql(value)

    def _user_metadata_sql(self, metadata: dict) -> str:
        """
        Build a SQL named_struct expression for nested user metadata.
        """
        return (
            "named_struct("
            f"'owner', {self._sql_nullable(metadata.get('owner'))}, "
            f"'owner_department', {self._sql_nullable(metadata.get('owner_department'))}, "
            f"'created_by', {self._sql(metadata['created_by'])}, "
            f"'created_at', {self._sql(metadata['created_at'])}, "
            f"'published_by', {self._sql_nullable(metadata.get('published_by'))}, "
            f"'published_at', {self._sql_nullable(metadata.get('published_at'))}, "
            f"'retired_by', {self._sql_nullable(metadata.get('retired_by'))}, "
            f"'retired_at', {self._sql_nullable(metadata.get('retired_at'))}"
            ")"
        )

    def _utc_now(self) -> str:
        """
        Return the current UTC timestamp in ISO-8601 string form.
        """
        return datetime.now(timezone.utc).isoformat()

    def _actor_or_system(self, value: str | None) -> str:
        """
        Normalize optional actor metadata to a non-empty string.

        Locked-down production jobs may omit actor arguments; in those cases
        ``system`` is stored explicitly.
        """
        if value is None:
            return "system"
        stripped = value.strip()
        return stripped or "system"

    def _function_to_spark_dict(self, row: FunctionRegistryRow) -> dict:
        """
        Convert a function registry row into the Spark table row shape.

        The in-memory model stores ``arg_contract_payload`` as a dictionary;
        the Delta table stores the same value as canonical JSON.
        """
        payload = asdict(row)
        payload["arg_contract_payload_json"] = json.dumps(
            payload.pop("arg_contract_payload"),
            sort_keys=True,
        )
        return payload
