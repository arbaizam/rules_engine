# Databricks notebook source
# MAGIC %md
# MAGIC # Retire Ruleset Pipeline
# MAGIC
# MAGIC This notebook retires one published ruleset version without publishing a
# MAGIC replacement. Retirement removes the version from runtime eligibility while
# MAGIC preserving the metadata row for audit.
# MAGIC
# MAGIC It writes a `ruleset_validation_logs` row with:
# MAGIC
# MAGIC - `operation = retire`
# MAGIC - `status = retired`
# MAGIC - `reason = <required job parameter>`

# COMMAND ----------

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import sys
import traceback

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Job Parameters

# COMMAND ----------

dbutils.widgets.text("schema", "YOUR_CATALOG.YOUR_SCHEMA")
dbutils.widgets.text("ruleset_id", "")
dbutils.widgets.text("version", "")
dbutils.widgets.text("retired_by", "rules-retire-pipeline")
dbutils.widgets.text("reason", "")
dbutils.widgets.dropdown("create_log_table", "true", ["false", "true"])
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"])

SCHEMA = dbutils.widgets.get("schema").strip()
RULESET_ID = dbutils.widgets.get("ruleset_id").strip()
VERSION = dbutils.widgets.get("version").strip()
RETIRED_BY = dbutils.widgets.get("retired_by").strip() or "rules-retire-pipeline"
REASON = dbutils.widgets.get("reason").strip()
CREATE_LOG_TABLE = dbutils.widgets.get("create_log_table") == "true"
DRY_RUN = dbutils.widgets.get("dry_run") == "true"

if not SCHEMA or SCHEMA == "YOUR_CATALOG.YOUR_SCHEMA":
    raise ValueError("Parameter schema must be set to a real catalog.schema value.")
if not RULESET_ID:
    raise ValueError("Parameter ruleset_id is required.")
if not VERSION:
    raise ValueError("Parameter version is required.")
if not REASON:
    raise ValueError("Parameter reason is required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Repository

# COMMAND ----------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def append_log(row: dict) -> None:
    repository.append_ruleset_validation_log(row)


PIPELINE_RUN_ID = safe_name(f"rules-retire-{utc_now()}")

table_names = RulesEngineTableNames.from_schema(SCHEMA)
repository = SparkDeltaRulesetRepository(spark, table_names)

if CREATE_LOG_TABLE:
    (
        spark.createDataFrame([], schema=repository.ruleset_validation_log_schema)
        .write.format("delta")
        .mode("ignore")
        .saveAsTable(table_names.ruleset_validation_logs)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retire And Log

# COMMAND ----------

row = repository._ruleset_row_dict(RULESET_ID, VERSION)
if row is None:
    append_log(
        {
            "pipeline_run_id": PIPELINE_RUN_ID,
            "event_time": utc_now(),
            "operation": "retire",
            "status": "failed",
            "reason": REASON,
            "ruleset_id": RULESET_ID,
            "ruleset_name": None,
            "version": VERSION,
            "content_hash": None,
            "source_yaml_path": None,
            "canonical_yaml_path": None,
            "original_yaml_archive_path": None,
            "published_by": RETIRED_BY,
            "retire_existing_published": None,
            "require_newer_version": None,
            "retired_ruleset_id": RULESET_ID,
            "retired_version": VERSION,
            "validation_issue_count": None,
            "validation_issues_json": None,
            "error_message": "Ruleset version not found.",
            "error_traceback": None,
        }
    )
    raise ValueError(f"Ruleset version not found: ruleset_id={RULESET_ID}, version={VERSION}")

if row["status"] != "published":
    append_log(
        {
            "pipeline_run_id": PIPELINE_RUN_ID,
            "event_time": utc_now(),
            "operation": "retire",
            "status": "failed",
            "reason": REASON,
            "ruleset_id": RULESET_ID,
            "ruleset_name": row["ruleset_name"],
            "version": VERSION,
            "content_hash": row["content_hash"],
            "source_yaml_path": None,
            "canonical_yaml_path": None,
            "original_yaml_archive_path": None,
            "published_by": RETIRED_BY,
            "retire_existing_published": None,
            "require_newer_version": None,
            "retired_ruleset_id": RULESET_ID,
            "retired_version": VERSION,
            "validation_issue_count": None,
            "validation_issues_json": None,
            "error_message": f"Only published rulesets can be retired; current status={row['status']}.",
            "error_traceback": None,
        }
    )
    raise ValueError(
        f"Only published rulesets can be retired: "
        f"ruleset_id={RULESET_ID}, version={VERSION}, status={row['status']}"
    )

if DRY_RUN:
    display(
        spark.table(table_names.ruleset_versions).where(
            f"ruleset_id = '{RULESET_ID}' AND version = '{VERSION}'"
        )
    )
    print(
        "Dry run only. Would retire "
        f"ruleset_id={RULESET_ID}, ruleset_name={row['ruleset_name']}, version={VERSION}"
    )
else:
    try:
        repository.retire(RULESET_ID, VERSION, retired_by=RETIRED_BY)
        append_log(
            {
                "pipeline_run_id": PIPELINE_RUN_ID,
                "event_time": utc_now(),
                "operation": "retire",
                "status": "retired",
                "reason": REASON,
                "ruleset_id": RULESET_ID,
                "ruleset_name": row["ruleset_name"],
                "version": VERSION,
                "content_hash": row["content_hash"],
                "source_yaml_path": None,
                "canonical_yaml_path": None,
                "original_yaml_archive_path": None,
                "published_by": RETIRED_BY,
                "retire_existing_published": None,
                "require_newer_version": None,
                "retired_ruleset_id": RULESET_ID,
                "retired_version": VERSION,
                "validation_issue_count": None,
                "validation_issues_json": None,
                "error_message": None,
                "error_traceback": None,
            }
        )
    except Exception as exc:
        append_log(
            {
                "pipeline_run_id": PIPELINE_RUN_ID,
                "event_time": utc_now(),
                "operation": "retire",
                "status": "failed",
                "reason": REASON,
                "ruleset_id": RULESET_ID,
                "ruleset_name": row["ruleset_name"],
                "version": VERSION,
                "content_hash": row["content_hash"],
                "source_yaml_path": None,
                "canonical_yaml_path": None,
                "original_yaml_archive_path": None,
                "published_by": RETIRED_BY,
                "retire_existing_published": None,
                "require_newer_version": None,
                "retired_ruleset_id": RULESET_ID,
                "retired_version": VERSION,
                "validation_issue_count": None,
                "validation_issues_json": None,
                "error_message": str(exc),
                "error_traceback": traceback.format_exc(),
            }
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verification

# COMMAND ----------

display(
    spark.table(table_names.ruleset_versions).where(
        f"ruleset_id = '{RULESET_ID}' AND version = '{VERSION}'"
    )
)

display(
    spark.table(table_names.ruleset_validation_logs).where(
        f"pipeline_run_id = '{PIPELINE_RUN_ID}'"
    )
)
