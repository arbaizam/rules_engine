# Databricks notebook source
# MAGIC %md
# MAGIC # Production YAML Publish Pipeline
# MAGIC
# MAGIC This notebook publishes a trusted production YAML artifact into the rules
# MAGIC engine registry.
# MAGIC
# MAGIC Intended job flow:
# MAGIC
# MAGIC 1. Read one YAML ruleset from a prod-controlled inbound path.
# MAGIC 2. Compile and normalize the ruleset.
# MAGIC 3. Validate it with the production Spark compatibility validator.
# MAGIC 4. Optionally retire the currently published version for the same ruleset.
# MAGIC 5. Publish it to the production ruleset registry.
# MAGIC 6. Export the canonical normalized YAML to an archive path.
# MAGIC 7. Append one publish log row to a Delta table.
# MAGIC
# MAGIC The rules engine package uses these tables under the configured `schema`:
# MAGIC
# MAGIC - `ruleset_versions`
# MAGIC - `function_registry`
# MAGIC
# MAGIC Logging is owned by this notebook, not by the rules engine package. When
# MAGIC enabled, this notebook writes to `<schema>.ruleset_validation_logs`.

# COMMAND ----------

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import traceback

from pyspark.sql import functions as F
from pyspark.sql import types as T

repo_root = Path.cwd()
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from rules_engine import (  # noqa: E402
    FunctionRegistry,
    PublishService,
    RulesetNormalizer,
    SparkRulesetCompatibilityValidator,
    YamlRulesetCompiler,
    YamlRulesetExporter,
    register_standard_functions,
)
from rules_engine.repository import RulesEngineTableNames, SparkDeltaRulesetRepository  # noqa: E402
from rules_engine.serializer import DeltaRowSerializer  # noqa: E402
from rules_engine.versioning import compare_versions  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Job Parameters
# MAGIC
# MAGIC Set these as Databricks job parameters/widgets.
# MAGIC
# MAGIC `schema` must be the target catalog and schema, for example
# MAGIC `catalog.schema`.
# MAGIC
# MAGIC `retire_existing_published=false` makes the job fail when another version
# MAGIC of the same `ruleset_name` is already published.
# MAGIC
# MAGIC `retire_existing_published=true` retires the currently published version
# MAGIC before publishing the incoming YAML. With `require_newer_version=true`, the
# MAGIC incoming version must use numeric dot notation and compare greater than
# MAGIC the currently published version, for example `2.1.0 > 1.0.0`.

# COMMAND ----------

dbutils.widgets.text("yaml_path", "")
dbutils.widgets.text("archive_dir", "")
dbutils.widgets.text("schema", "YOUR_CATALOG.YOUR_SCHEMA")
dbutils.widgets.text("published_by", "prod-rules-pipeline")
dbutils.widgets.dropdown("create_metadata_tables", "false", ["false", "true"])
dbutils.widgets.dropdown("create_log_table", "true", ["false", "true"])
dbutils.widgets.dropdown("retire_existing_published", "false", ["false", "true"])
dbutils.widgets.dropdown("require_newer_version", "true", ["true", "false"])

YAML_PATH = dbutils.widgets.get("yaml_path").strip()
ARCHIVE_DIR = dbutils.widgets.get("archive_dir").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
PUBLISHED_BY = dbutils.widgets.get("published_by").strip() or "prod-rules-pipeline"
CREATE_METADATA_TABLES = dbutils.widgets.get("create_metadata_tables") == "true"
CREATE_LOG_TABLE = dbutils.widgets.get("create_log_table") == "true"
RETIRE_EXISTING_PUBLISHED = dbutils.widgets.get("retire_existing_published") == "true"
REQUIRE_NEWER_VERSION = dbutils.widgets.get("require_newer_version") == "true"

if not YAML_PATH:
    raise ValueError("Parameter yaml_path is required.")
if not ARCHIVE_DIR:
    raise ValueError("Parameter archive_dir is required.")
if not SCHEMA or SCHEMA == "YOUR_CATALOG.YOUR_SCHEMA":
    raise ValueError("Parameter schema must be set to a real catalog.schema value.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers

# COMMAND ----------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def path_join(base: str, *parts: str) -> str:
    stripped = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts if part)
    return f"{stripped}/{suffix}" if suffix else stripped


def write_text(path: str, text: str, *, overwrite: bool = True) -> None:
    if path.startswith("dbfs:/"):
        dbutils.fs.put(path, text, overwrite)
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def copy_file(source: str, target: str) -> None:
    if source.startswith("dbfs:/") or target.startswith("dbfs:/"):
        dbutils.fs.cp(source, target, True)
        return
    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(Path(source).read_bytes())


def compile_ruleset(path: str):
    if path.startswith("dbfs:/"):
        return YamlRulesetCompiler().compile_text(dbutils.fs.head(path, 10 * 1024 * 1024))
    return YamlRulesetCompiler().compile_path(path)


def current_published_version(ruleset_name: str) -> dict | None:
    rows = (
        spark.table(table_names.ruleset_versions)
        .where(
            (F.col("ruleset_name") == ruleset_name)
            & (F.col("status") == "published")
        )
        .limit(2)
        .collect()
    )
    if not rows:
        return None
    if len(rows) > 1:
        raise ValueError(f"Multiple published versions found for {ruleset_name}.")
    return rows[0].asDict(recursive=True)


def append_log(row: dict) -> None:
    (
        spark.createDataFrame([row], schema=LOG_SCHEMA)
        .write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(LOG_TABLE)
    )


def validation_issues_json(validation) -> str:
    return json.dumps(
        [
            {
                "severity": issue.severity.value,
                "check_name": issue.check_name,
                "message": issue.message,
                "object_type": issue.object_type.value,
                "object_id": issue.object_id,
                "details": issue.details,
            }
            for issue in validation.issues
        ],
        sort_keys=True,
    )


PIPELINE_RUN_ID = safe_name(f"rules-publish-{utc_now()}")
LOG_TABLE = f"{SCHEMA}.ruleset_validation_logs"
LOG_SCHEMA = T.StructType(
    [
        T.StructField("pipeline_run_id", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("operation", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("reason", T.StringType(), True),
        T.StructField("ruleset_id", T.StringType(), True),
        T.StructField("ruleset_name", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("content_hash", T.StringType(), True),
        T.StructField("source_yaml_path", T.StringType(), True),
        T.StructField("canonical_yaml_path", T.StringType(), True),
        T.StructField("original_yaml_archive_path", T.StringType(), True),
        T.StructField("published_by", T.StringType(), True),
        T.StructField("retire_existing_published", T.BooleanType(), True),
        T.StructField("require_newer_version", T.BooleanType(), True),
        T.StructField("retired_ruleset_id", T.StringType(), True),
        T.StructField("retired_version", T.StringType(), True),
        T.StructField("validation_issue_count", T.IntegerType(), True),
        T.StructField("validation_issues_json", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("error_traceback", T.StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Repository

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

table_names = RulesEngineTableNames.from_schema(SCHEMA)

repository = SparkDeltaRulesetRepository(spark, table_names)

if CREATE_METADATA_TABLES:
    repository.create_base_tables(mode="error")

if CREATE_LOG_TABLE:
    (
        spark.createDataFrame([], schema=LOG_SCHEMA)
        .write.format("delta")
        .mode("ignore")
        .saveAsTable(LOG_TABLE)
    )

registry = register_standard_functions(FunctionRegistry())
normalizer = RulesetNormalizer()
validator = SparkRulesetCompatibilityValidator(registry)
publish_service = PublishService(
    repository=repository,
    validator=validator,
    normalizer=normalizer,
)
serializer = DeltaRowSerializer()
exporter = YamlRulesetExporter()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compile, Validate, Publish, Archive, Log

# COMMAND ----------

ruleset = None
normalized = None
content_hash = None
canonical_yaml_path = None
original_yaml_archive_path = None
validation = None
failure_logged = False
retired_ruleset_id = None
retired_version = None
log_reason = None

try:
    ruleset = compile_ruleset(YAML_PATH)
    normalized = normalizer.normalize_ruleset(ruleset)
    content_hash = serializer.content_hash(normalized)

    validation = validator.validate(normalized)
    if validation.has_errors():
        append_log(
            {
                "pipeline_run_id": PIPELINE_RUN_ID,
                "event_time": utc_now(),
                "operation": "publish",
                "status": "validation_failed",
                "reason": "validation failed",
                "ruleset_id": normalized.ruleset_id,
                "ruleset_name": normalized.ruleset_name,
                "version": normalized.version,
                "content_hash": content_hash,
                "source_yaml_path": YAML_PATH,
                "canonical_yaml_path": None,
                "original_yaml_archive_path": None,
                "published_by": PUBLISHED_BY,
                "retire_existing_published": RETIRE_EXISTING_PUBLISHED,
                "require_newer_version": REQUIRE_NEWER_VERSION,
                "retired_ruleset_id": None,
                "retired_version": None,
                "validation_issue_count": len(validation.issues),
                "validation_issues_json": validation_issues_json(validation),
                "error_message": validation.to_text(),
                "error_traceback": None,
            }
        )
        failure_logged = True
        raise ValueError(validation.to_text())

    existing_published = current_published_version(normalized.ruleset_name)
    if existing_published is not None:
        existing_version = existing_published["version"]
        if existing_published["ruleset_id"] == normalized.ruleset_id and existing_version == normalized.version:
            raise ValueError(
                f"Ruleset version is already published: "
                f"ruleset_id={normalized.ruleset_id}, version={normalized.version}"
            )
        if not RETIRE_EXISTING_PUBLISHED:
            raise ValueError(
                f"Another version is already published for {normalized.ruleset_name}: "
                f"version={existing_version}. Set retire_existing_published=true to cut over."
            )
        if REQUIRE_NEWER_VERSION and compare_versions(normalized.version, existing_version) <= 0:
            raise ValueError(
                f"Incoming version {normalized.version} must be greater than "
                f"existing published version {existing_version}."
            )
        repository.retire(
            existing_published["ruleset_id"],
            existing_version,
            retired_by=PUBLISHED_BY,
        )
        retired_ruleset_id = existing_published["ruleset_id"]
        retired_version = existing_version
        log_reason = (
            "auto-retired existing published version before publishing newer version"
        )

    publish_service.publish(
        normalized,
        published_by=PUBLISHED_BY,
    )

    archive_stamp = safe_name(utc_now())
    archive_leaf = path_join(
        safe_name(normalized.ruleset_id),
        f"v{safe_name(normalized.version)}",
        archive_stamp,
    )
    archive_base = path_join(ARCHIVE_DIR, archive_leaf)
    canonical_yaml_path = path_join(
        archive_base,
        f"{safe_name(normalized.ruleset_id)}_v{safe_name(normalized.version)}.canonical.yaml",
    )
    original_yaml_archive_path = path_join(
        archive_base,
        f"{safe_name(normalized.ruleset_id)}_v{safe_name(normalized.version)}.source.yaml",
    )

    write_text(canonical_yaml_path, exporter.export_text(normalized))
    copy_file(YAML_PATH, original_yaml_archive_path)

    append_log(
        {
            "pipeline_run_id": PIPELINE_RUN_ID,
            "event_time": utc_now(),
            "operation": "publish",
            "status": "published",
            "reason": log_reason,
            "ruleset_id": normalized.ruleset_id,
            "ruleset_name": normalized.ruleset_name,
            "version": normalized.version,
            "content_hash": content_hash,
            "source_yaml_path": YAML_PATH,
            "canonical_yaml_path": canonical_yaml_path,
            "original_yaml_archive_path": original_yaml_archive_path,
            "published_by": PUBLISHED_BY,
            "retire_existing_published": RETIRE_EXISTING_PUBLISHED,
            "require_newer_version": REQUIRE_NEWER_VERSION,
            "retired_ruleset_id": retired_ruleset_id,
            "retired_version": retired_version,
            "validation_issue_count": len(validation.issues),
            "validation_issues_json": validation_issues_json(validation),
            "error_message": None,
            "error_traceback": None,
        }
    )

except Exception as exc:
    if normalized is None:
        try:
            ruleset = compile_ruleset(YAML_PATH)
            normalized = normalizer.normalize_ruleset(ruleset)
            content_hash = serializer.content_hash(normalized)
        except Exception:
            pass

    if not failure_logged:
        append_log(
            {
                "pipeline_run_id": PIPELINE_RUN_ID,
                "event_time": utc_now(),
                "operation": "publish",
                "status": "failed",
                "reason": "pipeline failed",
                "ruleset_id": getattr(normalized, "ruleset_id", None),
                "ruleset_name": getattr(normalized, "ruleset_name", None),
                "version": getattr(normalized, "version", None),
                "content_hash": content_hash,
                "source_yaml_path": YAML_PATH,
                "canonical_yaml_path": canonical_yaml_path,
                "original_yaml_archive_path": original_yaml_archive_path,
                "published_by": PUBLISHED_BY,
                "retire_existing_published": RETIRE_EXISTING_PUBLISHED,
                "require_newer_version": REQUIRE_NEWER_VERSION,
                "retired_ruleset_id": retired_ruleset_id,
                "retired_version": retired_version,
                "validation_issue_count": (
                    len(validation.issues) if validation is not None else None
                ),
                "validation_issues_json": (
                    validation_issues_json(validation) if validation is not None else None
                ),
                "error_message": str(exc),
                "error_traceback": traceback.format_exc(),
            }
        )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification

# COMMAND ----------

published_ruleset = repository.load_published(
    normalized.ruleset_name,
    version=normalized.version,
)

display(
    spark.table(table_names.ruleset_versions).where(
        f"ruleset_id = '{normalized.ruleset_id}' AND version = '{normalized.version}'"
    )
)

display(
    spark.table(LOG_TABLE).where(
        f"pipeline_run_id = '{PIPELINE_RUN_ID}'"
    )
)

print(
    "Published "
    f"ruleset_id={published_ruleset.ruleset_id}, "
    f"ruleset_name={published_ruleset.ruleset_name}, "
    f"version={published_ruleset.version}, "
    f"content_hash={content_hash}"
)
