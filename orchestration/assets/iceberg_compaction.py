"""Iceberg maintenance job for the ``iceberg_hermes_features`` table.

Runs an in-process Spark 4 session inside the Dagster container and invokes
the standard Iceberg maintenance procedures via the Lakekeeper REST catalog:

    rewrite_data_files  ->  rewrite_position_delete_files  ->  rewrite_manifests
    ->  expire_snapshots  ->  remove_orphan_files

The Spark session is built once per run via a Dagster ``@resource`` so all ops
share the same JVM. Triggered on-demand from the Dagster UI; no schedule.

Configuration is read from environment variables (defaults set in
``Dockerfile.dagster``):
    ICEBERG_REST_URI, ICEBERG_WAREHOUSE, ICEBERG_CATALOG_NAME,
    ICEBERG_TARGET_TABLE, ICEBERG_JARS_DIR,
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY.
"""
from __future__ import annotations

import glob
import logging
import os
from datetime import datetime, timedelta, timezone

from dagster import (
    InitResourceContext,
    MetadataValue,
    job,
    op,
    resource,
)

logger = logging.getLogger(__name__)


# ---------- configuration helpers -----------------------------------------

ICEBERG_VERSION = "1.10.1"

# Trimmed --add-opens set required by Iceberg + Arrow on JDK 17/21
# (see /memories/repo/spark4-migration.md).
_JAVA_OPTS = (
    "-XX:+IgnoreUnrecognizedVMOptions "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)


def _catalog_name() -> str:
    return os.environ.get("ICEBERG_CATALOG_NAME", "lakekeeper")


def _target_table() -> str:
    """Fully-qualified table name including catalog: ``<catalog>.<namespace>.<table>``."""
    table = os.environ.get("ICEBERG_TARGET_TABLE", "public.iceberg_hermes_features")
    return f"{_catalog_name()}.{table}"


def _table_arg() -> str:
    """Table argument passed to Iceberg system procedures (namespace.table only)."""
    return os.environ.get("ICEBERG_TARGET_TABLE", "public.iceberg_hermes_features")


def _discover_iceberg_jars() -> str:
    jars_dir = os.environ.get("ICEBERG_JARS_DIR", "/opt/iceberg-jars")
    jars = sorted(glob.glob(os.path.join(jars_dir, "*.jar")))
    if not jars:
        raise RuntimeError(
            f"No Iceberg jars found in {jars_dir}. Rebuild Dockerfile.dagster "
            "(Iceberg jars are pre-staged at image build time)."
        )
    return ",".join(jars)


def _build_spark_session():
    """Build the in-process Spark 4 session used by all compaction ops.

    Mirrors ``scripts/user_activity_flow.py`` but uses container hostnames and
    the pre-baked Iceberg jars at ``/opt/iceberg-jars`` (no Ivy fetch).
    Caller is responsible for ``spark.stop()``.
    """
    # CRITICAL: clear any inherited Spark Connect env before importing pyspark.
    os.environ.pop("SPARK_REMOTE", None)
    os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    from pyspark.sql import SparkSession

    catalog = _catalog_name()
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://lakekeeper:8181/catalog")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "risingwave-warehouse")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio-0:9301")
    s3_access = os.environ.get("S3_ACCESS_KEY", "hummockadmin")
    s3_secret = os.environ.get("S3_SECRET_KEY", "hummockadmin")
    jars = _discover_iceberg_jars()

    return (
        SparkSession.builder
        .appName("IcebergCompaction")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.extraJavaOptions", _JAVA_OPTS)
        # Pre-baked jars — no network fetch at run time.
        .config("spark.jars", jars)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Lakekeeper REST catalog
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", rest_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.cache-enabled", "false")
        .config(f"spark.sql.catalog.{catalog}.cache.expiration-interval-ms", "0")
        # MinIO / S3
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", s3_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", s3_access)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", s3_secret)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.s3.ssl-enabled", "false")
        .config(f"spark.sql.catalog.{catalog}.s3.checksum-enabled", "false")
        .config(f"spark.sql.catalog.{catalog}.s3.connection-timeout-ms", "60000")
        .config(f"spark.sql.catalog.{catalog}.s3.socket-timeout-ms", "60000")
        .config(f"spark.sql.catalog.{catalog}.s3.connection-maximum-connections", "100")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------- Dagster resource ----------------------------------------------


@resource(description="In-process Spark 4 session shared across iceberg_compaction_job ops.")
def spark_session_resource(context: InitResourceContext):
    """Resource that yields a SparkSession and stops it on teardown.

    On setup failure (JDK/jar misconfig, catalog auth failure, etc.) the
    exception is logged and re-raised so the whole run fails before any op
    executes. Failures are not retried — Iceberg procedures are not safely
    idempotent mid-commit.
    """
    try:
        spark = _build_spark_session()
    except Exception as exc:
        context.log.error(f"Failed to build SparkSession for Iceberg compaction: {exc!r}")
        raise

    context.log.info(f"SparkSession started (version={spark.version})")
    try:
        yield spark
    finally:
        try:
            spark.stop()
            context.log.info("SparkSession stopped")
        except Exception as exc:  # noqa: BLE001 — teardown best-effort
            context.log.warning(f"Error stopping SparkSession: {exc!r}")


# ---------- op helpers -----------------------------------------------------


def _run_procedure(context, spark, label: str, sql: str) -> dict:
    """Execute a single Iceberg ``CALL ...`` and surface row metadata.

    On exception, logs the procedure name + stack and re-raises so downstream
    ops are skipped via Dagster's default failure propagation.
    """
    context.log.info(f"[{label}] running: {sql}")
    try:
        rows = spark.sql(sql).collect()
    except Exception as exc:
        context.log.exception(f"[{label}] procedure failed: {exc!r}")
        context.add_output_metadata({"error": MetadataValue.text(repr(exc))})
        raise

    result = rows[0].asDict() if rows else {}
    context.log.info(f"[{label}] result: {result}")
    context.add_output_metadata(
        {
            "procedure": MetadataValue.text(label),
            "sql": MetadataValue.md(f"```sql\n{sql}\n```"),
            **{k: MetadataValue.text(str(v)) for k, v in result.items()},
        }
    )
    return result


# ---------- ops ------------------------------------------------------------


@op(required_resource_keys={"spark"})
def op_rewrite_data_files(context) -> dict:
    catalog = _catalog_name()
    table = _table_arg()
    spark = context.resources.spark

    # Force the catalog to refresh and confirm Spark actually sees the data
    # files. Iceberg REST clients cache table metadata aggressively; without a
    # refresh, rewrite_data_files can resolve an older empty snapshot and
    # report 0/0/0 with no error.
    spark.sql(f"REFRESH TABLE {catalog}.{table}")
    files_seen = spark.sql(f"SELECT count(*) AS n FROM {catalog}.{table}.files").collect()[0]["n"]
    context.log.info(f"[rewrite_data_files] spark sees {files_seen} entries in $files")
    context.add_output_metadata({"files_seen_by_spark": MetadataValue.int(int(files_seen))})

    # Mirror RisingWave's built-in Iceberg compactor (compaction.type='full'):
    #   - rewrite-all=true forces every data file to be considered
    #   - target-file-size = 1024 MB (RisingWave default compaction.target_file_size_mb)
    # partial-progress.enabled=true is required here: the RisingWave sink keeps
    # committing to `main` while this rewrite runs, so a single all-or-nothing
    # commit always loses the race (CommitFailedException). With partial
    # progress, each file group commits independently; conflicting groups are
    # retried/dropped while the rest succeed.
    sql = (
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{table}', "
        "strategy => 'binpack', "
        "options => map("
        "'rewrite-all','true',"
        "'target-file-size-bytes','1073741824',"
        "'partial-progress.enabled','true',"
        "'partial-progress.max-commits','100',"
        "'max-concurrent-file-group-rewrites','4'))"
    )
    return _run_procedure(context, spark, "rewrite_data_files", sql)


@op(required_resource_keys={"spark"})
def op_rewrite_position_deletes(context, _prev: dict) -> dict:
    catalog = _catalog_name()
    table = _table_arg()
    sql = (
        f"CALL {catalog}.system.rewrite_position_delete_files("
        f"table => '{table}', "
        "options => map("
        "'rewrite-all','true',"
        "'partial-progress.enabled','true',"
        "'partial-progress.max-commits','10',"
        "'max-concurrent-file-group-rewrites','4'))"
    )
    return _run_procedure(context, context.resources.spark, "rewrite_position_delete_files", sql)


@op(required_resource_keys={"spark"})
def op_rewrite_manifests(context, _prev: dict) -> dict:
    catalog = _catalog_name()
    table = _table_arg()
    sql = f"CALL {catalog}.system.rewrite_manifests('{table}')"
    return _run_procedure(context, context.resources.spark, "rewrite_manifests", sql)


@op(required_resource_keys={"spark"})
def op_expire_snapshots(context, _prev: dict) -> dict:
    catalog = _catalog_name()
    table = _table_arg()
    # 1 minute cutoff: with commit_checkpoint_interval=2 the table accumulates
    # snapshots fast, so a multi-day cutoff would never expire anything. 1m
    # is aggressive but acceptable for this demo stack (no long-running reads).
    older_than = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', "
        "retain_last => 5, "
        "stream_results => true)"
    )
    return _run_procedure(context, context.resources.spark, "expire_snapshots", sql)


# NOTE: remove_orphan_files intentionally NOT included here. It uses Hadoop
# FileSystem (not Iceberg's S3FileIO) to walk the table directory, which pulls
# in hadoop-aws + AWS SDK v2 bundle (large, version-coupled to Hadoop). It is
# also rarely needed: orphan files only result from failed writes, and
# expire_snapshots already removes the bulk of obsolete files. Run it manually
# via bin/5_spark_iceberg.sh if/when actually needed.


# ---------- job ------------------------------------------------------------


@job(
    name="iceberg_compaction_job",
    description=(
        "On-demand Iceberg maintenance for iceberg_hermes_features: "
        "rewrite_data_files -> rewrite_position_delete_files -> "
        "rewrite_manifests -> expire_snapshots."
    ),
    tags={"compaction": "iceberg"},
)
def iceberg_compaction_job():
    r1 = op_rewrite_data_files()
    r2 = op_rewrite_position_deletes(r1)
    r3 = op_rewrite_manifests(r2)
    op_expire_snapshots(r3)
