"""Zero-DBU Spark backend for the reverse-ETL sync (extraction: oss_spark).

Mirrors the Databricks-SQL helpers in reverse_etl_cdf_setup.py against OSS Delta on
a local Spark session, so the sync asset reads the change feed, the watermark, the
schema and the table history straight from the table's storage — no SQL warehouse,
no DBUs. See docs/poc/REVERSE_ETL_DBU_COST_NOTES.md sections 4-5.

What stays on Databricks: the seed and make-change jobs (Databricks SQL writes to
the UniForm source table, which OSS Delta cannot commit to) and the notebook
trigger path. The notebook keeps sharing the watermark through the same plain
Delta state table (option A, section 5).

Auth and locations: table storage locations resolve through the Unity Catalog
REST API (control plane, free) with the same service-principal token the other
assets mint; the ADLS_* service principal from .env reads the tables' bytes
directly. The state table must be plain Delta (option A) — OSS Delta cannot
commit to UniForm tables ([DELTA_MISSING_ICEBERG_CLASS]).

pyspark/delta are imported lazily so definitions keep loading where they are not
installed.
"""

import os
import re

import requests

from .reverse_etl_cdf_setup import (
    COMMIT_VERSION_COLUMN,
    _MICROSECOND_TIMESTAMP_FORMAT,
    ZONED_TIMESTAMP,
    RowField,
)
from .reverse_etl_config import ReverseEtlSyncConfig

# Delta/Spark schema types -> Kafka Connect types, same semantics as
# _CONNECT_TYPE_BY_DATABRICKS_TYPE in reverse_etl_cdf_setup.py: anything not
# listed travels as a plain string.
_CONNECT_TYPE_BY_SPARK_TYPE = {
    "byte": "int8",
    "short": "int16",
    "integer": "int32",
    "long": "int64",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "string": "string",
    "timestamp": ZONED_TIMESTAMP,
}


def _require_storage_env() -> None:
    missing = [k for k in ("ADLS_CLIENT_ID", "ADLS_TENANT_ID", "ADLS_CLIENT_SECRET") if not os.environ.get(k)]
    if missing:
        raise ValueError(f"Missing required env vars for direct storage access: {missing}")


def get_table_location(token: str, cfg: ReverseEtlSyncConfig, fqn: str) -> str:
    """The table's abfss storage location, from the Unity Catalog REST API.

    Control-plane call — no compute, no DBUs. Replaces the DESCRIBE DETAIL the
    prototypes ran once on the warehouse.
    """
    from .databricks_optimize import DATABRICKS_HOST

    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{fqn}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"UC REST lookup for {fqn} failed ({resp.status_code}): {resp.text[:300]}")
    location = resp.json().get("storage_location", "")
    if not location:
        raise RuntimeError(f"UC REST returned no storage_location for {fqn}")
    return location


def get_spark_session(storage_account: str):
    """Local Spark session for one sync run. Small on purpose: it runs inside the
    webserver container (1536M) next to the webserver itself. `storage_account` is
    the account host prefix of the tables' abfss location (from get_table_location)."""
    os.environ.pop("SPARK_REMOTE", None)
    os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    _require_storage_env()
    host = f"{storage_account}.dfs.core.windows.net"

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("reverse-etl-oss-spark-extraction")
        # 4 threads: the ABFS/Delta log reads run small multi-task stages (a
        # 50-task listing stage took ~10s sequential on local[1]); 4 cuts it to
        # ~a quarter. The CDF replay itself is a chain of single-task stages --
        # cores don't help there. Heap stays at 512m: the data is KBs, and the
        # webserver container (1.5 GB total) hosts the JVM alongside Dagster.
        .master("local[4]")
        # 512m is Spark's own floor; the webserver container also hosts the server.
        .config("spark.driver.memory", "512m")
        # macOS: otherwise the driver fails to bind to the machine hostname.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config(f"fs.azure.account.auth.type.{host}", "OAuth")
        .config(
            f"fs.azure.account.oauth.provider.type.{host}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"fs.azure.account.oauth2.client.id.{host}", os.environ["ADLS_CLIENT_ID"])
        .config(f"fs.azure.account.oauth2.client.secret.{host}", os.environ["ADLS_CLIENT_SECRET"])
        .config(
            f"fs.azure.account.oauth2.client.endpoint.{host}",
            f"https://login.microsoftonline.com/{os.environ['ADLS_TENANT_ID']}/oauth2/token",
        )
    )
    # delta-spark installed as a pip package (the repo pins it) wires the Delta
    # jars in; hadoop-azure always comes from Maven (pyspark does not bundle it).
    from delta import configure_spark_with_delta_pip

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.4.2"]
    ).getOrCreate()
    # Copy the fs.* ABFS settings into the session's global Hadoop conf: Spark only
    # auto-propagates spark.hadoop.* there, while driver-side filesystem access
    # (e.g. _delta_log_versions) reads exactly this conf.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    all_conf = spark.conf.getAll
    if callable(all_conf):
        all_conf = all_conf()
    for key, value in all_conf.items():
        if key.startswith("fs."):
            hadoop_conf.set(key, value)
    # Timestamps render per this timezone in date_format; the collect path would
    # otherwise show them in the machine's zone (REVERSE_ETL_DRT_COMPARISON.md 3.4).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_row_fields(spark, location: str) -> list[RowField]:
    """The source table's current columns from its Delta schema (no scan, no SQL).

    Same shape and ordering as _get_row_fields()'s information_schema read, so the
    schema-evolution flow (message schema, Postgres types, RisingWave columns) is
    unchanged."""
    from pyspark.sql.types import StructType

    df = spark.read.format("delta").load(location)
    fields: list[RowField] = []
    for field in df.schema.fields:
        type_name = field.dataType.typeName() if not isinstance(field.dataType, StructType) else "string"
        fields.append((field.name, _CONNECT_TYPE_BY_SPARK_TYPE.get(type_name, "string"), field.nullable))
    if not fields:
        raise RuntimeError(f"No columns found in the Delta schema at {location}")
    return fields


def read_last_version(spark, cfg: ReverseEtlSyncConfig, state_location: str) -> int | None:
    rows = [
        r.asDict()
        for r in spark.read.format("delta")
        .load(state_location)
        .where(f"sync_name = '{cfg.sync_name}'")
        .collect()
    ]
    return int(rows[0]["last_commit_version"]) if rows else None


def write_last_version(spark, cfg: ReverseEtlSyncConfig, state_location: str, version: int) -> None:
    """Same MERGE as _write_last_version(), through OSS Delta — idempotent across
    retries, and the notebook keeps reading/writing the same row."""
    from delta.tables import DeltaTable

    dt = DeltaTable.forPath(spark, state_location)
    source = spark.createDataFrame(
        [(cfg.sync_name, version)], "sync_name string, last_commit_version bigint"
    )
    (
        dt.alias("t")
        .merge(source.alias("s"), "t.sync_name = s.sync_name")
        .whenMatchedUpdate(set={"last_commit_version": "s.last_commit_version"})
        .whenNotMatchedInsertAll()
        .execute()
    )


def _delta_log_versions(spark, location: str) -> list[int]:
    """The commit versions still present in the table's _delta_log, read driver-side
    through Hadoop's FS API -- no Spark job, no stage. Delta's own discovery rule:
    a commit is exactly a 20-digit .json file; checkpoints, _last_checkpoint and
    .crc sidecars don't count. Replaces history() reads, which ran a 50-task stage
    over the whole log (~5-10s of ABFS latency) just to find min/max."""
    jvm = spark._jvm
    log_path = jvm.org.apache.hadoop.fs.Path(location.rstrip("/") + "/_delta_log")
    fs = log_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    versions = [
        int(name[:20])
        for name in (status.getPath().getName() for status in fs.listStatus(log_path))
        if re.fullmatch(r"\d{20}\.json", name)
    ]
    if not versions:
        raise RuntimeError(f"No Delta commit files found under {log_path} -- is {location} a Delta table?")
    return versions


def get_current_version(spark, location: str) -> int:
    """The table's latest committed version (DESCRIBE HISTORY LIMIT 1 equivalent)."""
    return max(_delta_log_versions(spark, location))


def get_earliest_available_version(spark, location: str) -> int:
    """Earliest version still in the transaction log (full DESCRIBE HISTORY
    equivalent) — drives the first-run backfill-vs-baseline decision."""
    return min(_delta_log_versions(spark, location))


def read_changes(spark, cfg: ReverseEtlSyncConfig, location: str, since_version: int) -> list[dict]:
    """Batch-read CDF rows via readChangeFeed, ordered by commit version.

    Same output contract as _read_changes(): one dict per CDF row, key columns and
    payload columns typed, every TIMESTAMP column and _commit_timestamp rendered as
    a microsecond UTC string via date_format (the collect path must not carry
    TimestampType — see the module docstring of
    scripts/spark_cdf_zero_dbu_prototype.py)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", since_version)
        .load(location)
    )
    exprs = []
    for field in df.schema.fields:
        col = F.col(f"`{field.name}`")
        if isinstance(field.dataType, TimestampType):
            col = F.date_format(col, _MICROSECOND_TIMESTAMP_FORMAT)
        exprs.append(col.alias(field.name))
    rows = [r.asDict() for r in df.select(*exprs).orderBy(COMMIT_VERSION_COLUMN).collect()]
    return rows
