"""Prototype: zero-DBU change-data-feed read via self-managed OSS Delta on Spark.

Reads de_dev.sr_poc_external.reverse_etl_cdf_source straight from its abfss://
storage location (Unity Catalog managed table, UniForm) with OSS Delta +
hadoop-azure, authenticated with the ADLS_* service principal from .env. No
Databricks compute participates: no SQL warehouse, no clusters, no DBUs.

Verified working 2026-10-14 (see docs/poc/REVERSE_ETL_DBU_COST_NOTES.md section 4):
the current state, the complete change feed (6 rows across versions 1-3), a
Debezium-style replay reconstructing the current state, and the watermark all match
what Databricks itself reports.

Run today (laptop; pyspark must stay below 4.1 because Delta 4.0.0 is built for
Spark 4.0 and delta-spark does not pin pyspark itself):

  uv run --python 3.13 --with delta-spark==4.0.0 --with "pyspark<4.1" \
    python scripts/spark_cdf_zero_dbu_prototype.py

In the dagster container this needs the image rebuilt after the pyspark pin
(pyproject pins `pyspark>=4.0.0,<4.1.0` for Delta 4.0.0; the Iceberg runtime jar in
Dockerfile.dagster was switched to the Spark-4.0 artifact to match). Then run:

  docker compose build dagster-webserver   # off VPN, per the demo docs
  docker exec dagster-webserver python /workspace/scripts/spark_cdf_zero_dbu_prototype.py

Timestamps are rendered with date_format in the projection and never collected as
TimestampType: the collect path converts to naive datetimes in the machine's local
zone (the 3-hour shift documented in REVERSE_ETL_DRT_COMPARISON.md section 3.4)
regardless of spark.sql.session.timeZone. date_format honors the session timezone
and produces exactly the format _read_changes() sends to Kafka.
"""

from pathlib import Path

ENV_PATH = str(Path(__file__).resolve().parent.parent / ".env")
LOCATION = ("abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/"
            "sr_poc_external/__unitystorage/schemas/6fa9db04-0d77-41dd-bc2d-2e8a8aeced7f/"
            "tables/747277c1-fdbb-4757-8055-78d6c2a0b1ed")
STORAGE_ACCOUNT = "stkznneusrpoccdddevstd"
# io.delta:delta-spark_2.13:4.0.0 is the latest Delta for Spark 4 (Scala 2.13);
# hadoop-azure 3.4.2 matches the Hadoop 3.4.2 client pyspark 4.0/4.1 bundles.
PACKAGES = "io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-azure:3.4.2"
# Same format _read_changes() uses for the Statement Execution API.
MICROSECOND_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


def load_env(path):
    env = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


env = load_env(ENV_PATH)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

host = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
builder = (
    SparkSession.builder.appName("reverse-etl-cdf-zero-dbu")
    .master("local[1]")
    # 512m is Spark's own floor (384m fails with INVALID_DRIVER_MEMORY); small on
    # purpose because the webserver container has a 1536M limit and is running.
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
    .config(f"fs.azure.account.oauth2.client.id.{host}", env["ADLS_CLIENT_ID"])
    .config(f"fs.azure.account.oauth2.client.secret.{host}", env["ADLS_CLIENT_SECRET"])
    .config(
        f"fs.azure.account.oauth2.client.endpoint.{host}",
        f"https://login.microsoftonline.com/{env['ADLS_TENANT_ID']}/oauth2/token",
    )
)

# delta-spark installed as a pip package (the uv run above) wires its own jars in;
# otherwise (the container) pull them from Maven at session start. hadoop-azure is
# always pulled via extra_packages/packages: pyspark does not bundle it.
try:
    from delta import configure_spark_with_delta_pip

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.4.2"]
    ).getOrCreate()
except ImportError:
    spark = builder.config("spark.jars.packages", PACKAGES).getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

import pyspark

print(f"\npyspark {pyspark.__version__}")

# Timestamps render via date_format only (see module docstring).
state_df = spark.read.format("delta").load(LOCATION).select(
    "rid", "id", "value",
    F.date_format("updated_at", MICROSECOND_TIMESTAMP_FORMAT).alias("updated_at"),
)

print("\n=== 1. current state (OSS Delta, direct abfss read, no Databricks) ===")
state_rows = [r.asDict() for r in state_df.collect()]
for row in state_rows:
    print("  ", row)

print("\n=== 2. change feed, versions 0..latest (readChangeFeed) ===")
cdf_rows = [
    r.asDict()
    for r in (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(LOCATION)
        .select(
            "rid", "id", "value",
            F.date_format("updated_at", MICROSECOND_TIMESTAMP_FORMAT).alias("updated_at"),
            "_change_type", "_commit_version",
            F.date_format("_commit_timestamp", MICROSECOND_TIMESTAMP_FORMAT).alias("_commit_timestamp"),
        )
        .orderBy("_commit_version")
        .collect()
    )
]
print(f"{len(cdf_rows)} CDF row(s)")
for row in cdf_rows:
    print("  ", row)

print("\n=== 3. replay check (Debezium-style application of the feed) ===")
final = {}
for row in cdf_rows:
    ct = row["_change_type"]
    if ct in ("insert", "update_postimage"):
        final[row["rid"]] = row["value"]
    elif ct == "delete":
        final.pop(row["rid"], None)
actual = {r["rid"]: r["value"] for r in state_rows}
print("replayed:", final)
print("actual:  ", actual)
print("match:   ", final == actual)

print("\n=== 4. watermark ===")
max_version = max(r["_commit_version"] for r in cdf_rows)
print(f"highest _commit_version read from storage: {max_version} (Databricks recorded watermark: 3)")

spark.stop()
print("\ndone — no Databricks compute was used for any of the above")
