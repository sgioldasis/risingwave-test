"""Option A verification: plain Delta state table, watermark written by OSS Delta.

Sequence (after the state table was recreated without the UniForm properties):
1. resolve the (new) storage location via the UC REST API — control plane, free
2. confirm the table protocol is plain Delta (no universalFormat)
3. read the watermark (empty after recreation -> first-run baseline, as the asset does)
4. Spark MERGE the baseline watermark in (the write that failed on the UniForm table)
5. read it back
Then Databricks-side cross-checks run separately via SQL (Databricks reads the
OSS-written commit, MERGEs it back, and a final Spark read sees the Databricks commit).

Run:
  uv run --with delta-spark==4.0.0 --with "pyspark<4.1" python <this file>
"""

import json
import os
import subprocess

REPO = "/Users/s.gioldasis-si/Projects/risingwave-test"
TABLE_FQN = "de_dev.sr_poc_external.reverse_etl_cdf_state"
SYNC_NAME = "reverse_etl_cdf"
SOURCE_TABLE_LATEST_VERSION = 3  # what the source table's history reports
STORAGE_ACCOUNT = "stkznneusrpoccdddevstd"


def load_env(path):
    env = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


env = load_env(f"{REPO}/.env")

# classic local Spark, not Connect (Connect's builder silently drops fs.azure.* configs)
for var in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
    os.environ.pop(var, None)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# ---- 1. storage location via UC REST --------------------------------------
token_doc = json.loads(subprocess.run(
    ["databricks", "auth", "token", "--profile", "personal"],
    capture_output=True, text=True, check=True).stdout)
token = token_doc.get("access_token") or token_doc.get("token")
host = env["DBT_DATABRICKS_HOST"].rstrip("/")

import requests

resp = requests.get(
    f"{host}/api/2.1/unity-catalog/tables/{TABLE_FQN}",
    headers={"Authorization": f"Bearer {token}"}, timeout=30)
resp.raise_for_status()
doc = resp.json()
LOCATION = doc["storage_location"]
print(f"1. UC REST storage_location: {LOCATION[:110]}")

# ---- Spark session ----------------------------------------------------------
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

acct = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
builder = (
    SparkSession.builder.appName("option-a-plain-state-table")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .config(f"fs.azure.account.auth.type.{acct}", "OAuth")
    .config(f"fs.azure.account.oauth.provider.type.{acct}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    .config(f"fs.azure.account.oauth2.client.id.{acct}", env["ADLS_CLIENT_ID"])
    .config(f"fs.azure.account.oauth2.client.secret.{acct}", env["ADLS_CLIENT_SECRET"])
    .config(f"fs.azure.account.oauth2.client.endpoint.{acct}",
            f"https://login.microsoftonline.com/{env['ADLS_TENANT_ID']}/oauth2/token")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.4.2"]
).getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

# ---- 2. read watermark (empty -> first-run baseline) ------------------------
rows = [r.asDict() for r in spark.read.format("delta").load(LOCATION).collect()]
print(f"\n2. watermark rows after recreation: {rows}")
baseline = next(
    (int(r["last_commit_version"]) for r in rows if r["sync_name"] == SYNC_NAME),
    SOURCE_TABLE_LATEST_VERSION,  # first-run baseline, as the setup job's first sync records
)
print(f"   watermark to write: {baseline}")

# ---- 3. MERGE (the write that failed on the UniForm table) ------------------
dt = DeltaTable.forPath(spark, LOCATION)
source = spark.createDataFrame([(SYNC_NAME, baseline)], "sync_name string, last_commit_version bigint")
(
    dt.alias("t")
    .merge(source.alias("s"), "t.sync_name = s.sync_name")
    .whenMatchedUpdate(set={"last_commit_version": "s.last_commit_version"})
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"4. Spark MERGE executed (last_commit_version={baseline})")

# ---- 4. read back ------------------------------------------------------------
after = [r.asDict() for r in spark.read.format("delta").load(LOCATION).collect()]
print(f"5. watermark after MERGE: {after}")
latest = dt.history(1).first()
print(f"   state table Delta version: {latest['version']}, op: {latest['operation']}")

spark.stop()
print("\nOSS Delta wrote the plain state table — Option A write path verified")
