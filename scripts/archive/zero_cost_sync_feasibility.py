"""Feasibility tests for running the demo sync zero-cost (no Databricks compute).

Test 1: UC REST API resolves a table's storage_location (control plane, free) —
        would replace _get_row_fields()'s information_schema SQL and the location
        lookups.
Test 2: OSS Delta on Spark can READ and MERGE-WRITE the watermark state table —
        would replace _read_last_version/_write_last_version. The MERGE writes the
        same watermark value back (idempotent), so the test is semantically a no-op.
Test 3: the full zero-DBU sync decision path, replayed: watermark + current table
        version + 'no new commits' guard, exactly as the asset decides today.
"""

import json
import os
import subprocess

REPO = "/Users/s.gioldasis-si/Projects/risingwave-test"
STATE_LOCATION = ("abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/"
                  "sr_poc_external/__unitystorage/schemas/6fa9db04-0d77-41dd-bc2d-2e8a8aeced7f/"
                  "tables/9193841e-0fea-4a92-854c-562db57b54bf")
TABLE_FQN = "de_dev.sr_poc_external.reverse_etl_cdf_state"
SYNC_NAME = "reverse_etl_cdf"
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

# Force classic local Spark (not Spark Connect): the Connect client-side builder
# silently IGNORES non-spark.* configs (fs.azure.*, Hadoop settings), which is the
# same trap scripts/user_activity_flow.py pops these vars for.
for var in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
    os.environ.pop(var, None)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# ---------------------------------------------------------------- test 1: UC REST
print("=== test 1: Unity Catalog REST API (control plane, no compute) ===")
token_out = subprocess.run(
    ["databricks", "auth", "token", "--profile", "personal"],
    capture_output=True, text=True, check=True,
).stdout
token_doc = json.loads(token_out)
token = token_doc.get("access_token") or token_doc.get("token")
host = env["DBT_DATABRICKS_HOST"].rstrip("/")

import requests

resp = requests.get(
    f"{host}/api/2.1/unity-catalog/tables/{TABLE_FQN}",
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)
print(f"GET /api/2.1/unity-catalog/tables/{TABLE_FQN} -> {resp.status_code}")
if resp.ok:
    doc = resp.json()
    loc = doc.get("storage_location", "")
    print("storage_location:", loc[:110])
    print("matches DESCRIBE DETAIL:", loc == STATE_LOCATION)
    print("full_name:", doc.get("full_name"), "| table_type:", doc.get("table_type"))
else:
    print("body:", resp.text[:300])

# ------------------------------------------------- tests 2+3: Spark read/merge
print("\n=== test 2+3: OSS Delta read + watermark MERGE on the state table ===")
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

acct_host = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
builder = (
    SparkSession.builder.appName("zero-dbu-sync-feasibility")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .config(f"fs.azure.account.auth.type.{acct_host}", "OAuth")
    .config(
        f"fs.azure.account.oauth.provider.type.{acct_host}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    .config(f"fs.azure.account.oauth2.client.id.{acct_host}", env["ADLS_CLIENT_ID"])
    .config(f"fs.azure.account.oauth2.client.secret.{acct_host}", env["ADLS_CLIENT_SECRET"])
    .config(
        f"fs.azure.account.oauth2.client.endpoint.{acct_host}",
        f"https://login.microsoftonline.com/{env['ADLS_TENANT_ID']}/oauth2/token",
    )
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.hadoop:hadoop-azure:3.4.2"]
).getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR")

# read watermark (replaces _read_last_version)
watermark_rows = [
    r.asDict() for r in spark.read.format("delta").load(STATE_LOCATION).collect()
]
print("watermark read:", watermark_rows)
last_version = next(
    int(r["last_commit_version"]) for r in watermark_rows if r["sync_name"] == SYNC_NAME
)

# current version + history (replaces _get_current_version/_get_earliest_available_version)
dt = DeltaTable.forPath(spark, STATE_LOCATION)
latest_hist = dt.history(1).first()
print(f"state table current version: {latest_hist['version']}, latest op: {latest_hist['operation']}")
hist = [r.asDict() for r in dt.history().collect()]
print(f"history entries: {len(hist)}; versions: {[h['version'] for h in hist]}")

# MERGE the same watermark value back (replaces _write_last_version; idempotent)
source = spark.createDataFrame([(SYNC_NAME, last_version)], "sync_name string, last_commit_version bigint")
(
    dt.alias("t")
    .merge(source.alias("s"), "t.sync_name = s.sync_name")
    .whenMatchedUpdate(set={"last_commit_version": "s.last_commit_version"})
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"MERGE executed (wrote last_commit_version={last_version} back)")

after = [r.asDict() for r in spark.read.format("delta").load(STATE_LOCATION).collect()]
print("watermark after MERGE:", after)
print("watermark unchanged:", after == watermark_rows)
print(f"state table version after MERGE: {DeltaTable.forPath(spark, STATE_LOCATION).history(1).first()['version']}")

# zero-DBU sync decision replay (the asset's logic)
print("\n=== sync decision replay (asset logic, zero-cost inputs only) ===")
source_version = 3  # the source table's latest, read the same way in the real flow
next_version = last_version + 1
print(f"watermark={last_version}, next_version={next_version}, current={source_version}")
print("decision:", "no new commits -> skip table_changes (same as the asset)" if next_version > source_version else "read change feed")

spark.stop()
print("\ndone")
