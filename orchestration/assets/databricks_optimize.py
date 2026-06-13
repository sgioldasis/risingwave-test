"""Run OPTIMIZE on Databricks Iceberg tables to compact small files.

RisingWave commits one Parquet file per checkpoint interval (~10s at
commit_checkpoint_interval=5). Without compaction, hundreds of small files
accumulate and queries slow down. This asset runs OPTIMIZE before the
DataFusion demo queries to keep query times low.
"""
import os
import time

import requests
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

DATABRICKS_HOST = os.environ.get("DBT_DATABRICKS_HOST", "")
TENANT_ID       = os.environ.get("DATABRICKS_AZURE_TENANT_ID", "")
CLIENT_ID       = os.environ.get("DATABRICKS_AZURE_CLIENT_ID", "")
CLIENT_SECRET   = os.environ.get("DATABRICKS_AZURE_CLIENT_SECRET", "")
WAREHOUSE_ID    = "4d06eca1e71a9ccc"

TABLES = [
    "de_dev.rw_poc.rw_casino_transactions",
    "de_dev.rw_poc.rw_sportsbook_bets",
    "de_dev.rw_poc.rw_casino_turnover_90d",
]


def _get_token() -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _submit(token: str, statement: str) -> dict:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "statement":    statement,
            "warehouse_id": WAREHOUSE_ID,
            "wait_timeout": "50s",
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _poll(token: str, statement_id: str, poll_interval: int = 5, max_wait: int = 600) -> dict:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        state = data.get("status", {}).get("state", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            return data
        time.sleep(poll_interval)
    raise TimeoutError(f"Statement {statement_id} did not complete within {max_wait}s")


@asset(
    group_name="databricks_datafusion",
    deps=[
        AssetDep(AssetKey(["public", "sink_casino_transactions_databricks"])),
        AssetDep(AssetKey(["public", "sink_sportsbook_bets_databricks"])),
    ],
    description=(
        "OPTIMIZE the Databricks Iceberg tables (see TABLES) to compact small files written by RisingWave sinks. "
        "Runs before DataFusion queries to keep query latency low."
    ),
)
def databricks_optimize(context: AssetExecutionContext):
    """Compact small Parquet files in each Databricks Iceberg table in TABLES."""
    missing = [k for k, v in {
        "DBT_DATABRICKS_HOST":            DATABRICKS_HOST,
        "DATABRICKS_AZURE_TENANT_ID":     TENANT_ID,
        "DATABRICKS_AZURE_CLIENT_ID":     CLIENT_ID,
        "DATABRICKS_AZURE_CLIENT_SECRET": CLIENT_SECRET,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")

    token = _get_token()
    results = {}

    for table in TABLES:
        context.log.info(f"Running OPTIMIZE on {table}...")
        t0 = time.time()

        data = _submit(token, f"OPTIMIZE {table}")
        state = data.get("status", {}).get("state", "")

        if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            statement_id = data["statement_id"]
            context.log.info(f"  Polling statement {statement_id}...")
            data = _poll(token, statement_id)
            state = data.get("status", {}).get("state", "")

        elapsed = round(time.time() - t0, 1)

        if state == "SUCCEEDED":
            context.log.info(f"✓ OPTIMIZE {table} completed in {elapsed}s")
            results[table] = f"✓ {elapsed}s"
        else:
            error = data.get("status", {}).get("error", {}).get("message", state)
            context.log.warning(f"✗ OPTIMIZE {table} failed: {error}")
            results[table] = f"✗ {error}"

    context.add_output_metadata({
        "tables_optimized": MetadataValue.int(sum(1 for v in results.values() if v.startswith("✓"))),
        **{f"optimize_{t.split('.')[-1]}": MetadataValue.text(v) for t, v in results.items()},
    })
