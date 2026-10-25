"""Create the Databricks-side "latest turnover per customer" view.

RisingWave lands per-event rolling-turnover snapshots append-only into
de_dev.rw_poc.rw_casino_turnover_90d (sink_casino_turnover_90d_databricks).
Unity Catalog does not support Iceberg delete files, so the upsert / latest-row
collapse cannot happen at the sink (see docs/poc/DATABRICKS_ICEBERG_SINK.md §16).
Instead it is applied here at read time: a view that keeps only the most recent
row per customer via QUALIFY ROW_NUMBER(). This reproduces the RisingWave MV
mv_casino_turnover_latest without any window recompute on the Databricks side.
"""
import time

from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

from .databricks_optimize import (
    CLIENT_ID,
    CLIENT_SECRET,
    DATABRICKS_HOST,
    TENANT_ID,
    _get_token,
    _poll,
    _submit,
)

SOURCE_TABLE = "de_dev.rw_poc.rw_casino_turnover_90d"
VIEW_NAME = "de_dev.rw_poc.v_casino_turnover_latest"

# Mirrors mv_casino_turnover_latest: latest row per customer over the
# append-only turnover snapshots. ORDER BY event_ts DESC matches the MV exactly
# (ties broken arbitrarily, same as the MV's ROW_NUMBER).
VIEW_DDL = f"""
CREATE OR REPLACE VIEW {VIEW_NAME} AS
SELECT
    customer_id,
    rolling_7d_turnover AS casino_turnover,
    event_ts
FROM {SOURCE_TABLE}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY event_ts DESC
) = 1
""".strip()


def _run(token: str, statement: str) -> dict:
    """Submit a statement and poll to completion, returning the final response."""
    data = _submit(token, statement)
    state = data.get("status", {}).get("state", "")
    if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        data = _poll(token, data["statement_id"])
    return data


@asset(
    group_name="casino_databricks",
    deps=[AssetDep(AssetKey(["public", "sink_casino_turnover_90d_databricks"]))],
    description=(
        "CREATE OR REPLACE the Databricks view v_casino_turnover_latest — latest "
        "rolling-turnover row per customer (QUALIFY ROW_NUMBER) over the "
        "append-only rw_casino_turnover_90d table. Provides upsert semantics "
        "without Iceberg delete files. Recreated each run."
    ),
)
def databricks_turnover_latest_view(context: AssetExecutionContext):
    """Wait for the turnover table to be queryable, then (re)create the view."""
    missing = [k for k, v in {
        "DBT_DATABRICKS_HOST":            DATABRICKS_HOST,
        "DATABRICKS_AZURE_TENANT_ID":     TENANT_ID,
        "DATABRICKS_AZURE_CLIENT_ID":     CLIENT_ID,
        "DATABRICKS_AZURE_CLIENT_SECRET": CLIENT_SECRET,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")

    token = _get_token()

    # Gate on the table being queryable. The sink commits its first snapshot
    # within a few checkpoint intervals; until then the table may not exist.
    # Best-effort like casino_trino_views — warn and proceed rather than fail
    # the run, so a slow first commit doesn't hard-block the pipeline.
    # Probe every 5s so the view appears within a few seconds of the first
    # commit (~10s at commit_checkpoint_interval=5). 60 attempts keeps the same
    # ~5 min safety ceiling for the case where the sink never commits.
    context.log.info(f"Waiting for {SOURCE_TABLE} to be queryable...")
    table_ready = False
    for attempt in range(60):  # ~5 min at 5s
        probe = _run(token, f"SELECT 1 FROM {SOURCE_TABLE} LIMIT 1")
        if probe.get("status", {}).get("state", "") == "SUCCEEDED":
            context.log.info(f"{SOURCE_TABLE} queryable after ~{attempt * 5}s")
            table_ready = True
            break
        time.sleep(5)

    if not table_ready:
        context.log.warning(
            f"{SOURCE_TABLE} not queryable after ~300s — skipping view creation. "
            "Re-materialize this asset once the sink has committed its first snapshot."
        )
        return {"view": MetadataValue.text(VIEW_NAME), "status": MetadataValue.text("skipped — table not ready")}

    context.log.info(f"Creating view {VIEW_NAME}")
    data = _run(token, VIEW_DDL)
    state = data.get("status", {}).get("state", "")
    if state != "SUCCEEDED":
        error = data.get("status", {}).get("error", {}).get("message", state)
        raise RuntimeError(f"Failed to create {VIEW_NAME}: {error}")

    context.log.info(f"✓ View {VIEW_NAME} created")
    return {
        "view":   MetadataValue.text(VIEW_NAME),
        "source": MetadataValue.text(SOURCE_TABLE),
        "status": MetadataValue.text("created"),
    }
