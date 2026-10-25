"""Trigger the landing-to-bronze Databricks notebook from Dagster.

Reads raw Protobuf bytes from rw_casino_landing (Databricks Iceberg) and
decodes them into rw_casino_landing_bronze using the notebook at:
  /Workspace/Shared/rw_poc/landing_to_bronze_casino
"""
import os
import time

import requests
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

DATABRICKS_HOST = os.environ.get("DBT_DATABRICKS_HOST", "")
TENANT_ID       = os.environ.get("DATABRICKS_AZURE_TENANT_ID", "")
CLIENT_ID       = os.environ.get("DATABRICKS_AZURE_CLIENT_ID", "")
CLIENT_SECRET   = os.environ.get("DATABRICKS_AZURE_CLIENT_SECRET", "")

NOTEBOOK_PATH = "/Workspace/Shared/rw_poc/landing_to_bronze_casino"


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


def _submit_run(token: str) -> dict:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "run_name":          "landing_to_bronze_casino",
            "existing_cluster_id": "1216-211611-qcydcvqf",
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH,
            },
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _get_notebook_row_count(token: str, run_id: int) -> int | None:
    """Read the integer returned by dbutils.notebook.exit() from the completed run."""
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
        headers={"Authorization": f"Bearer {token}"},
        params={"run_id": run_id},
        timeout=30,
    )
    resp.raise_for_status()
    exit_value = resp.json().get("notebook_output", {}).get("result", "")
    try:
        return int(exit_value.strip())
    except (ValueError, AttributeError):
        return None


def _poll_run(token: str, run_id: int, poll_interval: int = 10, max_wait: int = 1800) -> dict:
    """Poll until the run reaches a terminal state."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers={"Authorization": f"Bearer {token}"},
            params={"run_id": run_id},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        life_cycle = data.get("state", {}).get("life_cycle_state", "")
        if life_cycle in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return data
        time.sleep(poll_interval)
    raise TimeoutError(f"Run {run_id} did not complete within {max_wait}s")


@asset(
    group_name="casino_databricks",
    deps=[
        AssetDep(AssetKey(["public", "sink_casino_landing_databricks"])),
    ],
    description=(
        "Decode raw Protobuf bytes from rw_casino_landing into typed bronze table "
        "rw_casino_landing_bronze. Runs the casino_landing_to_bronze Databricks notebook."
    ),
)
def casino_landing_to_bronze(context: AssetExecutionContext):
    """Submit and wait for the landing-to-bronze Databricks notebook run."""
    missing = [k for k, v in {
        "DBT_DATABRICKS_HOST":            DATABRICKS_HOST,
        "DATABRICKS_AZURE_TENANT_ID":     TENANT_ID,
        "DATABRICKS_AZURE_CLIENT_ID":     CLIENT_ID,
        "DATABRICKS_AZURE_CLIENT_SECRET": CLIENT_SECRET,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")

    token = _get_token()

    context.log.info(f"Submitting notebook run: {NOTEBOOK_PATH}")
    run_data = _submit_run(token)
    run_id = run_data["run_id"]
    context.log.info(f"Run submitted — run_id={run_id}.")

    result = _poll_run(token, run_id)

    life_cycle  = result.get("state", {}).get("life_cycle_state", "UNKNOWN")
    result_state = result.get("state", {}).get("result_state", "UNKNOWN")
    run_page_url = result.get("run_page_url", "")

    context.log.info(f"Run {run_id} finished: {life_cycle} / {result_state}")

    if result_state != "SUCCESS":
        error = result.get("state", {}).get("state_message", result_state)
        raise RuntimeError(f"Notebook run failed ({result_state}): {error}")

    row_count = _get_notebook_row_count(token, run_id)
    if row_count is not None:
        context.log.info(f"Bronze table row count: {row_count:,}")

    metadata = {
        "run_id":       MetadataValue.int(run_id),
        "result":       MetadataValue.text(result_state),
        "run_page_url": MetadataValue.url(run_page_url),
    }
    if row_count is not None:
        metadata["row_count"] = MetadataValue.int(row_count)
    context.add_output_metadata(metadata)
