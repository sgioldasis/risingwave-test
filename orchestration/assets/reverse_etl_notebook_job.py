"""Dagster job that triggers the reverse-ETL sync notebook on Databricks for one sync.

The Databricks job `reverse_etl_notebook_sync` (one notebook task, the `label` and `encoding` job
parameters, one concurrent run) runs the notebook notebooks/reverse_etl_cdf_to_kafka.py
as the job's owner, on the job's cluster. This Dagster job first adds any column the
source table has and the RisingWave table lacks (the notebook cannot reach RisingWave;
RisingWave only fills a column from messages read after it exists), then triggers the
Databricks job with `run-now`, as the service principal (which needs Can Manage Run on
that job), and waits for the result. The notebook and the Dagster asset `reverse_etl_<label>_to_kafka`
share one watermark per sync, so use one trigger per change.

Run config (label defaults to cdf): {"ops": {"reverse_etl_trigger_notebook_sync": {"config": {"label": "orders"}}}}
"""

import re
import time
from pathlib import Path

import requests
import yaml
from dagster import Config, MetadataValue, OpExecutionContext, in_process_executor, job, op

from .databricks_optimize import DATABRICKS_HOST, _get_token
from .reverse_etl_cdf_setup import _get_row_fields, _require_databricks_env
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_risingwave_setup import add_missing_columns

# Where the ReverseEtlCdfSync instances live (orchestration/defs/<folder>/defs.yaml).
_DEFS_DIR = Path(__file__).resolve().parent.parent / "defs"

DATABRICKS_JOB_NAME = "reverse_etl_notebook_sync"
# Labels become table and topic names (reverse_etl_<label>_*), so keep them plain.
_LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class TriggerNotebookSyncConfig(Config):
    label: str = "cdf"  # the sync, for example "cdf" (the POC) or "orders"
    # Add columns the source has and the RisingWave table lacks, before the notebook runs.
    add_risingwave_columns: bool = True


def _load_sync_config(label: str) -> ReverseEtlSyncConfig:
    """The config of the ReverseEtlCdfSync instance whose `name` is `label`, read from its
    defs.yaml so overrides in it are honoured."""
    # Imported here: the component module imports the asset modules, not the other way round.
    from ..components.reverse_etl_cdf_sync import ReverseEtlCdfSync

    known = []
    for path in sorted(_DEFS_DIR.glob("*/defs.yaml")):
        doc = yaml.safe_load(path.read_text()) or {}
        if not str(doc.get("type", "")).endswith(".ReverseEtlCdfSync"):
            continue
        attributes = doc.get("attributes", {})
        known.append(attributes.get("name"))
        if attributes.get("name") == label:
            return ReverseEtlCdfSync(**attributes).to_config()
    raise ValueError(f"No ReverseEtlCdfSync instance named {label!r} under {_DEFS_DIR}; known labels: {known}")


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _find_job_id(token: str) -> int:
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/list",
        headers=_headers(token),
        params={"name": DATABRICKS_JOB_NAME},
        timeout=30,
    )
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    if not jobs:
        raise RuntimeError(
            f"Databricks job {DATABRICKS_JOB_NAME} not found, or the service principal cannot see it "
            "(it needs Can Manage Run on the job)"
        )
    if len(jobs) > 1:
        raise RuntimeError(f"{len(jobs)} Databricks jobs are named {DATABRICKS_JOB_NAME}; expected exactly one")
    return jobs[0]["job_id"]


def _wait_for_run(token: str, run_id: int, poll_interval: int = 10, max_wait: int = 1800) -> dict:
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get", headers=_headers(token), params={"run_id": run_id}, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("state", {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return data
        time.sleep(poll_interval)
    raise TimeoutError(f"Databricks run {run_id} did not finish within {max_wait}s")


@op(name="reverse_etl_trigger_notebook_sync")
def trigger_notebook_sync(context: OpExecutionContext, config: TriggerNotebookSyncConfig) -> None:
    label = config.label.strip()
    if not _LABEL_PATTERN.fullmatch(label):
        raise ValueError(f"Invalid label {label!r}: use lowercase letters, digits and underscores, starting with a letter")

    _require_databricks_env()
    token = _get_token()
    sync_config = _load_sync_config(label)
    job_id = _find_job_id(token)

    columns_added: list[str] = []
    if config.add_risingwave_columns:
        # Before the run, so the new column exists when RisingWave reads the first message carrying it.
        columns_added = add_missing_columns(sync_config, _get_row_fields(token, sync_config))
        if columns_added:
            context.log.info(f"Added column(s) to RisingWave table {sync_config.risingwave_table}: {columns_added}")

    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=_headers(token),
        json={"job_id": job_id, "job_parameters": {"label": label, "encoding": sync_config.encoding}},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Triggering {DATABRICKS_JOB_NAME} failed ({resp.status_code}): {resp.text[:500]}")
    run_id = resp.json()["run_id"]
    context.log.info(f"Started {DATABRICKS_JOB_NAME} run {run_id} for label {label}")

    result = _wait_for_run(token, run_id)
    state = result.get("state", {})
    result_state = state.get("result_state", "UNKNOWN")
    context.log.info(f"Run {run_id} finished: {state.get('life_cycle_state')} / {result_state}")

    context.add_output_metadata({
        "label": MetadataValue.text(label),
        "risingwave_columns_added": MetadataValue.json(columns_added),
        "run_id": MetadataValue.int(run_id),
        "result": MetadataValue.text(result_state),
        "run_page_url": MetadataValue.url(result.get("run_page_url", "")),
    })
    if result_state != "SUCCESS":
        raise RuntimeError(f"Notebook run for {label} failed ({result_state}): {state.get('state_message', '')}")


@job(
    name="reverse_etl_notebook_sync_job",
    description=(
        "Run the reverse-ETL sync notebook on Databricks for one sync, chosen by the `label` run "
        "config (for example cdf or orders). Adds any missing RisingWave columns first, then triggers "
        "the Databricks job reverse_etl_notebook_sync and waits. Use this or the reverse_etl_<label>_to_kafka asset for a change, not both: they "
        "share one watermark."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_notebook_sync_job():
    trigger_notebook_sync()
