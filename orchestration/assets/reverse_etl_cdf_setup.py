"""Demo-only helpers the vendored library does not carry: Databricks Statement
Execution (with the demo's cost meter) and the meter-to-metadata bridge.

The sync itself — the CDF version-range read, Debezium/Avro envelopes, chunking,
watermark state, hooks — lives in the vendored library
(kaizen_dagster_reverse_etl.components.cdf_sync.sync.build_sync_asset), which
replaced this module's former fork of the same machinery (APR-302 port; the fork
was retired 2026-11-09 after it drifted from the library — the "versions 14-14"
heartbeat wording fix had to be applied twice).

What remains here and why:

- `_run_sql` / `_run_sql_unmetered` / `_with_all_chunks`: the Databricks
  Statement Execution API flow (same pattern as databricks_optimize.py /
  casino_prd_setup.py). `_run_sql` meters its wall time into
  databricks_cost_meter so the demo page can render warehouse-second chips —
  demo telemetry, not library concern.
- `_cost_metadata`: the meter snapshot as run metadata entries
  (databricks_warehouse_seconds / databricks_statements), which
  scripts/reverse_etl_demo_server.py reads through GraphQL to render the cost
  chips. The DemoSyncHooks (reverse_etl_demo_hooks.py) spread it into the sync
  run's hook metadata.
- `_rows_as_dicts`: the Statement Execution response shape unpacked.

Auth: reuses `_get_token`/`_submit`/`_poll` from databricks_optimize.py —
the same Azure AD service-principal client-credentials + Statement Execution
API flow `casino_prd_setup.py` already uses. An earlier version of the sync
used databricks-sdk's WorkspaceClient with the `personal` CLI profile, which
works from a laptop shell but not inside the Dagster container (no
~/.databrickscfg there, and the container's own
DATABRICKS_AUTH_TYPE=azure-client-secret conflicts with profile-based
resolution) — switched to match what's actually deployable. See "Auth for
the live run" in the design doc.
"""

import os
import time
from typing import Any

import requests
from dagster import MetadataValue

from .databricks_cost_meter import record as meter_record
from .databricks_cost_meter import snapshot as meter_snapshot
from .databricks_optimize import DATABRICKS_HOST, _poll, _submit


# --- Databricks Statement Execution API (REST, same pattern as
# databricks_optimize.py / casino_prd_setup.py's _run_sql) -----------------


def _run_sql(token: str, statement: str) -> dict:
    """Run one warehouse statement and meter its wall time (databricks_cost_meter)."""
    started = time.monotonic()
    try:
        return _run_sql_unmetered(token, statement)
    finally:
        meter_record(time.monotonic() - started)


def _run_sql_unmetered(token: str, statement: str) -> dict:
    try:
        data = _submit(token, statement)
    except requests.exceptions.HTTPError as e:
        # _submit()/_poll() call resp.raise_for_status(), which discards the
        # response body -- surface it here since Databricks' actual error
        # detail (error_code / message) lives there, not in the HTTPError str.
        body = e.response.text if e.response is not None else "<no response body>"
        raise RuntimeError(f"Databricks Statement Execution API request failed: {e}\nResponse body: {body}") from e
    state = data.get("status", {}).get("state", "")
    if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        try:
            data = _poll(token, data["statement_id"])
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else "<no response body>"
            raise RuntimeError(f"Databricks Statement Execution API poll failed: {e}\nResponse body: {body}") from e
    if data.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"Statement failed: {data.get('status', {})}")
    return _with_all_chunks(token, data)


def _cost_metadata() -> dict:
    """Databricks compute usage since the last meter reset, as run metadata.

    The demo helper (scripts/reverse_etl_demo_server.py) reads these entries
    through the GraphQL API and renders cost chips on the demo page. The meter
    is demo-only telemetry (release design step 7): with the Databricks env
    unset the sync runs unmetered and omits the metadata instead of raising."""
    if not os.environ.get("DBT_DATABRICKS_HOST", ""):
        return {}
    usage = meter_snapshot()
    return {
        "databricks_warehouse_seconds": MetadataValue.float(usage["warehouse_seconds"]),
        "databricks_statements": MetadataValue.int(usage["statements"]),
    }


def _with_all_chunks(token: str, data: dict) -> dict:
    """The API returns a large result in chunks and the first response holds only the first one; without
    the others a change read is silently partial and the watermark moves past the rows that were never read
    (found 2026-10-07: 49,152 of 80,300 delete events). Fetch every chunk into result.data_array and fail if
    the rows collected differ from the manifest's total_row_count or the result is flagged truncated."""
    result = data.get("result") or {}
    manifest = data.get("manifest") or {}
    if "data_array" not in result:
        return data
    rows = list(result["data_array"])
    next_index = result.get("next_chunk_index")
    while next_index is not None:
        try:
            resp = requests.get(
                f"{DATABRICKS_HOST}/api/2.0/sql/statements/{data['statement_id']}/result/chunks/{next_index}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else "<no response body>"
            raise RuntimeError(f"Fetching result chunk {next_index} failed: {e}\nResponse body: {body}") from e
        chunk = resp.json()
        rows.extend(chunk.get("data_array") or [])
        next_index = chunk.get("next_chunk_index")
    total = manifest.get("total_row_count")
    if manifest.get("truncated") or (total is not None and len(rows) != total):
        raise RuntimeError(
            f"Incomplete result: read {len(rows)} row(s), manifest total_row_count={total}, "
            f"truncated={manifest.get('truncated')}"
        )
    return {**data, "result": {**result, "data_array": rows, "next_chunk_index": None}}


def _rows_as_dicts(response: dict) -> list[dict[str, Any]]:
    columns = [col["name"] for col in response.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = response.get("result", {}).get("data_array") or []
    return [dict(zip(columns, row, strict=True)) for row in data_array]
