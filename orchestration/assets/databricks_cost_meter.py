"""Per-process meter for the demo's Databricks compute usage.

`_run_sql` (the single choke point for warehouse SQL in the reverse-ETL demo)
records every statement's measured duration here; the instrumented assets and
ops reset the meter when they start and emit the snapshot as run metadata
(`databricks_warehouse_seconds`). The demo helper
(scripts/reverse_etl_demo_server.py) reads that metadata back through the
Dagster GraphQL API and shows cost chips on the demo page — e.g. the sync asset
on the oss_spark path emits 0.0, structurally, because it touches no Databricks
compute at all (see docs/poc/REVERSE_ETL_DBU_COST_NOTES.md section 5).

The meter is deliberately per-process: Dagster's multiprocess executor runs
each step in its own process, so a step's snapshot is its own usage. For the
in-process setup job each instrumented asset/opus resets at entry, which keeps
the snapshots per-step there too.
"""

import threading

_LOCK = threading.Lock()
_SECONDS = 0.0
_STATEMENTS = 0


def record(seconds: float) -> None:
    """Record one warehouse statement's measured duration (raises still count)."""
    global _SECONDS, _STATEMENTS
    with _LOCK:
        _SECONDS += max(seconds, 0.0)
        _STATEMENTS += 1


def reset() -> None:
    """Zero the meter — called at the start of every instrumented asset/op."""
    global _SECONDS, _STATEMENTS
    with _LOCK:
        _SECONDS = 0.0
        _STATEMENTS = 0


def snapshot() -> dict:
    """Usage since the last reset: {"warehouse_seconds": float, "statements": int}."""
    with _LOCK:
        return {"warehouse_seconds": round(_SECONDS, 2), "statements": _STATEMENTS}
