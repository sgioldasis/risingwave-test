---
title: StarRocks Serving Layer — Live Demo Runbook
description: Exact commands and order to run a gapless live demo of the modern dashboard's StarRocks serving layer
---

<!-- markdownlint-disable-file -->

## Why this doc exists

The SQL query endpoints (`/api/query/funnel`, `/api/query/funnel/aggregate`,
`/api/funnel/enriched`, `/api/funnel/health`) serve recent data (< 3 minutes
old) live from RisingWave, but anything older comes from
`mv_unified_funnel_summary`, which only self-refreshes on its own schedule
(every 5 minutes — see
[SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md#remaining-questions-for-part-3)).
A window that just aged past the 3-minute boundary can be briefly invisible
to those endpoints until the next scheduled refresh.

The live SSE dashboard (`/api/funnel`, `/api/stats`, `/api/funnel/stream`) is
**not** affected by any of this — it reads straight from the Kafka consumer's
in-memory cache and is always real-time.

To make the SQL query endpoints gapless for a demo, run `demo_warm_job`
right before you start — it synchronously forces the MV refresh via Dagster,
so you never have to run ad hoc SQL by hand.

## Order of operations

### 1. Confirm the stack is up

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Look for `risingwave` (`meta-node-0`, `compute-node-0`, `frontend-node-0`,
`compactor-0`/`compactor-1`), `redpanda`, `starrocks`, `lakekeeper`,
`minio-0`, `dagster-webserver`, `dagster-daemon` all `Up` / `healthy`.

If anything is missing, open Script Runner at **http://localhost:4001**
(launched from inside a devbox shell via `./bin/0_script_runner.sh`) and
click the **🚀 Start Services** button (`1_up.sh` — "Start Docker Compose
services and install dependencies").

### 2. Confirm the producer and dashboard backend are running

In Script Runner (http://localhost:4001), check the status indicator on:

- **🚀 Start Producer** (`3_run_producer.sh` — "Run the event producer with
  configurable TPS") — generates funnel events. This one prompts for a TPS
  value before running.
- **✨ Run Modern Dashboard** (`4_run_modern.sh` — "Start the modern
  dashboard") — dashboard backend (port 4000) + frontend.

Both are background services in Script Runner (it tracks whether they're
still running and shows a running indicator), so if either isn't already
running, click its button to start it.

### 3. Warm the StarRocks unified MV (run this right before the demo starts)

Open the **Dagster UI** at **http://localhost:3000**:

1. Go to **Jobs → `demo_warm_job`**.
2. Click **Launchpad**, then **Launch Run** (no config needed).
3. Wait for the run to finish (observed ~14s) — it should show a single
   successful step, `starrocks_mv_warm`.

This job runs only the `starrocks_mv_warm` asset — it issues
`REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary
WITH SYNC MODE` and blocks until StarRocks confirms the refresh is fully
complete. It does **not** touch RisingWave, Databricks, or any dbt models —
safe to run with the stack live and the demo about to start.

**Do not use `modern_dashboard_setup_job` for this step.** That job also
rebuilds the RisingWave dbt models (sources, `funnel_summary`, sinks), which
briefly drop and recreate live streaming objects — exactly what you don't
want seconds before a demo. `starrocks_mv_warm` is also included in
`modern_dashboard_setup_job`'s selection (so a full setup run always ends in
a warm MV too), but for a live demo, launch the standalone `demo_warm_job`
from the Dagster UI instead.

### 4. Verify before you start talking

```bash
curl -s http://localhost:4000/api/serving/status | python3 -m json.tool
```

Confirm:

- `"status": "ready"`
- `"catalogs": {"missing": []}`
- `hot_window`, `cold_window`, and `serving_window` all show the **same,
  current** minute. If `cold_window` lags `hot_window` by more than a
  minute or so, the warm-up didn't take — re-run step 3.

### 5. Run the demo

- For the "real-time" story: use the live dashboard view / SSE stream — no
  further action needed, it's always current.
- For anything that queries a specific historical time range via the SQL
  endpoints: the warm-up in step 3 covers you for windows up to now. If the
  demo runs long (several minutes) and you query ranges close to "now" again
  later, the boundary gap can reappear — re-run step 3 if you need another
  gapless window right before a specific query.

## Reference: what `demo_warm_job` actually runs

Defined in
[orchestration/definitions.py](../orchestration/definitions.py) and
[orchestration/assets/starrocks_mv_warm.py](../orchestration/assets/starrocks_mv_warm.py):

```python
demo_warm_job = define_asset_job(
    name="demo_warm_job",
    selection=AssetSelection.assets(starrocks_mv_warm),
    ...
)
```

`starrocks_mv_warm` connects to StarRocks via the same SQLAlchemy
(`mysql+pymysql`) pattern used elsewhere in this project
([orchestration/assets/modern_dashboard_preflight.py](../orchestration/assets/modern_dashboard_preflight.py))
and runs:

```sql
REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary WITH SYNC MODE;
```

`WITH SYNC MODE` is what makes this useful for a demo: it blocks the calling
session until the refresh is fully done, unlike the MV's own async schedule
which just submits a background task.

## Known environment issue: headless `devbox run`

While preparing `demo_warm_job`, non-interactive invocation via
`devbox run -- <cmd>` failed in the automation environment used to test this
runbook, with:

```text
/…/.devbox/virtenv/python/bin/venvShellHook.sh: line 3: DEVBOX_PROJECT_ROOT: unbound variable
```

This reproduced even for a trivial `devbox run -- echo hello` and is
unrelated to any specific command — an issue with the devbox init hook under
headless/non-interactive invocation specifically. It was worked around there
by sourcing `.env` manually and running via plain `uv run` instead of
`devbox run`. This should **not** affect your normal workflow (interactive
`devbox shell` + Script Runner + Dagster UI, as described above) — noting it
here only in case you ever script this runbook's steps non-interactively and
hit the same error.
