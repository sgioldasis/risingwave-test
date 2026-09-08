---
title: StarRocks Serving Layer — Live Demo Runbook
description: Exact commands and order to run a gapless live demo of the modern dashboard's StarRocks serving layer
---

<!-- markdownlint-disable-file -->

## Why this doc exists

The SQL query endpoints (`/api/query/funnel`, `/api/query/funnel/aggregate`,
`/api/funnel/enriched`, `/api/funnel/health` in
[modern-dashboard/backend/api.py](../modern-dashboard/backend/api.py)) never
query RisingWave or Databricks directly — they all go through a single
StarRocks SQLAlchemy engine (`mysql+pymysql`, port 9030), reading the
governed view `dashboard_funnel_serving`
([dbt_starrocks/models/dashboard_funnel_serving.sql](../dbt_starrocks/models/dashboard_funnel_serving.sql)).
That view itself is a StarRocks-side union of two sources, and as of
2026-09-08 **both are read live at query time — this is a zero-copy
architecture, no local materialized copy of either source exists** (see
[SR_POC_ICEBERG_COUNTRIES_MIGRATION.md](SR_POC_ICEBERG_COUNTRIES_MIGRATION.md)'s
twelfth follow-up for the full history and why):

- **Windows younger than 3 minutes**: read live from
  `risingwave.public.funnel_summary` through StarRocks' JDBC federation
  catalog.
- **Windows older than 3 minutes**: read live from
  `databricks_uc.sr_poc_external.funnel_summary_historical` through
  StarRocks' Iceberg REST catalog, deduped per-window in the view itself.

(If the `risingwave` catalog is unreachable, the backend falls back to
querying `funnel_summary_historical` directly — cold-only — and marks the
response `degraded: true`.)

**The gap this doc originally existed to close no longer exists.** Before
2026-09-08, the cold side was a materialized view that only refreshed on a
schedule (or manually via a `demo_warm_job`/`starrocks_mv_warm` Dagster
asset, since removed), so a window that just aged past the 3-minute
boundary could be briefly invisible until the next refresh. With both sides
now read live, there is no schedule, no cache, and no possible gap — every
query sees current data by construction. The rest of this doc's cold-start
sequence (Steps 1-6 below) is still accurate and still worth following for
a live demo; the "keep it gapless" concern that used to follow it is not.

The live SSE dashboard (`/api/funnel`, `/api/stats`, `/api/funnel/stream`) is
**not** affected by any of this — it reads straight from the Kafka consumer's
in-memory cache, with no StarRocks or RisingWave query involved at all, and
is always real-time.

## Demo script: cold start to hot+cold unified view

This assumes you're starting from **nothing running** — Script Runner just
launched from a devbox shell, no containers up yet. Each step below has a
concrete purpose and an expected outcome to check before moving on; if the
outcome doesn't match, stop and fix it there rather than continuing.

This narrative deliberately proves the hot/cold split is real, not just
configured: the Databricks historical table is **external** to this stack —
it survives a full local teardown untouched — while RisingWave's live path
starts genuinely empty every time. Querying *before* starting the producer
proves the cold path works on its own; querying *after* proves the hot path
adds in on top of it.

### Step 1 — Start the stack

**Action:** Script Runner (http://localhost:4001) → click **🚀 Start
Services** (`1_up.sh`).

**Purpose:** bring up every container from nothing — RisingWave, Redpanda,
StarRocks, Lakekeeper, MinIO, Dagster, Trino, Grafana, Postgres.

**Expected outcome:** `docker ps` shows all of them `Up`/`healthy`. Nothing
inside them exists yet — no RisingWave sources, no StarRocks catalogs, no
`sr_local_db_sr_local_db` schema. That's expected; the next step creates it
all.

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### Step 2 — Build everything (Dagster), before starting anything else

**Action:** Dagster UI (http://localhost:3000) → **Jobs →
`modern_dashboard_setup_job`** → Launchpad → Launch Run.

**Purpose:** create every object the demo depends on in one pass — RisingWave
sources/`funnel_summary`/sinks, the StarRocks catalogs (`databricks_uc`,
`lakekeeper_local`, `risingwave`), and all `dbt_starrocks` models
(`hot_funnel_summary`, `dashboard_funnel_serving`,
`dashboard_funnel_enriched`, `mv_funnel_daily_country_rollup` — see
[SR_POC_QUERY_REWRITE_DEMO.md](SR_POC_QUERY_REWRITE_DEMO.md)).
`dashboard_funnel_serving` is a plain view with nothing to warm — both its
branches read live at query time (see "Why this doc exists" above).

**Expected outcome:** `RUN_SUCCESS`. `mv_funnel_daily_country_rollup` will
still have zero rows afterward — it's `MANUAL`-refresh and no asset is wired
to warm it, so if you also want the query-rewrite demo, refresh it once by
hand:
```bash
docker exec starrocks mysql -h127.0.0.1 -P9030 -uroot -e "REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_funnel_daily_country_rollup WITH SYNC MODE;"
```

**If this fails with `Kafka topic 'funnel' is unavailable`:** that's
`modern_dashboard_preflight` — it hard-requires the `funnel` topic to exist,
but nothing had created it yet on a truly fresh Redpanda volume (the sink
that writes to it runs *after* preflight in this same job). Fixed on
2026-09-07 by adding `funnel` to the topic list `redpanda-init` pre-creates
in `docker-compose.yml` — if you still hit this, that fix didn't take effect
(e.g. stale container), and manually creating the topic
(`docker exec redpanda rpk topic create funnel --brokers redpanda:9092`)
before re-running the job is the workaround.

### Step 3 — Start the dashboard backend + frontend

**Action:** Script Runner → click **✨ Run Modern Dashboard** (`4_run_modern.sh`).

**Purpose:** bring up the API (port 4000) and frontend so there's something
to query.

**Expected outcome:** frontend loads. The live SSE view shows no events yet
("No events" / nothing ticking) — correct, since the producer hasn't started.

### Step 4 — Query *before* starting the producer

**Action:** Open the **Queries** tab, pick a time range spanning back
several days (covering whenever data was last produced in an earlier
session), and run it.

**Purpose:** prove the cold path stands on its own. `dashboard_funnel_serving`
federates live RisingWave data for the last 3 minutes and Databricks history
for everything older — with no producer running, the live branch legitimately
returns zero rows for "now", but the historical rows from Databricks are
still there because that table is external and untouched by last night's
teardown.

**Expected outcome:** results show only older historical windows — nothing
from the last few minutes (there isn't any yet), no "Degraded" banner (both
catalogs are reachable; the hot branch is just correctly empty for the
current moment, which is different from being unreachable).

### Step 5 — Start the producer

**Action:** Script Runner → click **🚀 Start Producer** (`3_run_producer.sh`),
enter a TPS value.

**Purpose:** generate live events into Kafka → RisingWave → `funnel_summary`.

**Expected outcome:** the live SSE dashboard view starts ticking ("Last
event: ..." updates). **Wait at least ~60-70 seconds** before querying again —
`funnel_summary` is a 1-minute tumbling window with `EMIT ON WINDOW CLOSE`
([dbt/models/funnel_summary.sql](../dbt/models/funnel_summary.sql)), so the
first row doesn't materialize until the first full window closes plus the
5-second watermark delay on the source. Querying immediately will still show
cold-only data — that's expected, not a bug.

### Step 6 — Query again, after the producer has run for a minute or two

**Action:** Run the same (or another) query in the Queries tab, covering a
range that includes right now.

**Purpose:** show the same query surface now blending both sources.

**Expected outcome:** results now include one or more windows from the last
few minutes *in addition to* the historical days from Step 4 — the union is
real, not just configured. This is also the moment to point at
`/api/serving/status` (`hot_window`/`cold_window`/`serving_window`) to show
both sides are populated and current.

### Ongoing: nothing to do, by design

Before 2026-09-08 this section covered re-running a warm-up job periodically
during a long demo session, since the cold side could fall behind its
refresh schedule. That's no longer applicable — both sides of
`dashboard_funnel_serving` read live on every query, so there's no schedule
to fall behind and nothing to re-run, no matter how long the session runs.

## Demo: graceful degradation (RisingWave outage)

Shows two things at once: the SQL query endpoints fail over cleanly to
cold-only history when the live RisingWave catalog is unreachable (and the
UI now visibly says so via a red "Degraded" banner), while the real-time SSE
dashboard keeps working the whole time because it never depends on that
catalog at all.

**Do this as its own segment, separate from the gapless-query demo above** —
it deliberately breaks the hot path, so don't run it before you still need
fresh hot data for something else.

### Setup

Run this *before* the demo, so you have a clean baseline to contrast against:

1. Confirm the stack is healthy and the banner is **not** currently showing:
   ```bash
   curl -s http://localhost:4000/api/serving/status | python3 -m json.tool
   ```
   Expect `"status": "ready"`, `"catalogs": {"missing": []}`.
2. Open the dashboard frontend (http://localhost:4000, or whatever port your
   `4_run_modern.sh` reports) and go to the **Queries** tab. Run one query
   against a recent time range so you have a "before" result on screen —
   point out there's no warning banner.

### Live sequence

**1. Break it** — stop just the RisingWave SQL-serving frontend (not the
whole stack; this is a single `docker` command, not a Script Runner button,
since Script Runner only manages whole-stack/process-level scripts):

```bash
docker stop frontend-node-0
```

Say out loud what this does: RisingWave's compute layer (`compute-node-0`)
and the Kafka sinks keep running untouched — only the SQL-serving frontend
that StarRocks' `risingwave` JDBC catalog connects to goes down.

**2. Show the live view is unaffected** — point at the main dashboard view
(SSE stream, `/api/funnel` / `/api/stats`). Numbers keep updating. Say why:
this path reads from the backend's in-memory Kafka-consumer cache, never
touches StarRocks or RisingWave's SQL layer at all.

**3. Trigger the degraded fallback** — go to the **Queries** tab (or the
**Enriched** tab, which polls automatically every 5s and will pick this up
on its own) and run a query covering recent time again:

- `dashboard_funnel_serving`'s live branch (reading
  `risingwave.public.funnel_summary` via JDBC) fails.
- The backend catches the exception, falls back to querying
  `databricks_uc.sr_poc_external.funnel_summary_historical` directly
  (cold-only), and marks the response `degraded: true`.
- The red **"Degraded: the live RisingWave catalog is unavailable — showing
  cold/historical data only, results may be stale"** banner appears above
  the results.

**4. Confirm it from the API too**, if you want the raw evidence on screen
alongside the UI:

```bash
curl -s http://localhost:4000/api/serving/status | python3 -m json.tool
```

Expect `hot_error` populated and/or `"status": "degraded"` — this is the
same `serving/status` endpoint from step 4 of the main runbook, now showing
the failure state instead of `ready`.

**5. Restore it**:

```bash
docker start frontend-node-0
```

Wait for it to report `healthy` again:

```bash
watch -n 2 "docker ps --filter name=frontend-node-0 --format '{{.Status}}'"
```

(Ctrl-C once it shows `healthy` — typically ~10-20s.)

**6. Show recovery** — re-run the same query in the Queries/Enriched tab.
The banner should disappear (`degraded: false`) once StarRocks can reach the
catalog again. Re-check `serving/status` too, if you showed it in step 4 —
should be back to `"status": "ready"`.

### Talking points while it's broken

- This is exactly the "isolated hot-catalog failure" scenario already
  validated in [SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md) — you're
  reproducing a previously-tested failure mode live, not improvising a new
  one.
- The degradation is *silent data staleness*, not an error page — that's
  deliberate (the dashboard keeps answering queries), but it's exactly why
  the visible banner matters: before this UI change, an operator had no way
  to tell "fresh" results from "stale, cold-only" results without inspecting
  the raw HTTP response.
- If asked "what if the whole thing failed instead of just RisingWave" —
  that's the separate, already-tested "StarRocks outage" scenario noted in
  the same doc: the aggregate endpoint returns HTTP `503` with
  `detail.source = "starrocks"` rather than degrading, since there's no cold
  fallback path when StarRocks itself is the thing that's down.

## Known devbox bug: `devbox run` fails with `DEVBOX_PROJECT_ROOT: unbound variable`

`devbox run -- <cmd>` fails unconditionally in this project (even for a
trivial `devbox run -- echo hello`) with:

```text
/…/.devbox/virtenv/python/bin/venvShellHook.sh: line 3: DEVBOX_PROJECT_ROOT: unbound variable
```

**Root cause:** `venvShellHook.sh` is auto-generated by devbox's Python
plugin (triggered by the `python@3.13` package) and runs under `set -eu`,
referencing `$DEVBOX_PROJECT_ROOT` with no default. Confirmed via
`devbox --debug run` that `devbox run` never exports `DEVBOX_PROJECT_ROOT`
(or `VENV_DIR`) into the environment before invoking it — and exporting it
manually beforehand doesn't help, since devbox rebuilds its environment
internally. This reproduced on the latest available version
(`devbox version update` confirmed 0.18.0 is current), so it isn't a
version we've fallen behind on.

Since this project already manages its own venv via `uv` in `devbox.json`'s
own `init_hook`, the Python plugin's auto-venv hook is redundant anyway —
it's just broken in a way that happens to matter for `devbox run`
specifically.

**Workaround (confirmed working):** `devbox shell` — the same code path used
interactively — sets these variables correctly. Pipe a command into it
non-interactively instead of using `devbox run`:

```bash
echo 'dg launch --job dbt_starrocks_build_job; exit' | devbox shell
```

A **fresh** `devbox shell` also auto-loads `.env` (confirmed 2026-09-07:
`ADLS_CLIENT_SECRET`, which exists only in `.env` and nowhere in
`devbox.json`, is populated inside a brand-new shell with no manual
`source .env` needed) — for a host-side StarRocks connection you still need
to point `STARROCKS_URL` at `localhost` rather than the in-Docker-network
default:

```bash
echo 'export STARROCKS_URL="mysql+pymysql://root@localhost:9030"; dg launch --job dbt_starrocks_build_job; exit' | devbox shell
```

(An earlier version of this note cited `DATABRICKS_CATALOG` as proof of
`.env` auto-loading — that was wrong. `DATABRICKS_CATALOG` is a hardcoded
literal in `devbox.json`'s own `env` block, so it proved nothing about
`.env` loading; it just happened to be correct for an unrelated reason.)

This does **not** affect your normal interactive `devbox shell` + Script
Runner + Dagster UI workflow — it only matters if you (or a script/CI job)
need to invoke a devbox-wrapped command non-interactively.

## Trap: a long-running devbox shell doesn't pick up `.env` changes

Hit this 2026-09-07: `modern_dashboard_setup_job` failed on a fresh stack
with four unrelated-looking asset failures
(`modern_dashboard_databricks_table`, `iceberg_countries`,
`modern_dashboard_preflight`, `starrocks_mv_warm`) — all four query StarRocks
catalogs (`databricks_uc`, `lakekeeper_local`, `risingwave`) that turned out
not to exist. Root cause was one level down: the `starrocks-init` container
(created by `1_up.sh` via `docker-compose.yml`, responsible for creating
those exact catalogs at stack startup) had exited with
`DATABRICKS_AZURE_CLIENT_SECRET must be set`.

`.env` had a correct, non-empty value the whole time — verified with a
**brand-new** `devbox shell`, which resolved it correctly. The problem was
that **Script Runner had been running since the previous day**, launched
from a devbox shell that was still open from that session. A devbox shell's
environment is computed once at shell-start time; it does not refresh if
`.env` or `devbox.json` changes afterward, or just because time has passed.
Script Runner (and everything it launches, including `1_up.sh` →
`docker compose up`) inherited that shell's environment at the moment
Script Runner itself started — which in this case resolved
`DATABRICKS_AZURE_CLIENT_SECRET` to empty, even though a fresh shell opened
at the same moment would have gotten the right value.

**Symptom to watch for:** `modern_dashboard_setup_job` (or
`dbt_starrocks_build_job`) fails with several StarRocks-catalog-related
assets all failing together, right after a fresh `1_up.sh`. Check
`docker logs starrocks-init` — if it shows a `must be set` error for any
`DATABRICKS_*`/`ADLS_*` variable, this is the same trap, not a code bug.

**Fix:**
1. Stop Script Runner (Ctrl-C in its terminal).
2. `exit` the old devbox shell.
3. Open a **new** devbox shell, then `./bin/0_script_runner.sh` from there.
4. Re-run **🚀 Start Services** (`1_up.sh`) from the fresh Script Runner.
5. Verify before moving on:
   ```bash
   docker logs starrocks-init
   docker exec starrocks mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CATALOGS;"
   ```
   Expect no `must be set` error, and `databricks_uc`, `lakekeeper_local`,
   `risingwave` all listed alongside `default_catalog`.
6. Only then proceed to Step 2 (`modern_dashboard_setup_job`) above.

**General rule:** if you've had a devbox shell (and anything launched from
it, including Script Runner) open since a previous session, don't trust it
after any `.env` or `devbox.json` change — restart it fresh before relying
on it for a demo.
