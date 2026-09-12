---
title: StarRocks Superset Demos — Step-by-Step Runbook
description: Exact script runner / Dagster / Superset steps to bring up each of the three Superset-based StarRocks demos from a cold stack
---

<!-- markdownlint-disable-file -->

## Which demo is which

Three Superset dashboards, all reading from StarRocks, each proving a
different thing:

| Dashboard | Proves | Deep-dive doc |
|---|---|---|
| **Funnel Dashboard** | Live (RisingWave) + historical (Databricks) funnel data served through one StarRocks view, zero-copy | [SR_POC_LIVE_DEMO_RUNBOOK.md](SR_POC_LIVE_DEMO_RUNBOOK.md), [SR_POC_ICEBERG_COUNTRIES_MIGRATION.md](SR_POC_ICEBERG_COUNTRIES_MIGRATION.md) |
| **StarRocks Query Rewrite Demo** | StarRocks transparently redirects a query against a raw Iceberg table to a pre-aggregated materialized view, ~10x faster, no query changes | [SR_POC_QUERY_REWRITE_DEMO.md](SR_POC_QUERY_REWRITE_DEMO.md) |
| **Wallet Upsert Demo** | StarRocks Primary Key table upsert semantics (a reversal event overwrites the original row, `COUNT(*) == COUNT(DISTINCT transaction_id)`) — plus a side-by-side comparison of the same live data ingested via RisingWave vs. direct Kafka → StarRocks Routine Load | [SR_POC_WALLET_UPSERT_DEMO.md](SR_POC_WALLET_UPSERT_DEMO.md) |

This doc is the "what do I click, in what order" guide. For *why* things are
built the way they are, or the real bugs found while building them, follow
the links above.

## 0. One-time understanding: how these dashboards get onto a fresh machine

The Superset dashboards/charts/datasets/database connections are **not**
stored anywhere in git by default — Superset keeps them in the
`superset-home` Docker volume, which is local to whichever machine created
them. To make them reproducible:

- [`superset/assets/export/`](../superset/assets/export/) is a git-tracked,
  human-readable YAML export of every dashboard/chart/dataset/database
  connection (produced via Superset's own `/api/v1/dashboard/export/` API).
- `superset-init` (in `docker-compose.yml`) re-imports that bundle on
  **every** stack startup, via `superset import-dashboards`. It's
  idempotent — re-importing over existing objects (matched by UUID) updates
  them in place rather than duplicating, so this is safe to run repeatedly,
  including against a machine that already has these dashboards.
- If you edit a dashboard/chart/dataset in the live Superset UI and want
  that change to persist for the next person (or the next fresh volume),
  run:
  ```bash
  ./superset/export_assets.sh
  ```
  then `git add superset/assets/export/ && git commit`. Until you do this,
  your edit only exists in your local `superset-home` volume.

You do not need to think about this to *run* a demo — it happens
automatically as part of `1_up.sh`/`superset-init`. It only matters if
you're *editing* a dashboard and want the edit to survive.

## 1. Prerequisites (do this once per session)

```bash
devbox shell          # sets up venv, starts local Postgres, loads .env
```

Use a **fresh** `devbox shell` if one has been open since before any `.env`
change — a long-lived shell doesn't pick up `.env` edits (see the trap
documented at the bottom of
[SR_POC_LIVE_DEMO_RUNBOOK.md](SR_POC_LIVE_DEMO_RUNBOOK.md)).

```bash
./bin/0_script_runner.sh   # opens the Script Runner web UI, port 4001
```

From here on, "Script Runner" means clicking a button at
http://localhost:4001; "Dagster" means http://localhost:3000; "Superset"
means http://localhost:3002 (login: `admin` / `sr_poc_admin_2026`, unless
overridden via `SUPERSET_ADMIN_USERNAME`/`SUPERSET_ADMIN_PASSWORD`).

## 2. Start the stack

**Script Runner → 🚀 Start Services** (`1_up.sh`).

Brings up every container: RisingWave, Redpanda, StarRocks, Lakekeeper,
MinIO, Dagster, Trino, Grafana, Postgres, Superset. Takes 30-90s depending
on whether images need pulling.

**Check:**
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```
Everything should be `Up`/`healthy`. At this point Superset already has all
three dashboards imported (via `superset-init`, see §0) — but every chart
will show empty results or errors, because none of the underlying
RisingWave/StarRocks objects have been built yet. That's expected; the next
step creates them.

## 3. Build the pipeline objects (Dagster)

Two jobs cover the three dashboards between them. Both are safe to run
back-to-back.

### 3a. Funnel Dashboard + StarRocks Query Rewrite Demo

**Dagster UI → Jobs → `modern_dashboard_setup_job` → Launchpad → Launch
Run.**

Creates RisingWave sources/sinks for the funnel pipeline, the StarRocks
external catalogs (`databricks_uc`, `lakekeeper_local`, `risingwave`), and
all `dbt_starrocks` models — including `dashboard_funnel_serving` (the
Funnel Dashboard's data source) and `mv_funnel_daily_country_rollup` (the
Query Rewrite Demo's materialized view).

**Expected outcome:** run status `SUCCESS`.

**One extra manual step for the Query Rewrite Demo only:**
`mv_funnel_daily_country_rollup` is `MANUAL`-refresh with no asset wired to
warm it, so it's still empty right after this job. Refresh it once:
```bash
docker exec starrocks mysql -h127.0.0.1 -P9030 -uroot -e \
  "REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_funnel_daily_country_rollup WITH SYNC MODE;"
```
(The Funnel Dashboard needs no such step — its view reads both sources
live, no cache to warm.)

**If this fails with `Kafka topic 'funnel' is unavailable`:** see the same
snag documented in
[SR_POC_LIVE_DEMO_RUNBOOK.md](SR_POC_LIVE_DEMO_RUNBOOK.md#step-2--build-everything-dagster-before-starting-anything-else).

### 3b. Wallet Upsert Demo

**Dagster UI → Jobs → `wallet_pipeline_setup_job` → Launchpad → Launch
Run.**

Creates the RisingWave source (`src_wallet_transactions`), the
RisingWave → StarRocks upsert sink, and the StarRocks Primary Key table
(`wallet_transactions`) — **and**, independently, the direct-Kafka
comparison path: a second StarRocks Primary Key table
(`wallet_transactions_direct_kafka`) plus a StarRocks Routine Load job
(`wallet_direct_kafka_load`) reading the same Kafka topic with no
RisingWave involved at all.

**Expected outcome:** run status `SUCCESS`. The RisingWave-mediated table
exists but has **zero rows** immediately after — this job's
`materialized='table'` model always drops and recreates the table on every
run (this is expected, not a bug; see
[SR_POC_WALLET_UPSERT_DEMO.md](SR_POC_WALLET_UPSERT_DEMO.md#rebuilding-the-job-wipes-the-starrocks-table-expected-not-a-bug)).
The direct-Kafka table, by contrast, is **not** wiped on every run (the
Dagster asset is idempotent — it only creates the table/Routine Load job if
missing, and the Routine Load job itself replays from `OFFSET_BEGINNING`
the first time it's created) — it'll already have rows from the whole
topic history once the producer has been running a while. The next step
populates/keeps populating both.

**Don't use `uv run dg launch --job <name>` from a host shell for either
job** — it hits a DNS false-positive on Docker-internal hostnames (see
[SR_POC_WALLET_UPSERT_DEMO.md](SR_POC_WALLET_UPSERT_DEMO.md#dg-launch-from-the-host-hits-a-dns-false-positive-not-a-real-limitation)).
Always launch through the Dagster UI (or its GraphQL API), which runs
inside the same Docker network.

## 4. Start the producers (Script Runner)

| Demo | Producer | Script Runner button |
|---|---|---|
| Funnel Dashboard, Query Rewrite Demo | `scripts/producer.py` | 🚀 Start Producer (`3_run_producer.sh`) |
| Wallet Upsert Demo | `scripts/wallet_producer.py` | 💳 Start Wallet Producer (`3_run_wallet_producer.sh`) |

Both accept a TPS value. The Query Rewrite Demo doesn't strictly need the
funnel producer running (its two charts read Databricks history + the
already-refreshed MV, not live RisingWave data) — start it anyway if you're
also showing the Funnel Dashboard in the same session.

**Wait before checking results:**
- Funnel: `funnel_summary` is a 1-minute tumbling window with
  `EMIT ON WINDOW CLOSE` — allow ~60-70s before the first row appears.
- Wallet: each transaction gets a reversal ~5s later (configurable via
  `--reversal-delay`); allow at least that long before checking that
  reversed rows show up correctly upserted.

## 5. View it in Superset

**Superset → Dashboards** (http://localhost:3002/dashboard/list/) — all
three should already be listed (imported automatically in §2):

- **Funnel Dashboard** (`/superset/dashboard/funnel-dashboard/`)
- **StarRocks Query Rewrite Demo** (`/superset/dashboard/query-rewrite-demo/`)
- **Wallet Upsert Demo** (`/superset/dashboard/wallet-upsert-demo/`)

Open each and hit refresh. If a chart shows an error instead of data, it
almost always means one of the steps in §3/§4 hasn't run yet or hasn't had
time to produce data — check the underlying table directly before assuming
Superset itself is broken:

```bash
# Wallet: are there any rows at all yet?
docker exec starrocks mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT COUNT(*) FROM sr_local_db_sr_local_db.wallet_transactions;"

# Query Rewrite Demo: was the MV actually refreshed?
docker exec starrocks mysql -h127.0.0.1 -P9030 -uroot -e \
  "SELECT QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON FROM information_schema.materialized_views WHERE TABLE_NAME='mv_funnel_daily_country_rollup';"

# RisingWave: do the expected sources/sinks exist?
psql -h localhost -p 4566 -U root -d dev -c "SHOW SOURCES;"
psql -h localhost -p 4566 -U root -d dev -c "SHOW SINKS;"
```

If those all look right and a chart still errors, it's more likely a
Superset-side issue (stale dataset pointing at a dropped/renamed table,
broken `position_json`, etc.) — see §0 and the "Superset dashboard" section
of each demo's deep-dive doc for the specific bugs already found and fixed
in this project.

## 6. Tearing down

**Script Runner → ⛔ Stop Everything** (`6_down.sh`) stops all containers
and the producers (both `scripts/producer.py` and
`scripts/wallet_producer.py` are matched and killed). The `superset-home`
volume is **not** wiped by this — your local Superset edits survive a
normal stop/start cycle. Only an explicit `docker volume rm
risingwave-test_superset-home` wipes it, in which case §0's auto-import
mechanism rebuilds everything from `superset/assets/export/` on next
startup.
