# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A real-time streaming analytics platform built around **RisingWave v3.0.0** (distributed mode), demonstrating an e-commerce conversion funnel and a casino/sportsbook PoC. Events flow through Redpanda (Kafka), get transformed by RisingWave materialized views, and land in multiple sinks: Iceberg (via Lakekeeper + MinIO), PostgreSQL, and back to Kafka. A React dashboard reads from Kafka via SSE. Online ML predictions use River (incremental learning).

The `docs/poc/` directory contains a separate production casino demo (`CASINO_DEMO_RUNBOOK.md`, `PRODUCTION_CASINO_DEMO.md`).

## Development environment

Requires **devbox** (not plain Docker). `devbox shell` installs Python 3.13 deps via `uv`, starts a local PostgreSQL instance, and sets env vars. Run this before anything else.

```bash
devbox shell          # sets up venv, starts local Postgres, loads .env
```

## Common commands

```bash
# Start full stack (RisingWave cluster, Redpanda, Iceberg, Trino, Dagster, monitoring)
./bin/1_up.sh
./bin/1_up.sh --offline   # skip image pulls

# Run dbt models against RisingWave
./bin/3_run_dbt.sh        # via dbt CLI
./bin/3_run_psql.sh       # pre-compiled SQL via psql (faster for demo resets)

# Run the event producer
./bin/3_run_producer.sh

# Start services
./bin/4_run_modern.sh      # React dashboard (port 4000) + FastAPI backend
./bin/4_run_ml_serving.sh  # ML prediction API (port 8001)

# Query Iceberg externally
./bin/5_duckdb_iceberg.sh
./bin/5_spark_iceberg.sh

# Tear down + drop PostgreSQL tables
./bin/6_down.sh

# Script runner web UI (lists all bin scripts)
./bin/0_script_runner.sh   # port 4001
```

### dbt directly

```bash
cd dbt
dbt compile              # compile without running
dbt run                  # run all models
dbt run -s <model_name>  # single model
dbt parse                # validate configs (required after any tag/config change)
```

### Connect to RisingWave SQL

```bash
psql -h localhost -p 4566 -U root -d dev
```

## Architecture

### Data flow

```
Redpanda (Kafka) topics
  → RisingWave Sources (src_page, src_cart, src_purchase)
  → Materialized Views (funnel, funnel_summary, funnel_enriched, funnel_for_iceberg)
  → Sinks:
      sink_funnel_to_kafka      → Redpanda topic "funnel"
      sink_funnel_to_postgres   → PostgreSQL (JDBC upsert)
      sink_funnel_to_rw_iceberg → MinIO (S3) + Lakekeeper REST catalog
```

### dbt models

Located in `dbt/models/`. Default materialization is `materialized_view` (RisingWave streaming). Sources are defined in `sources.yml` — Kafka topics are declared as RisingWave sources. `dbt_project.yml` sets vars for `kafka_bootstrap_servers`, `iceberg_catalog_uri`, `iceberg_warehouse`, `s3_endpoint`.

On-run-start hooks in `dbt_project.yml` create Python UDFs and the Iceberg connection in RisingWave before models run.

### Iceberg integration (two paths)

**RisingWave-managed (real-time)**: `sink_funnel_to_rw_iceberg` writes directly; `compactor-1` runs in `dedicated-iceberg` mode for auto-compaction. Accessible via Trino as `datalake.public.rw_managed_funnel`.

**Trino-written (reference data)**: Dagster asset (`orchestration/assets/iceberg_countries.py`) writes via Trino JDBC; RisingWave reads it back as a native source with auto-refresh.

### Dashboard (push-based, no polling)

`modern-dashboard/api.py` (FastAPI) runs a background Kafka consumer thread and exposes an SSE endpoint `/api/funnel/stream`. The React frontend (`modern-dashboard/frontend/`) uses the browser `EventSource` API with 3s auto-reconnect.

### ML pipeline

`ml/` contains River-based online learning. `OnlineLearner` / `KafkaOnlineLearner` call `learn_one()` incrementally, checkpoint to MinIO every 60s. `ml/serving/api.py` (FastAPI, port 8001) exposes `/predict`, `/online/status`, `/learn`.

### Orchestration

Dagster (`orchestration/`) runs on ports 3000 (webserver) + daemon. Storage is SQLite under `./dagster_storage`. After any dbt model tag or config change, run `dbt parse` and restart the Dagster daemon.

## Service URLs

| Service | URL |
|---|---|
| RisingWave SQL | `localhost:4566` (psql) |
| RisingWave dashboard | http://localhost:5691 |
| Dagster | http://localhost:3000 |
| Grafana | http://localhost:3001 |
| Prometheus | http://localhost:9500 |
| Redpanda console | http://localhost:9090 |
| Lakekeeper (Iceberg REST) | http://localhost:8181 |
| MinIO console | http://localhost:9400 (hummockadmin/hummockadmin) |
| Trino | http://localhost:9080 |
| React dashboard | http://localhost:4000 |
| ML API docs | http://localhost:8001/docs |
| PostgreSQL | `localhost:5432` |

## Known RisingWave constraints

- `CREATE VIEW` over an Iceberg SOURCE fails in RW 3.0 with the DataFusion engine. Workaround: `SET datafusion_enabled = false` or use `CREATE MATERIALIZED VIEW` + `FULL_RELOAD` instead.
- Iceberg snapshot expiration does not prune data files when using a REST catalog (Lakekeeper). Do not rely on it for storage reclaim.
- Upsert sinks to Unity Catalog stall silently — only append-only sinks work against UC.

## Environment variables

The `.env` file uses colon syntax (`KEY: "value"`), not `KEY=value`. Devbox loads it automatically.

## Performance tuning

`risingwave.toml` is tuned for high-volume single-node workloads (see `docs/BRAZIL_WORKLOAD_TUNING.md`). Key settings: SST retention 6h, compaction interval 60s, space-reclaim interval 1h, `table_high_write_throughput_threshold = 16M`.
