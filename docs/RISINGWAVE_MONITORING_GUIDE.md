---
title: RisingWave Monitoring Guide
description: How to access and use Prometheus and Grafana to monitor the RisingWave stack in this repository, including available dashboards, PromQL queries, and extension guidance.
author: Copilot
ms.date: 2026-04-27
ms.topic: how-to
keywords:
  - risingwave
  - prometheus
  - grafana
  - monitoring
  - observability
estimated_reading_time: 8
---

## Purpose

This guide explains how to use the monitoring stack included in this repository to observe RisingWave and its supporting services.

The monitoring stack includes:

* Prometheus for metrics scraping and query exploration
* Grafana for dashboards and visualization

It is designed around the current local Docker Compose environment.

## What Is Monitored

The default Prometheus configuration scrapes these jobs:

* `risingwave-meta`
* `risingwave-frontend`
* `risingwave-compute`
* `risingwave-compactor`
* `minio`
* `redpanda`

The stack also includes `kafka-head-observer`, which writes Kafka head event-time checkpoints into RisingWave table `kafka_head_event_time` for exact lag measurement.

That gives you visibility into:

* RisingWave control-plane and execution-plane health
* MinIO dependency health
* Redpanda dependency health
* Prometheus scrape health and target availability

## Starting The Monitoring Stack

The monitoring services are part of Docker Compose and start with the rest of the project.

```bash
./bin/1_up.sh
```

After startup, the main monitoring endpoints are:

* Grafana: <http://localhost:3001>
* Prometheus: <http://localhost:9500>

## Accessing Grafana

Grafana is configured for anonymous access with no login required (suitable for local demo use).

1. Open <http://localhost:3001> — you are taken straight to the home screen.
2. Open the `RisingWave` folder in the dashboard browser.

> **Note:** Grafana is pinned to version `11.6.3`. The `latest` tag (Grafana 13+) introduced a new unified storage backend that breaks traditional file-based dashboard provisioning and is not used here.

Provisioned dashboards:

* `RisingWave Overview`
* `RisingWave Pipeline Health`
* `RisingWave Funnel Business Health`

## What The Dashboards Show

## RisingWave Overview

This dashboard is the first place to look when you want to answer: is the stack healthy right now?

It includes panels for:

* RisingWave targets up
* Redpanda up
* MinIO up
* total scrape targets
* target availability by job
* RisingWave process memory
* RisingWave CPU rate
* scrape duration

Use it for:

* quick environment checks after startup
* seeing whether metrics are flowing
* identifying obvious service outages

## RisingWave Pipeline Health

This dashboard focuses more on the monitoring pipeline itself and the health of the scraped services.

It includes panels for:

* RisingWave target health
* scraped sample counts by job
* open file descriptors
* max file descriptors
* dependency health
* dependency scrape duration

Use it for:

* detecting scrape regressions
* spotting process pressure signals
* confirming that Redpanda and MinIO remain reachable

## RisingWave Funnel Business Health

This dashboard is business-level and SQL-backed.

It includes panels for:

* funnel freshness in seconds
* latest processed window end timestamp
* Kafka to RisingWave lag in seconds
* windows processed in the last 15 minutes
* freshness trend over time
* latest 20 processed windows with conversion metrics

This dashboard uses the provisioned `RisingWave SQL` datasource and queries `funnel_summary` directly.

Prerequisite:

* run `./bin/3_run_dbt.sh` so `funnel_summary` exists and has data

### How To Interpret The Top Numbers

* `Funnel Freshness (seconds)`:
  * computed as `now - max(window_start)` from `funnel_summary`
  * lower is better
  * this is business freshness of processed funnel windows

* `Latest Processed Window End`:
  * latest `window_end` in `funnel_summary`
  * shown as a timestamp in Grafana

* `Windows Processed (last 15m)`:
  * count of `funnel_summary` rows with `window_end` in last 15 minutes
  * indicates whether processing is continuously producing windows

* `Viewers TPS`:
  * page-view throughput in ops/sec over a rolling 30-second window
  * computed as `COUNT(*) / elapsed_seconds` where elapsed is the span between oldest and newest `produced_at` in the window
  * reflects the producer rate as seen by RisingWave, including any small timing jitter

* `Kafka → RisingWave Lag (seconds)`:
  * exact lag based on event-time checkpoints, not current time
  * computed as `max(event_time in Kafka) - max(event_time in RisingWave)` per corresponding topic
  * panel shows the max lag across `page_views`, `cart_events`, and `purchases`

> [!IMPORTANT]
> `Kafka → RisingWave Lag` is event-time lag, not transport RTT. If producer event timestamps are skewed or delayed, this metric reflects that skew.

## Accessing Prometheus

Open <http://localhost:9500>.

Prometheus is useful for:

* validating that targets are being scraped
* exploring available metric names
* testing PromQL before creating new Grafana panels
* troubleshooting whether a missing panel is a Grafana problem or a scrape problem

## How Prometheus Is Updated

Prometheus **pulls** (scrapes) metrics from each target every **15 seconds**. Each target exposes an HTTP endpoint that Prometheus polls on schedule.

| Job | Target | Metrics path |
|---|---|---|
| `risingwave-meta` | `meta-node-0:1250` | `/metrics` |
| `risingwave-frontend` | `frontend-node-0:2222` | `/metrics` |
| `risingwave-compute` | `compute-node-0:1222` | `/metrics` |
| `risingwave-compactor` | `compactor-0:1260`, `compactor-1:1261` | `/metrics` |
| `minio` | `minio-0:9301` | `/minio/v2/metrics/cluster` |
| `redpanda` | `redpanda:9644` | `/public_metrics` |

Grafana panels backed by Prometheus are therefore at most 15 seconds stale. The SQL-backed panels (such as Viewers TPS and Funnel Business Health) bypass Prometheus entirely — they query RisingWave directly and are limited only by Grafana's dashboard refresh interval.

## Useful Starter PromQL Queries

### Check That RisingWave Targets Are Up

```promql
up{job=~"risingwave-.*"}
```

### Check RisingWave Memory Usage

```promql
process_resident_memory_bytes{job=~"risingwave-.*"}
```

### Check RisingWave CPU Rate

```promql
rate(process_cpu_seconds_total{job=~"risingwave-.*"}[5m])
```

### Check Scrape Durations

```promql
scrape_duration_seconds{job=~"risingwave-.*|redpanda|minio"}
```

### Check Total RisingWave Targets Up

```promql
sum(up{job=~"risingwave-.*"})
```

## Recommended Monitoring Workflow

Use the tools in this order.

1. Open Grafana.
2. Check `RisingWave Overview`.
3. If something looks wrong, open Prometheus and inspect the target metrics directly.
4. If you need SQL-level confirmation, query RisingWave system catalogs.

Useful SQL checks:

```sql
SELECT id, host, type, state FROM rw_catalog.rw_worker_nodes;
SELECT name, connector FROM rw_catalog.rw_sources;
SELECT name, sink_type, connector FROM rw_catalog.rw_sinks;
SELECT ddl_id, ddl_statement, progress FROM rw_catalog.rw_ddl_progress;
SELECT max(window_start), count(*) FROM datalake.public.rw_managed_funnel;
SELECT committed_at, snapshot_id
FROM datalake.public."rw_managed_funnel$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

## Extending The Dashboards

The included dashboards are starter dashboards. You will likely want to extend them as your stack evolves.

Good next additions:

* Kafka lag panels for input topics
* funnel freshness indicators
* sink throughput and error panels
* application-level business freshness panels
* Postgres sink freshness if you add serving-layer monitoring

Workflow for extending:

1. Find a useful metric in Prometheus.
2. Test a PromQL query in the Prometheus UI.
3. Add a Grafana panel.
4. Export the dashboard JSON back into `monitoring/grafana/dashboards/`.

## Troubleshooting

## Grafana loads but no dashboards appear

Check:

* container `grafana-0` is running
* dashboard provisioning files exist under `monitoring/grafana/provisioning/`
* JSON dashboard files exist under `monitoring/grafana/dashboards/`

## Grafana dashboards load but panels are empty

Check:

* Prometheus is reachable at <http://localhost:9500>
* scrape targets are up in the Prometheus `Targets` page
* the queried metric actually exists in the current RisingWave build
* for `RisingWave Funnel Business Health`, verify `funnel_summary` exists and has data
* for RW-managed Iceberg output, verify `datalake.public.rw_managed_funnel` has a recent `max(window_start)` and advancing entries in `rw_managed_funnel$snapshots` (compactor-1 should be actively compacting files)

## Prometheus is up but RisingWave targets are down

Check:

* `docker compose ps`
* RisingWave services are healthy
* internal service names are unchanged
* metrics ports are still the ones configured in `docker-compose.yml`

## Important Caveat

Prometheus and Grafana are excellent for infrastructure and runtime monitoring, but they do not replace data validation.

For Hermes-style correctness, keep both layers:

* metrics-based monitoring for service health and throughput
* SQL-based checks for data freshness and correctness
