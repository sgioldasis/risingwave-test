---
title: Hermes in Flink SQL
description: Concrete Flink SQL design for the Hermes flow using existing Kafka topics, including job boundaries, storage strategy, correction protocols, and deployment options.
author: Copilot
ms.date: 2026-04-27
ms.topic: how-to
keywords:
  - flink
  - risingwave
  - kafka
  - iceberg
  - streaming
estimated_reading_time: 12
---

## Purpose

This guide shows how to implement your current Hermes flow in Apache Flink using the same Kafka topics:

* page_views
* cart_events
* purchases

It focuses on practical architecture, exact job boundaries, data storage, correction and recalculation strategy, and deployability.

This version assumes Postgres is required as the fast serving layer.

## Answer At A Glance

* Always-on jobs needed: 1
* On-demand recompute jobs needed: 1
* Practical production setup: 2 jobs total

Job 1 handles real-time stream processing and continuous outputs.
Job 2 is a bounded backfill and correction job used when you need historical restatement.

## Mapping From Current Hermes Logic

Your current Hermes model computes three tumbling windows and unions them:

* 10 seconds
* 30 seconds
* 60 seconds

Per window, compute:

* viewers = count(distinct page.user_id)
* carters = count(distinct cart.user_id)
* purchasers = count(distinct purchase.user_id)
* view_to_cart_rate
* cart_to_buy_rate

In Flink SQL, this maps cleanly to three TUMBLE pipelines plus UNION ALL.

## Concrete Topology And Exact Job Boundaries

## Job 1: hermes_realtime

Type: continuous streaming job

Inputs:

* Kafka page_views
* Kafka cart_events
* Kafka purchases
* Optional Kafka hermes_corrections topic

Boundary A: ingest and event-time normalization

* Parse JSON payloads
* Validate required fields
* Assign event-time watermarks
* Standardize timestamps to TIMESTAMP(3)

Boundary B: correction application

* If using a correction topic, merge correction events before aggregation
* Convert upstream events into an explicit changelog model
* Keep deterministic event keys for updates and deletes

Boundary C: multi-window feature computation

* Compute 10s features
* Compute 30s features
* Compute 60s features
* UNION ALL into a single result stream with time_interval column

Boundary D: sink fan-out

* Primary sink: Iceberg table iceberg_hermes_features
* Required serving sink: Postgres table hermes_features_serving
* Optional Kafka sink: hermes_features topic for event subscribers

Outputs:

* Latest continuously updated Hermes features
* Durable snapshots in Iceberg
* Low-latency query surface in Postgres

## Job 2: hermes_backfill

Type: bounded batch or bounded-stream job

Trigger:

* Manual trigger from orchestration
* Scheduled nightly reconciliation
* Incident-driven correction run

Inputs:

* Kafka earliest offsets in a bounded time range, or
* Bronze raw table in Iceberg if retained

Boundary A: bounded read and normalization

* Read only affected interval
* Reuse same parsing and validation rules as Job 1

Boundary B: deterministic recompute

* Run the same feature logic as Job 1 for parity
* Write to a shadow table, for example iceberg_hermes_features_rebuild
* Rebuild matching Postgres shadow table, for example hermes_features_serving_rebuild

Boundary C: validation and promotion

* Compare aggregates and row counts
* Compare sampled windows
* Promote Iceberg via table swap or view pointer switch
* Promote Postgres by atomic table rename or partition switch

Outputs:

* Corrected historical slice
* Auditable backfill artifact

## Where Data Should Live

Use three storage layers.

1. Flink state

* RocksDB state backend for joins and distinct window state
* Checkpoints and savepoints in object storage such as MinIO or S3

2. Analytical system of record

* Iceberg table for Hermes features
* Partition by date(window_start)
* Use fields window_start and time_interval as business key

3. Serving layer

* Postgres table for API and dashboard reads
* Primary key: window_start, time_interval
* Upsert writes from Flink JDBC sink

4. Optional event fan-out

* Kafka sink for subscribers that need stream-native updates

## Update And Correction Protocol

There are two correction paths.

### Small fixes

Use event-driven corrections.

1. Emit UPSERT or DELETE correction events keyed by stable event_id.
2. Job 1 consumes corrections and updates state.
3. Updated window results are emitted to both Iceberg and Postgres sinks.
4. Run reconciliation queries for affected windows in both stores.

Use this when:

* The correction scope is small.
* You need near-real-time repair.

### Full backfill

Use bounded restatement.

1. Define impacted time range.
2. Run Job 2 to recompute that range.
3. Write results to Iceberg and Postgres shadow tables.
4. Validate parity checks.
5. Promote corrected output in both stores.

Use this when:

* Corrections are large.
* Logic changed and history must be restated.
* Upstream quality incidents affected broad intervals.

## Flink SQL Example

This example is intentionally concise but executable with connector properties adjusted to your environment.

```sql
-- Source: page_views
CREATE TABLE page_views (
  user_id INT,
  page_id STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'page_views',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'hermes-flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Source: cart_events
CREATE TABLE cart_events (
  user_id INT,
  item_id STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cart_events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'hermes-flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Source: purchases
CREATE TABLE purchases (
  user_id INT,
  amount DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'purchases',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'hermes-flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Iceberg sink
CREATE TABLE iceberg_hermes_features (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  time_interval STRING,
  minute_start TIMESTAMP(3),
  viewers BIGINT,
  carters BIGINT,
  purchasers BIGINT,
  view_to_cart_rate DOUBLE,
  cart_to_buy_rate DOUBLE
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'lakekeeper',
  'catalog-type' = 'rest',
  'uri' = 'http://lakekeeper:8181',
  'warehouse' = 's3://warehouse',
  'database-name' = 'public',
  'table-name' = 'iceberg_hermes_features',
  'format-version' = '2'
);

-- Postgres serving sink
CREATE TABLE postgres_hermes_features_serving (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  time_interval STRING,
  minute_start TIMESTAMP(3),
  viewers BIGINT,
  carters BIGINT,
  purchasers BIGINT,
  view_to_cart_rate DOUBLE,
  cart_to_buy_rate DOUBLE,
  PRIMARY KEY (window_start, time_interval) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres-0:5432/postgres',
  'table-name' = 'public.hermes_features_serving',
  'driver' = 'org.postgresql.Driver',
  'username' = 'postgres',
  'password' = 'postgres'
);

CREATE TEMPORARY VIEW hermes_features_unified AS
WITH
interval_10_seconds AS (
  SELECT
    p.window_start,
    p.window_end,
    '10 SECONDS' AS time_interval,
    DATE_TRUNC('MINUTE', p.window_start) AS minute_start,
    COUNT(DISTINCT p.user_id) AS viewers,
    COUNT(DISTINCT c.user_id) AS carters,
    COUNT(DISTINCT pur.user_id) AS purchasers
  FROM TABLE(
    TUMBLE(TABLE page_views, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
  ) AS p
  LEFT JOIN cart_events c
    ON p.user_id = c.user_id
   AND c.event_time BETWEEN p.window_start AND p.window_end
  LEFT JOIN purchases pur
    ON p.user_id = pur.user_id
   AND pur.event_time BETWEEN p.window_start AND p.window_end
  GROUP BY p.window_start, p.window_end
),
interval_30_seconds AS (
  SELECT
    p.window_start,
    p.window_end,
    '30 SECONDS' AS time_interval,
    DATE_TRUNC('MINUTE', p.window_start) AS minute_start,
    COUNT(DISTINCT p.user_id) AS viewers,
    COUNT(DISTINCT c.user_id) AS carters,
    COUNT(DISTINCT pur.user_id) AS purchasers
  FROM TABLE(
    TUMBLE(TABLE page_views, DESCRIPTOR(event_time), INTERVAL '30' SECOND)
  ) AS p
  LEFT JOIN cart_events c
    ON p.user_id = c.user_id
   AND c.event_time BETWEEN p.window_start AND p.window_end
  LEFT JOIN purchases pur
    ON p.user_id = pur.user_id
   AND pur.event_time BETWEEN p.window_start AND p.window_end
  GROUP BY p.window_start, p.window_end
),
interval_1_minute AS (
  SELECT
    p.window_start,
    p.window_end,
    '60 SECONDS' AS time_interval,
    DATE_TRUNC('MINUTE', p.window_start) AS minute_start,
    COUNT(DISTINCT p.user_id) AS viewers,
    COUNT(DISTINCT c.user_id) AS carters,
    COUNT(DISTINCT pur.user_id) AS purchasers
  FROM TABLE(
    TUMBLE(TABLE page_views, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
  ) AS p
  LEFT JOIN cart_events c
    ON p.user_id = c.user_id
   AND c.event_time BETWEEN p.window_start AND p.window_end
  LEFT JOIN purchases pur
    ON p.user_id = pur.user_id
   AND pur.event_time BETWEEN p.window_start AND p.window_end
  GROUP BY p.window_start, p.window_end
),
stats AS (
  SELECT * FROM interval_10_seconds
  UNION ALL
  SELECT * FROM interval_30_seconds
  UNION ALL
  SELECT * FROM interval_1_minute
)
SELECT
  window_start,
  window_end,
  time_interval,
  minute_start,
  viewers,
  carters,
  purchasers,
  ROUND(COALESCE(CAST(carters AS DOUBLE) / NULLIF(CAST(viewers AS DOUBLE), 0.0), 0.0), 2) AS view_to_cart_rate,
  ROUND(COALESCE(CAST(purchasers AS DOUBLE) / NULLIF(CAST(carters AS DOUBLE), 0.0), 0.0), 2) AS cart_to_buy_rate
FROM stats;

INSERT INTO iceberg_hermes_features
SELECT * FROM hermes_features_unified;

INSERT INTO postgres_hermes_features_serving
SELECT * FROM hermes_features_unified;
```

> [!IMPORTANT]
> COUNT DISTINCT over joined streams can produce large state. Size RocksDB and checkpoint intervals carefully, and test with production-like cardinality.

## Deployment Options

You can deploy Flink jobs in several ways.

### Option A: Flink CLI or REST deployment

* Package SQL job or SQL client script.
* Submit through Flink SQL Client, REST API, or CLI.
* Manage lifecycle with savepoints for upgrades.

### Option B: Kubernetes with Flink Kubernetes Operator

* Define FlinkDeployment and FlinkSessionJob manifests.
* GitOps-friendly.
* Good for repeatable environments.

### Option C: Dagster orchestration

This repository already uses Dagster and dbt orchestration.
You can add a Dagster asset or op that submits Flink jobs, monitors status, and runs Postgres refresh validation.

### Can dbt deploy Flink jobs

Short answer: not as your primary job lifecycle tool.

* dbt can model SQL objects and execute SQL transformations.
* dbt is not designed to manage long-running streaming job lifecycle, savepoints, and upgrades.
* If you use dbt-flink adapter, use it for DDL and SQL artifact generation, not for production job control.

Recommended split:

1. Use dbt for data model definitions and SQL generation if desired.
2. Use Flink native deployment tooling for runtime lifecycle.
3. Use Dagster to orchestrate submit, monitor, backfill, and validation flows across Iceberg and Postgres.

## Minimal Rollout Plan

1. Build Job 1 and write to a shadow Iceberg table.
2. Add Postgres serving sink and validate upsert behavior.
3. Compare against current Hermes outputs for parity windows in both stores.
4. Add Job 2 bounded backfill template with dual shadow tables.
5. Productionize checkpoints, savepoints, and alerting.
6. Promote Flink outputs after reconciliation thresholds pass.
