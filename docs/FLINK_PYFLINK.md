---
title: Hermes in PyFlink DataStream
description: Concrete PyFlink DataStream design for the Hermes flow using existing Kafka topics, including two-job boundaries, correction protocols, and deployment guidance.
author: Copilot
ms.date: 2026-04-27
ms.topic: how-to
keywords:
  - pyflink
  - flink
  - datastream
  - kafka
  - iceberg
estimated_reading_time: 14
---

## Purpose

This guide provides a PyFlink DataStream version of the Hermes design with the same two-job boundary:

* Job 1: continuous realtime computation
* Job 2: bounded backfill and correction restatement

It preserves your existing Kafka sources and current window logic.

This version assumes Postgres is required as the fast serving layer.

## Two-Job Boundary

## Job 1: hermes_realtime_ds

Type: long-running DataStream job

Inputs:

* page_views Kafka topic
* cart_events Kafka topic
* purchases Kafka topic
* Optional hermes_corrections Kafka topic

Boundaries:

1. Source and parsing boundary

* Deserialize events from JSON
* Normalize to canonical schema
* Assign timestamps and watermarks

2. Correction boundary

* Apply correction events as keyed updates and deletes
* Maintain latest record per event key when required

3. Window feature boundary

* Build 10s, 30s, and 60s pipelines
* Join by user_id and event-time window containment
* Compute distinct viewers, carters, purchasers
* Compute conversion rates

4. Sink boundary

* Write feature rows to Iceberg sink
* Write feature rows to Postgres serving sink
* Optional write to Kafka hermes_features topic

## Job 2: hermes_backfill_ds

Type: bounded batch-style DataStream job

Inputs:

* bounded Kafka range, or
* historical Bronze source from Iceberg

Boundaries:

1. Read only impacted range
2. Recompute with identical business logic
3. Write to Iceberg and Postgres shadow output tables
4. Validate and promote both stores

## Data Storage In PyFlink Design

1. Runtime state

* RocksDB keyed state
* Checkpoints and savepoints in MinIO or S3

2. Durable output

* Iceberg table as system of record for Hermes features

3. Serving output

* Postgres table hermes_features_serving for low-latency API reads
* Primary key: window_start, time_interval

4. Optional event output

* Kafka sink for push-based consumers

## Update Correction Protocol

Use a two-lane protocol.

### Lane A: small fixes

1. Emit correction events to hermes_corrections.
2. Realtime job applies updates or deletes to keyed state.
3. Recompute affected windows incrementally.
4. Persist corrected outputs to both Iceberg and Postgres.

### Lane B: full backfill

1. Define affected historical interval.
2. Run bounded backfill job for that interval.
3. Write to Iceberg and Postgres shadow tables.
4. Validate, then promote both stores.

## PyFlink DataStream Example

The sample below is a practical skeleton showing structure and boundaries.
It is intentionally concise and omits environment-specific connector setup details.

```python
from dataclasses import dataclass
from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.time import Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows


@dataclass
class Event:
    stream_type: str  # page | cart | purchase
    user_id: int
    event_time_ms: int
    payload: dict
    op: str = "UPSERT"  # UPSERT | DELETE


@dataclass
class HermesFeature:
    window_start_ms: int
    window_end_ms: int
    time_interval: str
    viewers: int
    carters: int
    purchasers: int
    view_to_cart_rate: float
    cart_to_buy_rate: float


def build_kafka_source(topic: str, bootstrap: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000)
    # Configure state backend and checkpoint storage in flink-conf or env settings.
    return env


def parse_json_to_event(stream_type: str):
    def _fn(raw: str) -> Event:
        import json
        obj = json.loads(raw)
        return Event(
            stream_type=stream_type,
            user_id=int(obj["user_id"]),
            event_time_ms=int(obj.get("event_time_ms", 0)),
            payload=obj,
            op=obj.get("op", "UPSERT"),
        )
    return _fn


def watermark_strategy() -> WatermarkStrategy:
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(lambda e, ts: e.event_time_ms)
    )


def compute_interval_features(unified_stream, interval_seconds: int, label: str):
    # Production implementation options:
    # 1) Key by user_id and keep per-window set state for distinct counts.
    # 2) Or split by stream_type and perform interval joins before aggregation.
    # This skeleton keeps the boundary visible without full custom state code.
    return (
        unified_stream
        .key_by(lambda e: e.user_id, key_type=Types.INT())
        .window(TumblingEventTimeWindows.of(Time.seconds(interval_seconds)))
        .process(ComputeHermesWindow(label),
                 output_type=Types.PICKLED_BYTE_ARRAY())
    )


class ComputeHermesWindow:
    def __init__(self, label: str):
        self.label = label

    def process(self, key, context, elements, out):
        # Replace with robust distinct-state logic by stream type.
        viewers = 0
        carters = 0
        purchasers = 0
        # Emit placeholder feature shape.
        v2c = round((carters / viewers), 2) if viewers else 0.0
        c2b = round((purchasers / carters), 2) if carters else 0.0
        out.collect(
            HermesFeature(
                window_start_ms=context.window().start,
                window_end_ms=context.window().end,
                time_interval=self.label,
                viewers=viewers,
                carters=carters,
                purchasers=purchasers,
                view_to_cart_rate=v2c,
                cart_to_buy_rate=c2b,
            )
        )


def build_realtime_job() -> None:
    env = build_env()
    bootstrap = "redpanda:9092"
    group_id = "hermes-pyflink"

    page = env.from_source(
        build_kafka_source("page_views", bootstrap, group_id),
        watermark_strategy(),
        "page_views",
    ).map(parse_json_to_event("page"))

    cart = env.from_source(
        build_kafka_source("cart_events", bootstrap, group_id),
        watermark_strategy(),
        "cart_events",
    ).map(parse_json_to_event("cart"))

    purchase = env.from_source(
        build_kafka_source("purchases", bootstrap, group_id),
        watermark_strategy(),
        "purchases",
    ).map(parse_json_to_event("purchase"))

    unified = page.union(cart).union(purchase)

    f10 = compute_interval_features(unified, 10, "10 SECONDS")
    f30 = compute_interval_features(unified, 30, "30 SECONDS")
    f60 = compute_interval_features(unified, 60, "60 SECONDS")

    features = f10.union(f30).union(f60)

    # Replace with Iceberg sink plus JDBC Postgres sink implementation.
    # Postgres table should use primary key (window_start, time_interval)
    # to support idempotent upserts for corrections.
    features.print()

    env.execute("hermes_realtime_ds")


if __name__ == "__main__":
    build_realtime_job()
```

> [!IMPORTANT]
> In production, implement exact distinct-count and join semantics with managed keyed state, and verify parity against your existing Hermes outputs.

## Bounded Backfill Skeleton

Use the same transformation graph with bounded source configuration.

```python
def build_backfill_job(start_offsets, stop_offsets):
    env = build_env()
    # Configure Kafka sources with bounded offsets or bounded timestamps.
    # Reuse parse, watermark, correction, and feature logic.
    # Write to shadow Iceberg and Postgres tables for validation and promotion.
    env.execute("hermes_backfill_ds")
```

## Deployment Options

### Flink native deployment

* Submit PyFlink jobs with Flink CLI.
* Use savepoints for zero-loss upgrades.
* Keep job artifacts and config versioned.

### Kubernetes with Flink Operator

* Deploy as FlinkDeployment or FlinkSessionJob.
* Recommended for reproducible environments and managed lifecycle.

### Dagster orchestration

This repository already uses Dagster.
Add assets or ops that:

1. Submit realtime job.
2. Monitor status and lag.
3. Trigger and validate backfill job.
4. Promote Iceberg and Postgres shadow tables after checks.

## Could You Use dbt To Deploy These Flink Jobs

You can use dbt for some pieces, but not for full job lifecycle.

What dbt can do:

* Manage SQL DDL models if you choose a dbt-flink adapter.
* Define schemas and SQL transformations.

What dbt should not be your primary tool for:

* Long-running streaming job deployment
* Savepoint-driven upgrades
* Runtime scaling and rollback lifecycle

Recommended split:

1. Use dbt optionally for SQL modeling.
2. Use Flink native tooling for runtime.
3. Use Dagster for orchestration, triggering, and validation across Iceberg and Postgres.

## Rollout Checklist

1. Stand up Job 1 in shadow mode and sink to shadow Iceberg table.
2. Add Postgres serving sink and verify upsert correctness.
3. Run parity checks versus existing Hermes logic in both Iceberg and Postgres.
4. Create and test Job 2 bounded backfill path with dual shadow tables.
5. Add promotion and rollback runbook.
6. Add alerts for checkpoint health, lag, state growth, and Postgres sink failures.
