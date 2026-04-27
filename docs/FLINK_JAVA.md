---
title: Hermes in Flink Java
description: Concrete Java implementation design for the Hermes flow in Apache Flink using the existing Kafka topics, with Postgres serving, Iceberg durability, correction protocols, and deployment guidance.
author: Copilot
ms.date: 2026-04-27
ms.topic: how-to
keywords:
  - flink
  - java
  - kafka
  - iceberg
  - postgres
estimated_reading_time: 15
---

## Purpose

This guide shows what a Java implementation of Hermes would look like in Apache Flink using the same Kafka topics:

* page_views
* cart_events
* purchases

It follows the same architectural assumptions as the other Flink documents:

* one continuous realtime job
* one bounded backfill job
* Iceberg as the analytical system of record
* Postgres as the required fast serving layer

## Why Java For Flink

Java is the strongest implementation option for Flink when you need:

* the deepest API coverage
* the best connector and state-management support
* the lowest friction for custom operators, state, and production tuning

For your Hermes flow, Java is the safest choice if you expect heavy state, exact control over corrections, or a need to evolve beyond purely relational SQL.

## Answer At A Glance

* Always-on jobs needed: 1
* On-demand recompute jobs needed: 1
* Practical production setup: 2 jobs total

Job 1 handles continuous event ingestion, correction handling, feature computation, and sink fan-out.
Job 2 handles bounded historical recompute and shadow-table promotion.

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

In Java, there are two practical implementation styles:

1. Table API and SQL for the relational parts
2. DataStream API with keyed state for explicit control

For Hermes, a hybrid approach is usually best:

* use Java for job lifecycle, sources, sinks, and custom correction logic
* use Table API or SQL for the windowed relational computation when that keeps the code simpler

## Concrete Topology And Exact Job Boundaries

## Job 1: hermes_realtime_java

Type: continuous streaming job

Inputs:

* Kafka page_views
* Kafka cart_events
* Kafka purchases
* Optional Kafka hermes_corrections topic

Boundary A: source ingestion and normalization

* Read Kafka topics with KafkaSource
* Deserialize JSON into typed Java event classes
* Validate required fields
* Assign event-time timestamps and watermarks

Boundary B: correction application

* Merge correction events into the logical event stream
* Model corrections as UPSERT or DELETE events keyed by stable event_id
* Maintain deterministic event identity for idempotent replay

Boundary C: feature computation

* Compute 10-second window features
* Compute 30-second window features
* Compute 60-second window features
* Union all feature records with a timeInterval field

Boundary D: sink fan-out

* Primary sink: Iceberg table iceberg_hermes_features
* Required serving sink: Postgres table hermes_features_serving
* Optional Kafka sink: hermes_features topic for subscribers

Outputs:

* Durable analytical history in Iceberg
* Low-latency serving surface in Postgres
* Optional stream-native fan-out in Kafka

## Job 2: hermes_backfill_java

Type: bounded replay and recompute job

Trigger:

* manual replay
* nightly reconciliation
* incident-driven correction run

Inputs:

* Kafka bounded range, or
* Bronze raw table in Iceberg if retained

Boundary A: bounded read and normalization

* Read only the impacted time range
* Reuse the same parse, validation, and watermark rules as Job 1

Boundary B: deterministic recompute

* Run the same feature logic as Job 1
* Write to Iceberg shadow table, for example iceberg_hermes_features_rebuild
* Write to Postgres shadow table, for example hermes_features_serving_rebuild

Boundary C: validation and promotion

* Compare row counts and aggregates
* Compare sampled windows
* Promote Iceberg by table swap or view switch
* Promote Postgres by atomic rename or partition switch

Outputs:

* Corrected historical slice
* Auditable recompute artifact

## Where Data Should Live

Use four storage layers.

1. Flink runtime state

* RocksDB keyed state backend
* Checkpoints and savepoints in MinIO or S3

2. Analytical system of record

* Iceberg table for Hermes features
* Partition by date(window_start)
* Business key: window_start, time_interval

3. Fast serving layer

* Postgres table for API and dashboard reads
* Primary key: window_start, time_interval
* Upsert semantics from Flink JDBC sink

4. Optional event distribution layer

* Kafka sink for downstream event subscribers

## Update And Correction Protocol

There are two correction paths.

### Small fixes

Use event-driven corrections.

1. Emit UPSERT or DELETE correction events to hermes_corrections.
2. Job 1 consumes corrections and updates state.
3. Affected windows are recalculated incrementally.
4. Updated rows are written to both Iceberg and Postgres.
5. Reconcile affected windows in both stores.

Use this when:

* the correction scope is small
* the fix is operational and near-real-time

### Full backfill

Use bounded restatement.

1. Define the impacted interval.
2. Run Job 2 over that bounded range.
3. Write results to Iceberg and Postgres shadow tables.
4. Validate parity checks.
5. Promote corrected output in both stores.

Use this when:

* corrections are broad
* logic changed and history must be restated
* upstream data quality incidents affected a large range

## Java Implementation Sketch

The example below is a practical structure, not a complete production-ready job. It shows how to model the main implementation surfaces in Java.

```java
package com.example.hermes;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HermesRealtimeJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<String> pageSource = KafkaSource.<String>builder()
            .setBootstrapServers("redpanda:9092")
            .setTopics("page_views")
            .setGroupId("hermes-java")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> cartSource = KafkaSource.<String>builder()
            .setBootstrapServers("redpanda:9092")
            .setTopics("cart_events")
            .setGroupId("hermes-java")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> purchaseSource = KafkaSource.<String>builder()
            .setBootstrapServers("redpanda:9092")
            .setTopics("purchases")
            .setGroupId("hermes-java")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        WatermarkStrategy<BaseEvent> watermarks = WatermarkStrategy
            .<BaseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((SerializableTimestampAssigner<BaseEvent>) (event, ts) -> event.eventTimeMs());

        DataStream<PageViewEvent> pageViews = env
            .fromSource(pageSource, WatermarkStrategy.noWatermarks(), "page_views")
            .map(EventParsers::parsePageView)
            .assignTimestampsAndWatermarks(EventWatermarks.pageWatermarks());

        DataStream<CartEvent> cartEvents = env
            .fromSource(cartSource, WatermarkStrategy.noWatermarks(), "cart_events")
            .map(EventParsers::parseCartEvent)
            .assignTimestampsAndWatermarks(EventWatermarks.cartWatermarks());

        DataStream<PurchaseEvent> purchaseEvents = env
            .fromSource(purchaseSource, WatermarkStrategy.noWatermarks(), "purchases")
            .map(EventParsers::parsePurchaseEvent)
            .assignTimestampsAndWatermarks(EventWatermarks.purchaseWatermarks());

        // Optional correction stream would be merged here.
        // A production version would normalize these streams into a shared event model,
        // apply changelog semantics, and then compute the three intervals.

        DataStream<HermesFeature> features10s = HermesFeaturePipelines.compute(pageViews, cartEvents, purchaseEvents, 10, "10 SECONDS");
        DataStream<HermesFeature> features30s = HermesFeaturePipelines.compute(pageViews, cartEvents, purchaseEvents, 30, "30 SECONDS");
        DataStream<HermesFeature> features60s = HermesFeaturePipelines.compute(pageViews, cartEvents, purchaseEvents, 60, "60 SECONDS");

        DataStream<HermesFeature> allFeatures = features10s.union(features30s).union(features60s);

        // Sink 1: Iceberg
        allFeatures.sinkTo(Sinks.icebergSink());

        // Sink 2: Postgres serving
        allFeatures.sinkTo(Sinks.postgresJdbcSink());

        env.execute("hermes_realtime_java");
    }
}
```

## Suggested Java Project Structure

```text
flink-java/
  build.gradle or pom.xml
  src/main/java/com/example/hermes/
    HermesRealtimeJob.java
    HermesBackfillJob.java
    model/
      BaseEvent.java
      PageViewEvent.java
      CartEvent.java
      PurchaseEvent.java
      CorrectionEvent.java
      HermesFeature.java
    parsing/
      EventParsers.java
    watermark/
      EventWatermarks.java
    pipeline/
      HermesFeaturePipelines.java
    sink/
      Sinks.java
    validation/
      ReconciliationChecks.java
```

## Hybrid Java And Table API Option

If you want Java as the deployment language but do not want to hand-code all window logic in the DataStream API, use a TableEnvironment inside the Java job.

That gives you this split:

1. Java manages sources, configuration, corrections, savepoints, and deployment.
2. SQL or Table API handles the three tumbling windows and UNION ALL logic.
3. Java manages Iceberg and Postgres sink lifecycle.

That is often the cleanest approach for a Hermes-style flow.

## Postgres Serving Table Design

Recommended serving table shape:

```sql
CREATE TABLE public.hermes_features_serving (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  time_interval TEXT NOT NULL,
  minute_start TIMESTAMP NOT NULL,
  viewers BIGINT NOT NULL,
  carters BIGINT NOT NULL,
  purchasers BIGINT NOT NULL,
  view_to_cart_rate DOUBLE PRECISION NOT NULL,
  cart_to_buy_rate DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (window_start, time_interval)
);

CREATE INDEX idx_hermes_features_serving_interval_start
  ON public.hermes_features_serving (time_interval, window_start DESC);
```

The primary key enables idempotent upserts from the Flink JDBC sink and keeps correction replay manageable.

## Deployment Options

### Option A: Flink CLI or REST

* Package the Java job as a JAR.
* Submit with Flink CLI or REST API.
* Use savepoints for upgrades and controlled restarts.

### Option B: Kubernetes With Flink Operator

* Deploy the JAR as a FlinkDeployment or FlinkSessionJob.
* Recommended if you want repeatable environment management and GitOps workflows.

### Option C: Dagster orchestration

This repository already uses Dagster.
Add ops or assets that:

1. submit the realtime job
2. monitor job health and lag
3. trigger the backfill job
4. validate Iceberg and Postgres shadow outputs
5. promote both stores after validation

## Could You Use dbt

Short answer: not as the primary deployment tool for Java Flink jobs.

What dbt can do:

* define relational models and SQL artifacts
* help document schemas and transformation logic
* optionally generate SQL used by a Table API based Java job

What dbt should not own:

* long-running Flink job lifecycle
* savepoint upgrade workflow
* runtime scaling, restart, and rollback

Recommended split:

1. Use Java and Flink native tooling for runtime lifecycle.
2. Use dbt only if you want SQL artifact management.
3. Use Dagster to orchestrate submit, monitor, backfill, validation, and promotion.

## Rollout Checklist

1. Build Job 1 and sink to shadow Iceberg and Postgres tables.
2. Compare parity against your current Hermes outputs.
3. Add Job 2 bounded recompute with dual shadow-table writes.
4. Validate correction replay in both Iceberg and Postgres.
5. Productionize checkpoints, savepoints, alerts, and runbooks.
6. Promote Java Flink outputs after reconciliation thresholds pass.