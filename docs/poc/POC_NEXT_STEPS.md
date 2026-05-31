# Next Steps to Complete UC1 & UC2 PoC

## Context

The UC1 and UC2 pipelines are functionally implemented (sources, MVs, Iceberg sinks, dbt, Dagster). The PoC document defines specific requirements (R1–R4) and verification items (V1–V3) with a concrete success criterion. This document tracks what is remaining, what has been completed since the initial plan, and known limitations of the current RisingWave version.

---

## Gap Analysis

### What is done
- UC1 and UC2 pipelines: Kafka source → MVs → Iceberg sinks (Lakekeeper + MinIO)
- Kafka output sinks → Redpanda (`casino_real_bet_output`, `casino_turnover_percentage_output`)
- dbt models + Dagster orchestration (`casino_prd_full_job`)
- Grafana dashboard: UC1/UC2 business metrics + Iceberg/Kafka sink health panels (Trino + Prometheus datasources)
- RisingWave-native Iceberg compaction (`connector='iceberg'` + `compaction.trigger_snapshot_count` — no Spark)
- Iceberg tables queried via Trino (`casino_trino_views` Dagster asset creates the Grafana metadata views)

### Known Limitations (RisingWave 2.7.4)

| Limitation | Detail | Status / Workaround |
|-----------|--------|--------------------|
| **Compaction needs `connector='iceberg'` + `trigger_snapshot_count`** | `enable_compaction` is silently ignored on `ENGINE = iceberg` managed tables; and even on `connector='iceberg'` sinks it triggers unreliably without an explicit `compaction.trigger_snapshot_count`. | **Resolved** — pipeline uses `connector='iceberg'` sinks with `compaction.trigger_snapshot_count='5'`. Compaction confirmed working (`replace` ops in the snapshot log). |
| **Snapshot expiration does not prune** | `enable_snapshot_expiration` (and `snapshot_expiration_max_age_millis`/`_retain_last`) are accepted but never prune snapshots with the Lakekeeper REST catalog. Metadata grows unbounded. | Cosmetic — compaction (data files) works, only snapshot *metadata* accumulates. Spark `expire_snapshots` (funnel pipeline's `iceberg_compaction_job`) works if needed. |
| **`commit_checkpoint_interval` is checkpoints, not seconds** | `= 40` means 40 checkpoints × `barrier_interval_ms` (250 ms) = 10 s, not 40 s. | Documented; sinks set to `40` for a 10 s commit cadence. |
| **`ALTER SINK SET` limited** | Cannot change compaction options on a running sink — must DROP and recreate. Iceberg data in Lakekeeper is preserved. | Drop + recreate sinks when changing options. |
| **No watermarks on nested proto sources** | `transaction_created_at` is derived from a nested protobuf field and cannot be used directly as a watermark column. Window state is unbounded. | Promote `transaction_created_at` as a generated column on the source table (requires schema change). |

### What is missing

| PoC Item | Status | Gap |
|----------|--------|-----|
| **Kafka output sinks** | ✅ Done | `sink_casino_real_bet_kafka` + `sink_turnover_percentage_kafka` → Redpanda topics. |
| **R1 — Catch-up** | ✅ Works (untested) | Set `scan.startup.mode='earliest'` + TABLE to replay backlog. Needs a scripted test + documented result. |
| **R2 — Backfill** | ✅ Works (untested) | DROP + recreate (idempotent DDL) handles this. Needs a scripted test + documented result. |
| **R3 — Late-Arriving Data** | ⚠️ Known limitation | No watermarks defined. Windows are unbounded. Needs documented position: current behaviour + what would be needed. |
| **R4 — p95 < 1s latency** | ❌ Missing | Requires Kafka sinks + a latency benchmark using the casino producer against local Redpanda. |
| **V1 — State Failure Recovery** | ❌ Missing | No checkpoint recovery test documented. |
| **V2 — Data Spikes** | ❌ Missing | No burst load test documented. |
| **V3 — Failure Recovery** | ❌ Missing | No end-to-end failure+recovery test documented. |
| **Casino Grafana dashboard** | ✅ Done | `casino-uc-metrics.json` — UC1/UC2 business metrics + Iceberg/Kafka sink health. |
| **PoC results document** | ❌ Missing | No R1–R4 / V1–V3 test results writeup. |

---

## Recommended Next Steps (in order)

### Step 1 — Kafka output sinks for UC1 and UC2

The PoC spec says both UCs must "emit updates to destination Kafka topic in near-real-time." This is a blocker for R4. Add two Kafka sinks pointing at Redpanda (already used by the funnel demo):

**UC1 Kafka sink** — from `mv_casino_real_bet`:
```sql
CREATE SINK sink_casino_real_bet_kafka
FROM mv_casino_real_bet
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'casino_real_bet_output'
)
FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
```

**UC2 Kafka sink** — from `mv_turnover_percentage`:
```sql
CREATE SINK sink_turnover_percentage_kafka
FROM mv_turnover_percentage
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'casino_turnover_percentage_output'
)
FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
```

Add as dbt models (`sink_casino_real_bet_kafka.sql`, `sink_turnover_percentage_kafka.sql`) in `dbt/models/casino_prd/`, tagged `casino_uc1` / `casino_uc2` respectively. Also add to the raw SQL in `sql/casino_prd_funnel_iceberg.sql`.

Redpanda needs to be re-enabled in `docker-compose.yml` (currently commented out). Alternatively, output to a topic on the production Kafka cluster — but local Redpanda is safer and controllable.

**Files:** `dbt/models/casino_prd/`, `sql/casino_prd_funnel_iceberg.sql`, `docker-compose.yml`

---

### Step 2 — R4 Latency benchmark

R4 requires p95 end-to-end latency from Kafka source to Kafka sink < 1 second. With production Kafka as source, we can't control message rate or inject custom timestamps. The benchmark should run against **local Redpanda** using `scripts/produce_protobuf_casino_rounds.py`.

Setup:
1. Point `src_casino_prd` at local Redpanda `casino_rounds` topic instead of prd2
2. Run the casino producer at a sustained TPS (e.g. 100/s, 500/s, 1000/s)
3. Each message includes a `roundCreated` timestamp
4. Measure the time delta from message `roundCreated` timestamp to when the output row appears in `casino_real_bet_output` Kafka topic

The producer already emits realistic nested `CasinoRoundInfoDto` events. A small consumer script needs to be written to consume the output topic and compute p50/p95/p99 latency percentiles.

**Files:** new `scripts/benchmark_latency_r4.py`, update `sql/casino_prd_source.sql` or create a local-Redpanda variant

---

### Step 3 — Casino Grafana dashboard

Add a new Grafana dashboard (`monitoring/grafana/dashboards/casino-uc-metrics.json`) showing:
- UC1: row count in `mv_casino_real_bet` over time, sink throughput (rows/s), latest rolling real bet amounts by top customer
- UC2: row count in `mv_turnover_percentage`, casino vs sportsbook ratio distribution
- Sink health: commit rate for `sink_casino_real_bet` and `sink_turnover_percentage`
- Latency panel (from Step 2 benchmark results or live if producer is running)

Reuse the existing Prometheus scrape config — RisingWave already exposes sink throughput and source lag metrics.

**Files:** `monitoring/grafana/dashboards/casino-uc-metrics.json`

---

### Step 4 — R1 (Catch-up) scripted test + documentation

Demonstrate and document:
1. Start pipeline with an empty RisingWave against a Redpanda topic that already has N messages
2. Measure time to fully catch up (all N messages processed, MV row count stable)
3. Record catch-up throughput (rows/s)

This is already supported by `scan.startup.mode='earliest'` — the test just needs to be run and results recorded.

**Files:** new `scripts/test_r1_catchup.sh`, results section in `docs/POC_RESULTS.md`

---

### Step 5 — R2 (Backfill) scripted test + documentation

Demonstrate:
1. Pipeline is running with N rows in `mv_casino_real_bet`
2. DROP + recreate the MV (idempotent DDL)
3. Measure time to backfill back to N rows
4. Verify row counts and rolling window values are identical before/after

**Files:** `scripts/test_r2_backfill.sh`, results in `docs/POC_RESULTS.md`

---

### Step 6 — R3 (Late-Arriving Data) documentation

RisingWave supports watermarks but they require a top-level `TIMESTAMPTZ` column on the source table — not possible with the current nested protobuf schema where timestamps are `STRUCT<seconds BIGINT, nanos INT>` buried inside arrays. Document:
- Current behaviour: window state is unbounded; late-arriving events within the 14d/90d window are included regardless of arrival order
- What would be needed: promote `transaction_created_at` as a generated column on the source and define a watermark there
- Trade-off: for the casino use case, full inclusion of late events is likely correct behaviour (a late-arriving bet still counts toward the customer's 14-day total)

**Files:** `docs/POC_RESULTS.md`

---

### Step 7 — V1/V2/V3 Resilience tests

**V1 — State Failure Recovery:**
1. Pipeline running with data flowing
2. Kill `compute-node-0`: `docker stop compute-node-0`
3. Restart: `docker start compute-node-0`
4. Verify: row counts in MVs resume from checkpoint, no rows lost, sink lag recovers

**V2 — Data Spikes:**
Using the casino producer: ramp from 100 TPS → 2000 TPS suddenly, observe:
- RisingWave backpressure metrics in Grafana
- No messages dropped (verify via Redpanda consumer offset vs MV row count)
- Latency degradation (measure p95 during spike vs baseline)

**V3 — Failure Recovery:**
1. Kill the entire RisingWave cluster (`docker compose stop`)
2. Restart (`docker compose up -d`)
3. Measure time to full recovery
4. Verify data correctness

**Files:** `scripts/test_v1_state_recovery.sh`, `scripts/test_v2_data_spikes.sh`, `scripts/test_v3_failure_recovery.sh`, results in `docs/POC_RESULTS.md`

---

### Step 8 — PoC results document

Create `docs/POC_RESULTS.md` documenting all test results in the format the PoC deliverables require:

```
# PoC Test Results

## Requirements Phase
R1 Catch-up — PASS/FAIL — [measurement]
R2 Backfill — PASS/FAIL — [measurement]
R3 Late-Arriving Data — PASS/FAIL (with caveats) — [position]
R4 Subsecond Latency — PASS/FAIL — p50/p95/p99 at [TPS]

## Verification Phase
V1 State Failure Recovery — PASS/FAIL — [recovery time, rows lost]
V2 Data Spikes — PASS/FAIL — [max TPS tested, latency at peak]
V3 Failure Recovery — PASS/FAIL — [recovery time]

## Integration Findings
- Kafka connectors (source + sink): protobuf, Avro, JSON
- Schema registry: Apicurio native v2 + ccompat
- Lakehouse integration: Lakekeeper + Iceberg + Trino/Spark
- dbt integration: custom materializations for RisingWave objects
- Dagster orchestration
- ...

## Recommendation
adopt / conditional adopt / reject + rationale
```

---

## Prioritisation

If time is limited, the minimum for a complete PoC demo:
1. **Step 2** — R4 latency benchmark (non-negotiable per PoC) — Kafka sinks already done ✅
2. **Step 8** — Results document (required deliverable)

Steps 3–7 strengthen the PoC but are not blockers if time is constrained.

---

## Version Notes — RisingWave 2.7.4

The pipeline runs on **2.7.4** (downgraded from 2.8.4). On 2.7.4, native Iceberg compaction works via `connector='iceberg'` sinks + `compaction.trigger_snapshot_count` — no Spark needed for compaction. Snapshot **expiration** still doesn't prune (see Known Limitations).

When RisingWave v3.0 stable ships, re-test on it:

1. Change the image tag in `docker-compose.yml`:
   ```yaml
   image: ${RW_IMAGE:-risingwavelabs/risingwave:v3.0.0}
   ```
2. Restart the stack, run `casino_prd_full_job`
3. Check whether **snapshot expiration** now prunes (Trino: `SELECT COUNT(*) FROM datalake.public."rw_managed_casino_real_bet$snapshots"` should stop growing unbounded)
4. If expiration works, the snapshot-metadata-growth limitation can be closed.
