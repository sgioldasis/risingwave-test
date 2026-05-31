# Next Steps to Complete UC1 & UC2 PoC

## Context

The UC1 and UC2 pipelines are functionally implemented (sources, MVs, Iceberg sinks, dbt, Dagster). The PoC scope has two layers:
- **Scoping doc** (`RisingWave_PoC_Document.txt` Part 1): R1–R4 requirements, V1–V3 verifications, UC1–UC3.
- **Statement of Work v2.0** (Part 2, May 2026): a broader set of success criteria (throughput, serving latency, scaling/cost) and an ecosystem interop matrix (Atlan, Databricks, Power BI, incident.io, …).

This document tracks what is remaining, what is complete, known limitations of the current RisingWave version, and (in the **SOW v2.0 Scope Reconciliation** section) how the casino pipeline maps to the fuller SOW criteria.

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

## SOW v2.0 Scope Reconciliation

The May 2026 Statement of Work (Part 2 of `RisingWave_PoC_Document.txt`) is broader
than the original Scoping doc. This maps its success criteria + interop matrix to
what the casino pipeline currently demonstrates.

### Success criteria (SOW §5)

| Criterion | Priority | Status | Evidence / gap |
|-----------|----------|--------|----------------|
| End-to-end latency (MV < 500ms, target <100ms) | Critical | ⚠️ Untested | MVs update in real time; not benchmarked. Tied to R4. |
| Throughput (≥10k events/sec) | Critical | ❌ Untested | Production topics run far below 10k/s; needs a synthetic-load test (casino producer at high TPS on local Redpanda). |
| Query latency / serving (200ms p99 via PG wire) | Critical | ❌ Missing | No serving-latency benchmark against the MVs. |
| Kafka integration (zero-loss, exactly/at-least-once) | Critical | ✅ Demonstrated | Live prd2/prd4 ingestion via `CREATE TABLE` + Kafka output sinks. Loss/semantics not formally measured. |
| Schema Registry (Confluent-compatible, Avro+Protobuf) | Critical | ◑ Partial | Protobuf via Apicurio (FDS on MinIO) proven for casino+bets. Avro + live SR resolution + evolution still to demo. |
| Avro & Protobuf end-to-end + evolution | Critical | ◑ Partial | Protobuf done (nested, JSONB fallback). Avro + schema-evolution test outstanding. |
| Complex data types (nested JSON, arrays, maps, structs) | High | ✅ Demonstrated | Double-UNNEST of nested protobuf structs/arrays in `mv_casino_transactions`. |
| Complex streams (joins, sessionization, late data) | High | ◑ Partial | UNION-ALL pivot + Top-N + sliding windows done. Multi-stream joins/sessionization/late-data not yet. |
| Interop — Iceberg (continuous sink + compaction) | High | ✅ Done | `connector='iceberg'` sinks → Lakekeeper; native compaction working. |
| Interop — dbt (define/deploy RW objects) | High | ✅ Done | `dbt/models/casino_prd/` + custom materializations. |
| Interop — Dagster (assets, trigger, monitor) | High | ✅ Done | `casino_prd_full_job`, asset groups, `casino_trino_views`. |
| Interop — Databricks (Delta read/write) | High | ❌ Missing | Not attempted. |
| Interop — Power BI (PG connector) | High | ❌ Missing | RW is PG-wire compatible; not validated from Power BI. |
| Interop — Atlan (catalog + lineage) | Medium | ❌ Missing | Not attempted. |
| Interop — incident.io (alerts → incidents) | Medium | ❌ Missing | Prometheus/Grafana in place; no incident.io webhook. |
| Scaling — performance (linear w/ nodes) | High | ❌ Missing | No multi-node scaling experiment. |
| Scaling — cost (1x/2x/4x, no cost cliff) | High | ❌ Missing | No cost tracking. |
| Fault recovery (single-node < 60s, no loss) | High | ❌ Missing | Maps to V1/V3 — untested. |
| SQL expressiveness (3 UCs without workarounds) | High | ◑ Partial | UC1 + UC2 expressed in RW SQL. UC3 (betslip recommendation engine) not implemented. |
| Operational overhead (< Spark/Flink) | Medium | ◑ Anecdotal | dbt+Dagster setup is light; not formally compared. |

### Notable scope items beyond the original plan

- **UC3 — Sportsbook Live Trends / Betslip Recommendation Engine** is in the SOW
  but not implemented. High write rate (~1400/s) + co-occurrence/affinity queries —
  a substantially larger build than UC1/UC2.
- **Serving-layer latency** (PG wire point-lookup <10ms p50 / <50ms p99, range scan,
  50 concurrent reads) — a distinct benchmark from R4's Kafka→Kafka latency.
- **Throughput + scaling + cost** experiments need a synthetic load harness; the live
  production topics alone won't exercise ≥10k/s or multi-node scaling.

---

## Recommended Next Steps (in order)

### Step 1 — Kafka output sinks for UC1 and UC2 — ✅ DONE

`sink_casino_real_bet_kafka` (→ `casino_real_bet_output`) and `sink_turnover_percentage_kafka` (→ `casino_turnover_percentage_output`) are implemented as dbt models in `dbt/models/casino_prd/`, in the raw SQL (`sql/casino_prd_funnel_iceberg.sql`), and Redpanda is enabled in `docker-compose.yml`. See PRODUCTION_CASINO_DEMO.md §4.5 / §5.8. They remain the prerequisite for the R4 benchmark below.

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

### Step 3 — Casino Grafana dashboard — ✅ DONE

`monitoring/grafana/dashboards/casino-uc-metrics.json` is built: UC1/UC2 business metrics (customers, volumes, top-customer tables), Iceberg sink health (snapshot count + operations/min via Trino, commits/min, write throughput), and source ingestion. Uses the Trino + Prometheus + RisingWave-SQL datasources. The `casino_trino_views` Dagster asset provisions the dollar-free metadata views it queries.

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

For the original Scoping-doc demo (UC1/UC2 + R/V), the minimum is:
1. **Step 2** — R4 latency benchmark (non-negotiable per PoC) — Kafka sinks already done ✅
2. **Step 8** — Results document (required deliverable)

Steps 3–7 strengthen that but are not blockers if time is constrained.

For the broader **SOW v2.0**, the highest-value additions (see reconciliation table) are:
1. **Throughput + R4 latency** via a synthetic-load harness (casino producer at high TPS on local Redpanda) — covers two Critical criteria at once.
2. **Serving-layer latency** benchmark (PG-wire point-lookup / range-scan / concurrent reads against the MVs).
3. **Avro + schema-evolution** demo (Protobuf already covered) to close the Critical Schema Registry criterion.
4. **Interop quick wins**: Power BI via PG connector, Databricks Delta — both Medium/High and low-effort connectivity checks.
UC3 (betslip recommendation engine) is a large separate build — scope it only if explicitly required.

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
