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
- **Databricks Unity Catalog**: append-only Iceberg sinks working (`rw_casino_transactions`, `rw_sportsbook_bets`, `rw_casino_turnover_90d`); Azure AD OAuth2 + explicit ADLS Gen2 account key; Trino federation for reads; `QUALIFY ROW_NUMBER()` VIEW workaround for upsert semantics (UC rejects delete files — see `DATABRICKS_ICEBERG_SINK.md §16`)
- **Staging Kafka verified**: `stg-ocp-kfk01-bootstrap.kaizengaming.net:9096` SASL_SSL/SCRAM-SHA-512 confirmed; both `cronus.casino.out.br` and `bets-out-br` topics present
- **SASL support in SQL**: `sql/casino_prd_source.sql` and `sql/casino_prd_bets_source.sql` use `\if :USE_SASL` conditional; `.env`-driven via `KAFKA_CASINO_BOOTSTRAP`, `KAFKA_BETS_BOOTSTRAP`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`
- **R4 latency**: sub-second source-to-sink confirmed on production data (~1,500 msg/s); formal benchmark at ≥10k/s still outstanding

### Known Limitations (RisingWave 3.0.0)

| Limitation | Detail | Status / Workaround |
|-----------|--------|--------------------|
| **Snapshot expiration unverified** | `enable_snapshot_expiration = 'true'` is set on both Iceberg sinks. Was broken with the Lakekeeper REST catalog in ≤ v2.8.0 (snapshots accumulated unbounded). In v3.0.0 the option is accepted — **not yet verified** whether pruning actually occurs. | Monitor with `SELECT COUNT(*) FROM datalake.public."rw_managed_casino_real_bet$snapshots"` over time. If count keeps growing, fall back to Spark `expire_snapshots`. |
| **`commit_checkpoint_interval` is checkpoints, not seconds** | `= 5` on `sink_turnover_percentage` means 5 × 250 ms barrier = ~1.25 s commit cadence. `= 20` on `sink_casino_real_bet` = ~5 s. The unit is checkpoints, not wall-clock seconds. | Documented; current values intentional. Adjust `barrier_interval_ms` in `risingwave.toml` if cadence needs changing. |
| **`ALTER SINK SET` limited** | Cannot change compaction or expiration options on a running Iceberg sink — must DROP and recreate. Iceberg data in Lakekeeper is preserved across the drop. | Drop + recreate sinks when changing options. |
| **No watermarks on nested proto source directly** | `transaction_created_at` is buried inside a nested protobuf struct — cannot be used as a watermark column on `src_casino_prd` directly. | **Resolved via Kafka round-trip**: `mv_casino_transactions` flattens and sinks to `casino_bets_flat` Redpanda topic; `src_casino_bets_flat` re-ingests as a TABLE with `WATERMARK FOR transaction_created_at`. Sliding-window MVs read from the re-ingested table. |
| **UNNEST blocks `TUMBLE` + `EMIT ON WINDOW CLOSE`** | In ≤ v2.7.4, `TUMBLE` over an `UNNEST`-ed nested source was blocked. | **Fixed in v2.8** — resolved in our v3.0.0 stack. |

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
| End-to-end latency (MV < 500ms, target <100ms) | Critical | ⚠️ Partial | Sub-second confirmed on production at ~1,500 msg/s. Formal benchmark at ≥10k/s still outstanding. |
| Throughput (≥10k events/sec) | Critical | ❌ Untested | Production topics run far below 10k/s; needs a synthetic-load test (casino producer at high TPS on local Redpanda). |
| Query latency / serving (200ms p99 via PG wire) | Critical | ❌ Missing | No serving-latency benchmark against the MVs. |
| Kafka integration (zero-loss, exactly/at-least-once) | Critical | ✅ Demonstrated | Live prd2/prd4 (SSL) + staging (SASL_SSL/SCRAM-SHA-512) ingestion via `CREATE TABLE` + Kafka output sinks. Loss/semantics not formally measured. |
| Schema Registry (Confluent-compatible, Avro+Protobuf) | Critical | ◑ Partial | Protobuf via Apicurio (FDS on MinIO) proven for casino+bets. Avro round-trip (`src_casino_avro`) working. Schema evolution test outstanding. |
| Avro & Protobuf end-to-end + evolution | Critical | ◑ Partial | Protobuf done (nested, JSONB fallback). Avro end-to-end done. Schema evolution scenario not yet tested. |
| Complex data types (nested JSON, arrays, maps, structs) | High | ✅ Demonstrated | Double-UNNEST of nested protobuf structs/arrays in `mv_casino_transactions`. |
| Complex streams (joins, sessionization, late data) | High | ◑ Partial | UNION-ALL pivot + Top-N + sliding windows done. Multi-stream joins/sessionization/late-data not yet. |
| Interop — Iceberg (continuous sink + compaction) | High | ✅ Done | `connector='iceberg'` sinks → Lakekeeper; native compaction working. |
| Interop — dbt (define/deploy RW objects) | High | ✅ Done | `dbt/models/casino_prd/` + custom materializations. |
| Interop — Dagster (assets, trigger, monitor) | High | ✅ Done | `casino_prd_full_job`, asset groups, `casino_trino_views`. |
| Interop — Databricks (Iceberg read/write) | High | ⚠️ Partial | Append-only Iceberg sinks to UC working. Upsert blocked (UC rejects delete files). Read from Databricks into RW not tested. See `DATABRICKS_ICEBERG_SINK.md §16–18`. |
| Interop — Power BI (PG connector) | High | ❌ Missing | RW is PG-wire compatible; not validated from Power BI. |
| Interop — Atlan (catalog + lineage) | Medium | ❌ Missing | See Step 9 below. Two paths: PG connector + dbt lineage import. |
| Interop — incident.io (alerts → incidents) | Medium | ❌ Missing | Prometheus/Grafana in place. See Step 10 below. |
| Scaling — performance (linear w/ nodes) | High | ❌ Missing | No multi-node scaling experiment. Requires OpenShift deployment. |
| Scaling — cost (1x/2x/4x, no cost cliff) | High | ❌ Missing | No cost tracking. |
| Fault recovery (single-node < 60s, no loss) | High | ❌ Missing | Maps to V1/V3 — untested. Local Mac not meaningful (`DatabaseFailureIsolation` disabled). |
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

### Step 9 — Atlan catalog + lineage

Atlan has two practical integration paths; the dbt route is lower-risk and higher-value.

**Path A — dbt lineage import (recommended first)**

1. Generate dbt docs from the casino pipeline:
   ```bash
   cd dbt
   dbt docs generate   # produces target/manifest.json + target/catalog.json
   ```
2. In Atlan, configure a dbt integration pointing at those JSON artifacts (upload directly or via S3/GCS).
3. Verify Atlan surfaces the full lineage graph: Kafka source → dbt model (source) → MV → Iceberg sink → Trino view.
4. Check column-level lineage if supported (Atlan can often derive it from dbt's column tests and refs).

**Path B — PostgreSQL connector**

1. In Atlan, add a new connection: PostgreSQL connector, `host:4566`, database `dev`, user `root` (no password).
2. Run a metadata crawl — Atlan should discover `src_casino_prd`, `mv_casino_real_bet`, `mv_turnover_percentage`, sinks, and any tables.
3. Verify RisingWave-specific object types (materialized views, sinks) appear correctly vs being misclassified as standard tables/views.

**What we need:** Access to a Kaizen Atlan workspace. Path A requires no Atlan connector compatibility — just JSON files.

---

### Step 10 — incident.io alert routing

The infrastructure (Prometheus + Grafana) is already running. The integration adds a Grafana contact point that fires an incident.io incident when a RisingWave alert triggers.

**Setup:**

1. In incident.io, create an inbound webhook (Settings → Integrations → Inbound webhooks). Copy the webhook URL.
2. In Grafana (http://localhost:3000), add a Contact Point:
   - Type: **Webhook**
   - URL: the incident.io webhook URL
   - Method: POST
3. Create an Alert Rule against one of the existing panels, for example:
   - **"Casino ingestion stopped"**: `increase(rw_source_rows_received_total[2m]) == 0` (Prometheus metric from the RW meta node on `:1250`)
   - Or simpler: a Grafana SQL alert on `SELECT COUNT(*) FROM mv_casino_transactions` returning 0
4. Assign the Contact Point to the alert rule.
5. Test: use Grafana's **"Test"** button on the contact point to fire a synthetic alert, then verify an incident appears in incident.io.
6. Optionally test a real condition: pause the Kafka source (`ALTER TABLE src_casino_prd SET PARALLELISM = 0` or just stop Redpanda), wait for the alert to fire.

**What we need:** A Kaizen incident.io workspace with permission to create inbound webhooks. Grafana is already provisioned at `http://localhost:3000`.

---

## Prioritisation

For the original Scoping-doc demo (UC1/UC2 + R/V), the minimum is:
1. **Step 2** — R4 latency benchmark (non-negotiable per PoC) — Kafka sinks already done ✅
2. **Step 8** — Results document (required deliverable)

Steps 3–7 strengthen that but are not blockers if time is constrained.

For the broader **SOW v2.0**, the highest-value additions (see reconciliation table) are:
1. **Throughput + R4 latency** via a synthetic-load harness (casino producer at high TPS on local Redpanda) — covers two Critical criteria at once.
2. **Serving-layer latency** benchmark (PG-wire point-lookup / range-scan / concurrent reads against the MVs).
3. **Avro + schema-evolution** demo (Protobuf + Avro ingestion already done) to close the Critical Schema Registry criterion.
4. **Interop quick wins** (no new infrastructure needed):
   - **Power BI** via PG connector — connect to `localhost:4566`, open `mv_casino_real_bet`
   - **Atlan** — dbt `manifest.json` upload (Step 9 Path A); requires Atlan workspace access
   - **incident.io** — Grafana webhook contact point (Step 10); requires incident.io webhook URL
5. **Databricks read path** — test reading from Unity Catalog back into RisingWave to close the "bidirectional" criterion.
UC3 (betslip recommendation engine) is a large separate build — scope it only if explicitly required.

---

## Version Notes — RisingWave 3.0.0

The pipeline runs on **v3.0.0** (`risingwavelabs/risingwave:v3.0.0` in `docker-compose.yml`).

**Changes from 2.7.4:**
- `enable_compaction = 'true'` + `compaction_interval_sec` + `force_compaction = 'true'` is the working compaction pattern (replacing the `compaction.trigger_snapshot_count`-only approach needed in 2.7.4). Both options are set in the current sinks for belt-and-suspenders.
- `TUMBLE` + `EMIT ON WINDOW CLOSE` over `UNNEST`-ed sources works (fixed in v2.8).
- `enable_snapshot_expiration = 'true'` is set on both Iceberg sinks — **not yet confirmed** whether pruning occurs with the Lakekeeper REST catalog. Monitor and verify.

**Outstanding re-test item:**
Verify snapshot expiration is now pruning in v3.0.0:
```sql
-- Run via Trino; count should plateau rather than grow indefinitely
SELECT COUNT(*) FROM datalake.public."rw_managed_casino_real_bet$snapshots";
SELECT COUNT(*) FROM datalake.public."rw_managed_turnover_percentage$snapshots";
```
If counts are still growing after 24h of operation, fall back to Spark `expire_snapshots` or the `iceberg_compaction_job` Dagster asset.
