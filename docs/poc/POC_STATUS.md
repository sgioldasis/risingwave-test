# RisingWave PoC — Status Report

**Last updated:** 2026-06-16 (Week 4)
**Source document:** [RisingWave_PoC_Document](https://docs.google.com/document/d/1Ia2_8v1pn1Syj0xUBLHLzAv4rxSGLDmtUhKTA-MxYlI)
**Tracking sheet:** [Project tracker](https://docs.google.com/spreadsheets/d/1cmLFZ_emV7xOsNv43D_lT5hNI5F6p8vwkNb1pdRmKHE/edit?gid=1924086747#gid=1924086747)

> **Note:** The tracking spreadsheet has several statuses that are behind our actual progress. Items marked with ⚠️ below differ from the sheet.

---

## Phase Overview

| Phase | Focus | Status |
|-------|-------|--------|
| **Phase 0** | Environment setup, SOW sign-off | ⚠️ Mostly done — OpenShift deployment still in progress |
| **Phase 1** | Core Kafka, latency/throughput, Schema Registry, data types | ⚠️ Mostly done — throughput baseline, Avro formal test, schema evolution pending |
| **Phase 2** | Complex streams: joins, windows, late data | ❌ Not started |
| **Phase 3** | Ecosystem: Databricks, Iceberg, dbt, Dagster, Power BI | ⚠️ Mostly done — Power BI, Atlan, incident.io pending |
| **Phase 4** | Scaling experiments, cost analysis, serving layer, fault recovery | 🔄 **Current week** — not started |
| **Phase 5** | Results compilation, final report, go/no-go | ❌ Not started |

---

## Phase 0 — Setup

| Action Item | Status | Notes |
|-------------|--------|-------|
| Disk cache benchmark data from RisingWave | ✅ Done | |
| Storage options / ADLS Gen2 documentation | ✅ Done | |
| Apicurio / Red Hat Schema Registry compatibility | ✅ Done | Not fully compatible; workaround found (not a blocker). Internal alignment needed on which registry to use. |
| ADLS Gen2 customer references from RisingWave | 🔄 In Progress | |
| RisingWave Console links | ✅ Done | |
| Helm chart + sizing recommendations for OpenShift | ✅ Done | |
| Terry & Yu Li added to Kaizen Slack | ✅ Done | |
| SOW sign-off by all stakeholders | ❌ Pending | |
| Deploy RisingWave on OpenShift via Helm | 🔄 In Progress | Triantafyllos + RW team started |

---

## Phase 1 — Core Kafka & Data Formats

| Action Item | Status | Notes |
|-------------|--------|-------|
| Configure Kafka source connector | ✅ Done | ⚠️ Sheet says "In Progress". Production Kafka (prd2 SSL, prd4 SSL) working. Staging cluster (`stg-ocp-kfk01`, SASL_SSL/SCRAM-SHA-512) also verified — both topics present. |
| Throughput baseline (≥10k ev/s, 30-min run) | ❌ Not started | Formal test not run. Production ingesting at ~1,500 msg/s peak. |
| Schema Registry integration (Confluent-compatible) | ✅ Done | ⚠️ Sheet says "In Progress". Tested with Apicurio (proto→Avro→Redpanda SR). Internal alignment still needed on which registry to standardise on. |
| Avro end-to-end ingestion | ✅ Done | ⚠️ Sheet says "Not Started". Full round-trip: `src_casino_prd` → `sink_casino_avro_redpanda` → `casino_out_avro` topic → `src_casino_avro` → `mv_casino_transactions_full`. |
| Protobuf end-to-end ingestion (proto3, nested) | ✅ Done | ⚠️ Sheet says "In Progress". Both `CasinoRoundInfoDto` and `PandoraBetInfoVm` (with self-referential type stored as JSONB) working in production. |
| Schema evolution scenarios | ❌ Not started | Approach not yet designed. |
| Complex data types (nested JSON, arrays, maps, structs) | ✅ Done | Nested protobuf structs including arrays-of-structs (`Messages[]`, `Transactions[]`) UNNESTed in MVs. |

---

## Phase 2 — Complex Streams

| Action Item | Status | Notes |
|-------------|--------|-------|
| Multi-stream join (3+ streams) | ❌ Not started | |
| Temporal / as-of join | ❌ Not started | |
| Tumbling window aggregation (1-min) | ❌ Not started | 5-min rolling window implemented for UC1/UC2 but no formal tumbling window test. |
| Sliding window aggregation (5-min) | ✅ Done | `mv_casino_real_bet` and `mv_casino_turnover_90d` use `RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW` over `transaction_created_at`. |
| Session window (30s and 5-min inactivity) | ❌ Not started | |
| Late & out-of-order data handling | ❌ Not started | Watermarks implemented; no formal injection test. |

---

## Phase 3 — Ecosystem Interoperability

| Action Item | Status | Notes |
|-------------|--------|-------|
| Databricks — bidirectional data exchange | ✅ Done | ⚠️ Sheet says "Not Started". Append-only Iceberg sinks to UC working (`rw_casino_transactions`, `rw_sportsbook_bets`, `rw_casino_turnover_90d`). Azure AD OAuth2 + explicit ADLS Gen2 account key. Trino federation for reads. See `DATABRICKS_ICEBERG_SINK.md`. |
| Apache Iceberg sink — continuous ingest + compaction | ✅ Done | ⚠️ Sheet says "In Progress". Lakekeeper REST catalog + MinIO. Upsert sinks working. Compaction working. Snapshot expiration broken (RW ≤ 2.8). |
| dbt integration | ✅ Done | ⚠️ Sheet says "In Progress". All MVs, sources, and sinks defined as dbt models using `dbt-risingwave` adapter. |
| Dagster orchestration | ✅ Done | Full pipeline orchestrated: proto compile, MinIO upload, Avro schema registration, source creation, Databricks view, Trino views. |
| Power BI — DirectQuery via PostgreSQL connector | ❌ Not started | Easiest next step — no new infrastructure needed. |
| Atlan — catalog discovery and lineage | ❌ Not started | Low priority. |
| incident.io — alert routing | ❌ Not started | Low priority. |

---

## Phase 4 — Scaling, Serving Layer & Fault Recovery ← Current Week

| Action Item | Status | Notes |
|-------------|--------|-------|
| Serving layer — PG wire point lookup (p50 <10ms, p99 <50ms) | ❌ Not started | |
| Serving layer — PG wire range scan (p50 <50ms, p99 <200ms) | ❌ Not started | |
| Kafka sink lag (p50 <500ms, p99 <1s) | ✅ Done | ⚠️ Sheet says "Not Started". Confirmed sub-second source-to-sink on production data at ~1,500 msg/s via Redpanda output topics. |
| Concurrent read load test (50 connections, p50 <50ms, p99 <300ms) | ❌ Not started | |
| Scaling experiments (1×/2×/4× compute nodes at 50k/100k/200k ev/s) | ❌ Not started | Requires OpenShift deployment. |
| Cost analysis (TCO vs Spark Streaming) | ❌ Not started | |
| Fault recovery — single node failure (<60s, no data loss) | ❌ Not started | Local Mac testing not meaningful (DatabaseFailureIsolation disabled). Requires OpenShift deployment. |

---

## Phase 5 — Report

| Action Item | Status |
|-------------|--------|
| Compile all test results against success criteria | ❌ Not started |
| Draft final PoC report | ❌ Not started |
| Go / No-Go decision meeting | ❌ Not started |

---

## Use Cases

| ID | Name | Status | Notes |
|----|------|--------|-------|
| **UC1** | Casino Real Bet Amount | ✅ Done | 5-min rolling SUM per customer/currency. Kafka + Lakekeeper + Databricks sinks. Grafana dashboard. |
| **UC2** | Casino Turnover Percentage | ✅ Done | UNION ALL + GROUP BY pivot. Same sink stack. Databricks `QUALIFY ROW_NUMBER()` VIEW as upsert workaround. |
| **UC3** | Sportsbook Live Trends / Betslip Recommendation | ❌ Not started | High cardinality (~55k selections × 70 timeframe buckets). Different data model. |
| **UC4** | Game Category Popularity Ranking | ❌ Not started | Join of `Game.Launch` + `cronus.casino.out`. High write rate (~500k tx/min). |

---

## Success Criteria — Current Results

| Criterion | Threshold | Priority | Result |
|-----------|-----------|----------|--------|
| End-to-end latency | <500ms (target <100ms) | Critical | ✅ Confirmed sub-second on production data |
| Throughput | ≥10,000 ev/s | Critical | ❌ Not formally tested |
| PG wire — point lookup | p50 <10ms, p99 <50ms | Critical | ❌ Not tested |
| PG wire — range scan | p50 <50ms, p99 <200ms | Critical | ❌ Not tested |
| Schema Registry | Avro + Protobuf auto-resolved | Critical | ✅ Working (Apicurio + Redpanda SR) |
| Kafka sink lag | p50 <500ms, p99 <1s | High | ✅ Confirmed on production data |
| Concurrent reads (50 conns) | p50 <50ms, p99 <300ms | High | ❌ Not tested |
| Fault recovery — single node | <60s, no data loss | High | ❌ Not tested (requires OpenShift) |
| Scaling | Linear+ throughput as nodes added | High | ❌ Not tested |
| Complex data types | Nested JSON ≥3 levels, arrays, maps, structs | High | ✅ Validated with production protobuf schemas |
| Databricks interop | Bidirectional read/write | High | ⚠️ Write (append-only) ✅. Read from Databricks into RW ❌ not tested. |
| Apache Iceberg sink | Continuous ingest + compaction | High | ✅ Done (Lakekeeper) |
| dbt integration | dbt models deploy to RisingWave | High | ✅ Done |

---

## Key Findings

### What Works Well
- Protobuf ingestion via `schema.location` (compiled `.pb`/`.desc` in MinIO) reliable for complex nested schemas.
- Avro round-trip via Redpanda Schema Registry works end-to-end.
- Lakekeeper upsert sinks work natively — best target for rolling-window MVs.
- dbt + Dagster integration clean and reproducible.
- R4 subsecond latency confirmed on production data.

### Limitations & Workarounds

| Finding | Impact | Workaround | Reference |
|---------|--------|------------|-----------|
| Unity Catalog rejects Iceberg delete files | Upsert sinks stall silently | Append-only + read-side `QUALIFY ROW_NUMBER()` VIEW | `DATABRICKS_ICEBERG_SINK.md §16` |
| ADLS Gen2 not supported by OpenDAL (RW ≤ 2.8.4) | `vended_credentials` and S3FileIO both fail | Explicit `adlsgen2.account_key` | `DATABRICKS_ICEBERG_SINK.md §7` |
| Iceberg snapshot expiration broken with REST catalog | Snapshots accumulate indefinitely | Manual `expire_snapshots` / `VACUUM` | `DATABRICKS_ICEBERG_SINK.md §11` |
| UNNEST blocks `TUMBLE` + `EMIT ON WINDOW CLOSE` (v2.7.4) | Windowed aggregation on nested arrays not possible | Fixed in v2.8 | Memory: `reference_rw_unnest_watermark` |
| `DatabaseFailureIsolation` disabled (local Mac license) | Single job failure resets full DB | Retry-wrapped DDL; not an issue in proper deployment | `PRODUCTION_CASINO_DEMO.md §6` |
