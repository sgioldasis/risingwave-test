# RisingWave PoC — Status Report

**Last updated:** 2026-06-16
**Source document:** [RisingWave_PoC_Document](https://docs.google.com/document/d/1Ia2_8v1pn1Syj0xUBLHLzAv4rxSGLDmtUhKTA-MxYlI)
**Tracking sheet:** [Project tracker](https://docs.google.com/spreadsheets/d/1cmLFZ_emV7xOsNv43D_lT5hNI5F6p8vwkNb1pdRmKHE/edit?gid=1924086747#gid=1924086747)

---

## Use Cases

| ID | Name | Status | Notes |
|----|------|--------|-------|
| **UC1** | Casino Real Bet Amount | ✅ Done | 5-min rolling SUM per customer/currency. Kafka sink (Redpanda), Lakekeeper upsert, Databricks append-only, Grafana dashboard. |
| **UC2** | Casino Turnover Percentage | ✅ Done | UNION ALL + GROUP BY pivot across casino + sportsbook turnover. Same sink stack as UC1. Databricks read-side `QUALIFY ROW_NUMBER()` VIEW as upsert workaround. |
| **UC3** | Sportsbook Live Trends / Betslip Recommendation | ❌ Not started | Different data model (BetBuilders, accumulators, co-occurrence). High cardinality (~55k selections × 70 timeframe buckets). |
| **UC4** | Game Category Popularity Ranking | ❌ Not started | Requires join of `Game.Launch` + `cronus.casino.out` topics. High write rate (~500k tx/min). |

---

## Requirements Phase (R1–R4)

| ID | Requirement | Status | Notes |
|----|-------------|--------|-------|
| **R1** | Catch-up | ✅ Validated | `scan.startup.mode = 'earliest'` replays full topic backlog. Confirmed with production data. |
| **R2** | Backfill | ✅ Validated | `BACKGROUND_DDL` for large MV creation. Confirmed during pipeline setup. |
| **R3** | Late-Arriving Data | ⚠️ Partial | Watermarks implemented on `src_casino_bets_flat`. UNNEST watermark limitation hit in v2.7.4 (fixed in v2.8). No formal out-of-order injection test run. |
| **R4** | Subsecond Latency | ✅ Validated | Kafka source → Kafka sink (Redpanda output topics) confirmed sub-second on production data at ~1,500 msg/s peak. |

---

## Verification Phase (V1–V3)

| ID | Requirement | Status | Notes |
|----|-------------|--------|-------|
| **V1** | State Failure Recovery | ⚠️ Not formally tested | Known issue: `DatabaseFailureIsolation` disabled on local Mac (license limit — more CPU cores than allowed). Single streaming job failure causes full DB reset. Retry-wrapped DDL path exists as mitigation. Not representative of a proper deployment. |
| **V2** | Data Spikes | ❌ Not tested | No burst/spike test run. Production peak is ~1,500 msg/s; no synthetic spike above that attempted. |
| **V3** | Failure Recovery | ⚠️ Not formally tested | Pipeline recovery tested informally. No timed benchmark against the "recovery within seconds" criterion. |

---

## Ecosystem Interoperability

| Tool | Priority (SOW) | Status | Notes |
|------|---------------|--------|-------|
| **Kafka** | Critical | ✅ Done | Production Kafka prd2 (SSL, `cronus.casino.out.br`) and prd4 (SSL, `bets-out-br`). Redpanda for local output topics. Staging cluster (`stg-ocp-kfk01`) verified — both topics present. |
| **Apache Iceberg** | Medium | ✅ Done | Lakekeeper REST catalog + MinIO. Upsert sinks working. Compaction working. Snapshot expiration broken with REST catalog (RW ≤ 2.8). |
| **Databricks** | High | ✅ Done | Append-only Iceberg sinks via Unity Catalog IRC. Azure AD OAuth2 + explicit ADLS Gen2 account key. Trino federation for reads. |
| **dbt** | High | ✅ Done | `dbt-risingwave` adapter. All MVs, sources, and sinks defined as dbt models. |
| **Dagster** | Medium | ✅ Done | Full orchestration: proto compile, MinIO upload, Avro schema registration, source creation, Databricks view, Trino views. |
| **Prometheus / Grafana** | — | ✅ Done | Grafana dashboards for UC1 + UC2 metrics, Iceberg snapshots, row counts. |
| **Trino** | — | ✅ Done | Federated queries across Lakekeeper (`datalake` catalog) and Databricks (`databricks` catalog). |
| **Power BI** | High | ❌ Not tested | PostgreSQL connector path exists; no test run performed. |
| **Atlan** | Low | ❌ Not tested | — |
| **incident.io** | Low | ❌ Not tested | — |

---

## Key Findings

### What Works Well
- **Protobuf ingestion** via `schema.location` (compiled `.pb`/`.desc` stored in MinIO) is reliable for complex nested schemas including self-referential types (stored as JSONB).
- **Avro round-trip** via Redpanda Schema Registry works end-to-end (`sink → topic → source`).
- **Lakekeeper upsert sinks** work natively — the right target for rolling-window MVs that continuously update rows.
- **dbt + Dagster integration** works well; the full pipeline can be orchestrated and reproduced cleanly.
- **R4 subsecond latency** confirmed on production data via Kafka output sinks.

### Limitations & Workarounds

| Finding | Impact | Workaround | Detail |
|---------|--------|------------|--------|
| Unity Catalog rejects Iceberg delete files | Upsert sinks to UC stall silently | Append-only sink + read-side `QUALIFY ROW_NUMBER()` VIEW | `DATABRICKS_ICEBERG_SINK.md §16` |
| ADLS Gen2 not supported by OpenDAL (RW ≤ 2.8.4) | `vended_credentials` and S3FileIO both fail for Azure storage | Explicit `adlsgen2.account_key` bypassing credential vending | `DATABRICKS_ICEBERG_SINK.md §7` |
| Iceberg snapshot expiration broken with REST catalog | Snapshots accumulate indefinitely on Lakekeeper | Manual `expire_snapshots` / `VACUUM` | `DATABRICKS_ICEBERG_SINK.md §11` |
| UNNEST blocks `TUMBLE` + `EMIT ON WINDOW CLOSE` (v2.7.4) | Windowed aggregation on nested arrays not possible | Fixed in v2.8 | `reference_rw_unnest_watermark` |
| `DatabaseFailureIsolation` disabled (local Mac license) | Single job failure resets full DB | Retry-wrapped DDL; not an issue in proper deployment | `PRODUCTION_CASINO_DEMO.md §6` |

---

## Pending

### Immediate
- [ ] **UC3** — Sportsbook Live Trends / Betslip Recommendation Engine
- [ ] **UC4** — Game Category Popularity Ranking

### Verification Tests
- [ ] **V1** — Formal state failure recovery test (requires properly licensed deployment)
- [ ] **V2** — Data spike test (synthetic burst above 1,500 msg/s)
- [ ] **V3** — Timed failure recovery benchmark

### Integrations
- [ ] **Power BI** — DirectQuery via PostgreSQL connector; latency benchmarks (§11 of SOW)
- [ ] **Atlan** — Metadata discovery and lineage
- [ ] **incident.io** — Alert routing via Prometheus

### Performance & Cost
- [ ] Scaling experiments: 1× / 2× / 4× compute nodes at 50k / 100k / 200k events/sec
- [ ] Cost analysis: TCO vs equivalent Spark Streaming cluster
- [ ] Formal serving layer latency benchmarks (point lookups, range scans, concurrent reads)

### Infrastructure
- [ ] Production deployment on OpenShift with object storage
- [ ] Staging Kafka (`stg-ocp-kfk01`) end-to-end pipeline test (topics confirmed available; credentials in `.env`)
