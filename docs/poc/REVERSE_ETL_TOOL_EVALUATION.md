# Reverse-ETL tool evaluation: why a custom Dagster component (APR-233)

Decision record for the tool candidates named in the [APR-233] epic ("Explore &
Standardize Batch Reverse ETL"), evaluated against the working pipeline in this
repository (`ReverseEtlCdfSync` — Databricks CDF → Kafka → RisingWave/Postgres).
Written 2026-10-15.

Related: [REVERSE_ETL_CDF_POC_PLAN.md](REVERSE_ETL_CDF_POC_PLAN.md) (design and
lessons), [REVERSE_ETL_DEBEZIUM_JDBC_SINK.md](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md)
(sink and costs), [REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md)
(zero-DBU read path).

## 1. The candidates named in the epic

The epic's constraint: "avoid adding SaaS cost (Hightouch, Census, RudderStack,
etc.) for a need that open-source tooling can likely satisfy", with Dagster
already the committed orchestration layer. Two open-source candidates were named.

### drt (drt-hub/drt)

The epic's closest fit on paper ("reverse ETL for the code-first data stack,
community-maintained Dagster integration"). Ruled out before this evaluation, in
[REVERSE_ETL_CDF_POC_PLAN.md](REVERSE_ETL_CDF_POC_PLAN.md) and confirmed by a
live side-by-side demo in
[REVERSE_ETL_DRT_COMPARISON.md](REVERSE_ETL_DRT_COMPARISON.md) (drt 1.0.0): by
its own architecture (ADR 0004) **drt has no Kafka destination and deliberately
does not do CDC-based extraction** — both are the core of this pattern. Its demo
sync reads the source with a `SELECT *` (SQL warehouse, DBUs per sync) and
mirrors current state into Postgres only — no Kafka, no change feed, no
RisingWave.

### Commercial SaaS (RudderStack, Hightouch, Census)

Excluded on the epic's own cost grounds. Independent of price, a SaaS reverse-ETL
tool would still need to read the Databricks tables somehow (a warehouse
connection — DBUs per sync — or the same direct-to-storage credential setup this
repo already uses), so it would add cost without removing any engineering.

### RisingWave native sinks

Out of scope by the epic: real-time/streaming activation is already covered by
RisingWave's sink connectors (Kafka, Postgres, Redis, Elasticsearch, ... with
sub-second latency). The batch pattern here and the streaming pattern there are
complementary, not competing.

## 2. dlt (dlthub) — the remaining candidate

The epic positioned dlt as the fallback: "not reverse-ETL-specific, but supports
custom destination functions; could be used to hand-roll Kafka sink pipelines
orchestrated by Dagster if drt proves too immature or its connector catalog too
thin."

What dlt offers as of 2026-10-15 (verified against the project README and docs):

- **20+ destinations** (Snowflake, BigQuery, Postgres, Databricks, filesystem,
  Iceberg, Delta, ...) and explicit support for **custom reverse-ETL
  destinations** — a supported extension point.
- **No Kafka destination**, and upstream states new destinations are unlikely to
  be merged ("due to high maintenance cost") — a Kafka destination is a
  permanent, never-merged custom module.
- **Sources**: REST APIs, SQL databases (SQLAlchemy), filesystem, DataFrames /
  Arrow. **No Databricks CDF source.** The `sql_database` source against
  Databricks reads through a SQL warehouse — the per-sync DBU cost this POC just
  eliminated (see [REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md)). Our OSS Delta read only fits
  as a custom DataFrame resource.
- **Incremental loading** with dlt-managed state, kept in dlt's own state store.
- A **Dagster integration** (dagster-dlt) exposing pipelines as assets.

### Fit against the working pipeline

| Pipeline concern                    | dlt gives                          | Still hand-built |
| ----------------------------------- | ---------------------------------- | ---------------- |
| Read the CDF                        | nothing (no CDF source)            | all of it        |
| Produce to Kafka                    | nothing (custom destination)       | all of it        |
| Debezium envelopes (`c`/`u`/`d`, before/after, preimage+postimage join) | nothing — dlt's model is row-level merge/append and has **no change-op concept** | all of it |
| Avro + schema registry              | nothing                            | all of it        |
| Watermark                           | its own state store, conflicting with the shared Databricks watermark table used by both the Dagster asset and the notebook paths | reconciliation glue |
| Column evolution to RisingWave/Postgres, sink health checks | outside its scope | all of it |
| Dagster lineage                     | yes, via dagster-dlt               | — the existing assets already carry richer lineage, checks and run metadata |

The deepest mismatch is semantic: dlt loads *rows* with upsert/merge semantics;
reverse ETL must transmit *change events* — including deletes as events and
updates as one joined before/after pair. That shaping is the heart of this
pipeline and cannot be delegated.

### Where dlt would be the right tool

- A destination that is a **direct analytics load** (Databricks → Snowflake /
  Postgres, bypassing Kafka): dlt's `databricks` source + incremental merge is a
  mature path there.
- Deployments that want many SQL/analytics destinations and are indifferent to
  change-event semantics.

Neither is this epic's shape: activation goes through Kafka, and every other
activation destination already consumes the same topic (RisingWave, Debezium
JDBC sink) without any loader library.

## 3. Decision

Build and keep the custom Dagster component. By the epic's own fallback criteria
("if drt proves too immature or its connector catalog too thin"), both open-source
candidates fail the same test:

- **drt**: no Kafka destination, no CDC-based extraction (architecture).
- **dlt**: thin catalog for this shape — no Kafka destination (and none will be
  merged), no CDF source — so it would become the custom source + custom
  destination this component already implements, minus a dependency, with worse
  lineage and a second, conflicting state store.
- **SaaS**: excluded on cost, and it would not remove the extraction engineering
  either.

This document closes the tool evaluation for APR-233. The zero-DBU read path
(OSS Delta on Spark) and its cost measurements live in
[REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md).

## 4. Broader OSS scan (2026-10-15)

A web scan of the 2026 open-source reverse-ETL landscape found no tool meeting
the epic's criteria better than the built component. The recurring theme: every
scanned platform reads warehouses with SQL (Databricks → DBUs per sync) and
ships row-syncs to SaaS business tools — none produce change events (Debezium
format) to Kafka, and none read Databricks storage directly.

- **Multiwoven** (1.6k stars, Ruby, Elastic License 2.0 — not OSI-open): the
  "open-source Hightouch/Census". UI-driven batch syncs, warehouse → SaaS
  destinations (Salesforce, HubSpot, ...). Databricks read via warehouse. No
  Kafka destination, no change events.
- **drt** (v0.10, Apache-2.0): notably evolved since the live comparison —
  Delta Lake and Iceberg **sources** now read storage directly, dagster-drt
  ships event-driven sensors, 20+ destinations. But the destination list still
  has **no Kafka** (ADR 0004 holds in 2026): REST/Slack/HubSpot, Sheets, SQL
  upserts, Notion, Twilio, Salesforce, S3/Azure/Elasticsearch, File.
- **Estuary Flow** (Apache-2.0 core): real-time CDC *capture* from operational
  databases (Postgres/MySQL/Mongo/Kafka) — does not read Databricks at all;
  wrong direction for activation from the lakehouse.
- **Airbyte** (MIT core / ELv2): has a Kafka destination and a Databricks
  source, but the source reads via JDBC (warehouse DBUs), syncs are batch
  snapshots without change-event semantics, and it is a standalone platform
  with its own orchestration — the "invisible side effect" the epic rejects.
- **Early / not viable**: condor-retl (1 star), Duckle (no-code DuckDB
  platform), ETLX (Markdown-config framework), PyReverseETL (proprietary
  license; Kafka explicitly not wired in).

The three properties that make this pipeline non-generic — Databricks CDF as
change events, Debezium format to Kafka, and zero-DBU direct-storage reads —
are absent from every scanned tool. The closest architectural alternative is
Databricks-native streaming to Kafka (Structured Streaming / Lakeflow Kafka
sink on Databricks compute), which is the notebook path's cost profile —
Databricks DBUs per run — already measured in section 14.1.2 of the sink doc.

## Sources

- APR-233 "Explore & Standardize Batch Reverse ETL" (epic, read 2026-10-15 via ACLI).
- dlt project README and docs (github.com/dlt-hub/dlt, fetched 2026-10-15).
- [REVERSE_ETL_CDF_POC_PLAN.md](REVERSE_ETL_CDF_POC_PLAN.md) — drt / ADR 0004 finding.
- [REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md) — warehouse vs OSS Delta cost measurements.

[APR-233]: https://kaizengaming.atlassian.net/browse/APR-233
