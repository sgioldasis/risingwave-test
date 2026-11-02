# The reverse-ETL journey: from PoC to a released zero-DBU component

Summary of the batch reverse-ETL work (epic APR-233, component ticket APR-302):
what problem it solves, the road it took, the decisions that shaped it, the end
solution, and what it costs.

The paper trail, in reading order:

| Doc | What it covers |
| --- | --- |
| `REVERSE_ETL_CDF_POC_PLAN.md` | The original PoC plan |
| `REVERSE_ETL_TOOL_EVALUATION.md`, `REVERSE_ETL_DRT_COMPARISON.md`, `REVERSE_ETL_SPARK_FREE_FINDINGS.md` | How the engine was chosen |
| `REVERSE_ETL_DEBEZIUM_JDBC_SINK.md` | The wire-format + sink demo |
| `REVERSE_ETL_RELEASE_DESIGN.md` | The sync/target seam and naming policy |
| `REVERSE_ETL_PORT_PLAN.md` | The port to dagster-workspace (APR-302) |
| `REVERSE_ETL_COMPONENT_GUIDE.md`, `REVERSE_ETL_CODE_WALKTHROUGH.md` | How to use / read the component |

---

## 1. The business use case

Curated Delta tables in Databricks are the system of record, but their
consumers live elsewhere: operational Postgres databases, dashboards on other
engines, downstream event pipelines. Getting warehouse data *out* into those
systems is reverse ETL — and the naive ways to do it all cost Databricks
compute or manual labour:

- A scheduled query on a SQL warehouse (or serverless) pays DBUs every run.
- A Spark job reading the table pays cluster cost for what is, at heart, a diff.
- `readChangeFeed` (CDF) avoids re-scanning but requires the feature enabled on
  the source table — a change to a governed table most teams cannot make.
- One-off ad-hoc scripts per team: no replay safety, no standard, no reuse.

**Goal:** a standardized component that moves a Delta table's changes to
consumers with **zero Databricks compute in the read path**, adoptable by any
team with one YAML file, and safe to re-run.

---

## 2. The main steps

### 2.1 PoC (this repo, risingwave-test)

The idea was proven in the casino/e-commerce PoC: a Python component family
(`ReverseEtlCdfSync` + Postgres/RisingWave/DuckDB targets) reading Delta
changes, wrapping them in Debezium-style envelopes, and producing to Kafka;
a Debezium JDBC sink connector draining the topic into Postgres with upserts
and tombstone deletes.

### 2.2 Choosing the engine — the zero-DBU seam

The decisive experiment: **delta-rs over ADLS reads Delta versions directly**
— the transaction log is fetched over the storage endpoint, and the change
between two table versions is reconstructed by diffing (the "version-diff"
path, handling schema-change commits). No Spark, no warehouse, no statement
execution, no CDF enablement. That made the read-path cost **zero DBU**, and
it became the acceptance bar, not a nice-to-have.

### 2.3 The release design

The PoC family was split for release: one sync component + slim target
components, a minted-name policy (`reverse_etl_<label>[_<role>]`, derived by
arithmetic so both sides of a seam agree), the sync/target seam rule (targets
see the topic, never the source table), and the env contract (no Dagster
resources; connections resolve from the environment at call time).

### 2.4 The port to dagster-workspace (APR-302)

The released family moved into a capability library
(`libraries/kaizen-dagster-reverse-etl/`) in the dagster-workspace monorepo —
same pattern as the Jobotron port (APR-293): uv src-layout library, a
component instance in an existing code location (`projects/inverness`),
CI/CD wiring, pre-commit component-schema generation, and the import-hygiene
test that keeps demo code out of the released surface.

### 2.5 Review-phase simplification — the pure producer

Iteration after review progressively stripped the component down:

1. The instance was repointed at the real table
   (`de_dev.sr_poc_external.reverse_etl_delta_source`, key `rid`).
2. `asset_key_prefix` was added (jobotron parity) so a location's assets nest
   under it in the multi-location catalog.
3. The topic→sync dependency and then the entire topic asset/bootstrap job
   were removed: **the component creates no infrastructure** — the topic
   (compacted) and the state table are external, and a missing one fails the
   run loudly with what to create.
4. The target components were deleted entirely: **the library's only target
   is Kafka**. A target is the consuming project's own code.
5. A final sweep removed every trace of the deleted side (the RisingWave
   connection, the Kafka Connect HTTP client, the offset-reading consumer,
   all target prose) — the library now knows nothing downstream of the topic.

Each strip made the contract sharper, not weaker: the deleted functionality
lives on as the consumer project's own code, and the component's blast radius
shrunk to one topic and one watermark.

---

## 3. The architectural decisions and why

| Decision | Why |
| --- | --- |
| **delta-rs version-diff, not CDF, not Spark** | Zero DBU is the point. CDF needs source-table enablement (often blocked); Spark/warehouse compute costs money on every run. Works on plain Delta tables — no prerequisites on the source at all. |
| **Debezium-style envelopes on Kafka** | The industry-neutral change-event format: any CDC consumer (Debezium JDBC sink, a stream engine, a custom consumer) can drain the topic. Kafka is the seam, not any particular target. |
| **JSON encoding first** (Connect JSON, schema embedded in every message) | Zero external dependencies — no schema registry to run or grant. Avro + registry stays available for teams that want it. |
| **Append-only watermark, freshest-row-wins** | Replay safety: the watermark is appended only AFTER the final Kafka flush succeeded, so a failed run moves nothing and the next run replays the same versions. Duplicate events are harmless — consumers upsert by the key column. |
| **The component creates no infrastructure** | A library that creates topics/tables needs broad permissions and hides operational ownership. The topic (compacted — a change topic's documented default) and the state table belong to the deployment; a missing one fails with the DDL to run. |
| **No Dagster resources — the env contract** | One defs.yaml deploys to staging and production unchanged; connections read the environment at call time. Names and decisions in files, values in the environment. |
| **`asset_key_prefix` (jobotron parity)** | A location's assets nest under it in the multi-location catalog. Dagster keys only — external names (topic, watermark key) are never prefixed. |
| **Targets are the consuming project's code** | The library stays small and permission-free; consumers keep full ownership of their own sink/connector. The consumer job (topic setup → table → JSON connector → report) demonstrates the seam from the other side. |
| **Minted-name arithmetic** | `reverse_etl_<label>[_<role>]` is derived in one module, so every consumer of the names derives byte-identical values — the wire contract is correct by construction, not by discipline. |
| **History ages out to baselines, never backfills** | If log retention has eaten the commits between the watermark and the oldest available version, the sync baselines at the current version. It never pretends to backfill what it can no longer read — the trade-off is documented, and keeping retention ahead of the schedule avoids it. |

---

## 4. The end solution

```
Databricks Delta (de_dev.sr_poc_external.reverse_etl_delta_source)
  │  delta-rs version-diff over ADLS — zero DBU
  ▼
inverness sync asset (libraries/kaizen-dagster-reverse-etl)          dagster-workspace
  │  Debezium-style JSON envelopes, key = rid
  │  watermark: append-only rows in Postgres (reverse_etl_sync_state)
  ▼
STG Kafka topic (reverse_etl_delta_topic, compacted)                 external infrastructure
  │
  ▼
reverse_etl_consumer_job (risingwave-test)                           the consumer side
  │  topic setup → target table → Debezium JDBC sink connector (JSON)
  ▼
PostgreSQL target table — upserts by rid, tombstone deletes
```

- **dagster-workspace** (`feature/APR-302-reverse-etl`): the library — one
  component, one sync asset, no infrastructure, no targets — plus the
  inverness instance (one YAML).
- **risingwave-test** (`feature/component-decoupling`): the consumer job,
  built from this repo's own demo-derived builders, owning everything
  downstream of the topic.

Verified live end to end: a change in the Delta table reached the Postgres
target as an upsert, a deletion reached it as a tombstone, and re-running the
sync replayed safely.

---

## 5. Pros and cons

### Pros

- **Zero Databricks compute in the read path** — the whole point. No cluster,
  no warehouse, no statement execution; delta-rs reads the transaction log
  over storage.
- **No source-table prerequisites** — works on plain Delta; no CDF flag, no
  table changes, no governance negotiation.
- **Replay-safe by construction** — failed runs move nothing; duplicates are
  harmless upserts; renaming the watermark key fails loudly instead of
  silently restarting the sync.
- **Adoption is one YAML** — no Python, no Dagster resources, no
  environment-specific values in the file; one defs.yaml serves staging and
  production.
- **Small blast radius** — the sync touches one topic and one watermark row
  family; consumers are decoupled and independently deployed.
- **Industry-neutral output** — Debezium-style events on Kafka; any CDC
  consumer can drain it, and the format is pinned by golden-file tests.
- **Correct by construction** — minted-name arithmetic, the import-hygiene
  scan, and the config-surface pin keep drift and demo code out.

### Cons

- **Batch, not streaming** — latency is the schedule cadence plus the diff
  run; there is no continuous CDC. Big backfills re-read versions in chunks
  (bounded, but not free).
- **Infrastructure prerequisites stay with the deployment** — the compacted
  topic and the `reverse_etl_sync_state` table (one DDL per deployment) must
  exist before the first run. Deliberate, but it is setup someone must do.
- **JSON payloads carry their schema in every message** — larger than
  registry-based Avro for high-volume topics (Avro encoding remains available
  when that matters).
- **Log retention can age out baselines** — if commits between the watermark
  and the oldest available version expire, the sync silently baselines at the
  current version: no backfill, so a long-downstream-outage sync is silently
  incomplete.
- **Consumers build their own target side** — the seam is deliberate, but
  "targets are your code" means each consuming project owns its table, its
  connector and its monitoring.
- **The wire contract is discipline at the boundary** — never mix encodings
  across the assets pointing at one topic, and the connector must match the
  sync's encoding; the library cannot enforce what happens on the consumer's
  side of the topic.
- **One SRE prerequisite gates a first live instance** — UC storage-credential
  grants for the sync's service principal (the same blocker the Jobotron
  investigation hit).
