# Brazil Workload Tuning Guide

Detailed record of every configuration change made to run the casino production demo
(UC1 — Real Bet Amount, UC2 — Casino Turnover Percentage) against the **Brazil** Kafka
topics on a local single-node stack, with the reasoning behind each one.

## Background: why Brazil needed tuning

The demo originally ran against the **Ghana** topics (`cronus.casino.out.gh`, `bets-out-gh`).
We repointed it to **Brazil** (`cronus.casino.out.br`, `bets-out-br`). Brazil is a far higher
volume tenant — bursts of ~95,000 casino transactions in a single minute were observed
(~1,500 msg/s peak), versus a comparatively trickle from Ghana.

The whole stack runs on **one developer Mac** (16 GB RAM, OrbStack Docker VM raised to
12 GB during this work) with a **single-node** RisingWave cluster and a **single-node**
MinIO acting as the Hummock state-store backend *and* the Iceberg/Lakekeeper S3 backend.
That shared, un-replicated MinIO is the central bottleneck: at Brazil volume the default
RisingWave settings flooded it with concurrent object writes, causing upload timeouts
(`write part timeout`, `NoSuchUpload`) that tripped full-cluster recovery
(the license disables `DatabaseFailureIsolation` because the host exceeds the licensed core
count, so any single storage failure resets the whole database).

Almost every change below exists to **reduce or smooth the write pressure on the single
MinIO**, **shrink the in-memory/state footprint so the compute node fits its budget**, or
**make the pipeline start up and become observable quickly** for demos.

---

## 1. Kafka source ingestion rate — `source_rate_limit`

**Files:** `dbt/models/casino_prd/src_casino_prd.sql`, `dbt/models/casino_prd/src_bets_br.sql`,
`sql/casino_prd_source.sql`, `sql/casino_prd_bets_source.sql`
**Value:** `source_rate_limit = 1` (build-time), ramped to `200` post-build (see §9)

**What it does:** Caps how many rows/second each Kafka source table ingests.

**Reasoning:**
- At Brazil's native rate (~1,500 msg/s) the source firehosed every downstream MV and the
  Iceberg sinks all at once. The resulting SST + parquet write storm saturated MinIO CPU
  (observed 200–350 %) and produced the upload-timeout → cluster-reset failures.
- We landed on a **two-phase strategy** rather than a single steady value:
  - **`1` during the dbt build.** When the source tables are (re)created, a near-zero rate
    means almost no rows accumulate in `mv_casino_transactions` while the 6 downstream MVs
    are being created one-by-one (`max_concurrent_creating_streaming_jobs = 1`). Each MV
    therefore backfills against an almost-empty upstream, so creation is near-instant. This
    is what cut `casino_prd_dbt_assets` from 500 s+ (compounding backfill) down to ~50–70 s.
  - **`200` after the build** (set by the Dagster asset via `ALTER TABLE … SET
    source_rate_limit`). 200 rows/s is the highest steady rate the single MinIO sustained
    without the upload timeouts. We tested 1000 (crashed MinIO) and 500 (usable but the
    backfill lag compounded faster than it drained); 200 was the stable ceiling.
- **Trade-off:** at 200 rows/s the pipeline consumes far less than Brazil's production rate,
  so consumer lag grows during bursts and never fully catches up. This is an accepted
  limitation of the single-Mac PoC — `scan.startup.mode = 'latest'` means each fresh stack
  start resets the offset to "now," so demos begin at ~0 lag.

---

## 2. Rolling window sizes (UC1 / UC2 materialized views)

**Files:** `dbt/models/casino_prd/mv_casino_real_bet.sql`,
`mv_casino_turnover_90d.sql`, `mv_sportsbook_turnover_90d.sql` (+ raw `sql/casino_prd_funnel_iceberg.sql`)

| MV | Original window | Tuned window | Column |
|----|-----------------|--------------|--------|
| `mv_casino_real_bet` (UC1) | 14 days (`1209600s`) | **1 day (`86400s`)** | `rolling_1d_real_bet_amount` |
| `mv_casino_turnover_90d` (UC2) | 90 days (`7776000s`) | **7 days (`604800s`)** | `rolling_7d_turnover` |
| `mv_sportsbook_turnover_90d` (UC2) | 90 days (`7776000s`) | **7 days (`604800s`)** | `rolling_7d_turnover` |

**What it does:** These are `SUM(...) OVER (PARTITION BY customer ORDER BY ts RANGE BETWEEN
INTERVAL 'N' PRECEDING AND CURRENT ROW)` sliding-window aggregates. RisingWave must keep
**every row inside the window** in the state store so it can retract values as they age out.

**Reasoning:**
- Window state size is proportional to `(rows/sec passing the filter) × (window length)`. At
  Brazil volume a 90-day window per customer is an enormous amount of Hummock state — which
  means huge memory pressure on the compute node and constant spill-to-MinIO.
- Shrinking 90 d → 7 d cuts that state ~13×; 14 d → 1 d cuts it ~14×. This was the single
  biggest reducer of compute-node memory and of the backfill volume the `_latest` / ratio
  MVs downstream have to process.
- **Trade-off:** the metrics now describe a shorter trailing window than the business spec
  (1-day real bet, 7-day turnover). Acceptable for a PoC whose goal is to prove the pipeline,
  not to produce production-accurate 90-day figures. The column names were renamed to reflect
  the real window so dashboards don't misrepresent the number.

---

## 3. Object-store write concurrency — `opendal_upload_concurrency`

**File:** `risingwave.toml`  **Default:** `256` → **Tuned:** `4`

**What it does:** Maximum number of concurrent multipart-upload streams RisingWave's object
store client opens against MinIO.

**Reasoning:**
- `256` concurrent uploads is designed for a horizontally-scaled, multi-node S3 backend. Fired
  at one MinIO container it instantly saturated its CPU and exhausted its connection handling,
  producing the `write part timeout` / `NoSuchUpload` errors that reset the cluster.
- We stepped it down 256 → 16 → 8 → **4**. Each reduction lowered MinIO CPU; 4 was where
  uploads completed reliably without the storm. This is *the* key MinIO-protection knob.
- **Trade-off:** lower write parallelism means a large flush drains more slowly, but at our
  rate-limited ingest that is never the binding constraint — MinIO CPU is.

---

## 4. Checkpoint / commit cadence — `barrier_interval_ms`, `checkpoint_frequency`

**File:** `risingwave.toml`
**`barrier_interval_ms`:** `250` → **`2000`**   **`checkpoint_frequency`:** `1` → **`2`**

**What it does:** Barriers are injected every `barrier_interval_ms`; a checkpoint (which
flushes shared buffers as SSTs to MinIO) happens every `checkpoint_frequency` barriers.
Effective checkpoint cadence = `2000ms × 2 = 4 s` (was `250ms × 1 = 0.25 s`).

**Reasoning:**
- Default 4 checkpoints/second means 4 waves of SST uploads/second to MinIO — far more write
  transactions than the single node could absorb at Brazil volume.
- Stretching to one checkpoint every ~4 s cut the upload *frequency* ~16×, letting each batch
  be larger and the uploads spaced out. This dramatically reduced the sustained MinIO load.
- **Trade-off:** higher end-to-end latency (state is durably checkpointed every ~4 s instead
  of sub-second). Irrelevant for an analytics dashboard refreshing on the order of seconds.

---

## 5. SST sizing & flush batching — `sstable_size_mb`, `shared_buffer_min_batch_flush_size_mb`

**File:** `risingwave.toml`
**`sstable_size_mb`:** `256` → **`64`**
**`shared_buffer_min_batch_flush_size_mb`:** `800` → **`128`**
**`streaming_upload_attempt_timeout_ms`:** `5000` → **`15000`**

**What it does:**
- `shared_buffer_min_batch_flush_size_mb` — how much data accumulates in memory before a flush.
- `sstable_size_mb` — target size of each SST object written to MinIO.
- `streaming_upload_attempt_timeout_ms` — per-attempt timeout for a streaming upload.

**Reasoning:**
- The original `800 MB` buffer + `256 MB` SST target meant individual object writes of tens to
  hundreds of MB. On a 1 GB/2 GB MinIO container a single 50–250 MB multipart PUT could not
  finish inside the 5 s attempt timeout → `write part timeout` → reset. (This was the exact
  error in the first UC2 failures.)
- Dropping the flush threshold to `128 MB` and SST target to `64 MB` makes each MinIO write a
  few MB per part — comfortably inside the timeout even under memory pressure. (We briefly
  tried `32 MB`, which fixed timeouts but flipped the problem: too many tiny writes
  CPU-saturated MinIO. `128 MB` flush / `64 MB` SST is the balance between "writes too big to
  finish" and "writes too many to keep up.")
- The upload timeout was simultaneously raised 5 s → 15 s as a safety margin so a transiently
  busy MinIO doesn't fail an otherwise-fine upload.

---

## 6. Streaming parallelism & memory footprint — `default_parallelism`, `stream_chunk_size`, `in_flight_barrier_nums`

**File:** `risingwave.toml`
**`default_parallelism`:** `"Full"` → **`4`**
**`stream_chunk_size`:** `256` → **`1024`**
**`in_flight_barrier_nums`:** `10000` → **`200`**

**What it does:**
- `default_parallelism` — actors spawned per streaming operator.
- `stream_chunk_size` — rows processed per internal batch.
- `in_flight_barrier_nums` — how many barriers may be in flight before back-pressure.

**Reasoning:**
- `"Full"` spawns an actor per available CPU slot (~10 on this VM). On a single compute node
  that means heavy context-switching and per-actor state overhead with no throughput gain.
  Pinning to `4` gives focused parallelism matched to the box and frees memory.
- `stream_chunk_size 256 → 1024` processes 4× more rows per operator invocation, cutting
  per-row function-call overhead — meaningful during the window-MV backfill.
- `in_flight_barrier_nums 10000 → 200` stops RisingWave pre-allocating barrier state for ten
  thousand in-flight barriers (pointless on a single node) — pure memory savings.
- Together these reduced compute-node memory and CPU churn so it fits comfortably in its 6 GB
  budget (observed ~2–3 GB / 6 GB after the change vs hugging the 4 GB limit before).

---

## 7. Container memory limits (docker-compose.yml)

Right-sized for the **12 GB** OrbStack VM (raised from 7.8 GB; the original 42 GB-of-limits
layout from an over-scaling attempt would have thrashed the VM).

| Container | limit / reservation | Reasoning |
|-----------|---------------------|-----------|
| `compute-node-0` | **6 G / 4 G** | Holds all streaming actor state + window MVs; the memory-hungriest node, given the most headroom. |
| `compactor-0` | **2 G / 1 G** | General Hummock SST compaction. |
| `compactor-1` | **1 G / 512 M** | Dedicated-iceberg compactor; observed near-idle (~13 % CPU), so trimmed to free RAM. |
| `minio-0` | **2 G / 1 G** | Raised from 1 G — it serves both Hummock and Iceberg I/O; the busiest container. |
| `meta-node-0` | **1 G / 512 M** | Metadata only; light. |
| `frontend-node-0` | **1 G / 512 M** | SQL frontend; light. |

Total reservations (~7.5 G) leave room for Trino, Dagster, Lakekeeper, Redpanda, Postgres,
Grafana on the 12 GB VM. **A 2nd compute node and 4-node distributed MinIO were attempted and
reverted** — they don't fit a 16 GB Mac and distributed MinIO can't be added to a running
single-node instance without wiping state. Node-count scaling is a real-server move, not a
laptop one.

---

## 8. Iceberg sink commit cadence — `commit_checkpoint_interval`

**Files:** `dbt/models/casino_prd/sink_casino_real_bet.sql`,
`dbt/models/casino_prd/sink_turnover_percentage.sql` (+ raw `sql/casino_prd_funnel_iceberg.sql`)
**Value:** `40` → **`20`**

**What it does:** The Iceberg sink buffers writes and commits a snapshot every
`commit_checkpoint_interval` checkpoints. At `2000ms × checkpoint_frequency 2`, `40` ≈ a
commit every ~60–80 s (the "first commit takes ~1 min" symptom); `20` ≈ ~30–40 s.

**Reasoning:**
- The first Iceberg snapshot only appears after one full commit interval, so `40` meant ~1 min
  before any data was queryable in the lakehouse and before `casino_trino_views` (which polls
  for the first snapshot) could finish. Halving to `20` halves that latency.
- **Trade-off (deliberately bounded):** more frequent commits → more snapshots → more small
  parquet + equality-delete files → more compaction cycles and more S3 PUTs on the constrained
  MinIO. `20` (~30–40 s) was chosen as the balance — it's a meaningful latency cut, still
  under the 1-min Grafana refresh (so no wasted dashboard cycles), and not aggressive enough to
  re-saturate MinIO. Going to `10` (~15–20 s) was rejected as too much file/commit churn.
- Sink-side compaction stays on to keep the small-file count bounded: `enable_compaction =
  'true'`, `compaction_interval_sec = '60'`, `compaction.trigger_snapshot_count = '5'`, served
  by the dedicated `compactor-1`.

### Related Iceberg parquet sizing (`risingwave.toml`)
- `stream_iceberg_sink_write_parquet_max_row_group_rows = 500` — small row groups so a snapshot
  is emitted promptly at low effective throughput (the sink only sees the net upsert rate after
  the `message_type/account` filters, which is much lower than raw ingest). Lowered from the
  default to make the first commit appear quickly; the trade-off (more, smaller row groups) is
  acceptable at PoC volume.
- `iceberg_compaction_write_parquet_max_row_group_rows = 10000` — compaction rewrites into
  larger row groups, undoing the small-file effect after the fact.

---

## 9. Dagster pipeline structure & lifecycle (orchestration/definitions.py)

Several orchestration changes were needed so the job runs fast, reliably, and observably.

### 9a. Merged UC1 + UC2 into one dbt step
- **Before:** two separate `@dbt_assets` (`casino_uc1_dbt_assets`, `casino_uc2_dbt_assets`)
  selected by tag. Dagster scheduled them sequentially with a ~100 s gap between (daemon
  re-evaluating the plan, restarting the dbt subprocess).
- **After:** one `casino_prd_dbt_assets` with `select="casino_prd"` runs all UC1+UC2 models in
  a single `dbt build`. Eliminates the scheduling gap; individual models still show in the
  Dagster asset graph (grouped by their dbt tags).

### 9b. Post-build rate-limit ramp
- After `dbt build`, the asset opens a `psycopg2` connection to the RisingWave frontend and
  runs `ALTER TABLE src_casino_prd / src_bets_br SET source_rate_limit = 200`. This is the
  "phase 2" of the two-phase rate strategy in §1 — sources are *created* at rate `1` (fast
  build) then *ramped* to `200` (steady streaming) once all MVs exist.

### 9c. `drop_prebuild_sinks` via subprocess, not `dbt.cli().stream()`
- Calling the run-operation through `dbt.cli(...).stream()` fired dbt's `on-run-start` hooks
  (UDF/Iceberg-connection setup) which fail on a not-yet-warm cluster and aborted the whole
  step. Running it as a plain `subprocess` that *warns instead of raising* makes a fresh-start
  drop (where sinks don't exist yet) safe.

### 9d. Stop running `dbt parse` on every code-location load
- **Before:** `dbt parse` ran at module import on every Dagster code-server start (~45 s). The
  daemon's 20 s heartbeat timed out mid-parse, so the code server cycled endlessly and runs
  got stuck in `STARTING`.
- **After:** the manifest is parsed only if it doesn't exist; otherwise the cached
  `target/manifest.json` is used. Code location loads instantly. Run `dbt parse` manually only
  after changing model structure/tags/refs (not needed for SQL-body edits).

---

## 10. Trino startup reliability & catalog config

### 10a. `catalog.management` (trino/config.properties)
- Kept at **`dynamic`**. We briefly switched to `static`, which made Trino initialize all
  catalogs synchronously at boot and pushed cold-start to ~2.5 min; reverting to `dynamic`
  restored fast startup. The real fix for the catalog race was the dependency ordering (10b),
  not the management mode.

### 10b. Trino depends on `lakekeeper-bootstrap` completion (docker-compose.yml)
- **Before:** `depends_on: lakekeeper (service_started)`. Trino raced Lakekeeper and tried to
  initialize the `datalake` Iceberg catalog before Lakekeeper's REST API + warehouse were
  ready → catalog `failed to initialize and is disabled` (permanent for that session).
- **After:** `depends_on: lakekeeper-bootstrap: service_completed_successfully` + `minio-0:
  service_healthy`. `lakekeeper-bootstrap` only completes after the warehouse is registered,
  which is the true "catalog API ready" signal. The `datalake` catalog now initializes on the
  first try after a clean stack start.

### 10c. Removed unsupported Iceberg cache properties (trino/catalog/datalake.properties)
- We tried `iceberg.metadata-cache.expire-duration` and later `metadata.cache-ttl` /
  `metadata.cache-missing` to cut query latency. **All are rejected by the Trino 453 REST
  Iceberg connector** and caused the entire `datalake` catalog to fail to load — the actual
  root cause of repeated "catalog disabled" symptoms. They were removed. The connector has no
  supported metadata-cache TTL knob here, so query speed is addressed by §11 instead.

---

## 11. Grafana "Iceberg Rows" panels — metadata counts instead of `COUNT(*)`

**Files:** `orchestration/assets/casino_prd_setup.py` (`TRINO_VIEWS`),
`bin/3_run_casino_prd_demo.sh`, `monitoring/grafana/dashboards/casino-uc-metrics.json`

**What it does:** The two "Iceberg Rows" stat panels previously ran
`SELECT COUNT(*) FROM <full iceberg table>` every 10 s. Now they read
`SELECT SUM(record_count) FROM "<table>$partitions"` via the new `casino_real_bet_rowcount`
and `turnover_pct_rowcount` views.

**Reasoning:**
- The tables are **upsert / merge-on-read**, so `COUNT(*)` must scan every data file and apply
  every equality-delete file to reconcile — a 5–7 s full scan that grows with Brazil volume,
  re-run every 10 s.
- The `$partitions` metadata table carries `record_count` per partition and is **manifest-only**
  (no data-file scan) → near-instant. (We first tried `$snapshots.summary['total-records']`,
  but **RisingWave writes empty snapshot summaries**, so that key doesn't exist — hence the
  `$partitions` approach.)
- **Trade-off:** `record_count` counts data-file records, so during heavy upsert churn it reads
  higher than the deduped MV count and converges after compaction. Acceptable for a "rows landed
  in Iceberg" indicator.

### Dashboard refresh: `10s` → `1m`
- Iceberg commits land only every ~30–80 s, so a 10 s refresh re-ran every Trino query 6–8×
  between commits for no new data — pure load on Trino/MinIO. `1m` keeps panels effectively live
  while cutting Trino query load ~6×. Business-metric panels read from RisingWave directly and
  are unaffected.

---

## Quick reference — final tuned values

| Setting | Location | Value |
|---------|----------|-------|
| `source_rate_limit` (build / steady) | source models + `definitions.py` ramp | `1` → `200` |
| UC1 window | `mv_casino_real_bet.sql` | `86400s` (1 d) |
| UC2 windows | `mv_*_turnover_90d.sql` | `604800s` (7 d) |
| `opendal_upload_concurrency` | `risingwave.toml` | `4` |
| `barrier_interval_ms` | `risingwave.toml` | `2000` |
| `checkpoint_frequency` | `risingwave.toml` | `2` |
| `sstable_size_mb` | `risingwave.toml` | `64` |
| `shared_buffer_min_batch_flush_size_mb` | `risingwave.toml` | `128` |
| `streaming_upload_attempt_timeout_ms` | `risingwave.toml` | `15000` |
| `default_parallelism` | `risingwave.toml` | `4` |
| `stream_chunk_size` | `risingwave.toml` | `1024` |
| `in_flight_barrier_nums` | `risingwave.toml` | `200` |
| `stream_iceberg_sink_write_parquet_max_row_group_rows` | `risingwave.toml` | `500` |
| `iceberg_compaction_write_parquet_max_row_group_rows` | `risingwave.toml` | `10000` |
| `commit_checkpoint_interval` (casino sinks) | sink models | `20` |
| compute-node-0 memory | `docker-compose.yml` | `6 G / 4 G` |
| minio-0 memory | `docker-compose.yml` | `2 G / 1 G` |
| Trino `depends_on` | `docker-compose.yml` | `lakekeeper-bootstrap` completed |
| Grafana refresh | `casino-uc-metrics.json` | `1m` |

## If MinIO saturates again

The escape hatches, in order of preference: lower `source_rate_limit` (200 → 100), raise
`commit_checkpoint_interval` (20 → 40), or drop `opendal_upload_concurrency` (4 → 2). The
underlying cause is always the single-node MinIO; the durable fix is real horizontal storage,
not config.
