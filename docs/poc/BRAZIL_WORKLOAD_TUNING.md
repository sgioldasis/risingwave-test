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

## 12. MinIO server-side tuning (experimental — pending measurement, 2026-06)

**File:** `docker-compose.yml` (`minio-0`)
**Changes:** `MINIO_SCANNER_SPEED: slowest` (new); container memory `2G → 4G`

**Status:** ❌ **Measured — no throughput benefit (2026-06-01).** Both knobs are applied and
harmless (MinIO now uses ~2.5 GB of the 4 GB for cache), but they do **not** move throughput,
because measurement proved **MinIO is no longer the bottleneck in this configuration.** Kept as
inert/near-zero-cost; revert freely if you want minimal config. The real lever is compute-side
(see "Measured result" below).

### Measured result (2026-06-01, 10-core / 17 GB host)
After a clean restart with these changes, throughput followed the window-fill curve, not the
MinIO config:

| Moment | casino consume | MinIO CPU | compute CPU | backpressure |
|--------|---------------|-----------|-------------|--------------|
| fresh start (empty windows) | ~787/s | 136% | 461% | ~0 |
| ~1 min in | ~365/s | 17% | 503% | rising |
| warmed (~3–4 min, windows filling) | **~53–133/s** | **~4%** | 134% (stalled) | **5.7** |

- The ~787/s at fresh start is the **empty-window artifact** — with nearly no window state to
  maintain, the source runs near its `200 × ~4-actor` rate cap. It decays to the same ~53/s
  baseline as state accumulates. Not a MinIO-config effect.
- In the choked (warmed) state, **MinIO idles at ~4 % CPU** — far from §1's 200–350 %. The
  backpressure concentrates on **`mv_casino_transactions`** (the double-`UNNEST` fan-out;
  fragments 10/11/14) and propagates back to the source. Those window operators **stall on
  state access** (low CPU + high backpressure = blocked, not computing).
- **Interpretation:** §1's MinIO-bound regime was real *then* (full backfill, 90-day windows,
  pre-§3–§5). After the window shrink (§2) and write tuning (§3–§5), the bottleneck **migrated
  to compute window-state**. MinIO-side knobs can no longer help.
- **Next lever (the only real one on this hardware):** restructure the rolling `RANGE` window
  MVs (`mv_casino_real_bet`, `mv_casino_turnover_90d`, `mv_sportsbook_turnover_90d`) into
  `TUMBLE`/`HOP` + `EMIT ON WINDOW CLOSE` to cut per-event state work and write volume. Changes
  the metric from a continuous rolling sum to periodic window finals — a semantics trade-off to
  weigh against the throughput gain.

**Context:** MinIO is the established binding constraint for this PoC — `source_rate_limit=200`
(§1) is "the highest rate the single MinIO sustained," and §1 observed it CPU-bound at
200–350%. A live diagnosis on 2026-06-01 (10-core / 17 GB host) confirmed the symptom from the
other side: the source consumed only ~50 rows/s with **large, growing Kafka lag** and **heavy
output-buffer backpressure** on internal fragments, while the **compute node used only ~2.3 of
10 cores**. That idle CPU is compute *blocked on state-store flushes to MinIO*, not spare
capacity — consistent with §6's finding that raising `default_parallelism` ("Full" → 4) gave
**no throughput gain**. So the lever, if any, is on MinIO itself, not more compute actors.

**Reasoning for each knob:**
- `MINIO_SCANNER_SPEED = slowest` — frees background-scanner CPU/IO for serving writes.
  Expected gain is **small**: MinIO already deprioritizes the scanner for read/write by default
  (waits ~10× the scan-op time between scans), and the demo dataset is tiny, so the scanner
  isn't a major competitor during write bursts.
- memory `2G → 4G` — more request-buffer / metadata-cache headroom under bursts. The host has
  17 GB (the §7 layout assumed a 12 GB VM), so the extra 2 GB is comfortably available.

**Deliberately NOT changed — multi-drive MinIO.** Giving MinIO 4 directories to parallelize its
write path across the idle cores would enable **erasure-coding parity**, which *adds* CPU and
write amplification per object. Since MinIO is already CPU-bound, that risks making it slower,
and it requires wiping `/data`. Rejected unless the cheap knobs above prove insufficient and we
explicitly want to run that experiment.

**The honest ceiling.** Single-node MinIO is architecturally weak at high small-object write
rates; the high-value work was already done RisingWave-side (§3–§5: fewer, larger, less-frequent
writes). These knobs are expected to buy ~10–30% at most, not a multiple. The two real ceiling
lifts remain: (a) restructure the rolling `RANGE` window MVs into `TUMBLE`/`HOP` +
`EMIT ON WINDOW CLOSE` to cut per-row work and write volume (changes metric semantics), or
(b) real distributed storage — not a laptop move.

**How to validate after a restart:**
```bash
# 1. MinIO CPU under load (was 200–350% in §1 — is it still the wall after the bump?)
docker stats --no-stream minio-0

# 2. Source consume rate + backpressure (compare to the ~50 rows/s baseline)
PROM=http://localhost:9500
curl -s "$PROM/api/v1/query" --data-urlencode \
  "query=sum by (source_name) (rate(stream_source_output_rows_counts[1m]))"
curl -s "$PROM/api/v1/query" --data-urlencode \
  "query=topk(3, sum by (fragment_id) (rate(stream_actor_output_buffer_blocking_duration_ns[1m]))/1e9)"
```
If MinIO CPU is no longer pegged but throughput is unchanged, MinIO is no longer the wall and the
bottleneck has moved (likely to the window-MV compute) — pursue lever (a) instead.

---

## 13. Demo window shrink — 1d/7d → 5 min (measured 2026-06-01)

**Files:** `dbt/models/casino_prd/mv_casino_real_bet.sql`, `mv_casino_turnover_90d.sql`,
`mv_sportsbook_turnover_90d.sql` (+ raw `sql/casino_prd_funnel_iceberg.sql`)
**Change:** rolling `RANGE` window `86400s`/`604800s` → **`300s` (5 min)** on all three window MVs.

**Why:** For a **short demo** (`scan.startup.mode='latest'`, runs minutes–under an hour), a 1d/7d
window never evicts, so the `OverWindow` state grows unbounded with every ingested row and
throughput decays (787→~53/s within ~4 min — see §12). A window *shorter than the demo runtime*
evicts continuously, capping state. (1h was useless — still longer than the demo; 5 min evicts
within the run.)

**Measured result (⚠️ modest, not a fix):**

| Metric | 1d/7d (warmed) | 5-min window | 
|--------|----------------|--------------|
| casino throughput | ~53–67/s | **~120/s avg** (62–200 range), holds past 5 min |
| backpressure (max frag) | 4–8 | ~6.9 (**unchanged — still choked**) |
| MinIO CPU | ~4% (idle) | **175–360% spikes** (new bottleneck) |

- **~1.8× throughput** and it no longer decays — genuine steady state, good for a demo that
  otherwise collapses to ~53/s.
- **But the ceiling held.** Still backpressured (~6.9) at ~120/s vs the ~800/s rate cap. The
  bottleneck **shifted to MinIO**: a 5-min window makes every row enter *and* exit state within
  5 min → constant **retraction writes** to the state store, which intermittently saturate the
  single MinIO. Shrinking the window traded "large compute state" for "high eviction write churn."
- **Semantics:** metric is now a rolling **5-min** real-bet / turnover, not 1d/7d. Column names
  (`rolling_1d_real_bet_amount`, `rolling_7d_turnover`) were **left unchanged** to avoid a
  rename cascade through sinks/Iceberg/dashboard — they are now misnomers; rename if adopting
  permanently.

**The remaining lever:** `TUMBLE`/`HOP` + `EMIT ON WINDOW CLOSE` aggregates per fixed window
with **no per-event retractions**, which would avoid the eviction churn this change introduced —
at the cost of a larger semantics change (periodic finals, requires watermarks).

---

## 14. UC1 TUMBLE restructure — ❌ INFEASIBLE on RisingWave 2.7.4 (tested 2026-06-01, reverted)

**Outcome:** Attempted and **reverted.** A `TUMBLE` + `EMIT ON WINDOW CLOSE` aggregation cannot be
built on the casino transaction data in 2.7.4, because of a hard interaction between the nested
schema and watermark propagation. The code is back on the §13 5-min rolling window.

**Why it's blocked (the chain):**
1. The casino metric needs **transaction-level** rows → requires `UNNEST` of the nested
   `RoundInfo.Messages[].Transactions[]` arrays.
2. **`UNNEST` (ProjectSet) strips the watermark** in 2.7.4 — a column's watermark property does not
   survive the unnest, even inside a single CTE/MV.
3. **`EMIT ON WINDOW CLOSE` requires a watermark column in `GROUP BY`** (`Not supported:
   Emit-On-Window-Close mode requires a watermark column in GROUP BY`).
4. → No watermark reaches the TUMBLE → EOWC is rejected at bind time.

**Tested three ways (throwaway MVs):**
- ✅ `TUMBLE(src_casino_prd, round_created_at, ...)` **direct on the source** binds — the generated
  `round_created_at` + source watermark are valid.
- ❌ `TUMBLE(<subquery with UNNEST>, ...)` — rejected: window TVF's first arg must be a table/CTE/view.
- ❌ `TUMBLE(<CTE with UNNEST>, ...)` — rejected with the EOWC-needs-watermark error → **UNNEST drops
  the watermark.**

**No SQL workaround on this version:** you cannot re-declare a watermark inside an MV, and the UNNEST
is unavoidable for the nested arrays. The sportsbook side *could* tumble (`PlacedAt` is top-level, no
UNNEST) but it isn't the bottleneck, and UC2 casino turnover hits the same UNNEST wall. A RisingWave
version that propagates watermarks through ProjectSet would unblock this; 2.7.4 does not.

**Conclusion:** the **5-min rolling window (§13, ~120/s)** is the best achievable on this
hardware+version without breaking semantics or the build. The remaining real lever is horizontal
storage / more cores, not SQL.

---

### (Original attempt details — kept for reference)
**Files touched (now reverted):** `src_casino_prd.sql` (+ raw `sql/casino_prd_source.sql`),
`mv_casino_transactions.sql`, `mv_casino_real_bet.sql`, `sink_casino_real_bet.sql`
(+ raw `sql/casino_prd_funnel_iceberg.sql`). Scope was UC1 casino only.

**Goal:** §13 showed the 5-min rolling window doubled throughput but introduced **MinIO retraction
churn** (every row enters *and* exits state within the window → constant state-store writes). A
`TUMBLE` + `EMIT ON WINDOW CLOSE` aggregation has **no per-event retractions** — it accumulates a
partial sum per open window and emits once at close — so it should bound state *and* avoid the
churn. This restructures UC1 to test that.

**What changed:**
1. **Source watermark.** `src_casino_prd` gains a generated column
   `round_created_at = TO_TIMESTAMP(RoundInfo.RoundCreated.seconds)` and
   `WATERMARK FOR round_created_at AS round_created_at - INTERVAL '5 minutes'`. RoundCreated is the
   only **row-level** timestamp (transaction times are nested in `Messages[]/Transactions[]` arrays,
   so they can't anchor a watermark) — so the window event-time is an **approximation** of
   transaction time.
2. **`mv_casino_transactions`** carries `round_created_at` through (to propagate the watermark).
3. **`mv_casino_real_bet`** becomes `TUMBLE(..., round_created_at, INTERVAL '15 MINUTES')` +
   `GROUP BY customer_id, currency_id, window_start, window_end EMIT ON WINDOW CLOSE`. Output shape
   changed: `event_ts, rolling_1d_real_bet_amount` → `window_start, window_end, windowed_real_bet_amount`.
4. **`sink_casino_real_bet`** PK → `customer_id,currency_id,window_start`; writes to a **new** Iceberg
   table `rw_managed_casino_real_bet_windowed` (the old table's schema differs and persists in MinIO).

**⚠️ Must verify after rebuild (in priority order):**
1. **Watermark propagates through the double `UNNEST`.** If it doesn't, `EMIT ON WINDOW CLOSE` never
   fires and `mv_casino_real_bet` stays empty. Check: `SELECT count(*) FROM mv_casino_real_bet;`
   should grow once windows start closing (~window + watermark delay after start).
2. **No mass data drop.** The source watermark drops rows with `round_created_at < max - 5 min` — for
   **all** consumers incl. UC2. If `RoundCreated` times are spread out (the earlier ~1-day span in
   `mv_casino_transactions` is a warning), this could starve UC1 *and* UC2. Compare row growth vs a
   no-watermark baseline; raise the `INTERVAL` if data is lost.
3. **Throughput / backpressure / MinIO CPU** vs baselines (1d: ~67/s; 5-min rolling §13: ~120/s with
   MinIO spiking to 360%). The win would be: throughput holds *and* MinIO CPU drops (no retraction churn).

**Known out-of-scope breakage:** the Grafana UC1 panels query the old columns/table and will error
until updated — deferred to the full restructure if this prototype proves out. Measure via Prometheus,
not the dashboard.

---

## 15. UC1 TUMBLE via Kafka round-trip (workaround for §14 — implemented, pending measurement)

**Idea (answers "can we do all unnesting in a first-level MV?"):** Yes — and then **round-trip the
flat stream through Kafka** to re-acquire a watermark. §14 was blocked because `UNNEST` strips the
watermark and an MV can't re-declare one. But a *new TABLE* can. So:

```
mv_casino_transactions   (UNNEST — unchanged, still shared with UC2)
  → mv_casino_bets_flat            filter UC1 bets + project {customer_id, currency_id,
                                   transaction_created_at, amount_abs}
  → sink_casino_bets_flat_kafka    → Redpanda topic 'casino_bets_flat' (JSON, append-only)
  → src_casino_bets_flat           re-ingest as TABLE: transaction_created_at is now a TOP-LEVEL
                                   column → WATERMARK FOR it (no UNNEST downstream → survives)
  → mv_casino_real_bet             TUMBLE(src_casino_bets_flat, transaction_created_at, '15 min')
                                   + EMIT ON WINDOW CLOSE  ✅ binds
```

Uses the **true transaction event-time** (better than §14's round-time approximation). Files: new
`mv_casino_bets_flat.sql`, `sink_casino_bets_flat_kafka.sql`, `src_casino_bets_flat.sql`; rewritten
`mv_casino_real_bet.sql` (TUMBLE); `sink_casino_real_bet` → PK `...,window_start`, table
`rw_managed_casino_real_bet_windowed`; raw mirror in `sql/casino_prd_funnel_iceberg.sql`; topic
`casino_bets_flat` added to `redpanda-init`. **Requires `dbt parse`** (new models + changed ref).

**⚠️ Honest trade-off (must measure):** the round-trip adds load — re-serializing the bet stream to
JSON, a Redpanda hop, re-parsing, and a second table with its own Hummock state — on an already
MinIO/compute-bound single-node box. TUMBLE removes retraction churn; the round-trip adds ingestion
churn. **Net vs the §13 5-min rolling window (~120/s) is unknown until measured.** Verify per the plan:
(1) windows actually close (`SELECT count(*) FROM mv_casino_real_bet` grows ~window+5min in — else the
watermark didn't take); (2) `rpk topic consume casino_bets_flat -n 5` shows data; (3) throughput /
backpressure / MinIO CPU over >15 min vs baselines.

---

### Outcome (measured 2026-06-01) — decoupled-rolling shipped

| Setup | `src_casino_prd` throughput | backpressure | MinIO CPU |
|-------|------------------------------|--------------|-----------|
| §13 rolling **inline** (reads `mv_casino_transactions`) | ~120/s | ~6.9 | ~360% |
| rolling **decoupled** (reads `src_casino_bets_flat`) — **shipped** | **~380/s** (bursty 60–620) | ~5 | ~150% (peaks 328%) |
| TUMBLE **decoupled** | ~300/s | ~6 | ~400% |

**The lift (120 → ~380/s, ~3×) is the Kafka round-trip DECOUPLING, not TUMBLE.** Putting the UC1 window
behind a Kafka buffer takes it off `src_casino_prd`'s backpressure path; rolling-decoupled ≈
TUMBLE-decoupled on throughput, so TUMBLE's 20-min EOWC latency + watermark/UNNEST complexity buys
nothing here. **Shipped config: decoupled-rolling** — `mv_casino_real_bet` is a 5-min rolling `RANGE`
window reading `src_casino_bets_flat`, keeping continuous semantics and the original column/table
schema (Grafana UC1 works). The round-trip infra (`mv_casino_bets_flat` → Kafka `casino_bets_flat` →
`src_casino_bets_flat`) is retained for the decoupling; the TUMBLE (§14/§15) was reverted.

**~380/s is a MinIO state-store-churn ceiling** — the no-sink rolling-decoupled run still hit ~500%
MinIO, so the churn is **Hummock state**, not the Iceberg sink (dropping it, option 2, won't help).
**Raising `source_rate_limit` doesn't help either:** at steady state backpressure sits ~5
(downstream-bound, not rate-limit-bound — we hit ~800/s only with an empty window), so more input
just grows lag and risks the MinIO crash/reset (§1). Exceeding ~380/s needs relieving Hummock churn
(query-time windowing over append-only Iceberg) or horizontal storage — not a laptop move.

---

## 16. Hummock state store → local filesystem (demo-only — implemented, pending measurement)

**Files:** `docker-compose.yml` (state-store arg + shared volume), `bin/6_down.sh` (volume cleanup).

**Why:** MinIO is the ~380/s ceiling and it's **CPU-bound** on per-request processing for the heavy
Hummock SST flush/compaction churn — not disk-bandwidth-bound. RisingWave's Hummock supports a
**local-filesystem backend** for single-node deployments, which removes the S3/HTTP + MinIO-CPU layer
for the bottleneck workload (writes go straight to disk via the OS page cache). MinIO **stays** —
now lightly loaded — for the Iceberg warehouse + proto files.

**What changed:**
- `meta-node-0` `--state-store`: `hummock+minio://…@minio-0:9301/hummock001` → **`hummock+fs:///root/state_store`** (`--data-directory hummock_001` kept → subdir under the fs path).
- New named volume **`hummock-fs-store`** mounted at the **same path `/root/state_store` on every
  Hummock-touching node**: `meta-node-0`, `compute-node-0`, `compactor-0`, `compactor-1`. (frontend
  delegates to compute — mount there only if batch queries error.)
- `bin/6_down.sh` removes `hummock-fs-store` alongside the other volumes; `bin/1_up.sh` needs no
  change (compose `up` auto-creates the declared volume).

**⚠️ Risk — multi-container local-fs:** docs warn local-fs is "incompatible with distributed mode"
(separate containers treating their own fs as local → inconsistency). The mitigation is the **shared
Docker volume** mounted at the same path on all four nodes, giving one coherent fs. This is
**non-standard** — if MV builds, compaction, or queries throw "object not found"/version errors,
**revert to MinIO** (see Rollback). Fallback for a real fix: RisingWave `single_node` standalone mode
(one process, native local-fs).

**Clean slate:** switching the backend abandons the old MinIO-resident Hummock state; the meta store
(`postgres-0`, SQL-backed) must reset with it. "⛔ Stop Everything" (`bin/6_down.sh`) already removes
`postgres-0` + all volumes, so the flow is just **Stop Everything → Start Services → run the casino
job**.

**Measured (2026-06-01) — ✅ decisive win.** ~10 min sustained at **~770–800/s** (the rate-limit
cap) with **backpressure ~0** and **MinIO idle (~1–7%)** — vs decoupled-rolling's ~380/s bursty at
bp~5, and §13's ~120/s. **Zero coherence errors** across the run; SSTs wrote cleanly to local disk;
no instability. The MinIO state-store bottleneck is **eliminated** — the pipeline is now
**rate-limit-bound**, not storage-bound.

**Follow-on changes (shipped together with the local-fs switch):**
- **SST reclaim (`risingwave.toml`):** the local state dir grew **~3 GB/min at 800/s** (≈24 GB in
  8 min — would fill the laptop disk in ~10 min). Lowered `min_sst_retention_time_sec` 21600→600,
  `full_gc_interval_sec` 3600→300, `periodic_space_reclaim_compaction_interval_sec` 3600→60 so stale
  SSTs vacuum quickly. **⚠️ Reverted — see §18:** at 800/s the reclaim couldn't keep pace (disk still
  grew) *and* the frequent compaction caused backpressure spikes, so loose retention was restored.
- **Rate limit (`orchestration/definitions.py` ramp) — measured, kept at 200:** with MinIO gone the
  pipeline is rate-limit-bound at bp~0, so the cap finally matters. **Tested 400 (≈1,600/s cap) and it
  OVERSHOT:** it briefly touched 1,600/s, then compute saturated (~6 of 10 cores — the double-`UNNEST`
  + window-state + UC2 + round-trip), backpressure rose to ~5, and throughput **thrashed DOWN to
  ~515/s** (worse than the lower limit), while disk climbed ~2.4 GB/min. A later probe at **250
  (≈1,000/s)** pinned the ceiling more precisely: it reached **~980/s at bp~0** but then **decayed to
  ~670/s with backpressure climbing to ~6** — so even 1,000/s overshoots. **Reverted to 200.**
  **Conclusion: ~800/s (limit 200) is the sustainable compute ceiling** on this single-node stack —
  it holds at bp~0 with CPU/disk headroom. The full ~1,500/s topic **cannot** be sustained here
  (compute-bound, not storage-bound now); **800/s (~6.7× §13's 120/s) is the stable max.**

---

## 17. E2E best-practices review (applied — pending measurement)

A pass over the full UC1 + UC2 chains against the RisingWave best-practices rules. Already-correct
patterns left as-is: `APPEND ONLY` sources, shared source, `ROW_NUMBER` top-1 in the `_latest` MVs
(not `ORDER BY`), `UNION ALL + GROUP BY` for the ratio (avoids the join panic, §5.5), decoupled-rolling
+ local-fs. Three changes applied — all semantics-preserving and each *reduces* write/compute load:

1. **Projection-prune `mv_casino_transactions` 14 → 6 columns** (the **shared** foundation, so it helps
   both chains). Dropped `message_created_at, transaction_id, amount_raw, bonus_action,
   game_id/game_type/is_live, company_id` — including the per-row `GameInfo` struct accesses and an
   unused `TO_TIMESTAMP`. The two consumers' filter `amount_raw IS NOT NULL AND <> ''` → **`amount_abs
   IS NOT NULL`** (equivalent: `amount_abs` is NULL iff `Amount` was empty/null), which is what lets
   `amount_raw` be dropped. Cuts per-row width, struct-extraction CPU, and state-store I/O pipeline-wide.

2. **Dropped the two serving-MV indexes** (`idx_casino_real_bet_customer`,
   `idx_turnover_percentage_customer`). Per `perf-indexes-on-mv`, indexes only help **point lookups** —
   but the Grafana dashboard does **only full-scan / top-N / aggregate** queries (zero
   `WHERE customer_id = ?`). So they gave no read benefit while adding incremental-maintenance write
   overhead on every MV update — pure cost on a write/compute-bound pipeline.

3. **`force_compaction = true` on the two Iceberg upsert sinks** (`sink_casino_real_bet`,
   `sink_turnover_percentage`). Per `sink-force-compaction`: the rolling sum changes every event, so a
   key gets many upsert updates per barrier; `force_compaction` collapses them to one-per-key-per-barrier
   → fewer Iceberg upsert deltas → less MinIO write + compaction. *(Verify the Iceberg sink accepts it;
   drop the option if it rejects at DDL time.)*

**Expected impact:** modest per-row / per-update savings, not a ceiling-mover (the ~800/s compute
ceiling is the double-`UNNEST` fan-out + window-state, which these don't remove). **Deferred** options:
sportsbook `TUMBLE`+EOWC (low-volume, not worth it) and decoupling UC2 casino-turnover (lower ROI;
it's still inline on the production path but lower-volume and we're compute-bound). **A/B after rebuild.**

---

## 18. SST retention reverted to loose (compaction spikes outweighed the disk benefit)

The §16 aggressive SST reclaim (`min_sst_retention_time_sec=600`, `full_gc_interval_sec=300`,
`periodic_space_reclaim_compaction_interval_sec=60`) was **reverted to the loose defaults**
(`21600 / 3600 / 3600`). At the ~800/s steady rate it delivered the **worst of both**:

- it **didn't bound the disk** — reclaim couldn't keep pace; the local-fs state dir grew ~3 GB/min
  to **~20 GB over a ~12-min run**; and
- the forced compaction caused backpressure that **escalated from transient spikes into sustained
  red** (bp 3.5 → ~6, no longer recovering), dragging throughput **down from ~800/s to ~280–450/s**
  — clearly visible on the §17 Grafana panel. (Measured in the final test, 2026-06-01.)

**Decision:** for a **short demo** (the actual use case — runs ~10 min, then stopped), loose retention
is better: **smooth throughput** (bp stayed 0–0.85, no compaction spikes) and the disk doesn't fill
within the run window anyway. The disk-bounding aggressive retention was *meant* to provide only
matters for long-running deployments — where the real fix is horizontal storage, not retention tuning.
**Operational note:** on a long run, watch disk and stop/restart before it fills (~10–13 min at 800/s).

**Compaction bursts are inherent to local-fs — and not a retention/rate-limit artifact.** Even on
**loose retention at 600/s**, the growing local-fs LSM periodically triggers a heavy leveled
compaction (`compactor-0` ~290%) that momentarily competes with the streaming compute and
backpressures it — observed **bp spiking to ~4 with throughput dipping to ~170/s, then recovering to
green within ~1–2 min**. These are **transient and self-recover**; lowering the rate limit only delays
state growth, it doesn't eliminate them. **Reading the §17 backpressure panel:** a **spike that
returns to green = a compaction burst (normal, ignore)**; a line that **climbs and stays red = real
overload** (e.g. rate limit above the ~800/s compute ceiling). This is the local-fs trade-off vs MinIO
state store: higher throughput (~800/s vs ~380/s) in exchange for occasional self-recovering
compaction dips.

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
| minio-0 memory | `docker-compose.yml` | `4 G / 1 G` (§12, experimental) |
| `MINIO_SCANNER_SPEED` | `docker-compose.yml` | `slowest` (§12, experimental) |
| Trino `depends_on` | `docker-compose.yml` | `lakekeeper-bootstrap` completed |
| Grafana refresh | `casino-uc-metrics.json` | `1m` |

## If MinIO saturates again

The escape hatches, in order of preference: lower `source_rate_limit` (200 → 100), raise
`commit_checkpoint_interval` (20 → 40), or drop `opendal_upload_concurrency` (4 → 2). The
underlying cause is always the single-node MinIO; the durable fix is real horizontal storage,
not config.
