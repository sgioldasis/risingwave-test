# REVERSE_ETL_SPARK_FREE_FINDINGS.md — the reverse-ETL syncs without CDF or Spark

**2026-10-19.** The refactor that removed both extraction backends — the local
JVM (`extraction: oss_spark`, `reverse_etl_spark_reader.py`) and the
`table_changes()` SQL path — and replaced them with one delta-rs backend that
reconstructs the change feed from snapshot diffs alone. This doc records what
was built, what was verified live, the delta-rs findings it surfaced, the
measured performance against the Spark era, and the assessment of where each
approach belongs. Companion docs: `REVERSE_ETL_DBU_COST_NOTES.md` (the cost
experiments this graduated from), `REVERSE_ETL_CODE_WALKTHROUGH.md` §6 (the
backend's internals), `REVERSE_ETL_JOBOTRON.md` (the full-snapshot producer).

## 1. What changed

- **Deleted**: `reverse_etl_spark_reader.py` (a Spark session bootstrapped
  inside the Dagster webserver container), the `table_changes()` read path, the
  `update_preimage`/`update_postimage` collapse (the reconstruction yields
  before/after pairs directly), and the `extraction` config field with its
  dispatch. The four syncs' `defs.yaml` files lost their `extraction` line.
- **New**: `orchestration/assets/reverse_etl_delta_reader.py` (~400 lines,
  append fast-path included).
  For each commit version in the requested range: diff the snapshot at v-1
  against the snapshot at v → CDF-shaped rows (`insert`,
  `update_preimage` + `update_postimage`, `delete`), ordered by
  `_commit_version`, pinned to the version the watermark advances to.
- **Kept unchanged**: the Debezium envelope building, Kafka production,
  RisingWave + JDBC-sink targets, drop propagation, chunked backfill
  (`_version_chunks`, `REVERSE_ETL_CDF_CHUNK_VERSIONS`), the delta-rs no-op
  pre-check, and the Databricks notebook comparison path (Spark by definition,
  deliberately kept as the baseline).
- **The one warehouse touch left**: the watermark write — a single small SQL
  INSERT per sync that moved data (~1.5-1.7s, metered). delta-rs cannot commit
  to the warehouse-created state table (this runtime stamps rowTracking and
  domainMetadata writer features delta-rs 1.6 refuses); the state table keeps a
  single writer by design.

## 2. Verified delta-rs findings

1. **delta-rs 1.6.6 returns all-None columns for warehouse-written
   column-mapped sources.** Column mapping as a *writer* feature with no reader
   features, and `to_pyarrow_table()` produces the logical schema with every
   value None — the physical→logical rename (`delta.columnMapping.physicalName`)
   is not applied. Verified on **both** flavors the demo creates: UniForm v2
   (`icebergCompatV2` + `universalFormat=iceberg`) and the plain column-mapped
   one (`uniform: false`). Earlier claims that "OSS Delta reads column-mapped
   tables fine" held for delta-rs-written tables and are wrong for this shape.
2. **The fallback that makes reads correct**: the reader detects the all-None
   scan and re-reads the snapshot's parquet files directly, renaming via the
   schema's `physicalName` metadata with pyarrow's `AzureFileSystem` (the same
   service principal, no extra credentials). Deletion-vector sources are
   refused outright — file reads cannot apply DVs — and both source-creation
   paths keep DVs off anyway (see finding 3).
3. **DVs + CDF is a correctness trap the old path barely dodged** (verified
   2026-10-14, `REVERSE_ETL_DBU_COST_NOTES.md` §5): with DVs enabled, a 1-row
   DELETE made CDF emit the whole touched file — 300,000 change rows for a
   100k-row table, which relayed verbatim would have deleted 100,000 legitimate
   target rows. Snapshot diffs do not have this failure mode: a diff of two
   snapshots is exact regardless of how the engine physically rewrote files.
4. **Per-version diffing is required for correctness.** A key deleted in one
   commit and reinserted in the next must emit both events; a diff over a whole
   version range would net them out to nothing. The cost is one extra snapshot
   load per version, amortized by carrying the previous snapshot forward.
5. **delta-rs history() is unreliable on version-pinned handles** (verified
   2026-10-19). Pinned below the table's latest version, `history()` returns the
   full log with every entry's `version` field offset by the pin, and
   `history(1)` reports the *newest* commit's operation and timestamp under the
   pinned version's label. The full log through a *current* (non-pinned) handle
   is correct. The reader therefore builds its per-version commit-timestamp map
   from the current handle's `history()`, keyed by each entry's version — the
   first implementation used `history(1)` per version, and every event from a
   multi-version read carried one wrong `_commit_timestamp`.
6. **The schema a sync names its events with must be pinned to the same version
   as the data it read** (verified live 2026-10-19, the rename-column demo). The
   sync pinned its read range from the pre-check, but read the envelope's schema
   "live" — and a `RENAME COLUMN` committed *between* the two reads named the
   envelope's columns with the new schema while the rows still carried the value
   under the old name. `_coerce_row` projects by `row.get(name)`, so the
   populated value was silently replaced with null, the message carried
   `renamed_col_1: null` in both images, and the state row recorded the new
   schema — the next sync saw no rename to propagate, so the loss was permanent.
   Fix: `get_row_fields`/`get_column_ids` take a version, and the sync pins both
   to the chunk's top commit (this deployment sets single-version chunks,
   `REVERSE_ETL_CDF_CHUNK_VERSIONS=1`; the code default is 10, and a populate
   plus a rename inside one multi-version chunk can still misname). Re-verified
   after the fix: a populated renamed column reaches Postgres and RisingWave
   with its value intact.
7. **Three sync-flow defects the rename demo surfaced in one afternoon**
   (verified live 2026-10-19, all fixed and re-verified with the exact
   add → populate → rename → one-sync sequence):
   - **0-row chunks skipped the watermark.** The chunk loop `continue`d past
     the watermark update when a commit produced no rows, so a schema-only
     commit landing after the last data event in the same sync was read but
     never watermarked -- the watermark stranded on the last data commit and
     the change's propagation deferred to the next sync (the user's "rename
     didn't show in Postgres"). The watermark now advances for every chunk.
   - **The envelope schema was resolved once and reused.** `row_fields` was
     read at the first event-bearing chunk and reused for every later chunk,
     so a column added between chunks never made it into later events'
     envelopes -- `_coerce_row`'s projection silently dropped those values
     (the populate's new-column value was lost exactly this way when the seed
     backfill shared the sync). The schema is now re-read per event-bearing
     chunk, pinned to that chunk's top commit.
   - **Renames entirely inside the unsynced window were invisible.** The
     propagation diffs the state table's `last_schema` against the final
     schema -- but `last_schema` predates an add+rename that both happened
     since the last sync, so there was no recorded old name to detect. The
     sync now also diffs the schema the events were actually enveloped with
     (the last event-bearing chunk's) against the final one, and propagates
     what changed: the rename reaches the targets data-preserving in the SAME
     sync, matching the source.
8.  **Re-adding a column whose schema was registered before silently fails on
    RisingWave** (verified live 2026-10-19, the demo's second add → rename →
    drop → add cycle). Confluent-compatible registries dedupe registration by
    content: registering a schema whose bytes already exist as an OLDER version
    of the subject returns that version's schema id and does NOT create a new
    version, so the registry's latest stays wherever it was. RisingWave
    validates every `ADD COLUMN` against the registry's latest, so "the sync
    registered the schema" is not enough -- the built schema must BE the latest.
    The demo hits this on every second cycle: the re-add's envelope is
    content-identical to the first add's version, the register no-ops, and
    RisingWave answers `Column "new_col_1" is defined in SQL but not found in
    the source`. Two more registry behaviours surfaced while fixing it: the
    latest-version READ lags the writes for a few seconds (a register returned
    the new version's schema id while `get_latest_version` still reported the
    old one, which made a verify-once check fail against a state that was about
    to be true), and a SOFT-deleted version's content stays resolvable by id, so
    messages already on the topic still deserialize. Fix, in
    `_register_avro_schemas`: decide from the register's returned schema id
    against a snapshot taken BEFORE the write -- a fresh read after the write is
    useless, because the lag outlasted a 4s retry budget in live testing (the
    first fix verified the latest with `get_latest_version` after registering,
    and the check kept failing against the stale old value even though the
    register had just created the version). The returned id is authoritative:
    unknown to the snapshot = a new version was created (the latest is ours);
    matching the version that WAS the latest = the dedup landed there (the
    latest is ours); matching an OLDER version = retire the versions after it
    (soft delete) and register again -- the dedup then lands on that version,
    now the latest. Re-verified live with both failing sequences: add + populate +
    rename in one unsynced window, and then drop + add + populate + rename in one
    unsynced window (the second cycle) -- both propagate end to end, and a no-op
    sync confirms the watermark at the rename's commit.

## 3. Live proof — all four syncs, full cycles (2026-10-19)

Each sync ran: reset → setup → backfill sync → insert / update / delete change
ops, each followed by a sync. **18/18 steps green per cycle, all four syncs.**

| sync (envelope) | backfill | insert | update | delete |
|---|---|---|---|---|
| cdf (JSON) | `{c:3, d:1, u:1}` | `{c:1}` | `{u:1}` before/after | `{d:1}` preimage |
| avro (Avro wire, staging registry) | `{c:3, d:1, u:1}` | `{c:1}` | `{u:1}` before/after | `{d:1}` preimage |
| orders (JSON, extra `total` column) | `{c:3, d:1, u:1}` | `{c:1}` | `{u:1}` before/after | `{d:1}` preimage |
| delta (plain column-mapped Delta, Avro) | `{c:3, d:1, u:1}` | `{c:1}` | `{u:1}` before/after | `{d:1}` preimage |

The backfill `{c:3, d:1, u:1}` is the seed's own history (3-row MERGE insert,
then an update, then a delete) reconstructed from versions 0→3 — the same
picture `table_changes()` produced. Updates arrive as one Debezium `u` with the
full before/after (the reconstruction's preimage/postimage pair collapsed by
the unchanged `_to_debezium_events()`); deletes as `d` with the full preimage
and `after: null`. Watermarks advanced exactly one commit per cycle (3→4→5→6).

**Both targets per sync** — Postgres (Debezium JDBC sink) and RisingWave
(FORMAT DEBEZIUM) — agreed on the final state in every case (e.g. `first`,
`second-updated`; the inserted-then-deleted row gone from both), which means
upserts, deletes and ordering all survived the backend swap with zero
downstream changes.

## 4. Measured: Spark era vs now

| scenario | Spark + CDF (recorded) | delta-rs reconstruction (measured 2026-10-19) |
|---|---|---|
| Full loop, 1 update event | 36.7s including Spark startup | **16-18s** |
| Backfill of the seed history | same order | 24-27s |
| No-op sync | ~17s typical, ~8s best with the pre-check | **7-8s, `0.0` warehouse seconds** (no state write, no JVM) |
| Sync with data, Databricks cost | warehouse DBUs for `table_changes()` | **~1.5-1.7 warehouse seconds** — the watermark write only; reads free |
| 1M-row backlog (10 × 100k-row commits, the `bulk_1m` op) | 126-187s reading only change files | naive: 215s, `2.1` warehouse s — **with the append fast-path: 120s, `1.66` warehouse s**, 1,000,005 events, both targets caught up (1,000,002 rows each), no OOM on the 1.5GiB run worker |

The eliminated floor dominated at demo scale: every real sync paid Spark
session startup and planning cycles before reading a row. The reconstruction's
fixed cost is two snapshot loads. No-ops additionally cost nothing on
Databricks now, because the unchanged-version check runs before anything that
meters.

## 5. Assessment — which approach is better

**For this use case — per-operator reverse-ETL of eligibility segments
(small-to-medium dimension tables, batch cadence, worker-side dedup and
re-validation) — the non-Spark approach wins, and not closely.** The deciding
factors are not the DBUs:

1. **The worker contract neutralizes CDF's main advantage.** The RFC's own Safe
   Execution principle (stale input must not produce wrong actions; the worker
   dedups and re-validates) makes a full-snapshot producer correct by
   construction. CDF buys "only what changed", and the consumer architecture
   discards that information. The RFC's Spark/CDF producer and its own Safe
   Execution principle are in tension.
2. **The failure modes retired compound.** DV+CDF explosion, schema-change read
   errors and their bisection machinery, JVM memory inside the webserver
   container, duplicate-row state confusion — each was a live incident in this
   POC's history. The reconstruction has one job (diff two snapshots) and its
   edge cases are visible, owned code.
3. **The valuable part of CDF was the contract, not the feature.** The proof
   above shows snapshot diffs reproduce the Debezium-shaped contract exactly,
   with zero downstream changes. The feature — Databricks-managed change
   tracking — turned out to be unnecessary for every flow in this repo.

**Where CDF (with Spark or a streaming engine) remains the right tool**:

- Large, update-heavy sources with many commits per sync window — CDF reads
  only change bytes; the reconstruction reads two full snapshots per version.
- Sub-minute freshness — the reconstruction is inherently batch-per-run;
  continuous CDC wants CDF feeding a streaming engine (the repo's RisingWave /
  Debezium paths already cover that shape for streaming sources).
- Preference for vendor-supported semantics over owning the diff logic.

## 6. Honest limits — now measured, then optimized

- **The 1M-row benchmark (2026-10-19), two measurements.** The `bulk_1m` op
  loaded 1,000,000 rows in 10 × 100k-row commits (the commit size the Spark
  path proved safe); the Spark path measured 126-187s on the same load reading
  only change files.
  - **Naive reconstruction (full snapshot diff per version): 215s, `2.1`
    warehouse seconds** — it re-read ~11M row-versions (the cumulative
    snapshots) versus CDF's 1M changed rows, survived the 1.5GiB run worker
    without OOM, and still landed in the same order of magnitude as Spark.
  - **With the append fast-path (shipped after the naive measurement): 120s,
    `1.66` warehouse seconds** — 44% faster on the identical load, and the
    read-side share improved further (the 1M Avro serializations and Kafka
    production are a constant floor the optimization does not touch). Per
    commit, the reader compares file sets (a log-only read): an append-only
    commit reads only its NEW files' rows; rewritten files fall back to the
    full snapshot diff, whose base materializes lazily. Memory is now the
    commit size for append-heavy histories -- the same shape as CDF.
- **What would still hurt**: a table whose every commit rewrites the whole
  file set (the fast path never fires) and whose snapshots outgrow memory --
  the full-snapshot wall, the same one full-snapshot producers hit. A 1M-row
  **single** commit is also the same memory ceiling the Spark path documented.
- **Retention truncation**: if the requested start version has aged out of the
  log, `get_earliest_available_version()` (bisection on `load_as_version`)
  drives the existing baseline-vs-backfill decision; a mid-run truncation
  between reading the watermark and the diff fails loudly and is retried on the
  next sync.
- **The columnMapping fallback reads files directly**, so it bypasses any
  future delta-rs improvements to that path; if a delta-rs release fixes the
  rename, the all-None detection simply stops triggering and the direct scan
  takes over.
