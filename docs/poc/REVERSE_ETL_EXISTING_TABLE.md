# Reverse ETL from an existing Delta table in Databricks

The demo syncs all create their own source tables (the setup asset's `CREATE IF
NOT EXISTS`). Pointing the same pipeline at a table that already exists changes
only the definition and the first run — the sync machinery is identical. This
note records the steps and the traps. Written 2026-10-20.

## 1. Verify the table meets the reader's requirements

- **Change Data Feed is NOT needed** — that is the whole design: the delta-rs
  reader reconstructs changes from version diffs (append fast path,
  rewritten-file-pair diff, full-snapshot fallback; see
  [REVERSE_ETL_CODE_WALKTHROUGH.md](REVERSE_ETL_CODE_WALKTHROUGH.md) §6 and
  [REVERSE_ETL_SPARK_FREE_FINDINGS.md](REVERSE_ETL_SPARK_FREE_FINDINGS.md) §6).
- **Deletion vectors must be off — and to have never been on.** The reader
  refuses DV tables outright: a row hidden by a DV inside a Parquet file would
  silently corrupt the diff. The check is on the table's Delta *protocol*
  features, and a protocol feature is never removed — so a table created (or
  ever altered) with DVs enabled cannot be made reader-acceptable by
  disabling them afterwards, even after `REORG PURGE` clears every file
  (verified live 2026-10-20: writer v7 keeps `deletionVectors` listed and the
  reader still refuses). See the remediation below for what actually works.
- **UniForm is irrelevant** — the sync is UniForm-agnostic end to end (the
  `delta` sync exists to prove exactly that).
- **Column mapping** — only needed if you want drop/rename propagation. Without
  it, keep `propagate_drops: false`; column adds still flow.
- **Pick the key.** The reader diffs keyed on one column; choose the table's
  real primary key (unique, non-null). The demo's `rid` identity column will
  not exist on a real table.
- **Storage access.** The service principal needs direct read on the table's
  ADLS location (the same mechanism the demo syncs already use — no warehouse,
  no DBUs).

### The checks, as SQL (run in a Databricks SQL editor)

```sql
-- Table properties: what to look for —
--   * NO 'delta.deletionVectors.deletionVectorsEnabled' = 'true'
--     (and no 'delta.feature.deletionVectors' = 'enabled'/'required')
--   * 'delta.columnMapping.mode' = 'name' present only if you want drop/rename
--     propagation
--   * Change Data Feed is irrelevant: the reader does not use it
SHOW TBLPROPERTIES de_dev.my_schema.customers_raw;

-- Protocol versions and layout at a glance
DESCRIBE DETAIL de_dev.my_schema.customers_raw;
```

The reader enforces the DV rule itself: it inspects the table's Delta protocol
features (not the tblproperties) and fails the sync with a clear `RuntimeError`
if `deletionVectors` is among them — the SQL checks above just predict that.

```sql
-- The key must be unique and non-null: the reader diffs rows keyed on it
SELECT COUNT(*)                          AS total_rows,
       COUNT(DISTINCT customer_id)       AS distinct_keys,
       COUNT(*) - COUNT(customer_id)     AS null_keys
FROM de_dev.my_schema.customers_raw;
```

```sql
-- Find the table's storage location, then make sure the service principal
-- can read it (this is what the sync's delta-rs reads go through)
DESCRIBE EXTENDED de_dev.my_schema.customers_raw;
GRANT READ FILES ON EXTERNAL LOCATION `<location-url>` TO `<service-principal>`;
```

### Remediation SQL (only if a check fails)

```sql
-- Deletion vectors ENABLED? Disabling alone does NOT help the sync —
-- verified live 2026-10-20: ALTER SET '...' = 'false' + REORG PURGE leaves
-- the deletionVectors feature in the table's protocol, and the reader
-- refuses on the protocol (features are never removed). The sync-compatible
-- fix is a one-time rewrite into a fresh table created with DVs disabled at
-- creation — that protocol has no deletionVectors feature (it is how every
-- demo table is built, and the reader reads those daily). One-time warehouse
-- read+write: the only DBU cost in this whole setup.
CREATE OR REPLACE TABLE de_dev.my_schema.customers_raw_dvfree
USING DELTA
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'false'
  -- + 'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2',
  --   'delta.minWriterVersion' = '5'  if drop/rename propagation is wanted
)
AS SELECT * FROM de_dev.my_schema.customers_raw;
-- Then either point the sync's source_table at the copy, or swap names so
-- future writes land in the DV-free table (if the old table name is
-- recreated, mind the state-table drop+recreate trap below).

-- Want drop/rename propagation on a table without column mapping?
-- Metadata-only, but it bumps the protocol: pre-DBR-12 readers stop working.
ALTER TABLE de_dev.my_schema.customers_raw SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion'   = '2',
  'delta.minWriterVersion'   = '5'
);

-- Optional, only if another engine (e.g. StarRocks) must read the table
-- through UniForm's Iceberg metadata — the sync itself does not care:
ALTER TABLE de_dev.my_schema.customers_raw
  SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');
```

### The deletion-vectors trade-off (before running the ALTER)

DVs are Databricks' default row-level delete mechanism: DELETE/UPDATE/MERGE
record compact per-file bitmaps of dead rows instead of rewriting the affected
Parquet files, and reads filter them out. The trade-off is cheap writes vs.
read interop — and the sync needs the interop.

What disabling costs on an existing table:

- **Fast point-DML — the real loss.** Every DELETE/UPDATE/MERGE reverts to
  rewriting each affected file in full: slower DML, more write I/O, more
  compute, more OPTIMIZE work afterward.
- **A one-time rewrite burst** — `REORG TABLE … APPLY (PURGE)` rewrites every
  file that still carries DVs (proportional to DV-carrying files, not table
  size).
- **The protocol stays bumped** — disabling does not undo reader-v3/writer-v7;
  the lock-out of pre-feature readers is permanent either way (irrelevant for
  the sync: delta-rs reads v7 tables, it just cannot have DVs enabled).

What disabling does *not* cost:

- **Data** — no rows are touched; only future DML strategy changes.
- **Read performance** — scans get marginally simpler (no bitmap application,
  no dead rows read).
- **Re-enabling** — turning DVs back on later is just the property again; new
  DML starts creating DVs from that point.

The asymmetry that matters for the sync: **DVs off from creation keeps the
table readable by the sync; DVs enabled at any point puts the feature in the
table's protocol permanently, and no ALTER removes it.** Disabling afterwards
makes the table DV-*free* but not reader-acceptable — a table that has ever
had DVs enabled can only serve the sync after the one-time rewrite into a
fresh DV-free table shown in the remediation above.

Decision heuristic: an **append-mostly** source (inserts plus occasional
updates — the common reverse-ETL shape) loses almost nothing by disabling,
since updates already rewrite whole files without DVs. A **DML-heavy** source
(CDC-style MERGEs, frequent deletes) genuinely benefits from DVs — weigh the
rewrite cost honestly, but note there is no middle ground: the reader refuses
DV tables outright, a once-enabled table cannot be un-refused, and the
alternatives (a DV-free copy refreshed by scheduled warehouse CTAS, CDF feeds
read by Spark, a Spark-based reader) all give up more than the DVs were worth.

## 2. Define the sync (one YAML)

```yaml
# orchestration/defs/reverse_etl_customers/defs.yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync

attributes:
  name: customers            # label — all object names derive from it
  catalog: de_dev
  schema_name: my_schema     # the existing table's schema
  source_table: customers_raw   # OVERRIDE: point at the existing table (bare name)
  key_column: customer_id       # the table's real PK, not the demo's rid
  encoding: avro                # or json
  propagate_drops: false        # true only if the source has column mapping
  sink_tasks_max: 4             # raise for the initial backfill
  # NO seed_statements — leave unset for a real table (component docstring)
```

`source_table` is a bare table name within `catalog.schema` (the config builds
`catalog.schema.source_table`). Optional overrides if the derived names clash:
`kafka_topic`, `postgres_table`, `risingwave_table`, `connector_name`,
`group_name`.

## 3. Restart Dagster and run the setup job

`docker compose restart dagster-webserver dagster-daemon` (the code server must
reload definitions), then run `reverse_etl_customers_setup_job`. It creates
everything *around* the table:

- the watermark state table (`reverse_etl_customers_state`, deliberately plain
  Delta — no column mapping, DVs off, single SQL writer) — note it lands in the
  same `catalog.schema` as the source, clearly namespaced;
- the Kafka topic, the RisingWave target, and the Debezium JDBC sink connector;
- the registry subjects, when `encoding: avro`.

The source creation is `IF NOT EXISTS` — the existing table is untouched and
its properties are not altered.

The state table the job creates (exactly what to expect in your schema):

```sql
CREATE TABLE IF NOT EXISTS de_dev.my_schema.reverse_etl_customers_state (
    sync_name STRING, last_commit_version BIGINT, last_schema STRING
) USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
```

It is deliberately plain Delta, single-writer: the sync's delta-rs no-op
pre-check reads the watermark from it, and delta-rs cannot commit to
Databricks-written v7 protocol tables — so the only writer is one small SQL
`INSERT` per sync (below). Verify after the job:

```sql
SHOW TABLES IN de_dev.my_schema LIKE 'reverse_etl_customers*';
```

## 4. First sync = initial backfill

The watermark starts at 0, so the first sync walks the table's history and
emits its current state as a stream of `c` events — Postgres and RisingWave
fill up through Kafka. This is the documented O(history) first sync, a one-off;
`REVERSE_ETL_CDF_CHUNK_VERSIONS` chunks it and the per-chunk heartbeats show
progress in the run metadata. With `sink_tasks_max` raised, the JDBC sink keeps
up with the load.

Watching it from Databricks — the watermark, and the source's current version.
After a completed sync, `last_commit_version` equals the source's max version:

```sql
-- The watermark (append-only table; take the freshest row for the sync)
SELECT sync_name, last_commit_version
FROM de_dev.my_schema.reverse_etl_customers_state
WHERE sync_name = 'customers'
ORDER BY last_commit_version DESC
LIMIT 1;

-- The source's current version (top of the commit history)
DESCRIBE HISTORY de_dev.my_schema.customers_raw LIMIT 5;
```

For reference, the one SQL statement the sync itself runs on Databricks at the
end of every run (append-only; readers take the freshest row per `sync_name`):

```sql
INSERT INTO de_dev.my_schema.reverse_etl_customers_state
VALUES ('customers', <new_version>, '<last_schema-json>');
```

## 5. Schedule it

The same pattern as the demo's recurring job
([REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md) §6): a
scheduled Dagster run of the sync job. Every subsequent sync reads only the
commits since the watermark — change-proportional, warehouse-free, zero DBUs.

The periodic health check is the same pair of queries from §4: the state
table's `last_commit_version` tracks the source's top `DESCRIBE HISTORY`
version, and `last_schema` reflects the source's current columns (that is the
drop-propagation evidence). If the watermark stalls while `DESCRIBE HISTORY`
grows, the sync runs are failing — check Dagster before Databricks.

## Safety notes

- **The reset job refuses to run for this sync.** `_assert_reset_allowed`
  requires every name it drops (including `source_table`) to start with the
  config's `reset_name_prefixes` (default `reverse_etl_`) and to live in
  `reset_schema`. A real table's name fails that check → `RuntimeError`, by
  design. Never widen the allowlist to include a real table.
- **Backfill volume** — every existing row flows through Kafka once; size topic
  retention, message size limits, and sink batching accordingly.
- **Schema changes** — column adds propagate automatically (the targets alter,
  and with Avro the registry gets a new schema version *before* the alter).
  Drops/renames propagate only if the source has column mapping on and
  `propagate_drops: true`.
- **Do not run two syncs against the same source table** — each sync owns its
  watermark; two syncs on one table would both emit the same changes to their
  own topics (which may be what you want for two independent targets, but it
  doubles the reads).

## The state table

`reverse_etl_<name>_state` is the sync's only Databricks-side bookkeeping:
`sync_name`, `last_commit_version` (the watermark), and `last_schema` (JSON of
the source's columns with their `delta.columnMapping.id`s — the evidence the
drop-propagation pass diffs against).

- **Write pattern: append-only.** One `INSERT` per sync run; readers take the
  freshest row per `sync_name` (`max(last_commit_version)` wins). History
  accumulates harmlessly — one row per run, a few hundred rows a year on a
  daily schedule.
- **Single writer, by constraint.** The sync's delta-rs no-op pre-check *reads*
  this table, and delta-rs cannot commit to Databricks-written v7 protocol
  tables — so the only writer is the one small SQL `INSERT` at the end of each
  sync. This is why the table must stay plain Delta (no column mapping, no
  deletion vectors — delta-rs rejects those reader features) and must never be
  `CREATE OR REPLACE`d (that would inherit the source's bumped protocol).
- **The watermark key is `sync_name`.** Change a sync's `name`/`sync_name` and
  its state rows stop matching: the next sync finds no watermark and starts
  from 0 — a full backfill.

### The drop+recreate trap

If the **source** table is dropped and recreated while the state table
survives, the recreated table's version numbering restarts at 0 — below the
watermark. Every sync then reads 0 changes, and because the freshest state row
wins, the low fallback watermark the empty run writes never beats the stale
high row. **The sync stalls silently until the new source accumulates more
versions than the old watermark ever reached, and versions below it are never
synced.**

Re-baseline by clearing the state (the next sync then backfills from v0 —
safe, the targets upsert by key):

```sql
DELETE FROM de_dev.my_schema.reverse_etl_customers_state
WHERE sync_name = 'customers';
-- or drop the table entirely; the setup job recreates it
```

Related: a manual drop+recreate also moves a managed table to fresh storage,
while the sync caches storage locations for 10 minutes — a stale entry points
at the *old* table's files. The reset job clears the cache deliberately; after
a *manual* drop+recreate, either wait out the TTL or delete
`$DAGSTER_HOME/dagster_storage/reverse_etl_control_plane_cache.json` before
syncing.

## See also

- [REVERSE_ETL_CODE_WALKTHROUGH.md](REVERSE_ETL_CODE_WALKTHROUGH.md) — the
  sync's read paths, schema flow, and setup/reset assets.
- [REVERSE_ETL_SPARK_FREE_FINDINGS.md](REVERSE_ETL_SPARK_FREE_FINDINGS.md) —
  why no CDF/Spark, the reader's scaling behavior (§6).
- [REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md) — measured
  costs, the recurring-job audit (§6).
- [REVERSE_ETL_LIVE_DEMO.md](REVERSE_ETL_LIVE_DEMO.md) — operating the demo
  chain these steps reuse.
