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
- **Deletion vectors are supported — one constraint.** The reader is DV-aware
  (2026-10-20): it reads each commit's real actions from the JSON log, filters
  every touched file's rows through its deletion-vector keep-mask (delta-rs
  decodes the payloads), tracks the masks per version so pre-images stay
  correct, and injects partition columns from the add actions' typed partition
  values — so **partitioned DV tables read fine too**. The one constraint: the
  sync needs the **Azure filesystem** for raw reads (delta-rs's own scan
  refuses DV tables outright, so there is no fallback); a no-Azure DV source
  is refused with a clear message. A DV-only commit re-adds the SAME data
  file, which path comparison cannot see — hence the commit-JSON reading.
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
--   * deletion vectors: 'delta.deletionVectors.deletionVectorsEnabled' or
--     'delta.enableDeletionVectors' — the sync reads DV tables (unpartitioned
--     + Azure filesystem), so DVs are fine to keep; what matters is knowing
--     whether they are on
--   * 'delta.columnMapping.mode' = 'name' present only if you want drop/rename
--     propagation
--   * Change Data Feed is irrelevant: the reader does not use it
SHOW TBLPROPERTIES de_dev.my_schema.customers_raw;

-- Protocol versions, layout and partition columns at a glance
DESCRIBE DETAIL de_dev.my_schema.customers_raw;
```

The reader detects DVs from the table's Delta *protocol* features and fails
loudly when a DV source falls outside what it can read (no Azure filesystem
for raw reads) — the SQL checks above just predict that.

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

### The deletion-vectors trade-off

DVs are Databricks' default row-level delete mechanism: DELETE/UPDATE/MERGE
record compact per-file bitmaps of dead rows instead of rewriting the affected
Parquet files, and reads filter them out. Since 2026-10-20 the sync reads DV
tables wherever it can read at all (the Azure filesystem is available), so
**you can keep DVs** — the decision is about the table's own DML workload, not
about the sync anymore.

- **Keep DVs** when the table sees real point-DML (frequent DELETE/UPDATE/
  MERGE): that is what they are for, and the sync no longer stands in the way
  — partitioned and unpartitioned alike.
- **Disable DVs** (`'delta.enableDeletionVectors' = 'false'`) when the table is
  append-mostly and you value the simplest protocol: without DVs, updates
  already rewrite whole files anyway, so nothing of value is lost. What
  disabling costs on a DML-heavy table: every DELETE/UPDATE/MERGE reverts to
  rewriting each affected file in full (slower DML, more write I/O, more
  OPTIMIZE work afterward), plus a one-time `REORG TABLE … APPLY (PURGE)` burst
  that rewrites files still carrying DVs (proportional to DV-carrying files,
  not table size).
- **The protocol note, for completeness**: a table that has ever had DVs
  enabled carries the feature in its Delta protocol permanently (verified live
  2026-10-20 — disabling + PURGE does not remove it). That no longer blocks
  the sync, but it does keep pre-feature readers locked out; disabling cannot
  undo it, and re-enabling later is just the property again.

Historical note: before 2026-10-20 the reader refused DV tables outright, and
a once-enabled table could only serve the sync after a one-time rewrite into a
fresh DV-free table. The DV-aware reader removed that wall entirely — first
for unpartitioned sources, then (same day) for partitioned ones, once the raw
reads learned to inject partition columns from the add actions.

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
  # NO source_columns — optional; unset means "existing table": the setup asset
  # only checks it exists (and fails if it does not) instead of creating it, and the
  # sync gets no make_change asset or job (that helper needs source_columns and an `id` key)
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

## A worked example, from zero (defaults only)

A complete runbook: create a realistic table with pure Databricks defaults —
no special properties — define the sync, and walk every Dagster action end to
end. Every command is copy-pasteable; the only file edit is the YAML in Step 2.

### Step 1 — Create and seed the table (Databricks SQL editor)

```sql
CREATE TABLE de_dev.sr_poc_external.reverse_etl_demo_source (
    id BIGINT NOT NULL,
    description STRING,
    total DOUBLE,
    updated_at TIMESTAMP
)
USING DELTA;

INSERT INTO de_dev.sr_poc_external.reverse_etl_demo_source VALUES
(1, 'first',  9.5,  current_timestamp()),
(2, 'second', 20.0, current_timestamp()),
(3, 'third',  7.25, current_timestamp());
```

No `TBLPROPERTIES`, no opt-outs. The runtime defaults give this table deletion
vectors (enabled), a writer-v7 protocol (`deletionVectors`, `rowTracking`,
`domainMetadata`) and no Change Data Feed — and the sync reads it exactly as
is: the DV-aware reader applies the keep-masks, and CDF is never used. Naming
the table `reverse_etl_demo_source` means everything derives from the sync's
`name` — no `source_table` override — and the reset job's allowlist covers it.
Seeding before the setup job means the setup's own sync backfills these rows,
and both targets populate in that one run.

### Step 2 — Define the sync (the full YAML file)

Create `orchestration/defs/reverse_etl_demo/defs.yaml` with exactly this
content:

```yaml
# The from-zero demo sync (docs/poc/REVERSE_ETL_EXISTING_TABLE.md): one
# Databricks Delta table synced to Kafka, then to Postgres and RisingWave.
# The source table is created and seeded by hand with default table
# properties (Step 1); the setup job creates everything else.
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync

attributes:
  name: demo
  catalog: de_dev
  schema_name: sr_poc_external
  source_columns:
    - "id BIGINT NOT NULL"
    - "description STRING"
    - "total DOUBLE"
    - "updated_at TIMESTAMP"
  encoding: avro
  propagate_drops: false
```

That is the whole file: every other name (`reverse_etl_demo_source` / `_state`
/ `_topic` / `_target` / `_sink`, the setup/reset/make-change jobs) derives
from `name`, and `source_columns` is ignored while the table already exists.
`encoding: avro` puts the messages in the Confluent wire format with the
schemas in the staging registry (`reverse_etl_demo_topic-key` / `-value`
subjects); `json` embeds the schema in every message instead.

### Step 3 — Load the definitions (restart Dagster)

```bash
docker compose restart dagster-webserver dagster-daemon
```

Wait until `localhost:3000` responds again (~30s), then confirm the new
definitions are there: Assets shows the `reverse_etl_demo_*` assets and Jobs
shows `reverse_etl_demo_setup_job` / `_reset_job`. A restart is needed
whenever definitions change; the code server picks the files up on reload.

### Step 4 — Run the setup job (Dagster UI)

Jobs → `reverse_etl_demo_setup_job` → **Launch run**. In order, it:

1. creates the watermark state table `reverse_etl_demo_state` (plain Delta —
   DDL in "The state table" section below);
2. creates the Kafka topic `reverse_etl_demo_topic` and registers the
   `reverse_etl_demo_topic-key` / `-value` Avro subjects in the staging
   registry;
3. runs the **first sync** — a backfill emitting the three seeded rows as
   insert events to the topic;
4. creates the RisingWave target `reverse_etl_demo_target` and the Debezium
   JDBC sink connector, which consume the topic from the beginning — so the
   backfilled events land in both targets automatically.

### Step 5 — Verify the first sync

```bash
psql -h localhost -U postgres -d postgres -c \
  "SELECT * FROM reverse_etl_demo_target ORDER BY id;"
psql -h localhost -p 4566 -U root -d dev -c \
  "SELECT * FROM reverse_etl_demo_target ORDER BY id;"
curl -s localhost:4002/api/check/demo/postgres
```

Three rows each. The setup run's metadata shows the backfill (3 insert
events) and ~0 warehouse seconds — delta-rs reads storage directly; the only
warehouse touch a sync ever makes is one small SQL write.

### Step 6 — Make changes and sync incrementally

In Databricks, change some rows:

```sql
UPDATE de_dev.sr_poc_external.reverse_etl_demo_source
SET description = 'second (updated)', updated_at = current_timestamp() WHERE id = 2;
DELETE FROM de_dev.sr_poc_external.reverse_etl_demo_source WHERE id = 3;
INSERT INTO de_dev.sr_poc_external.reverse_etl_demo_source
VALUES (4, 'fourth', 12.0, current_timestamp());
```

Then sync: Assets → search `reverse_etl_demo_to_kafka` → select it →
**Materialize selected** → Launch run. Expect one run to emit: one
`update_preimage` + one `update_postimage` (paired into a single Debezium
update), one `delete` carrying the full preimage, one `insert`. With DVs
enabled, the UPDATE and DELETE create real deletion vectors and the sync's
DV-aware path applies the masks — exactly as on a production table. (The
repo's `reverse_etl_demo_make_change` asset drives the same demo changes with
run config; the SQL above is the by-hand equivalent.)

### Step 7 — Schedule recurring syncs

Quickest: Dagster UI → Automation → the `reverse_etl_demo_to_kafka` asset →
add a cron automation condition (e.g. daily). In code: add a schedule next to
the other definitions (the demo's sink-health schedule in
`reverse_etl_defs.py` shows the pattern).

### Step 8 — Watch the watermark (Databricks SQL)

```sql
SELECT sync_name, last_commit_version
FROM de_dev.sr_poc_external.reverse_etl_demo_state
WHERE sync_name = 'demo'
ORDER BY last_commit_version DESC
LIMIT 1;
```

After every completed sync, `last_commit_version` equals the source's top
`DESCRIBE HISTORY` version, and `last_schema` carries the source's current
columns (the drop-propagation evidence). If the watermark stalls while
`DESCRIBE HISTORY` grows, the sync runs are failing — check Dagster before
Databricks.

### Step 9 — Reset (start over)

Jobs → `reverse_etl_demo_reset_job` → **Launch run**. It drops the source
table, the state table, the Kafka topic and registry subjects, the connector
and both target tables — allowed because every name matches the
`reverse_etl_` prefix allowlist. Then Step 1 and Step 4 again to start fresh.

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

The reset job clears the watermark before it drops anything in Databricks (since
2026-10-25), so a reset that fails on a Databricks error no longer leaves this trap behind. A
hand-made table also needs `MANAGE` granted to the service principal, or the reset's drop is refused
([REVERSE_ETL_VIEW_COPY.md](REVERSE_ETL_VIEW_COPY.md)). The state now lives in Postgres
(`reverse_etl_sync_state`); the SQL below is the legacy Delta form, and the Postgres equivalent is
`DELETE FROM reverse_etl_sync_state WHERE sync_name = '<sync_name>'`.

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
