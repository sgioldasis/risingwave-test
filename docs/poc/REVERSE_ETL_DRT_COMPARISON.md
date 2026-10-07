# Reverse ETL with drt: findings and comparison with the CDF pipeline

A small demo of [drt](https://github.com/drt-hub/drt) (v1.0.0, Apache-2.0), an open-source
YAML-driven reverse-ETL tool, run against the same Databricks table our CDF pipeline syncs.
The goal was a side-by-side comparison, not a replacement. The pipeline it is compared with is
described in [REVERSE_ETL_DEBEZIUM_JDBC_SINK.md](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md).

Status: demo only, tested live on the local stack. The drt demo is separate from the CDF pipeline; the
only pipeline changes made along the way are the timestamp fixes in section 3.3.

## 1. What the demo does

```
Databricks  de_dev.sr_poc_external.reverse_etl_<label>_source
   │  (drt reads it with a SQL query: SELECT *)
   ▼
Postgres    reverse_etl_<label>_drt_target        (mode: mirror, strategy: tracked)
```

There is no Kafka, no change feed and no RisingWave in this path. The same source table feeds
our pipeline in parallel, which is what makes the comparison direct. It exists for two syncs,
label `cdf` (the POC) and `orders`; the rest of this document uses `cdf` in its examples and
section 2 says how a label is added.

| Piece | Where |
|---|---|
| drt project | `orchestration/drt_demo/drt_project.yml` (one project, one sync file per label) |
| Sync definitions | `orchestration/drt_demo/syncs/reverse_etl_<label>_drt.yml` |
| Generic code | `orchestration/assets/reverse_etl_drt.py` (`build_drt_defs(cfg)`), component `orchestration/components/reverse_etl_drt_sync.py` (`ReverseEtlDrtSync`) |
| One instance per label | `orchestration/defs/reverse_etl_<label>_drt/defs.yaml`, with `name: <label>` |
| Dagster asset | `reverse_etl_<label>_drt_to_postgres` (group `reverse_etl_<label>_drt`, built with `dagster-drt`'s `@drt_assets`) |
| Dagster jobs | `reverse_etl_<label>_drt_setup_job` (creates the target table) and `reverse_etl_<label>_drt_reset_job` (drops it again) |
| Install | `pyproject.toml`: `drt-core[databricks,postgres]==1.0.0` and `dagster-drt==0.4.0`, locked in `uv.lock` |
| Target table | Postgres `reverse_etl_<label>_drt_target` (plus drt's own `_drt_synced_keys`, shared by all drt syncs) |

The names follow `reverse_etl_<label>_<role>`; the drt sync's own name is `reverse_etl_<label>_drt`.

## 2. How it is installed and run

- drt and the community package [dagster-drt](https://github.com/drt-hub/drt/releases/tag/dagster-drt-v0.4.0)
  are regular dependencies in `pyproject.toml`, locked in `uv.lock` with the other packages, so the
  Dagster image and the devbox environment get them from the same lockfile and they survive
  container recreation. drt adds seven packages (`databricks-sql-connector`, `drt-core`,
  `et-xmlfile`, `oauthlib`, `openpyxl`, `pybreaker`, `thrift`) and dagster-drt one; none already
  locked changes. Earlier versions of the demo used a separate virtualenv and then a `drt`
  subprocess started by hand-written code; a dry-run install showed no dependency clash, and
  dagster-drt replaced the subprocess.
- **A reusable pattern, one label per sync.** `build_drt_defs(cfg)` takes the same
  `ReverseEtlSyncConfig` the pipeline uses (source table, key column, names) and builds the
  asset, the setup job and the reset job; every name is derived from `cfg.sync_name`. The
  component `ReverseEtlDrtSync` calls it for the label given in its `defs.yaml`, which must be the
  `name` of an existing `ReverseEtlCdfSync` instance. **To add a sync for another label:** add
  `orchestration/defs/reverse_etl_<label>_drt/defs.yaml` (`type:
  orchestration.components.reverse_etl_drt_sync.ReverseEtlDrtSync`, `name: <label>`) and
  `orchestration/drt_demo/syncs/reverse_etl_<label>_drt.yml` (copy an existing one and change the
  name, target table and source table). The sync file repeats names the config derives, so the
  asset checks it against the config before running (name, target table, key column, source
  table) and fails with the differences. That check was written but never triggered in a test.
- **The asset.** `reverse_etl_<label>_drt_to_postgres` is a Dagster asset made by `@drt_assets`
  from the sync file, renamed to our convention with a `DagsterDrtTranslator`, and depending on
  the Databricks source table's asset (`reverse_etl_<label>_table_setup`), so it appears
  downstream of the source in the asset graph. Each run records `rows_extracted`, `rows_synced`, `rows_failed`,
  `rows_skipped` and `duration_seconds` as metadata. It is materialized, not run as a job.
- The setup job `reverse_etl_<label>_drt_setup_job` creates the target table, because drt never
  does. It reads the Databricks source table's columns and creates `reverse_etl_<label>_drt_target`
  with the matching Postgres types (the mapping the Debezium sink uses) and `rid` as the primary
  key. It uses `CREATE TABLE IF NOT EXISTS`, so it does nothing when the table exists and never
  adds columns.
- The reset job `reverse_etl_<label>_drt_reset_job` drops the target table and deletes **only that
  sync's rows** from drt's `_drt_synced_keys` table. That table has a fixed name, lives in the
  destination database and is shared by every drt sync there, but has a `sync_name` column, so one
  sync can be reset without touching another (tested: after resetting `orders`, the `cdf` rows
  were intact and its next run did not baseline again). The job refuses to run unless the target
  table name starts with `reverse_etl_`, and it does not touch the Databricks source. drt's local
  run state in the work directory is not cleared; the tracked mirror's state is the key table.
  After a reset, run the setup job; the next sync baselines again.
- Before drt runs, the asset copies `orchestration/drt_demo/` to a writable work directory
  (`/home/dagster/drt-demo`; drt writes state next to the project and the source mount is
  read-only) and writes `~/.drt/profiles.yml` with only the workspace host and SQL warehouse path.
  The sync definitions are read from the read-only project at load time; the run uses the
  resource's `project_dir`, the copy.
- **Credentials:** the Databricks token is minted with the same service-principal flow the other
  assets use and put in an environment variable for the run, never written to a file. **dagster-drt
  runs drt inside the Dagster run process**, not as a subprocess, so drt can see that process's whole
  environment, including the service principal's client secret and the Kafka credentials. The
  earlier subprocess version started drt with a minimal environment; that isolation is gone
  [source: `dagster_drt/resource.py`]. The Postgres destination in the sync file
  sets host, port, database and user and **no password**: with neither `password` nor `password_env`
  set, drt's `resolve_env` returns `None` and `_connect` passes `password=None` to psycopg2
  (`destinations/postgres.py`), so this only works because the local Postgres accepts the
  connection without one. For a real database, set `password_env` in the sync file (drt reads the
  password from the named environment variable) and set that variable in the asset before the run.
- The asset sets `PGTZ=UTC` in the environment (see 3.4) and **raises when `rows_failed` is above
  zero**. Tested without that check, with a source column the target lacks: dagster-drt logged
  `2 extracted, 0 synced, 2 failed` and the row errors as WARNING lines, **the run ended
  SUCCESS**, and a materialization was recorded with `rows_synced: 0` and `rows_failed: 2`. With the
  check the same run fails and the row errors are in the log.
- With the earlier subprocess version, the stderr line `Token exchange failed, using external token:
  'access_token'` appeared on every run, including successful ones, and did not affect any result.
  It was not looked for in the dagster-drt runs.

## 3. Findings (tested live unless a point says it comes from source)

### 3.1 Inserts, updates and deletes

Changes made to the Databricks source, then one drt run and one run of our pipeline:

| Change | drt (`mirror: tracked`) | CDF pipeline |
|---|---|---|
| Insert a row | applied | applied |
| Update a row (with `updated_at` deliberately not changed) | applied | applied |
| Delete a row | applied | applied |

The drt target, our Postgres target and our RisingWave target ended with identical rows.

- drt's `mirror` mode reads the **whole table** every run and compares it with what it wrote
  before, so it does not depend on `updated_at` or any change column. The cost is a full read
  per run; our pipeline reads only the change feed.
- **Deletes are tracked by drt itself.** `tracked` keeps a `_drt_synced_keys` table in the
  destination. The first run only baselines it (log line: "no prior state ... baselining this
  run's 2 key(s); no deletes this run"); deletes are detected from the second run on.
- drt's summary line counts upserted rows ("2 synced" for an update plus an insert). The delete
  is **not** in that count, so confirm deletes in the table, not in the summary.

### 3.2 A new source column breaks the sync

After `ALTER TABLE ... ADD COLUMNS (drt_probe STRING)` on the source:

- drt: `0 synced, 2 failed`, with `column "drt_probe" of relation "reverse_etl_cdf_drt_target" does
  not exist`. drt does not create or alter the destination table, and the model is `SELECT *`, so
  the first row fails and the rest of the batch is rolled back. **One new source column stops the
  whole sync** until someone alters the target by hand.
- CDF pipeline: the Dagster job `reverse_etl_notebook_sync_job` added the column to RisingWave,
  the notebook sent it through Kafka, and the Debezium sink added it to Postgres. `hello` arrived
  in both targets.
- After `ALTER TABLE reverse_etl_cdf_drt_target ADD COLUMN drt_probe text` the drt sync succeeded
  and the targets matched.

A fixed column list in the drt `model` (instead of `SELECT *`) would keep the sync running, but
then new columns would not be synced at all (reasoning from how the model works; not tested).

### 3.3 Timestamps: sub-millisecond digits differ

Before the fix below, drt's `updated_at` carried microseconds (`04:39:21.903665`) while our
pipeline's copy showed milliseconds (`04:39:21.903`). **Our pipeline was the side that truncated.**
The Databricks column holds microseconds, and drt receives them untouched. The notebook rendered
timestamps with
`date_format(..., "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")` (`notebooks/reverse_etl_cdf_to_kafka.py`,
`as_json_friendly`), which keeps three fractional digits.

**Fixed in both paths.**
- Notebook: the format is now `.SSSSSS`; the notebook was uploaded to DEV and STG. A fresh update of
  one row arrived as `2026-10-05 02:53:05.595302` in our Postgres target, our RisingWave table and
  the drt target alike (tested on DEV; the STG copy was not run).
- Dagster asset `reverse_etl_<label>_to_kafka`: the Statement Execution API returns a `TIMESTAMP`
  with three fractional digits (`2026-10-05T02:55:42.345Z`) even though the column keeps six, so
  `_read_changes` now formats each timestamp column, and `_commit_timestamp`, with `date_format`
  (`orchestration/assets/reverse_etl_cdf_setup.py`). Tested: the source holds
  `02:55:42.345053` and both of our targets received `02:55:42.345053`.

Only changes made after these fixes carry the new precision; rows synced earlier keep their three
digits until they change again. Tested for both syncs: `orders` id 2 (source `02:58:14.167726`) arrived
as `02:58:14.167726` in Postgres and RisingWave.

### 3.4 Timestamps: a 3-hour shift unless the session timezone is UTC

Without any setting, drt's `updated_at` values were exactly 3 hours earlier than Databricks and
our pipeline. The Postgres server timezone is Europe/Athens (+03). Cause, from drt's installed
source and the connector it uses (drt 1.0.0, databricks-sql-connector 4.6.0):

- The connector maps `TIMESTAMP` to `pyarrow.timestamp("us", None)` (`backend/thrift_backend.py`)
  and builds Python objects with `timestamp_as_object=True` (`result_set.py`), so drt gets a
  **naive `datetime`** (no timezone) holding the UTC wall-clock value.
- drt's Postgres destination has no timezone handling at all (no `tzinfo`, `astimezone` or
  `timezone` in `destinations/postgres.py`, `sql_base.py` or the Databricks source). It hands the
  naive value to psycopg2, which sends it as a plain timestamp.
- Postgres reads a timestamp without a zone, written into a `timestamptz` column, in the
  **session timezone**. Europe/Athens is UTC+3 here, so 04:39 UTC became 04:39 Athens, which is
  01:39 UTC.

Confirmed by test: rerunning drt with `PGTZ=UTC` wrote values that match ours. The Dagster asset
sets `PGTZ=UTC`. When running drt by hand, set it too, or use a plain `timestamp` column. This
also means the result depends on the Databricks session timezone being UTC, which it was here; I
did not test a workspace configured differently.

## 4. Comparison: drt versus the Kafka / Debezium pipeline

Both read the same Databricks table and write to Postgres. The pipeline is: Delta change feed, a
Databricks notebook or Dagster asset, a Kafka topic with Debezium-shaped messages, the Debezium JDBC
sink into Postgres and a RisingWave table (see the sink doc). drt is: one SQL query, one YAML file,
one process.

Each claim is tagged by how I know it:
**[tested]** I ran it in this project; **[source]** I read it in drt's installed code or in our
code, without running it; **[judgement]** my assessment, not a measurement.

### 4.1 At a glance

| | drt (`mirror: tracked`) | Kafka / Debezium pipeline |
|---|---|---|
| What reads Databricks | SQL warehouse, the whole table, every run [source] | SQL warehouse (Dagster asset) or a cluster (notebook), only the changed rows via the change feed [tested] |
| Moving parts | drt, one YAML file, one Dagster asset [tested] | notebook or asset, Kafka topic, Kafka Connect worker, Debezium connector, RisingWave table, Dagster assets and jobs [tested] |
| Destinations | Postgres here; drt has many destination types, but no Kafka destination [source] | Postgres (JDBC sink) and RisingWave, from the same topic [tested] |
| Deletes | Yes, tracked in a `_drt_synced_keys` table in the destination [tested] | Yes, from the change feed [tested] |
| New source column | Sync fails until the target is altered by hand [tested] | Added to RisingWave and Postgres automatically [tested] |
| Dropped column or changed type | Not tested; a dropped column would break a `SELECT *` model [judgement] | Both tested; additive only in practice, see the sink doc section 10.4 [tested] |
| Prerequisites on the source table | None beyond read access [source] | Change Data Feed enabled, an identity key column (`rid`) from table creation [tested] |
| Setup effort | Minutes [judgement] | Large; this was the whole PoC [judgement] |

### 4.2 Where drt is better

1. **Far fewer moving parts.** One tool and one file replace the topic, connector, worker and
   RisingWave table. Less to deploy, monitor and break. [tested]
2. **No prerequisites on the source table.** It works on any table or query result, including
   tables without Change Data Feed and without an identity column. Our pipeline needs both, and
   `rid` cannot be added to an existing table (sink doc section 13). [source]
3. **Natural fit for transformed data.** The source is a SQL query, so you can join, filter or
   aggregate in Databricks and sync the result. Our pipeline syncs one table's change feed as it
   is. [source]
4. **No dependence on change feed history.** Our pipeline reads from a version number, and old
   versions age out of retention: on a first run it baselines instead of backfilling when history is
   gone (`_get_earliest_available_version` in `reverse_etl_cdf_setup.py`). I did not test a later run
   whose start version had aged out. drt re-reads the current table, so it has no such window. [source]
5. **Stateless with respect to versions.** No watermark shared between a notebook, an asset and
   a job, so there is no "use one trigger per change" rule. Its state is the key table in the
   destination. [tested]
6. **Many destination types** if the target changes later (warehouses, SaaS APIs, files); our
   pipeline's second leg is Kafka plus a JDBC sink, which only fits databases. [source]
7. **Row-level error handling in the Postgres destination.** drt can isolate a failing row with a
   savepoint when `on_error` is configured, and it retries transient Databricks errors. Our
   asset flushes to Kafka once and does not inspect delivery errors (sink doc section 13). [source;
   the drt error options were not exercised]
8. **Preview before writing.** `drt run --dry-run --diff` showed which rows would be added before
   anything was written. [tested]

### 4.3 Where drt is worse

1. **Full table read on every run.** Cost and run time grow with the table, not with the amount
   of change. Fine for thousands of rows; not tested at all on a large table (see 4.7 for what
   would happen at billions of rows). [source/judgement]
2. **Delete detection needs all keys each run.** The `tracked` strategy compares the full key set
   with the previous one, so it presumably scales with table size too [judgement]. The first run only
   baselines and detects no deletes [tested].
3. **Schema changes break the sync.** drt never creates or alters the destination table. One new
   source column fails the whole batch, rolled back, with no rows written, until someone alters the
   target. A fixed column list avoids the failure but then the new column is not synced. [tested]
4. **No Kafka and no RisingWave target.** drt cannot write to either, so it cannot feed streaming
   consumers directly. In our pipeline the same topic also builds the RisingWave table. [source]
5. **No history of changes.** drt writes the latest state only. A Kafka topic keeps the change
   events, so it can be replayed, audited or read by a new consumer later. [judgement]
6. **Timezone and type care.** Timestamps lost 3 hours until `PGTZ=UTC` was set (section 3.4); the
   destination column types are ours to define and keep right. [tested]
7. **Weaker summary.** The "N synced" line does not count deletes (section 3.1). [tested]
8. **Third-party dependencies.** drt is a v1.0.0 open-source project, and dagster-drt (v0.4.0,
   classified Alpha, community-maintained) wraps it. Both are installed
   from PyPI, pinned by version and by the hashes in `uv.lock`, and it shares Dagster's environment, so
   its dependencies can clash with Dagster's in a future upgrade. Our Debezium plugin is pinned by sha512 and
   verified at build time (sink doc section 8). [tested]
9. **Latency is bounded by how often it runs.** Each run is a full read, so running it every few
   seconds is expensive; our pipeline is also batch-triggered today but reads only changes. [judgement]

### 4.4 Where the Kafka / Debezium pipeline is better

1. **Reads only what changed**, so cost follows the change volume. [tested]
2. **Schema evolution for added columns**, end to end, with no manual step. [tested]
3. **Several consumers from one topic**: Postgres and RisingWave today, and any later one can
   subscribe without another read of Databricks. [tested]
4. **A replayable record of changes** in Kafka, with before and after images and operation types.
   [tested]
5. **Delete and update ordering handled by the change feed and commit versions**, with deletes
   exact rather than inferred from a key comparison. [tested]
6. **Runs inside the platform we already operate** (Kafka, Kafka Connect, Dagster), with a reset
   job, a seeding asset and a component to add syncs. [tested]
7. **Timestamps carry an explicit zone** (`ZonedTimestamp`, a `timestamptz` column), so the Postgres
   session timezone does not change the stored instant; drt needed `PGTZ=UTC` for that (section 3.4).
   [tested]

### 4.5 Where the Kafka / Debezium pipeline is worse

1. **Much more to build and run.** Six or more components, a custom message format (Debezium
   envelope with embedded JSON schema) that we construct by hand, and a Kafka Connect worker to
   keep alive. [tested]
2. **Source table prerequisites.** Change Data Feed enabled, and an identity `rid` key from the
   start. [tested]
3. **Change feed limits.** A read that spans a column drop fails
   (`DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE`, sink doc section 10.4) [tested], and old
   history can age out of retention (see 4.2 item 4) [source].
4. **Schema evolution is additive only** in practice; drops and type changes need a careful order,
   and the two targets can disagree afterwards. [tested]
5. **Type coverage is limited**; types outside the mapping table (for example `DECIMAL`, `DATE`)
   arrive as `text`. [source: sink doc section 13]
6. **Manual trigger and shared watermark.** No schedule or sensor, and the notebook, asset and
   Dagster job must not be mixed for one change. [tested]
7. **The notebook job lives outside git** and depends on one person's access and cluster. [tested]
8. **Delivery errors are not inspected**, and the watermark advances regardless. [source]
9. **Heavier per-message cost** (schema repeated in every JSON message). [source]

### 4.6 Which to choose

- **drt fits** a small or medium table that needs to land in one database, where the source has no
  change feed or identity key, where simplicity matters more than efficiency, and where a person
  can alter the target when columns are added. A transformed query as the source is also a good fit.
- **The Kafka / Debezium pipeline fits** large or fast-changing tables, several consumers (including
  RisingWave), automatic handling of new columns, and a need for a change history.
- **They are not exclusive.** drt could handle small reference tables while the pipeline carries
  the high-volume ones. This is a suggestion from this one demo, not a tested setup.

### 4.7 A very large table: billions of rows, millions of changes a day

Nothing in this section was run at that scale; the demo tables have a handful of rows. The first
points come from drt's installed source (drt 1.0.0, databricks-sql-connector 4.6.0) and the key table
as observed in the demo; the rest is reasoning, tagged as such.

**The setup used in this demo (`mirror`, strategy `tracked`) would not work.**
- **Full scan every run.** The model is `SELECT * FROM <source table>`, so every run reads the whole
  table, however little changed. The Databricks source reads rows by iterating the cursor, one row
  at a time, in a single Python process [source].
- **Every row is written again.** Each row, changed or not, is written with
  `INSERT ... ON CONFLICT ... DO UPDATE` [source], in batches of `sync.batch_size`, which defaults
  to 100 rows [source]. Millions of rows are then tens of thousands of batches. [judgement]
- **A key table as big as the source.** `tracked` keeps one row per key in `_drt_synced_keys`
  (`sync_name`, a 64-character `key_hash`, the key as JSON) and compares it with the source's keys
  on every run to find deletes [tested on two rows]. At billions of rows that table is as large as
  the source. [judgement: its cost and the run time were not measured]
- So the daily cost would follow the size of the history, not the size of the change. I would expect
  runs of many hours or failures. [judgement]

**What drt offers instead: `incremental` mode.** drt can fetch only rows whose cursor column is
greater than the last watermark, either through `cursor_field` (for example `updated_at`) or a
`{{ cursor_value }}` / `{{ watermark }}` placeholder in the model SQL (`engine/resolver.py`) [source].
That reads only the daily delta, if Databricks can apply the filter cheaply. The conditions:
- **No deletes.** Incremental mode does not detect deletes; that needs `mirror`, which is the full scan
  above. Deletes would have to be handled another way. [source: the delete tracking is a `mirror`
  feature]
- **A trustworthy cursor column.** It must be set on every insert and update and never backdated;
  rows that arrive late with an older value are missed. [judgement]
- **Where the watermark lives.** By default in drt's local state in the work directory, which in our
  setup is inside the Dagster container and is lost when the container is recreated. drt also
  supports the state backends `local`, `gcs`, `s3` and `warehouse` [source: `state/factory.py`];
  I did not evaluate them.
- **Scan cost in Databricks.** Without clustering or partitioning on the cursor column the query may
  still scan most of the table. [judgement]
- **Throughput.** A single process writing a few million rows a day row by row is plausible but was
  not measured. [judgement]

**Deletes in `incremental` mode: using the Delta change feed (tested).** drt's own source says
cursor-based incremental can never detect a delete, and that `incremental_strategy: diff` is the only
strategy that can; `diff` needs a Postgres source, so it does not apply to Databricks
(`config/sync_options.py`) [source]. The remaining idea is to read the change feed, which lists deleted
rows, through drt. I tried it on a scratch Delta table with the change feed on: `mode: incremental`,
`cursor_field: _commit_version`, `watermark.default_value` set to the table's version, and a model
`SELECT rid, id, value, _change_type, _commit_version FROM table_changes('<table>', {{ cursor_value }})
WHERE _change_type <> 'update_preimage' ORDER BY _commit_version` [tested]:
- The templated cursor and `table_changes()` run, the first run uses `default_value`, and the stored
  watermark advanced to the latest commit version.
- **A deleted row is upserted, not deleted.** After an insert, an update and a delete on the source,
  the deleted key arrived as an ordinary row with `_change_type = delete`. drt's Postgres destination
  deletes only in the `mirror` pass, never per row [source: `destinations/postgres.py`], so a separate
  `DELETE ... WHERE _change_type = 'delete'` in Postgres is needed.
- **Deleted rows come back.** The cursor window is inclusive, so the next run re-read the last commit,
  with no new source changes, and re-inserted the key I had just cleaned up.
- **`{{ cursor_value }} + 1` avoids that but fails when there are no new commits:**
  `DELTA_CDC_START_VERSION_AFTER_LATEST: Start version 5 ... exceeds the latest table version 4`. A guard
  that checks the table's latest version first would be needed (our asset has one).
- The target also receives the change-feed columns (`_change_type`, `_commit_version`) unless a view
  hides them, and the watermark is kept in `local`, `gcs` or `bigquery` storage (`WatermarkConfig`
  [source]); locally it is lost when the container is recreated.

So it works, but only with a cleanup step, a first-run value and a no-new-commits guard around drt,
which rebuilds the watermark, delete handling and error cases the change-feed pipeline already has.
For a table with deletes that must reach Postgres, the pipeline is the better fit. Other options
for deletes in `incremental` mode: soft deletes in the source (a flag and an updated timestamp instead
of a physical delete) or accepting stale rows; neither was tested.

**How the Kafka / Debezium pipeline compares at that size.** It reads only the change feed, including
deletes, so the billions of historical rows do not matter. The two ways of running it differ:
- **The notebook** runs on Spark and is the one that should scale.
- **The Dagster asset** reads through the Statement Execution API with inline results. A test on
  2026-10-07 found that the API returns a large result in chunks and the asset read only the first one:
  a delete commit of 80,300 rows produced 49,152 events and the run still succeeded (sink doc section
  13). That is fixed: the asset now reads every chunk and fails if the rows read differ from the
  manifest's `total_row_count`, and a 100,000-row insert and delete both arrived in full in Postgres and
  RisingWave. The roughly 25 MiB inline cap I remembered earlier is still not verified; a few million
  changed rows may well exceed it, in which case the notebook, or the API's external-links mode in the
  asset, would be needed.
- Neither path was run at the scale of a few million rows (the largest test was 100,000).

**Conclusion.** For billions of rows with millions of daily changes, `mirror`/`tracked` is not
viable, and drt only fits in `incremental` mode with a trusted cursor column and a separate answer
for deletes. The change-feed pipeline (through the notebook) is the better fit. This agrees with
section 4.6: drt for small or medium tables, the pipeline for large ones. A test with a large
synthetic table would be needed to put numbers on any of this.

### 4.8 Recommendation for tables with billions of rows and millions of daily changes

A recommendation from what was tested and read in this project. **None of it was run at that scale.**

**Use the change-feed pipeline through the notebook** (Delta change feed, Kafka, Debezium JDBC sink),
not drt.
- Cost follows the daily change, not the history: the change feed reads only the delta, where drt's
  `mirror` rescans everything every run and keeps a key table as large as the source (4.7). [source]
- Deletes come from the change feed itself. In drt's `incremental` mode they need a separate cleanup
  step and guards that rebuild what the pipeline already has (4.7). [tested]
- Added columns, microsecond timestamps, resets and the reusable component are already built and
  tested for this pipeline. [tested]
- A few million changes a day is on average only tens of rows per second (3 to 5 million a day is
  roughly 35 to 60), which should be modest for Spark, Kafka and Postgres, though bursts matter.
  [judgement]

**What to change or check before using it at that size**
1. **Run the notebook, not the Dagster asset.** The notebook runs on Spark. The asset reads through the
   Statement Execution API with inline results; it now reads all result chunks (fixed 2026-10-07, tested to
   100,000 rows), but I believe inline results are also capped at roughly 25 MiB (from memory, not
   tested), so a few million changed rows may still break it.
2. **The sink throughput is not the limit at this volume; raise `tasks.max` for backfills.** Tested
   locally on 2026-10-05 (300,000 insert messages of about 1.7 KB each, 12 partitions, local Redpanda
   and Postgres): one task drained about 35,000 to 40,000 rows a second, four tasks about 92,000.
   `dialect.postgres.unnest.insert.enabled` made no visible difference and larger batches
   (`batch.size` and `max.poll.records` 2000) were slower. A few million changes a day is tens of rows
   a second, so the default of one task is far more than needed; the setting is now `sink_tasks_max` on
   the component (sink doc section 13). For a billion-row backfill, extrapolating the local rates gives
   roughly 8 hours with one task and 3 hours with four [judgement]. Not tested: updates to existing
   keys, millions of rows, a real network to the staging Kafka, concurrent load on Postgres; the runs
   were 3 to 9 seconds, so differences under about 30% are noise. [tested]
3. **Load the history separately.** The pipeline's first run baselines at the table's current version
   without loading the existing rows, unless a backfill is requested (the asset's first-run logic
   in `reverse_etl_cdf_setup.py`, the notebook's `backfill_from_version`, and
   `REVERSE_ETL_CDF_POC_PLAN.md`). A backfill through the change feed is limited to the retained
   history, so the billions of historical rows need their own initial load, for example a Spark
   batch job into the target. That is not built. [source]
4. **Match the Delta history retention to the trigger schedule.** The change feed reaches back only as
   far as the table's retained history, so a run that waits longer than that cannot resume; a read
   that spans a column drop also fails (sink doc section 10.4). [tested for the drop]
5. **Reconsider the message format.** Each message carries its schema as JSON; at volume Avro with a
   schema registry (sink doc section 14.1) or compression would make messages smaller. [Avro now tried: the
   `avro` sync works and its messages were about 12.6 times smaller for a four-column update, sink doc section
   14.1.1; not tried at volume.]
6. **Check that Postgres is the right target** for billions of rows; that depends on what reads it.
   [judgement]
7. **Source prerequisites.** The table needs Change Data Feed enabled and an identity `rid` key from
   creation, and `rid` cannot be added to an existing table, so existing large tables would need a
   rebuild or another key choice (sink doc section 13). [tested]

**Where drt could still fit.** Small or medium reference tables; a large table that never has deletes;
or one that handles deletes as soft deletes (a flag and an updated timestamp), using `incremental`
mode with a trusted cursor column. [judgement; soft deletes were not tested]

**What to do first.** A scale test before committing: a synthetic table with the change feed on and a
few million changes per run, measuring the notebook's runtime, the Kafka Connect lag and the load on
Postgres, which would turn the points above from reasoning into numbers.

## 5. Not tested

- drt `incremental` mode (cursor on `updated_at`) and the `diff` strategy (needs a Postgres
  source, so not usable with Databricks as the source).
- Behaviour and run time on a large table (the demo table has a handful of rows); section 4.7 is
  reasoning from the source, not a measurement.
- Failure handling beyond the missing-column case (network loss, a partly failed batch).
- Running drt on a schedule.
- dagster-drt features beyond one asset: its own `DrtSyncComponent` YAML component (ours is a separate
  component that calls `build_drt_defs`), the dry-run run
  config, partitions, and `build_drt_change_sensor`. The sensor's README lists only `deltalake`,
  `iceberg`, `snowflake` and `sqlserver` source profiles, not the `databricks` profile used here
  [source].
- Other destinations (the demo only writes to Postgres).
- The sync-file check in `_check_sync_file` (see section 2), and the `orders` sync beyond setup, one
  sync and a reset: its failure cases, changes to the source after the first run and added
  columns were not repeated for `orders`.
- A dropped source column or a changed type in drt (section 4.1 reasons about it, nothing was run).
- drt's `on_error` options and its retry behaviour.
- The notebook change on STG (uploaded, last re-uploaded 2026-10-07, never run; STG has no job) and the drt demo against STG.

## 6. Reproducing it

1. Build the Dagster image (`docker compose build dagster-webserver`; it installs drt from the
   lockfile) and start the stack. Make sure the pipeline sync for the label is set up
   (`reverse_etl_<label>_setup_job`; labels `cdf` and `orders`).
2. Run `reverse_etl_<label>_drt_setup_job` to create the target table in Postgres.
3. Materialize the asset `reverse_etl_<label>_drt_to_postgres` (Assets tab). The first run
   baselines the tracked mirror; change the source and run it again to see updates and deletes.
4. To see the schema case, add a column to the source and materialize the asset again (the run
   fails, with the row errors in the log), then add the column to the target by hand (`ALTER TABLE`;
   the setup job does not add columns) and materialize it again.
5. To start over, run `reverse_etl_<label>_drt_reset_job`, then the setup job again. Tested for
   both labels: after the reset the target and that sync's key rows were gone, and setup plus sync
   rebuilt the target (the first sync of the `cdf` label baselined, "no prior state ...
   baselining this run's 2 key(s)").
