# Reverse ETL with drt: findings and comparison with the CDF pipeline

A small demo of [drt](https://github.com/drt-hub/drt) (v1.0.0, Apache-2.0), an open-source
YAML-driven reverse-ETL tool, run against the same Databricks table our CDF pipeline syncs.
The goal was a side-by-side comparison, not a replacement. The pipeline it is compared with is
described in [REVERSE_ETL_DEBEZIUM_JDBC_SINK.md](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md).

Status: demo only, tested live on the local stack. The drt demo is separate from the CDF pipeline; the
only pipeline changes made along the way are the timestamp fixes in section 3.3.

## 1. What the demo does

```
Databricks  de_dev.sr_poc_external.reverse_etl_cdf_source
   │  (drt reads it with a SQL query: SELECT *)
   ▼
Postgres    reverse_etl_cdf_drt_target        (mode: mirror, strategy: tracked)
```

There is no Kafka, no change feed and no RisingWave in this path. The same source table feeds
our pipeline in parallel, which is what makes the comparison direct.

| Piece | Where |
|---|---|
| drt project | `orchestration/drt_demo/drt_project.yml` |
| Sync definition | `orchestration/drt_demo/syncs/cdf_to_postgres.yml` |
| Dagster asset | `reverse_etl_cdf_drt_to_postgres` (group `reverse_etl_cdf_drt`, built with `dagster-drt`'s `@drt_assets`, in `orchestration/assets/drt_demo.py`) |
| Dagster jobs | `reverse_etl_cdf_drt_setup_job` (creates the target table) and `reverse_etl_cdf_drt_reset_job` (drops it again) |
| Install | `pyproject.toml`: `drt-core[databricks,postgres]==1.0.0` and `dagster-drt==0.4.0`, locked in `uv.lock` |
| Target table | Postgres `reverse_etl_cdf_drt_target` (plus drt's own `_drt_synced_keys`) |

The names follow `reverse_etl_<label>_<role>` with label `cdf_drt`.

## 2. How it is installed and run

- drt and the community package [dagster-drt](https://github.com/drt-hub/drt/releases/tag/dagster-drt-v0.4.0)
  are regular dependencies in `pyproject.toml`, locked in `uv.lock` with the other packages, so the
  Dagster image and the devbox environment get them from the same lockfile and they survive
  container recreation. drt adds seven packages (`databricks-sql-connector`, `drt-core`,
  `et-xmlfile`, `oauthlib`, `openpyxl`, `pybreaker`, `thrift`) and dagster-drt one; none already
  locked changes. Earlier versions of the demo used a separate virtualenv and then a `drt`
  subprocess started by hand-written code; a dry-run install showed no dependency clash, and
  dagster-drt replaced the subprocess.
- **The asset.** `reverse_etl_cdf_drt_to_postgres` is a Dagster asset made by `@drt_assets` from
  the sync file, renamed to our convention with a `DagsterDrtTranslator`, and depending on the
  Databricks source table's asset (`reverse_etl_cdf_table_setup`), so it appears downstream of the
  source in the asset graph. Each run records `rows_extracted`, `rows_synced`, `rows_failed`,
  `rows_skipped` and `duration_seconds` as metadata. It is materialized, not run as a job.
- The setup job `reverse_etl_cdf_drt_setup_job` creates the target table, because drt never does.
  It reads the Databricks source table's columns and creates `reverse_etl_cdf_drt_target` with the
  matching Postgres types (the mapping the Debezium sink uses) and `rid` as the primary key. It uses
  `CREATE TABLE IF NOT EXISTS`, so it does nothing when the table exists and never adds columns.
- The reset job `reverse_etl_cdf_drt_reset_job` drops `reverse_etl_cdf_drt_target` and drt's
  `_drt_synced_keys` table (a fixed drt name, in the same database, which would also hold the keys
  of any other drt sync writing there) and deletes drt's local run state (`.drt` and `target` in the
  work directory). It refuses to run unless the target table name starts with `reverse_etl_`. It does
  not touch the Databricks source. After it, run the setup job; the next sync baselines again.
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
- After `ALTER TABLE reverse_etl_cdf_drt_target ADD COLUMN drt_probe text` the drt job succeeded
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
   of change. Fine for thousands of rows; not tested at all on a large table. [source/judgement]
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

## 5. Not tested

- drt `incremental` mode (cursor on `updated_at`) and the `diff` strategy (needs a Postgres
  source, so not usable with Databricks as the source).
- Behaviour and run time on a large table (the demo table has a handful of rows).
- Failure handling beyond the missing-column case (network loss, a partly failed batch).
- Running drt on a schedule.
- dagster-drt features beyond one asset: the `DrtSyncComponent` YAML component, the dry-run run
  config, partitions, and `build_drt_change_sensor`. The sensor's README lists only `deltalake`,
  `iceberg`, `snowflake` and `sqlserver` source profiles, not the `databricks` profile used here
  [source].
- Other destinations (the demo only writes to Postgres).
- A dropped source column or a changed type in drt (section 4.1 reasons about it, nothing was run).
- drt's `on_error` options and its retry behaviour.
- The notebook change on STG (uploaded, not run) and the drt demo against STG.

## 6. Reproducing it

1. Build the Dagster image (`docker compose build dagster-webserver`; it installs drt from the
   lockfile) and start the stack. Make sure the `cdf` sync is set up (`reverse_etl_cdf_setup_job`).
2. Run `reverse_etl_cdf_drt_setup_job` to create the target table in Postgres.
3. Materialize the asset `reverse_etl_cdf_drt_to_postgres` (Assets tab). The first run baselines the tracked
   mirror; change the source and run it again to see updates and deletes.
4. To see the schema case, add a column to the source and materialize the asset again (the run fails, with the row errors in the log), then add the
   column to `reverse_etl_cdf_drt_target` by hand (`ALTER TABLE`; the setup job does not add columns)
   and materialize it again.
5. To start over, run `reverse_etl_cdf_drt_reset_job`, then the setup job again. Tested: after the
   reset both tables and drt's local state were gone, and setup plus sync rebuilt the target (the
   first sync baselined, "no prior state ... baselining this run's 2 key(s)").
