# Reverse-ETL POC: Databricks DBU cost of the read paths

Date: 2026-10-14. Answers two cost questions about this POC's architecture, from the
Databricks documentation plus one live experiment (section 5); the billing-system
check still has not been run (see "Not verified").

Related: [`REVERSE_ETL_DEBEZIUM_JDBC_SINK.md`](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md) (the
pipeline), [`REVERSE_ETL_LIVE_DEMO.md`](REVERSE_ETL_LIVE_DEMO.md) section 6 (StarRocks
reading the source tables through the `databricks_uc` catalog),
[`REVERSE_ETL_CDF_POC_PLAN.md`](REVERSE_ETL_CDF_POC_PLAN.md).

The questions:

1. Does a StarRocks query against the `databricks_uc` catalog consume Databricks DBUs?
2. Can `table_changes()` run without consuming DBUs?

---

## 1. StarRocks reads through `databricks_uc`: zero DBUs, with one exception

### The normal path is not billed

The `databricks_uc` external catalog is Unity Catalog's **Iceberg REST API +
credential vending** (see the UniForm setup in section 6 of the live-demo doc). When
StarRocks queries `databricks_uc.sr_poc_external.reverse_etl_avro_source`:

1. **Metadata** comes from Unity Catalog's Iceberg REST endpoint — a control-plane
   service, not billed compute.
2. **Credentials** are vended: short-lived, downscoped storage credentials (default
   expiry one hour) that let StarRocks reach the storage account directly.
3. **All query processing happens in StarRocks**, reading the table's Parquet files
   straight from storage.

No SQL warehouse, no cluster, and no serverless compute participates in the read, so
no DBU-metered component is involved. This matches the requirements the demo doc
lists for the integration (`EXTERNAL USE SCHEMA`, the UniForm table properties) —
nothing about compute, because none is needed.

### The exception: fine-grained access controls are billed

Databricks' docs (Cross-engine ABAC) are explicit: if a table has **row filters,
column masks, or ABAC policies** attached, Databricks enforces them server-side with a
specialized **serverless** compute layer, and customers are **charged DBUs** for it.
The charge appears in `system.billing.usage` with
`billing_origin_product = 'EXTERNAL_COMPATIBILITY'`.

None of the reverse-ETL POC tables have row filters or masks. If this pattern moves
to governed production tables, this is the cost to watch.

### Writes still cost DBUs (but not because of StarRocks)

UniForm's Iceberg metadata is generated inside the Databricks transaction that writes
the Delta table, and every sync run's `table_changes()` read runs on a SQL warehouse
(see section 2). A StarRocks read adds nothing on top of that.

### Only if the catalog were reconfigured

If `databricks_uc` were replaced by a JDBC-type catalog pointed at a Databricks SQL
warehouse, every query would burn warehouse DBUs. That is not how the catalog is set
up here.

---

## 2. `table_changes()`: always on Databricks compute, so always DBUs

`table_changes()` is a Delta Lake SQL function executed by Databricks SQL against the
transaction log and the change files. It runs on a SQL warehouse or a cluster, and
every Databricks compute SKU meters DBUs — there is no free execution path inside
Databricks.

What the sync actually consumes today: the Dagster asset reads the change feed
through the Statement Execution API against the sync's SQL warehouse (the one that
needed the `CAN_USE` grant in the CDF POC plan). The reads themselves are seconds and
a handful of rows; for a batch sync the dominant cost driver is warehouse startup and
idle time, not the query.

### Pay less: serverless SQL warehouse

A serverless SQL warehouse bills **per second**, starts near-instantly (no
minutes-long classic warm-up before each Statement Execution call), and with
aggressive auto-stop is only alive while the sync runs. Same `table_changes()` code,
no repo change. For this POC's batch syncs that collapses the bill to almost nothing.

### Zero DBUs: read the change feed without Databricks compute

The change data feed is just files in the storage account: change files plus entries
in the table's `_delta_log`, so in principle an OSS reader can consume it client-side
with no Databricks involvement at all. This was tried with delta-rs on 2026-10-14 and
does **not** work for these tables — section 3 has the experiment and the root cause.
The remaining zero-DBU option is self-managed OSS Delta on Spark.

What a zero-DBU reader takes on:

- **Governance/access.** Either direct storage credentials (simpler, but bypasses
  Unity Catalog governance — the experiment used the `ADLS_*` service principal from
  `.env`, which works) or the Unity REST API with vended credentials (keeps
  governance; not tested).
- **Correctness.** The CDF read and its ordering would be reimplemented against the
  OSS reader; the watermark logic, the Debezium grouping, and everything downstream
  stay as they are — `reverse_etl_cdf_setup.py` already isolates `_read_changes()`
  from the rest, so this is a backend swap, not a redesign.

The self-managed OSS Delta on Spark option was tried the same day and **works end to
end** — section 4 has that experiment.

---

## 3. Experiment: delta-rs reading the change feed directly (2026-10-14)

Ran the zero-DBU idea end to end against `reverse_etl_cdf_source` with the Python
`deltalake` package (delta-rs 1.6.6), reading straight from the table's
`abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/...` location with
the `ADLS_*` service principal from `.env`. Script:
`scripts/delta_rs_cdf_experiment.py`, run as
`uv run --with deltalake --with pyarrow python scripts/delta_rs_cdf_experiment.py`.

What worked, with no Databricks involvement at any point:

- **Direct storage access.** The ADLS service principal opened the Unity Catalog
  managed table directly from its abfss location. (The location came from one
  `DESCRIBE DETAIL` on the SQL warehouse — a few DBUs, once; a production version
  would record the location per table or resolve it through the Unity REST API.)
- **Table metadata.** delta-rs read the table at version 3, exactly matching the
  watermark in `reverse_etl_cdf_state` (3).

What failed, both from the same root cause:

- **Current-state scan returned NULLs.** Right row count and column names, but every
  value NULL. The table is UniForm, which requires `delta.columnMapping.mode = name`:
  the Parquet files store columns under physical names (`col-6560f525-...`), and
  delta-rs projected the logical names without applying the per-field
  `delta.columnMapping.physicalName` metadata. Reading the Parquet files directly
  with pyarrow shows the real values under the physical names, including the
  `_change_type` / `__is_cdc` columns Databricks writes alongside them.
- **Change-feed scan is rejected outright.**
  `DeltaError: Column mapping is not supported for read operation 'CHANGE DATA FEED
  scans' yet` — deliberately, not an accident: delta-rs
  [PR 4498](https://github.com/delta-io/delta-rs/pull/4498) (fixing
  [issue 4474](https://github.com/delta-io/delta-rs/issues/4474)) added the early
  error because the CDF scan path does not handle column mapping. Kernel-based CDF is
  an open draft ([delta-rs #3467](https://github.com/delta-io/delta-rs/pull/3467),
  tracked by [#3689](https://github.com/delta-io/delta-rs/issues/3689)).

Conclusion for this pipeline: the delta-rs route **cannot** replace `table_changes()`
for these tables, and column mapping cannot be dropped to work around it —
`delta.enableIcebergCompatV2` (what makes the UniForm Iceberg side work for
StarRocks) requires it. The remaining zero-DBU option is self-managed OSS Delta on
Spark: OSS Delta supports change-feed reads on column-mapped tables as long as the
read range spans no rename/drop (delta PR 1358; this pipeline only ever adds
columns), so a local Spark session reading the same abfss location works — verified
the same day, section 4. The trade is runtime weight: a JVM, the hadoop-azure jars,
and tens of seconds of startup per sync run.

---

## 4. Experiment: OSS Delta on Spark reading the change feed (2026-10-14)

The zero-DBU option that delta-rs could not deliver, tried immediately after with a
matched Spark/Delta pair. Same table, same abfss location, same ADLS service
principal; reader is OSS Delta 4.0.0 on self-managed Spark (pyspark 4.0.4, local
mode). Script: `scripts/spark_cdf_zero_dbu_prototype.py`, run as

    uv run --python 3.13 --with delta-spark==4.0.0 --with "pyspark<4.1" \
      python scripts/spark_cdf_zero_dbu_prototype.py

It works, end to end, with no Databricks involvement at any point:

- **Current state** reads correctly — rid 1 `first`, rid 2 `second-updated`
  (delta-rs returned NULLs for the same scan; OSS Delta applies the column-mapping
  physical names properly).
- **The whole change feed** (`readChangeFeed` from version 0) returns the table's
  complete history: 6 rows — inserts at version 1, the update's
  preimage/postimage pair at version 2, the delete at version 3 — exactly the seed
  changes and exactly what `table_changes()` produced per the CDF POC plan.
- **Replaying the feed** (upsert on insert/update_postimage, delete on delete)
  reconstructs the current state exactly (`match: True`), and the highest
  `_commit_version` is 3, matching the watermark in `reverse_etl_cdf_state`.
- **Timestamps come out pipeline-ready**: rendered in the projection with
  `date_format(..., "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")`, `updated_at` arrives as
  `2026-10-11T11:19:11.821229Z` — byte-identical to what `_read_changes()` sends
  today. Collecting TimestampType into Python datetimes instead would have the
  3-hour local-zone shift (see below), which is why the projection formats.

Findings and caveats, in the order they were hit:

1. **Version alignment is the real constraint.** Delta 4.0.0 is built for Spark
   4.0; on Spark 4.1.x every Delta read fails with `ClassNotFoundException:
   org.apache.spark.sql.catalyst.expressions.TimeAdd`. Also note `delta-spark==4.0.0`
   does **not** pin pyspark below 4.1 — `--with "pyspark<4.1"` is required or the
   mismatch resurfaces silently. **Resolved in pyproject on 2026-10-14:**
   `pyspark>=4.0.0,<4.1.0` (was `>=4.1.0,<4.2.0`). The old pin was gated by the
   Iceberg Spark runtime jar; that needed no downgrade — Iceberg 1.11.0 also
   publishes the Spark-4.0 artifact, so `Dockerfile.dagster` and
   `scripts/user_activity_flow.py` now stage/load
   `iceberg-spark-runtime-4.0_2.13` instead of `-4.1_2.13`. The running container
   still has pyspark 4.1.3 until the image is rebuilt.
2. **pyspark does not bundle hadoop-azure.** Without
   `org.apache.hadoop:hadoop-azure:3.4.2` on the classpath (matching the Hadoop
   3.4.2 client Spark 4.0/4.1 bundle), the table load fails with
   `ClassNotFoundException: Class org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem
   not found`. The script wires it via `configure_spark_with_delta_pip`'s
   `extra_packages`, or `spark.jars.packages` where the delta pip package is absent.
3. **Spark's own memory floor is 512m** (`INVALID_DRIVER_MEMORY` below that) — fine
   inside the 1536M webserver container, which also hosts the webserver itself.
4. **Never collect TimestampType into Python datetimes.** The collect path converts
   to naive datetimes in the machine's local zone — the same 3-hour shift as the
   drt comparison's section 3.4 — regardless of
   `spark.sql.session.timeZone = UTC` being set (verified: the conf reads back UTC
   while the collect still shifts). `date_format` in the projection honors it and
   renders UTC with microseconds.

What a production wiring would still need (not built): the backend swap in
`_read_changes()` (or a parallel asset), a Spark session in the Dagster container
with the ADLS credentials and the pyspark/Delta alignment from (1), and the table
location recorded per sync or resolved without Databricks. The watermark, Debezium
grouping and everything downstream stay as they are. Section 5 checks each of these
against the component-built demo jobs.

---

## 5. Feasibility for the component demo jobs (2026-10-14)

Whether the component-built demo sync (`ReverseEtlCdfSync` -> `build_sync_asset`,
asset `reverse_etl_<label>_to_kafka`) can run on the zero-cost path. Every Databricks
call in the sync asset was checked, and the two uncertain ones were tested live
(script: `scripts/zero_cost_sync_feasibility.py`, same matched Spark/Delta pair as
section 4).

| Sync-asset step | Today (Databricks compute) | Zero-cost replacement | Verified |
|---|---|---|---|
| read the change feed | `table_changes()` on the warehouse | Spark `readChangeFeed` | yes (section 4) |
| read live columns for the schema | `information_schema` SQL | the Delta schema Spark already read | yes (section 4) |
| resolve the table's storage location | `DESCRIBE DETAIL` (one-time) | UC REST `GET /tables/{fqn}` — control plane, free | yes, returns the same abfss location |
| read the watermark | SQL SELECT | spark-free: delta-rs pre-check reads it (+ `last_schema`), the sync reuses the values | yes (2026-10-17) |
| `DESCRIBE HISTORY` guards (first run, no-new-commits) | SQL | `DeltaTable.history()` | yes |
| **write the watermark** | SQL MERGE | Spark **append** (append-only state, freshest row wins) — needs the plain state table (see below) | yes (2026-10-17) |
| seed asset / make-change job | SQL INSERT/UPDATE/DELETE | stays on Databricks SQL (it is the demo's "change data in Databricks" step) | — |
| topic / RisingWave / sink / health assets | already Databricks-free | unchanged | — |

### The watermark-write blocker

OSS Delta **cannot commit to tables with
`delta.universalFormat.enabledFormats = 'iceberg'`**: the commit hook runs UniForm's
Iceberg conversion and fails with `[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED]` /
`[DELTA_MISSING_ICEBERG_CLASS]` — the converter class
(`org.apache.spark.sql.delta.icebergShaded.IcebergConverter`) ships only in the
Databricks runtime, not in OSS delta-spark. Found live on the `cdf` state table
(test MERGE wrote the same watermark value back and the commit landed before the
conversion failed — value unchanged, no harm; the reset job drops the table anyway).

Fix (applied and verified 2026-10-14): the state table is internal bookkeeping —
nothing reads it through Iceberg — so it does not need UniForm at all.
`build_table_setup_asset()` now creates it as **plain Delta** (no TBLPROPERTIES;
the source table keeps them for StarRocks). Verified on the live `cdf` state
table, recreated with the new DDL (`scripts/option_a_plain_state_verification.py`):

- OSS Delta MERGE wrote the watermark (Delta version 1, op MERGE) — the write that
  failed on the UniForm table.
- Databricks SQL read the OSS-written commit (watermark unchanged, 3).
- Databricks SQL MERGE wrote onto the OSS-touched table (`num_updated_rows: 1`) —
  the exact statement `_write_state()` builds, so the notebook's MERGE is
  the same shape.
- OSS Delta read the Databricks commit back (Delta version 2, op MERGE).

Both writers interoperate on the plain table, so the watermark stays shared
between the Dagster asset and the notebook while the sync's writes cost zero
DBUs. The `orders` and `avro` state tables were converted the same way on
2026-10-14 (`scripts/convert_state_tables_plain_delta.py`: drop, recreate plain,
preserve the watermark rows; avro had the UniForm property, orders came back
empty — consistent with a pre-demo state, the next sync replays and upserts
converge). Alternative without touching the state table: keep only the one
watermark MERGE on the Statement Execution API — near-zero (about one second of
warehouse per sync run).

Addendum (2026-10-17): the watermark write is now an **append, not a MERGE** —
one auditable row per sync, the *freshest* row wins on read (append-only made
the write a cheap single-commit insert, ~2s cheaper per sync, and killed the
duplicate-row ambiguity for good). The read side also moved spark-free: the
no-op pre-check reads the watermark + `last_schema` with delta-rs and the sync
reuses those values instead of re-reading them through Spark. Two further
findings from that change, both verified live against this workspace:

- **A Spark-written table cannot be delta-rs-written on this runtime.** Every
  table Spark touches carries the v7 protocol with a writer-feature list
  (rowTracking, domainMetadata — even as `'supported'`), and delta-rs 1.6
  refuses to commit to it (`CommitFailedError: Unsupported table features
  required`). The state table therefore has a *single writer* (Spark) while
  delta-rs does all the reads. This also rules out delta-rs's native CDF reader
  (`DeltaTable.load_cdf`) for the source tables: it rejects column-mapped
  tables outright ("not supported for CHANGE DATA FEED scans yet") — and column
  mapping is what makes DROP/RENAME COLUMN work.
- **There is no Databricks-created table shape delta-rs can write** (verified
  live 2026-10-19, asked directly): creating a table with pinned legacy
  properties (`'delta.minReaderVersion' = 2, 'delta.minWriterVersion' = 5`)
  is overridden — the table is born (reader 3, writer 7) with the runtime's
  full default feature list — and opting the mandatory features out at creation
  is refused outright: `[DELTA_UNSUPPORTED_FEATURE_STATUS] Expecting the status
  for table feature domainmetadata to be "supported", but got "unsupported"`.
  The runtime mandates rowTracking/domainMetadata at birth, so every
  Databricks-created table requires features delta-rs cannot commit. The only
  dual-writer shape would be a delta-rs-born table that Databricks also writes
  (plain Spark writes conform to a table's existing protocol rather than
  upgrading it), but registering a delta-rs-born table in Unity Catalog needs
  `CREATE EXTERNAL TABLE` on the external location — a privilege the demo's
  service principal does not hold (nor `MANAGE`, so it cannot self-grant).
- **Never re-create a state table via `CREATE OR REPLACE`**: the RTAS inherits
  the source selection's bumped protocol and lands exactly on the unwritable
  shape. Drop + re-create with the plain DDL instead (the setup asset does).

### Wiring plan

The component architecture makes the swap a config flip per sync: add an
`extraction` field (`databricks_sql` default | `oss_spark`) to
`ReverseEtlSyncConfig` and the `ReverseEtlCdfSync` component, branch in
`build_sync_asset()` (Spark-backed read path feeding the same row dicts — with
`date_format` timestamps, the downstream `_to_debezium_events`, Avro/JSON builders,
`add_missing_columns`, Kafka produce and run metadata are unchanged), then set
`extraction: oss_spark` in the syncs' `defs.yaml`.

Container prerequisites (after the pyspark pin rebuild): stage
`io.delta:delta-spark_2.13:4.0.0` + `hadoop-azure` jars in the image next to the
Iceberg jars, pass the `ADLS_*` credentials to the dagster services in compose, and
run the Spark session with the prototype's settings (512m driver, local[1], the
Spark-Connect env-var pops from `user_activity_flow.py`).

After the swap, the only Databricks compute left in the demo loop is the
make-change job's SQL (the insert/update/delete that creates the change) — seconds
per demo change. The notebook trigger path (section 6 of the live-demo doc) stays
as the Databricks-compute alternative and shares the watermark unchanged.

### Why the make-change itself is not worth de-costing (2026-10-14)

Considered making the remaining make-change write free via APIs. Conclusion: it
is already at the floor. Every Databricks API that can write a UC-managed Delta
table runs on Databricks compute — Statement Execution (SQL warehouse, the
current ~1–2s per change), Databricks Connect (cluster/session overhead), Jobs
(1–3 min serverless spin-up per run, strictly worse), Genie (warehouse again),
UC Iceberg REST writes (billed server-side). The only zero-compute write path is
direct-to-storage, and it is blocked for the source table: it is UniForm (so
StarRocks can read it through UC federation for free, section 1), and OSS Delta
cannot commit to UniForm tables (proven live — section 5's state-table blocker).
Dropping UniForm from the source would trade a fractions-of-a-DBU write for
breaking the free-read premise. So: make-change stays on the warehouse.
Measured 2026-10-15: the source table's property overhead (UniForm Iceberg
conversion + CDF + column mapping) costs about 0.7s per warm tiny write (3.6s vs
2.9s on the plain-Delta state table) — thousandths of a DBU. And the source must
stay UniForm anyway: the `databricks_uc` StarRocks catalog is UC's Iceberg REST
endpoint, which only serves UniForm tables; a plain Delta source would vanish
from the free read path. StarRocks does have a native `deltalake` external
catalog (reads the Delta log from ADLS2 with service-principal auth — supported
in the stack's 4.1.4, zero Databricks compute), but it requires a Hive metastore
or AWS Glue to map table names to locations — UC is neither — so for UC-managed
tables it would mean operating an HMS mirror. UniForm + Iceberg REST is exactly
the metastore-free alternative, which is why the demo pays the 0.7s.

Recurring-job audit (2026-10-14): the sink-health schedules (every 5 min per
sync) were checked for hidden Databricks usage — they read only Kafka Connect's
status endpoint and the sink's consumer-group lag from Kafka. Every recurring
touchpoint of the demo is therefore either Databricks-free (sync, health
checks, StarRocks reads) or at the write floor (make-change, setup/seed DDL).

The demo page shows these costs live (2026-10-15): every instrumented
asset/op emits its metered usage as run metadata
(`orchestration/assets/databricks_cost_meter.py` — `_run_sql` records each
warehouse statement's wall time; the oss_spark sync emits 0.0 structurally),
and the demo helper reads it back through the GraphQL API, rendering cost
chips per run plus a session total. Verified live: make-change reported
4.1s of SQL warehouse, the sync reported 0.0 with "no Databricks compute".
The meter is self-updating: flipping a sync back to `extraction:
databricks_sql` would change its chip from 0 to measured serverless SQL
warehouse seconds automatically (the warehouse bills on the serverless SKU —
see section 6).

### Status: wiring built (2026-10-14, later the same day)

The swap described above is built and enabled for **all four syncs** (`avro`, `cdf`, `orders`, `delta`):
`ReverseEtlSyncConfig.extraction` (`databricks_sql` | `oss_spark`), the component
field, a `build_sync_asset()` dispatcher and
`orchestration/assets/reverse_etl_spark_reader.py` (UC REST location lookup,
Spark session with the prototype's settings, watermark read + MERGE, CDF read,
Delta-schema row fields). All three `defs.yaml` files run `extraction: oss_spark`
(cdf first, orders/avro on 2026-10-14). `delta-spark==4.0.0` is pinned in
pyproject next to `pyspark>=4.0.0,<4.1.0`. The dagster image was rebuilt the
same day and now runs pyspark 4.0.4 + delta-spark 4.0.0 with the pre-warmed Ivy
cache.

Laptop verification, data-carrying (2026-10-14):

- **cdf**: full loop green — CDF read, 1 update event to `reverse_etl_cdf_topic`,
  watermark → 4, `zero-cost-demo` landed in both the RisingWave and Postgres
  target tables (see "Full-loop result" below).
- **avro**: full loop green — the Avro path (schema registry + wire format)
  needed no changes; a replay from watermark 0 produced 6 events
  (3c/2u/1d), RisingWave's `reverse_etl_avro_target` converged (id=2
  `zero-cost-avro`, id=3 deleted) and the watermark MERGE advanced it 0 → 4.
- **orders**: sync green — 6 events replayed to `reverse_etl_orders_topic`,
  watermark → 4, STEP_SUCCESS in 32.5s. Its RisingWave table and Debezium
  connector are still missing from the local stack (lost in the earlier stack
  reset); the demo page's "Reset and set up" recreates them, and the topic
  replay converges the targets.

In-container verification (2026-10-14, after the image rebuild): the avro sync
was driven through the demo page's own launch API
(`POST /api/launch {"sync":"avro","action":"sync"}`) and completed SUCCESS on
the OSS Spark path — CDF replay from watermark 0 to 3, watermark MERGE inside
the container, and both targets converged (RisingWave and Postgres
`reverse_etl_avro_target`: id=1 `first`, id=2 `second-updated`, id=3 deleted).
The pre-warmed Ivy cache is confirmed indirectly: the VPN was on (staging
Kafka reachable, Maven blocked) and the session still got its jars. All three
demos now run the zero-DBU sync from the demo page.

### Running the zero-DBU sync from the laptop (2026-10-14)

The first data-carrying laptop run hung ~7 minutes after "Reading CDF", at the
RisingWave connection, then failed fast once a connect timeout was added. Root
cause chain, all verified live:

- `dagster asset materialize` loads `.env` **from the current working directory**
  and overrides the process environment with it for the whole command
  (`_inject_local_env_file` + the `environ()` context manager in
  `dagster/_cli/utils.py`). Exported/inline env vars lose to `.env` values —
  including `RISINGWAVE_HOST=frontend-node-0`.
- On the laptop (VPN), `frontend-node-0` does not resolve to the local docker
  stack: the corporate DNS search domain returns 8 public GCP addresses that
  never answer on 4566. Spawned step children inherit the overridden value.
- The old code had no libpq `connect_timeout` (unlimited), so the first
  unreachable address hung the run forever. Adding `connect_timeout=10` in
  `_get_risingwave_connection()` (kept) turned the hang into a clean
  `OperationalError` listing all 8 addresses (8 × 10s ≈ the observed 84s).
- Inside the container this never bites: the CLI reads the same `.env`
  (CWD `/workspace`), and `frontend-node-0` is the correct docker-network alias.

Laptop workaround: run the CLI from a directory without `.env`, letting
`orchestration/definitions.py`'s own `.env` parser (which respects pre-set vars)
fill everything else:

```bash
mkdir -p /tmp/rwrun && cd /tmp/rwrun
PYTHONPATH=<repo> RISINGWAVE_HOST=127.0.0.1 KAFKA_CONNECT_URL=http://localhost:8083 \
  uv run --project <repo> dagster asset materialize \
  --select <asset> -m orchestration.definitions
```

(`KAFKA_CONNECT_URL` is needed for the Debezium sink asset — its default is the
container-internal `http://kafka-connect:8083`; the host port is published on
127.0.0.1:8083.)

One principal gotcha from the restore flow (2026-10-14): tables recreated
out-of-band with the personal CLI profile are owned by that user, and the
container's service principal then cannot drop them (`PERMISSION_DENIED: ...
MANAGE` in the reset's drop step). Drop the offending table with the owning
principal first, or recreate demo tables only through the setup assets (which
run as the SP).

### Full-loop result (2026-10-14)

With the local stack's RW target table and Debezium connector re-created via the
same assets, the zero-DBU sync ran the whole loop from the laptop: Databricks
UPDATE (version 4, the only paid step) → OSS Delta Spark sync read the CDF,
produced **1 update event** to `reverse_etl_cdf_topic`, and MERGE'd the watermark
to 4 — STEP_SUCCESS in 36.7s including Spark startup. The event landed in both
targets: RisingWave `reverse_etl_cdf_target` and host Postgres
`reverse_etl_cdf_target` both show `id=2, value='zero-cost-demo'`.

One cosmetic artifact seen in the logs: a
`ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtensions` line during
session startup that did not affect the run (Delta reads and the watermark MERGE
succeeded). Worth a look when the container is rebuilt.

---

### 2026-10-19 — the Spark path itself is gone (delta-rs version-range reconstruction)

Full live proof (all four syncs), the measured Spark-era comparison, and the
assessment: **[`REVERSE_ETL_SPARK_FREE_FINDINGS.md`](REVERSE_ETL_SPARK_FREE_FINDINGS.md)**.

The experiment graduated past Spark: both extraction backends are deleted
(`reverse_etl_spark_reader.py` with its local JVM, and the `table_changes()`
SQL path) and the `extraction` config field with them. The sync asset now reads
exclusively through `orchestration/assets/reverse_etl_delta_reader.py`: for
each commit version in the requested range it diffs the snapshot at v-1 against
the snapshot at v (two column-pruned scans) and emits CDF-shaped rows —
insert, update_preimage + update_postimage pairs, delete — ordered by
`_commit_version`, pinned to the version the watermark will advance to. That is
what `readChangeFeed`/`table_changes()` do internally for tables without the
CDF feature; per-version diffing is required so a key deleted in one commit and
reinserted in the next emits both events. The watermark write stays the one
small SQL statement (delta-rs cannot commit to the warehouse's v7 protocol
state table): a full sync now meters ~1.5-2 warehouse seconds and nothing else.

Two verified findings worth keeping:

- **delta-rs 1.6.6 reads warehouse-written column-mapped sources wrong**
  (columnMapping as a writer feature, no reader features — UniForm v2 and the
  plain column-mapped flavor alike): the scan returns the logical schema with
  every value None (the physical->logical rename is not applied). The reader
  detects the all-None scan and re-reads the snapshot's parquet files directly,
  renaming via `delta.columnMapping.physicalName` (pyarrow's AzureFileSystem,
  same service principal) — and refuses sources with deletion vectors, which
  file reads cannot apply.
- The reconstruction reproduces `table_changes()`'s output exactly on the demo
  loop: backfill of the 3-row seed as inserts, then insert/update/delete change
  ops each produced the same single event the CDF read produced.

---

## 6. How to verify against the billing system

`system.billing.usage` has SKU-level detail — **verified runnable on
2026-10-14** (the POC's SP has billing access). The demo warehouse
(`risingwave-poc`, 2X-Small) bills on `PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH`
(per-second serverless, no idle burn despite `warehouse_type: PRO`):

```sql
-- warehouse-attributed DBUs per day
SELECT usage_date, sku_name, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= '2026-10-12'
  AND usage_metadata.warehouse_id = '4d06eca1e71a9ccc'
GROUP BY 1, 2 ORDER BY 1, 2;
```

Actuals while the sync still ran `table_changes()` on the warehouse: **4.97 DBUs
(Oct 12), 0.45 (Oct 13)** — all serverless SQL, no other SKUs. That includes any
dbt/other statements on the same warehouse (billing does not attribute
per-statement), but the sync was the repeated per-change offender; from
2026-10-14 the sync reads the change feed and writes the watermark on OSS Spark
(zero DBUs), leaving only the make-change SQL and the one-time setup/seed DDL.
Note billing rows lag roughly a day, so the first zero-DBU days are not visible
in this table yet.

---

## 7. Not verified

- No `system.billing.usage` query had been run against the workspace when the
  cost statements were written; on 2026-10-14 the query in section 6 ran
  successfully (the SP has billing access) and confirmed the warehouse bills on
  serverless SQL only. Specific DBU **rates** are still deliberately not quoted
  here; they differ by SKU, region and the account's EUP, and the docs' pricing
  pages change.
- The zero-DBU sync backend is **built and verified end-to-end for all three
  syncs, on the laptop and inside the container** (section 5, status + full-loop
  result). The demo page drives the zero-DBU path in the rebuilt container
  (pyspark 4.0.4 + delta-spark 4.0.0, Ivy cache pre-warmed in the image).
- The Kafka-produce side now **verifies delivery**: `_produce_to_kafka` collects
  per-message `on_delivery` errors and bounds `flush()` at 120s, raising before
  the watermark MERGE if anything failed — previously a silent produce failure
  would still advance the watermark and lose those events. Verified both
  directions on 2026-10-14: against a dead bootstrap port the run fails
  (`Kafka delivery ... 1 undelivered after 120s`) with the watermark unchanged,
  and the immediate retry against the real broker delivers the same event and
  advances the watermark (self-healing replay).
- The Kafka-produce side of the sync does not inspect delivery results
  (`flush()` without callbacks), so a silent produce failure would still advance
  the watermark — a known gap, unchanged from the SQL-based backend.
- The state-table fix from section 5 is **applied and verified** (code change +
  live cross-writer test on the `cdf` state table). The `orders` and `avro` state
  tables still carry the old UniForm properties until their next reset + setup.
- Reading the change feed through Unity REST vended credentials instead of direct
  storage credentials: not tested.
- Specific DBU rates are deliberately not quoted here; they differ by SKU, region and
  the account's EUP, and the docs' pricing pages change.

## Sources

- Databricks docs: Unity Catalog credential vending for external system access
  (<https://docs.databricks.com/aws/en/external-access/credential-vending>)
- Databricks docs: Access Databricks tables from Apache Iceberg clients
  (<https://docs.databricks.com/aws/en/external-access/iceberg>)
- Databricks docs: Cross-engine attribute-based access controls — the page that
  documents the serverless charge for ABAC enforcement
  (<https://docs.databricks.com/aws/en/external-access/cross-engine-abac>)
- Databricks blog: Secure External Access to Unity Catalog Assets via Open APIs
  (<https://www.databricks.com/blog/secure-external-access-unity-catalog-assets-open-apis>)
- delta-rs PR 4498 / issue 4474: CDF scans deliberately rejected on column-mapping
  tables (<https://github.com/delta-io/delta-rs/pull/4498>)
- delta-rs issue 3689 / PR 3467: kernel-based CDF reader, open
  (<https://github.com/delta-io/delta-rs/issues/3689>)
- delta PR 1358: change-feed reads on column-mapping tables without rename/drop
  (<https://github.com/delta-io/delta/pull/1358>)
