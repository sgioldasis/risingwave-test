# Why StarRocks alongside RisingWave?

*2026-11-17. A written-out answer to the recurring question: "if we already
have RisingWave, what does adding StarRocks buy us — and can't RisingWave
just reach the lake tables?" Every claim below is verified in this project;
the supporting demos and docs are linked inline.*

## The overlap question

RisingWave and StarRocks overlap more than most pairing charts admit: both
consume Kafka, both maintain materialized views, both serve dashboards. The
honest framing is that they occupy different layers.

### 1. Different engine categories

- **RisingWave is a streaming state manager**: it ingests event streams,
  incrementally maintains materialized views, emits to sinks. Its storage
  exists to serve its own MVs.
- **StarRocks is an OLAP serving engine**: MPP execution, a cost-based
  optimizer, real joins and aggregations over large data at interactive
  latency.

RisingWave answers "what is the current state of the stream?" — StarRocks
answers "analyze and serve anything, fast, at scale."

### 2. Serving features RisingWave doesn't have

Demonstrated on the StarRocks Serving Features dashboard
(`docs/SR_POC_SERVING_FEATURES_DEMO.md`):

- **Table models with real semantics**: PRIMARY KEY point lookups and
  partial-column updates (`wallet_status_update_load` writing only `status`),
  UNIQUE KEY whole-row replace, AGGREGATE KEY ingest-time merging —
  `wallet_type_totals_agg` sums continuously with no MV and no query-time
  GROUP BY (see `docs/SR_POC_WALLET_UPSERT_DEMO.md` and
  `docs/SR_POC_FUNNEL_AGGREGATE_KEY_DEMO.md`).
- **Computed columns** — rates defined once at the table, no drift.
- **Query-time ASOF JOIN** over real tables.
- **Workload isolation**: resource groups pinning BI traffic
  (`rg_superset_demo`, cpu_weight 80) so MV refreshes can't starve the
  dashboard.

RisingWave can't do any of those four — its tables are MV backing stores,
not serving structures.

### 3. The lakehouse layer

StarRocks queries Iceberg/Delta/JDBC sources *in place* — the lake cache
benchmark, the UC federation, and the DuckDB-over-FlightSQL demo all live
here — with MV query rewrite accelerating lake scans
(`docs/SR_POC_QUERY_REWRITE_DEMO.md`). RisingWave can *read* Iceberg, but
as a source, not a lake query engine; nobody runs Trino-class batch
analytics through RisingWave.

### 4. Fewer hops for the hot path

Routine Load means Kafka → StarRocks with **no streaming engine in the
loop** — the "hot-only" contrast on the Funnel and Wallet dashboards exists
because StarRocks can ingest directly. That's an architectural option
RisingWave can't offer by definition.

### The honest counterbalance

- **Overlap is real**: in this very project, `funnel_summary` exists twice —
  a RisingWave MV and a StarRocks view over raw events. Same data, two
  engines, deliberately, to compare.
- **Operational cost is real**: another cluster (FE + CN + MinIO storage
  volume, `docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md`), another failure
  domain — on 2026-11-17 a CN blip paused all 10 Routine Loads silently and
  froze the funnel dashboard.
- **If you only need fresh aggregates on a dashboard, RisingWave alone is
  enough** — adding StarRocks for that is paying for capability you don't
  use.

**One line**: RisingWave keeps state current; StarRocks serves and analyzes
it — plus everything RisingWave can't reach (lake tables, batch history, BI
workload isolation, upsert serving structures). The moment questions involve
lake data, ad-hoc heavy joins, BI workload isolation, or upsert serving
structures, RisingWave stops being sufficient and StarRocks earns its place.

---

## "Can't RisingWave reach the lake tables?"

It can — and the precise version of the claim matters. RisingWave *does*
read lake tables in this project (the Iceberg countries source, the
Lakekeeper federation). The accurate statement is: **it reaches them, but
only in a streaming-ingestion posture, and every attempt to treat them as
queryable analytics data has hit a wall in this project.** Four concrete
ones:

### 1. Batch-query ergonomics are missing by design

RisingWave's model for external tables is *source* — data flows through it
into materialized views. Ad-hoc analytical querying over a lake table is
not the product shape. Proven concretely on RisingWave 3.0: `CREATE VIEW`
over an Iceberg source fails outright with the DataFusion engine; the
workaround is `CREATE TABLE ... WITH (FULL_RELOAD)` plus a join MV — i.e.
forcing the lake table through streaming machinery to query it at all.
RisingWave wants to *ingest* the lake, not *scan* it.

### 2. No scan-acceleration story

The lake cache benchmark makes the point: 732ms cold → 186ms warm for the
same Iceberg scan, because the StarRocks CN's datacache holds hot blocks
while MinIO stays the durable tier. RisingWave has no equivalent for
external object-storage scans — every read is a fresh scan. At lake-table
sizes that's the difference between interactive and batch.

### 3. Catalog/auth friction (the part that actually blocked us)

RisingWave 3.1.1 could not read Unity Catalog Iceberg tables on ADLS Gen2
at all — UC vended nothing from the managed identity and dropped the
`adlsgen2` properties. It took the v3.2.0-alpha build with `rest_rust` +
direct service-principal properties to make the read work. StarRocks read
the same tables via its REST catalog + OAuth2 credential from the start.
The connective tissue to lake *catalogs* is simply younger in RisingWave.

### 4. The write direction is worse

RisingWave → Iceberg works for its own managed tables
(`sink_funnel_to_rw_iceberg`) — and even there, snapshot expiration has a
known bug with REST catalogs (metadata expires but data files are never
pruned). Writing into *externally-owned* lake tables (Unity Catalog
managed) doesn't work: upsert sinks stall silently, and even append-only
was rejected on retest. StarRocks writes UC Iceberg tables verified — that
is how `funnel_summary_historical` is maintained today.

### The accurate picture

RisingWave touches the lake as a **pipeline endpoint** — pull incremental
data in, or push snapshots out. StarRocks touches it as a **query and write
surface** — federation in place, MV rewrite on top, datacache underneath,
catalog-native writes. "Can't reach" was shorthand; "can reach but only as
a source/sink, with catalog gaps and no scan acceleration" is the truth.
For a stack where the lake is just a cold archive, RisingWave's reach is
enough. The moment the lake becomes something you *query* — which is what
happened here with the historical table and the StarRocks serving layer —
StarRocks's relationship to it is structurally different.
