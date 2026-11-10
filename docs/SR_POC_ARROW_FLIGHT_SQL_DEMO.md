---
title: StarRocks Arrow Flight SQL — columnar serving spike and benchmark
description: Enabling and benchmarking StarRocks's Arrow Flight SQL endpoint against the MySQL wire protocol — 10x on large federated pulls, slower on point lookups, and every gotcha hit while enabling it
---

<!-- markdownlint-disable-file -->

## What this is

StarRocks ships an **Arrow Flight SQL** endpoint (gRPC, results delivered as
Arrow record batches) alongside the MySQL wire protocol. The arrow format is
what DuckDB/pandas/pyarrow consume natively, so a Flight client skips the
per-row conversion the MySQL protocol always pays. This doc records the
spike that enabled it on this stack (2026-11-10), the benchmark results, and
the five config gotchas that stood between "port enabled" and "query works".

**Status: working, benchmarked.** Full pull of the 420k-row federated Delta
source: **0.10s via Flight vs 1.05s via MySQL — ~10x**, 4.0M rows/s. Point
lookups: Flight ~2x *slower* (more round trips per query). Details below.

## The setup (what makes it work on this stack)

Three config pieces, all in the entrypoint scripts (idempotent append-on-start
pattern, same as every other fe.conf/cn.conf block there):

| Piece | File | What |
|---|---|---|
| FE Flight port | `starrocks/fe-entrypoint.sh` | `arrow_flight_port = 8010` in fe.conf, plus docker-compose publishes `8010:8010` |
| FE JVM flag | `starrocks/fe-entrypoint.sh` | `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` in JAVA_OPTS — without it every query dies in the FE→BE proxy with `Failed to initialize MemoryUtil` (Arrow's direct-memory access needs the add-opens on Java 17+) |
| CN Flight port | `starrocks/cn-entrypoint.sh` | `arrow_flight_port = 8010` in **be.conf** — the FE proxies DoGet result batches to the CN's own Flight service; without it the proxy fails with `port out of range:-1` |

### Gotchas found while enabling (the reason you spike first)

1. **The key defaults to `-1` (disabled) and is runtime-immutable** —
   `ADMIN SHOW FRONTEND CONFIG` shows `IsMutable: false`, so it's
   fe.conf + FE restart, not `ADMIN SET FRONTEND CONFIG`.
2. **The CN reads `be.conf`, not `cn.conf`**, for `arrow_flight_port`. The
   CN's own startup log says so explicitly (`You can modify arrow_flight_port
   in be.conf`); an append to cn.conf alone is silently ignored — the CN even
   logs "Arrow Flight SQL Server is disabled" and carries on healthy.
3. **The FE JVM add-opens** (table above). This one only appears at query
   time, deep in the FE log as `ExceptionInInitializerError`, not at startup.
4. **Client library**: pyarrow 25 has dropped `FlightSqlClient`. The supported
   Python client is **ADBC** (`pip install adbc-driver-flightsql`).
5. **Auth**: the server rejects unauthenticated connections with a bare
   `UNAUTHENTICATED (vendor code 16)`. StarRocks Flight SQL takes **basic
   auth as an RPC header**, not a token option:
   `adbc.flight.sql.authorization_header: "Basic <base64(user:pass)>"`.
6. **The FE advertises its endpoint as `grpc+tcp://starrocks:8010`** — the
   container hostname. Clients inside the docker network resolve it; host
   clients do not (it resolved to a public internet IP here) and fail at the
   final `DoGet` fetch with a timeout, *after* the query already ran. Two
   implications: benchmark/demo clients should run in a container on the
   compose network, and any external BI consumer needs a DNS/hosts entry for
   `starrocks`.

### Architecture note: FE + CN both serve Flight

The client only ever talks to the FE's port. Internally, after the FE plans
the query, the CN computes and the FE **proxies** the Arrow batches back over
its own gRPC channel to the CN's Flight service. That's why both processes
need `arrow_flight_port`, and why the CN's port needs no host publishing.

## The benchmark

`scripts/starrocks_flight_benchmark.py` (one-click: `./bin/5_starrocks_flight_benchmark.sh`).
Runs inside a container on the compose network — the dagster-webserver works
out of the box (pymysql + pyarrow are project deps; the ADBC driver is a
writable-layer install that `./bin/5_starrocks_flight_benchmark.sh` re-runs
each time, so an image rebuild doesn't break the script).

Both sides consume identically (streamed row-by-row / batch-by-batch), so the
comparison measures transfer + conversion, not client-side materialization.

Results (2026-11-10, FE 4.1.7, single CN, data on ADLS via the delta_federation
catalog / internal PK table on MinIO shared-data; second run, warm metadata —
first run was 0.10s vs 1.05s, same ratio):

```
1) Full pull of delta_federation.delta_fed.reverse_etl_delta_source (420,005 rows, streamed), 3 runs:
    mysql: best   1.02s  median   1.03s  (411,043 rows/s at best)
   flight: best   0.05s  median   0.06s  (7,855,578 rows/s at best)

2) Single-key point lookups on flight_bench.bench_txn (200 lookups, 10,000 rows):
   mysql : avg   2.48 ms  p50   2.27 ms
   flight: avg   7.68 ms  p50   6.72 ms
```

**Reading the numbers:**

- **~10x on the big pull.** This is the headline: large analytical pulls
  (BI extracts, notebook workloads, DuckDB clients) get the columnar path
  end-to-end. The gap widens with column count — the MySQL protocol pays the
  row-conversion tax per row, Flight pays zero conversion.
- **Point lookups regress ~2x** (4.4ms → 8.4ms). One FlightSQL query is
  several gRPC round trips (prepare/GetFlightInfo/GetSchema + DoGet) where
  MySQL is a single round trip. The dashboard's point-lookup workload should
  stay on the MySQL protocol — which is what it uses today. Both absolutes
  are far below any human-perceivable threshold; this is a "don't migrate for
  this" note, not a blocker.
- Everything reads through **all catalogs** — the benchmark's scan is the
  *federated Delta* catalog (delta_federation), proving Flight serves
  externally-sourced results, not just internal tables.

## Reproducing from a cold stack

1. Bring up the stack (`./bin/1_up.sh`). The entrypoints enable Flight; no
   manual steps.
2. If the FE container was recreated since the last `starrocks-init`, the
   external catalogs need re-registering (FE metadata is container-local):
   `docker compose up starrocks-init`, then
   `./bin/3_setup_delta_federation.sh` for the Delta catalog, and the CN may
   need `docker compose restart starrocks-cn` (see the FE-metadata note in
   the runbook — same recovery as any FE recreate).
3. Run the benchmark:
   ```bash
   ./bin/5_starrocks_flight_benchmark.sh
   # (installs the ADBC driver into the container's writable layer, then runs
   #  scripts/starrocks_flight_benchmark.py inside dagster-webserver)
   ```

The benchmark creates its own database (`flight_bench.bench_txn`, 10k-row PK
table) — self-contained, no dependency on the demo's dbt models being built.

## Querying through DuckDB

The natural client pairing: StarRocks computes, results arrive as Arrow
batches over Flight, and DuckDB scans those buffers **zero-copy** — no copy,
no conversion, on either side. Verified from the host venv (2026-11-10):

```python
import base64
import adbc_driver_flightsql.dbapi as flight_sql
import duckdb

auth = base64.b64encode(b"root:").decode()
with flight_sql.connect(
    uri="grpc+tcp://localhost:8010",  # host: published port; in-container: grpc+tcp://starrocks:8010
    db_kwargs={"adbc.flight.sql.authorization_header": f"Basic {auth}"},
) as con:
    with con.cursor() as cur:
        cur.execute("SELECT * FROM delta_federation.delta_fed.reverse_etl_delta_source")
        arrow = cur.fetch_arrow_table()

ddb = duckdb.connect()
ddb.register("delta_source", arrow)  # zero-copy: DuckDB scans the Arrow buffers in place
ddb.sql("SELECT description, COUNT(*) FROM delta_source GROUP BY 1 ORDER BY 2 DESC").show()
```

Works against every catalog — swap in `risingwave`, `databricks_uc`,
`lakekeeper_local` queries; whatever the engine returns rides the same wire.

**Host prerequisite** (container runs need none of this): the FE advertises
its Flight endpoint as `grpc+tcp://starrocks:8010`, so the host needs

```
127.0.0.1  starrocks
```

in `/etc/hosts`. Without it the query *executes* but the final DoGet fetch
times out resolving the advertised name (see gotcha 6 above). This entry
also makes the benchmark runnable from the host venv.

One-command version: `./bin/5_starrocks_duckdb.sh` →
`scripts/starrocks_duckdb_demo.py` — pulls the federated source from the
host venv, registers it zero-copy, and runs two DuckDB analyses over it
(warns if the /etc/hosts entry is missing).

**Why not DuckDB `ATTACH` directly?** The cleaner
`ATTACH 'grpc+tcp://…' AS sr (TYPE ADBC, DRIVER 'libadbc_driver_flightsql.so')`
— DuckDB speaking Flight SQL natively — is blocked twice on this machine:
DuckDB 2.0 no longer ships the `adbc` extension for linux_arm64 (and the
dagster container's Python 3.14 venv can't downgrade to a 1.x that had it),
and `ATTACH` offers no way to pass the basic-auth header anyway. The
pull-then-register pattern sidesteps both and is the supported client shape
today.

## Honest caveats

- **No TLS/auth beyond basic** on the Flight endpoint — fine for a local PoC,
  a real deployment would front it differently.
- The 10x was measured on one CN with warm metadata; treat it as order-of-
  magnitude evidence of the protocol advantage, not a precise factor.
- ADBC's Flight SQL driver is still 1.x — the API (e.g. the streaming
  reader living at `cursor._results.reader`) is not a stability promise.
- `arrow_flight_port` was verified static (IsMutable: false) on 4.1.7; if a
  future release makes it mutable, `ADMIN SET FRONTEND CONFIG` could replace
  the fe.conf append + restart.
