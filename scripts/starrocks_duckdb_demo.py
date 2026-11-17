"""DuckDB querying StarRocks over Arrow Flight SQL — the zero-copy pairing
(docs/SR_POC_ARROW_FLIGHT_SQL_DEMO.md, "Querying through DuckDB").

StarRocks computes, the result arrives as Arrow record batches over Flight,
and DuckDB scans those buffers in place — no copy, no conversion. The demo
pulls the federated Delta source (the analyst path: StarRocks reads ADLS in
place, zero DBU) and then analyzes it with DuckDB SQL StarRocks never
precomputed.

Runs from the host venv (needs `127.0.0.1 starrocks` in /etc/hosts — the FE
advertises its Flight endpoint by that name) or from any container on the
compose network. Requires: pip install adbc-driver-flightsql duckdb.
"""

import base64
import socket
import sys
import time
import warnings

warnings.filterwarnings("ignore", message="Cannot disable autocommit")  # adbc dbapi noise

try:
    import adbc_driver_flightsql.dbapi as flight_sql
    import duckdb
except ImportError as e:
    sys.exit(f"missing dependency: {e.name} — pip install adbc-driver-flightsql duckdb")

FEDERATED_TABLE = "delta_federation.delta_fed.reverse_etl_delta_source"


def flight_uri() -> str:
    """The FE publishes Flight on 8010. In-container the advertised name
    resolves on its own; from the host it takes the /etc/hosts entry."""
    for host in ("starrocks", "localhost"):
        try:
            s = socket.create_connection((host, 8010), timeout=2)
            s.close()
            return f"grpc+tcp://{host}:8010"
        except OSError:
            continue
    sys.exit("cannot reach the Flight port 8010 — is the stack up (./bin/1_up.sh)?")


def report_json() -> dict:
    """The same story, machine-readable — the demo UI's 'DuckDB over Flight'
    button calls this through the demo server (same venv, in-process)."""
    uri = flight_uri()
    auth = base64.b64encode(b"root:").decode()
    t0 = time.perf_counter()
    with flight_sql.connect(uri=uri, db_kwargs={"adbc.flight.sql.authorization_header": f"Basic {auth}"}) as con:
        with con.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            cur.execute(f"SELECT * FROM {FEDERATED_TABLE}")
            arrow = cur.fetch_arrow_table()
    seconds = round(time.perf_counter() - t0, 2)

    ddb = duckdb.connect()
    ddb.register("delta_source", arrow)
    q1 = ddb.sql(
        "SELECT description, COUNT(*) AS n FROM delta_source GROUP BY 1 ORDER BY n DESC LIMIT 5"
    )
    q2 = ddb.sql(
        """SELECT count(*) AS rows, count(DISTINCT rid) AS distinct_ids,
                  min(rid) AS min_rid, max(rid) AS max_rid
           FROM delta_source WHERE rid <= 100000"""
    )
    return {
        "endpoint": uri,
        "version": version,
        "rows": arrow.num_rows,
        "mib": round(arrow.nbytes / 1024 / 1024, 1),
        "seconds": seconds,
        "makeup": {"columns": [d for d in q1.columns], "rows": [[str(v) for v in r] for r in q1.fetchall()]},
        "stats": {"columns": [d for d in q2.columns], "rows": [[str(v) for v in r] for r in q2.fetchall()]},
    }


def main() -> None:
    uri = flight_uri()
    auth = base64.b64encode(b"root:").decode()
    print(
        """
Two engines, one Arrow wire. StarRocks federates the Databricks Delta source
in place (reads ADLS directly -- zero DBU, no warehouse spun up) and streams
the result as COLUMNAR Arrow record batches over Flight SQL. DuckDB then
registers those buffers ZERO-COPY -- it scans the Arrow memory in place, no
conversion, no import -- and runs local SQL over it.

The point of the pairing: StarRocks is the serving/federation layer that can
reach the lake; DuckDB is the analyst's swiss-army knife for ad-hoc cuts the
serving layer never precomputed. Neither is the other's replacement -- the
wire between them (Arrow Flight SQL) is what makes the combination cheap.

The two queries below are DuckDB-local: the first profiles the make-up of
the source (every change the demo's syncs ever produced), the second is an
analytic cut over a row-id filter. Each prints its input SQL first.
"""
    )
    print("=" * 72)
    print("🦆 DuckDB over Arrow Flight SQL — StarRocks as the compute, DuckDB as the analyst")
    print("=" * 72)
    print(f"\nFlight endpoint: {uri}")

    t0 = time.perf_counter()
    with flight_sql.connect(uri=uri, db_kwargs={"adbc.flight.sql.authorization_header": f"Basic {auth}"}) as con:
        with con.cursor() as cur:
            cur.execute("SELECT version()")
            print(f"StarRocks reports: {cur.fetchone()[0]}")
            cur.execute(f"SELECT * FROM {FEDERATED_TABLE}")
            arrow = cur.fetch_arrow_table()
    print(f"\nPulled {arrow.num_rows:,} rows from {FEDERATED_TABLE}")
    print(f"  input: SELECT * FROM {FEDERATED_TABLE}")
    print("  wire: Arrow Flight SQL (columnar batches, zero conversion)")
    print(f"  memory: {arrow.nbytes / 1024 / 1024:.1f} MiB as Arrow, pulled in {time.perf_counter() - t0:.2f}s")

    ddb = duckdb.connect()
    ddb.register("delta_source", arrow)  # zero-copy: DuckDB scans the Arrow buffers in place
    print("\nRegistered in DuckDB zero-copy (input: ddb.register('delta_source', arrow_table)).")
    print("Queries StarRocks never precomputed -- DuckDB SQL runs locally against the")
    print("Arrow buffers StarRocks streamed:")

    print("\n1) The make-up of the source (every change the demo ever produced):")
    print("   input: SELECT description, COUNT(*) AS n FROM delta_source")
    print("          GROUP BY 1 ORDER BY n DESC LIMIT 5")
    ddb.sql(
        "SELECT description, COUNT(*) AS n FROM delta_source GROUP BY 1 ORDER BY n DESC LIMIT 5"
    ).show()

    print("2) An analytic cut — summary stats over a filter:")
    print("   input: SELECT count(*) AS rows, count(DISTINCT rid) AS distinct_ids,")
    print("                 min(rid) AS min_rid, max(rid) AS max_rid")
    print("          FROM delta_source WHERE rid <= 100000")
    ddb.sql(
        """SELECT count(*) AS rows,
                  count(DISTINCT rid) AS distinct_ids,
                  min(rid) AS min_rid, max(rid) AS max_rid
           FROM delta_source WHERE rid <= 100000"""
    ).show()

    print("\nOne engine computed it (StarRocks, federation, zero DBU), the other")
    print("analyzed it (DuckDB), and the wire between them was Arrow.")

    print(
        """

=== Run it yourself, live ==============================================

To show this interactively, from the repo root:

  python scripts/duckdb_live.py           # devbox shell (venv already active)
  .venv/bin/python scripts/duckdb_live.py # plain host shell

That drops you into a live session with the federated source already
pulled over Flight SQL and registered zero-copy (as delta_source), plus
the Unity Catalog weekly rollup pre-registered (as uc_funnel -- so beat 4b
runs immediately; re-typing 4a is only to show the mechanism). At the
>>> prompt you type SQL DIRECTLY (it runs on DuckDB), or Python for the
helpers:

  star("<sql>")   push the query DOWN to StarRocks over Flight SQL; the
                  Arrow result registers in DuckDB (zero-copy) as duck_1,
                  duck_2, ... and prints
  duck("<sql>")   run DuckDB-LOCAL SQL over whatever is registered
  tables()        what is registered right now

A four-beat story to type in front of an audience:

  # profile the federation's make-up -- DuckDB-local over the Arrow buffers
  >>> SELECT description, COUNT(*) n FROM delta_source GROUP BY 1 ORDER BY n DESC LIMIT 5

  # query a kafka-fed StarRocks table live -- pushed DOWN to StarRocks over Flight SQL
  >>> star("SELECT country, SUM(viewers) AS v FROM sr_local_db.funnel_summary_direct GROUP BY 1")

  # re-cut that StarRocks result locally in DuckDB -- both directions of the pairing
  >>> SELECT country, v FROM duck_1 ORDER BY v DESC LIMIT 3

A fourth beat -- a query over a Databricks Unity Catalog table (the shape to
use when the table is LARGE; here it is demo-sized at ~2k rows, but the
mechanics are identical at hundreds of millions):

  # Beat 4a (uc_funnel is already registered at startup; re-type only to
  # show the mechanism): StarRocks federates the UC Iceberg table IN PLACE on ADLS (zero
  # DBU -- no Databricks compute involved) and does the heavy part: it prunes
  # to the window_date partitions it needs, reads only the four columns
  # named, and computes the weekly rollup with its MPP engine. What crosses
  # Flight is ONLY the aggregate -- 6 rows, a fraction of a KiB. This is the
  # shape every large-table query should have: filter on the partition
  # column, name the columns, aggregate server-side. Never SELECT * a big
  # table -- that streams every column of every row into this laptop's RAM.
  >>> star(\"\"\"
        SELECT DATE_TRUNC('week', window_date) AS week,
               SUM(viewers) AS viewers, SUM(purchasers) AS purchasers,
               ROUND(SUM(purchasers) / NULLIF(SUM(viewers), 0) * 100, 2) AS conv_pct
        FROM databricks_uc.sr_poc_external.funnel_summary_historical
        GROUP BY 1
      \"\"\", name="uc_funnel")

  # Beat 4b: DuckDB re-cuts the result locally -- Lakehouse-level stats over
  # what already arrived, without touching the lake (or the wire) again.
  >>> SELECT ROUND(AVG(conv_pct), 2) AS avg_conv, MIN(conv_pct) AS worst, MAX(conv_pct) AS best FROM uc_funnel

Ctrl-D (or exit()) to leave. From any container on the compose network the
same file works (python scripts/duckdb_live.py with the scripts/ dir
mounted); from the host it uses the starrocks:8010 / localhost:8010
fallback -- the /etc/hosts entry from the button's checks makes the first
resolve.

========================================================================"""
    )


if __name__ == "__main__":
    main()
