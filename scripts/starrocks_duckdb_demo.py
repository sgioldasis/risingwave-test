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
    print("  wire: Arrow Flight SQL (columnar batches, zero conversion)")
    print(f"  memory: {arrow.nbytes / 1024 / 1024:.1f} MiB as Arrow, pulled in {time.perf_counter() - t0:.2f}s")

    ddb = duckdb.connect()
    ddb.register("delta_source", arrow)  # zero-copy: DuckDB scans the Arrow buffers in place
    print("\nRegistered in DuckDB zero-copy. Queries StarRocks never precomputed:")

    print("\n1) The make-up of the source (every change the demo ever produced):")
    ddb.sql(
        "SELECT description, COUNT(*) AS n FROM delta_source GROUP BY 1 ORDER BY n DESC LIMIT 5"
    ).show()

    print("2) An analytic cut — summary stats over a filter:")
    ddb.sql(
        """SELECT count(*) AS rows,
                  count(DISTINCT rid) AS distinct_ids,
                  min(rid) AS min_rid, max(rid) AS max_rid
           FROM delta_source WHERE rid <= 100000"""
    ).show()

    print("\nOne engine computed it (StarRocks, federation, zero DBU), the other")
    print("analyzed it (DuckDB), and the wire between them was Arrow.")


if __name__ == "__main__":
    main()
