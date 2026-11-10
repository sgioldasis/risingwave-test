"""Arrow Flight SQL vs MySQL-protocol benchmark for StarRocks (SR_POC_ARROW_FLIGHT_SQL_DEMO.md).

Runs INSIDE a container on the compose network (the dagster-webserver works:
it has pymysql, pyarrow and, after `docker exec -u root dagster-webserver
python -m pip install adbc-driver-flightsql`, the ADBC Flight SQL driver).
The FE advertises its Flight endpoint as grpc+tcp://starrocks:8010, which only
resolves on the docker network — host runs would need an /etc/hosts entry.

Two benchmarks, both sides consuming identically (streamed, row-by-row) so
the comparison is transfer + conversion, not client-side materialization:

1. Full pull of the 420k-row federated Delta source
   (delta_federation.delta_fed.reverse_etl_delta_source) — the zero-copy story.
2. Single-key point lookups against an internal Primary Key table
   (flight_bench.bench_txn, created and seeded idempotently) — the dashboard
   workload StarRocks actually serves.

Usage:
    docker exec -i dagster-webserver python - < scripts/starrocks_flight_benchmark.py
"""

import base64
import statistics
import time

import pymysql
import pymysql.cursors
import adbc_driver_flightsql.dbapi as flight_sql

FEDERATED_TABLE = "delta_federation.delta_fed.reverse_etl_delta_source"
BENCH_DB = "flight_bench"
BENCH_TABLE = "flight_bench.bench_txn"
BENCH_ROWS = 10_000
LOOKUPS = 200
SCAN_REPETITIONS = 3

MYSQL_KW = dict(host="starrocks", port=9030, user="root")
FLIGHT_KW = dict(
    uri="grpc+tcp://starrocks:8010",
    db_kwargs={"adbc.flight.sql.authorization_header": f"Basic {base64.b64encode(b'root:').decode()}"},
)


def seed_bench_table():
    """Create + seed the point-lookup table idempotently (TRUNCATE then re-seed)."""
    with pymysql.connect(**MYSQL_KW) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {BENCH_DB}")
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {BENCH_TABLE} (
                        txn_id VARCHAR(64) NOT NULL,
                        amount DECIMAL(18,2),
                        status VARCHAR(16)
                    ) PRIMARY KEY (txn_id) DISTRIBUTED BY HASH(txn_id)"""
            )
            cur.execute(f"TRUNCATE TABLE {BENCH_TABLE}")
            for chunk_start in range(0, BENCH_ROWS, 500):
                rows = [
                    (f"txn-{i:08d}", round(10.0 + (i % 990) * 0.01, 2), "settled" if i % 3 else "pending")
                    for i in range(chunk_start, min(chunk_start + 500, BENCH_ROWS))
                ]
                cur.executemany(
                    f"INSERT INTO {BENCH_TABLE} (txn_id, amount, status) VALUES (%s, %s, %s)", rows
                )
        conn.commit()


def scan_mysql():
    with pymysql.connect(**MYSQL_KW, cursorclass=pymysql.cursors.SSCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {FEDERATED_TABLE}")
            n = 0
            for row in cur:  # streams row-by-row, same consumption as the Arrow side
                n += 1
            return n


def scan_flight():
    with flight_sql.connect(**FLIGHT_KW) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {FEDERATED_TABLE}")
            n = 0
            reader = cur._results.reader  # AdbcRecordBatchReader — stream batches like the MySQL side
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                n += batch.num_rows
            return n


def bench_scan():
    results = {"mysql": [], "flight": []}
    rows = None
    for _ in range(SCAN_REPETITIONS):
        t0 = time.perf_counter()
        rows = scan_mysql()
        results["mysql"].append(time.perf_counter() - t0)
    for _ in range(SCAN_REPETITIONS):
        t0 = time.perf_counter()
        n = scan_flight()
        assert n == rows, f"row mismatch: mysql={rows} flight={n}"
        results["flight"].append(time.perf_counter() - t0)
    return rows, results


def bench_lookups():
    keys = [f"txn-{i:08d}" for i in range(LOOKUPS)]

    with pymysql.connect(**MYSQL_KW) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {BENCH_TABLE}")
            assert cur.fetchone()[0] == BENCH_ROWS
            times = []
            for k in keys:
                t0 = time.perf_counter()
                cur.execute(f"SELECT amount, status FROM {BENCH_TABLE} WHERE txn_id = '{k}'")
                cur.fetchone()
                times.append(time.perf_counter() - t0)
    mysql_times = times

    with flight_sql.connect(**FLIGHT_KW) as conn:
        with conn.cursor() as cur:
            times = []
            for k in keys:
                t0 = time.perf_counter()
                cur.execute(f"SELECT amount, status FROM {BENCH_TABLE} WHERE txn_id = '{k}'")
                cur.fetchone()
                times.append(time.perf_counter() - t0)
    flight_times = times
    return mysql_times, flight_times


def report():
    print("=" * 72)
    print("StarRocks Arrow Flight SQL vs MySQL protocol benchmark")
    print("=" * 72)

    rows, scan = bench_scan()
    print(f"\n1) Full pull of {FEDERATED_TABLE} ({rows:,} rows, streamed), {SCAN_REPETITIONS} runs:")
    for proto, times in scan.items():
        best = min(times)
        med = statistics.median(times)
        print(f"   {proto:>6}: best {best:6.2f}s  median {med:6.2f}s  ({rows / best:,.0f} rows/s at best)")

    mysql_ms, flight_ms = bench_lookups()
    print(f"\n2) Single-key point lookups on {BENCH_TABLE} ({LOOKUPS} lookups, {BENCH_ROWS:,} rows):")
    print(f"   mysql : avg {statistics.mean(mysql_ms) * 1000:6.2f} ms  p50 {statistics.median(mysql_ms) * 1000:6.2f} ms")
    print(f"   flight: avg {statistics.mean(flight_ms) * 1000:6.2f} ms  p50 {statistics.median(flight_ms) * 1000:6.2f} ms")
    print("\n(Flight shines on large pulls — columnar batches vs per-row conversion;")
    print(" point lookups are round-trip-bound and should be near-identical.)")


def report_json() -> dict:
    """The same two benchmarks, machine-readable — the demo UI's 'Flight vs
    MySQL benchmark' button calls this via the demo server."""
    rows, scan = bench_scan()
    mysql_ms, flight_ms = bench_lookups()
    return {
        "scan": {
            "table": FEDERATED_TABLE,
            "rows": rows,
            "repetitions": SCAN_REPETITIONS,
            "mysql_best_s": round(min(scan["mysql"]), 3),
            "flight_best_s": round(min(scan["flight"]), 3),
        },
        "lookups": {
            "table": BENCH_TABLE,
            "table_rows": BENCH_ROWS,
            "n": LOOKUPS,
            "mysql_avg_ms": round(statistics.mean(mysql_ms) * 1000, 2),
            "flight_avg_ms": round(statistics.mean(flight_ms) * 1000, 2),
        },
    }


if __name__ == "__main__":
    import argparse
    import json as _json

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON (demo UI)")
    args = ap.parse_args()
    seed_bench_table()
    if args.json:
        print(_json.dumps(report_json()))
    else:
        report()
