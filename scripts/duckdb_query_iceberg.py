#!/usr/bin/env python3
"""
Query Iceberg Funnel Table via DuckDB

Shows the latest 5 minutes of funnel data in descending order.
"""

import duckdb
import sys
from tabulate import tabulate

# Lakekeeper REST catalog configuration
LAKEKEEPER_BASE = "http://127.0.0.1:8181"
WAREHOUSE = "risingwave-warehouse"
S3_ENDPOINT = "localhost:9301"
S3_ACCESS_KEY = "hummockadmin"
S3_SECRET_KEY = "hummockadmin"


def setup_duckdb():
    """Set up DuckDB connection with required extensions."""
    conn = duckdb.connect(":memory:")

    # Install and load extensions
    for ext in ["aws", "httpfs", "avro", "iceberg"]:
        conn.execute(f"INSTALL {ext};")
        conn.execute(f"LOAD {ext};")

    return conn


def configure_s3(conn):
    """Configure S3 connection to MinIO."""
    conn.execute(f"SET s3_endpoint = 'http://{S3_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id = '{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key = '{S3_SECRET_KEY}';")
    conn.execute("SET s3_url_style = 'path';")
    conn.execute("SET s3_use_ssl = false;")


def attach_catalog(conn):
    """Attach to the Lakekeeper Iceberg catalog."""
    conn.execute(f"""
        ATTACH '{WAREHOUSE}' AS lakekeeper_catalog (
            TYPE ICEBERG,
            ENDPOINT '{LAKEKEEPER_BASE}/catalog',
            AUTHORIZATION_TYPE 'none'
        );
    """)
    conn.execute("USE lakekeeper_catalog.public;")


def main():
    try:
        conn = setup_duckdb()
        configure_s3(conn)
        attach_catalog(conn)

        # Get last 5 minutes of data in descending order.
        # Some readers can expose multiple physical versions for a minute key,
        # so keep one logical row per window_start using row_number.
        results = conn.execute("""
            WITH ranked AS (
                SELECT
                    window_start,
                    window_end,
                    viewers,
                    carters,
                    purchasers,
                    view_to_cart_rate,
                    cart_to_buy_rate,
                    ROW_NUMBER() OVER (
                        PARTITION BY window_start
                        ORDER BY
                            COALESCE(viewers, 0) DESC,
                            COALESCE(carters, 0) DESC,
                            COALESCE(purchasers, 0) DESC,
                            window_end DESC
                    ) AS rn
                FROM rw_managed_funnel
            )
            SELECT
                window_start,
                window_end,
                viewers,
                carters,
                purchasers,
                ROUND(view_to_cart_rate * 100, 1) as v2c_pct,
                ROUND(cart_to_buy_rate * 100, 1) as c2b_pct
            FROM ranked
            WHERE rn = 1
            ORDER BY window_start DESC
            LIMIT 5
        """).fetchall()

        if not results:
            print("No data available in rw_managed_funnel table")
            return

        # Build consistently aligned output via tabulate.
        rows = []
        for row in results:
            window_start = str(row[0])[:19] if row[0] else "N/A"
            window_end = str(row[1])[:19] if row[1] else "N/A"
            viewers = int(row[2] or 0)
            carters = int(row[3] or 0)
            purchasers = int(row[4] or 0)
            v2c = float(row[5] or 0)
            c2b = float(row[6] or 0)
            rows.append([window_start, window_end, viewers, carters, purchasers, v2c, c2b])

        headers = ["Window Start", "Window End", "Viewers", "Carters", "Buyers", "V->C%", "C->B%"]
        print(tabulate(
            rows,
            headers=headers,
            tablefmt="psql",
            stralign="left",
            numalign="right",
            floatfmt=("", "", "d", "d", "d", ".1f", ".1f")
        ))

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
