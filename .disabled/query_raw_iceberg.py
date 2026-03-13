#!/usr/bin/env python3
"""
Query Iceberg Funnel Table via DuckDB

Shows the latest 5 minutes of funnel data in descending order.
"""

import duckdb
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

        # Get last 5 minutes of data in descending order
        results = conn.execute("""
            SELECT
                window_start,
                window_end,
                viewers,
                carters,
                purchasers,
                ROUND(view_to_cart_rate * 100, 1) as v2c_pct,
                ROUND(cart_to_buy_rate * 100, 1) as c2b_pct
            FROM iceberg_funnel
            ORDER BY window_start DESC
            LIMIT 5
        """).fetchall()

        if not results:
            logger.info("No data available in iceberg_funnel table")
            return

        # Print header
        logger.info("=" * 95)
        logger.info(f"{'Window Start':<20} {'Window End':<20} {'Viewers':>8} {'Carters':>8} {'Buyers':>8} {'V→C%':>6} {'C→B%':>6}")
        logger.info("-" * 95)

        # Print rows
        for row in results:
            window_start = str(row[0])[:19] if row[0] else "N/A"
            window_end = str(row[1])[:19] if row[1] else "N/A"
            viewers = row[2] or 0
            carters = row[3] or 0
            purchasers = row[4] or 0
            v2c = row[5] or 0
            c2b = row[6] or 0

            logger.info(f"{window_start:<20} {window_end:<20} {viewers:>8} {carters:>8} {purchasers:>8} {v2c:>6.1f} {c2b:>6.1f}")

        logger.info("=" * 95)

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
