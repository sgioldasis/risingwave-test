"""DataFusion batch analytics demo asset.

Runs OLAP-style queries on the casino Iceberg tables directly from RisingWave,
showcasing the unified stream + batch capability introduced in RisingWave 2.8.
"""
import os
import textwrap

import psycopg2
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

RW_HOST     = os.environ.get("RISINGWAVE_HOST",     "localhost")
RW_PORT     = int(os.environ.get("RISINGWAVE_PORT", "4566"))
RW_USER     = os.environ.get("RISINGWAVE_USER",     "root")
RW_PASSWORD = os.environ.get("RISINGWAVE_PASSWORD", "")
RW_DB       = os.environ.get("RISINGWAVE_DB",       "dev")

QUERIES = [
    (
        "Top 10 customers by real bet amount",
        """
        SELECT customer_id, currency_id,
               SUM(rolling_1d_real_bet_amount)::numeric AS total_bet
        FROM src_iceberg_casino_real_bet
        GROUP BY customer_id, currency_id
        ORDER BY total_bet DESC
        LIMIT 10
        """,
    ),
    (
        "Turnover ratio segmentation",
        """
        SELECT
            CASE
                WHEN casino_ratio > 0.7    THEN 'casino-heavy'
                WHEN sportsbook_ratio > 0.7 THEN 'sports-heavy'
                ELSE 'balanced'
            END AS segment,
            COUNT(*) AS customers,
            ROUND(AVG(total_turnover)::numeric, 2) AS avg_turnover
        FROM src_iceberg_turnover_percentage
        GROUP BY segment
        ORDER BY customers DESC
        """,
    ),
    (
        "Cross-table: top 20 customers — bets joined with turnover ratio",
        """
        SELECT b.customer_id,
               SUM(b.rolling_1d_real_bet_amount)::numeric AS real_bet,
               ROUND(t.casino_ratio::numeric, 3) AS casino_ratio
        FROM src_iceberg_casino_real_bet b
        JOIN src_iceberg_turnover_percentage t ON b.customer_id = t.customer_id
        GROUP BY b.customer_id, t.casino_ratio
        ORDER BY real_bet DESC
        LIMIT 20
        """,
    ),
    (
        "Data volume: row count and latest event",
        """
        SELECT COUNT(*) AS rows,
               MAX(event_ts) AS latest_event
        FROM src_iceberg_casino_real_bet
        """,
    ),
]


ICEBERG_SOURCES = [
    ("src_iceberg_casino_real_bet",    "rw_managed_casino_real_bet"),
    ("src_iceberg_turnover_percentage", "rw_managed_turnover_percentage"),
]

CREATE_SOURCE_TEMPLATE = """
CREATE SOURCE IF NOT EXISTS {name}
WITH (
    connector             = 'iceberg',
    catalog.type          = 'rest',
    catalog.uri           = 'http://lakekeeper:8181/catalog/',
    warehouse.path        = 'risingwave-warehouse',
    database.name         = 'public',
    table.name            = '{table}',
    s3.endpoint           = 'http://minio-0:9301',
    s3.access.key         = 'hummockadmin',
    s3.secret.key         = 'hummockadmin',
    s3.region             = 'us-east-1',
    s3.path.style.access  = 'true'
)
"""


def _rw_conn():
    return psycopg2.connect(
        host=RW_HOST, port=RW_PORT, user=RW_USER, password=RW_PASSWORD,
        dbname=RW_DB, connect_timeout=10,
    )


def _run_query(cur, sql: str):
    cur.execute(textwrap.dedent(sql).strip())
    return cur.fetchall(), [d[0] for d in cur.description]


def _to_md_table(rows, cols) -> str:
    header = "| " + " | ".join(cols) + " |"
    sep    = "| " + " | ".join(["---"] * len(cols)) + " |"
    body   = "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in rows)
    return f"{header}\n{sep}\n{body}" if rows else "_no rows_"


@asset(
    group_name="casino_datafusion",
    deps=[
        AssetDep(AssetKey(["public", "sink_casino_real_bet"])),
        AssetDep(AssetKey(["public", "sink_turnover_percentage"])),
        AssetDep(AssetKey(["public", "src_iceberg_casino_real_bet"])),
        AssetDep(AssetKey(["public", "src_iceberg_turnover_percentage"])),
    ],
    description=(
        "Run OLAP-style batch queries on the casino Iceberg tables using "
        "RisingWave's DataFusion engine (vectorized, Arrow-native). "
        "Demonstrates unified stream + batch: same data, same connection, no Trino needed."
    ),
)
def casino_datafusion_demo(context: AssetExecutionContext):
    """Create Iceberg read-sources (if needed) then run analytical queries via DataFusion."""
    results_summary = {}

    conn = _rw_conn()
    try:
        cur = conn.cursor()
        for source_name, table_name in ICEBERG_SOURCES:
            try:
                cur.execute(f"DROP SOURCE IF EXISTS {source_name}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                context.log.warning(f"Drop '{source_name}': {e}")
            sql = CREATE_SOURCE_TEMPLATE.format(name=source_name, table=table_name).strip()
            try:
                cur.execute(sql)
                conn.commit()
                context.log.info(f"✓ Source '{source_name}' ready")
            except Exception as e:
                conn.rollback()
                context.log.warning(f"Source '{source_name}' creation: {e}")
    finally:
        conn.close()

    md_results = {}
    conn = _rw_conn()
    try:
        cur = conn.cursor()
        for title, sql in QUERIES:
            try:
                rows, cols = _run_query(cur, sql)
                context.log.info(f"{title}: {len(rows)} row(s)")
                md_results[title] = MetadataValue.md(_to_md_table(rows, cols))
            except Exception as e:
                context.log.warning(f"Query failed ({title}): {e}")
                md_results[title] = MetadataValue.md(f"_Error: {e}_")
    finally:
        conn.close()

    context.add_output_metadata({"queries_run": MetadataValue.int(len(QUERIES)), **md_results})
