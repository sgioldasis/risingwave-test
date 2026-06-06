"""DataFusion batch analytics demo — reading from Databricks Unity Catalog Iceberg tables.

Demonstrates RisingWave reading Databricks-managed Iceberg tables via the Unity Catalog
Iceberg REST API (IRC) and executing OLAP queries through the embedded DataFusion engine.
Same psql connection, same SQL dialect — no SQL Warehouse cost, no Spark needed.

Sources and views are created once by the databricks_iceberg_sources asset.
This asset only runs the analytical queries.
"""
import os
import textwrap

import psycopg2
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

RW_HOST = os.environ.get("RISINGWAVE_HOST", "localhost")
RW_PORT = 4566
RW_USER = "root"
RW_DB   = "dev"

QUERIES = [
    (
        "Data volume & freshness",
        """
        SELECT
            'casino_transactions'          AS table_name,
            COUNT(*)                       AS row_count,
            MIN(transaction_created_at)    AS earliest,
            MAX(transaction_created_at)    AS latest
        FROM v_databricks_casino_transactions
        UNION ALL
        SELECT
            'sportsbook_bets',
            COUNT(*),
            MIN(placed_at),
            MAX(placed_at)
        FROM v_databricks_sportsbook_bets
        """,
    ),
    (
        "Top 10 customers by real-bet amount (UC1 equivalent)",
        """
        SELECT
            customer_id,
            currency_id,
            SUM(amount_abs)::numeric AS total_real_bet
        FROM v_databricks_casino_transactions
        WHERE message_type_id = 1
          AND account_id = 1
          AND amount_abs IS NOT NULL
        GROUP BY customer_id, currency_id
        ORDER BY total_real_bet DESC
        LIMIT 10
        """,
    ),
    (
        "Turnover ratio per customer — casino vs sportsbook (UC2 equivalent)",
        """
        WITH casino AS (
            SELECT customer_id, SUM(amount_abs) AS casino_turnover
            FROM v_databricks_casino_transactions
            WHERE message_type_id = 2 AND account_id IN (1, 4) AND amount_abs IS NOT NULL
            GROUP BY customer_id
        ),
        sportsbook AS (
            SELECT customer_id, SUM(stake_euro) AS sportsbook_turnover
            FROM v_databricks_sportsbook_bets
            GROUP BY customer_id
        ),
        combined AS (
            SELECT
                COALESCE(c.customer_id, s.customer_id)   AS customer_id,
                COALESCE(c.casino_turnover, 0)            AS casino_turnover,
                COALESCE(s.sportsbook_turnover, 0)        AS sportsbook_turnover
            FROM casino c FULL OUTER JOIN sportsbook s ON c.customer_id = s.customer_id
        )
        SELECT
            customer_id,
            ROUND(casino_turnover::numeric, 2)                                       AS casino_turnover,
            ROUND(sportsbook_turnover::numeric, 2)                                   AS sportsbook_turnover,
            ROUND((casino_turnover + sportsbook_turnover)::numeric, 2)               AS total_turnover,
            ROUND(CASE
                WHEN casino_turnover + sportsbook_turnover = 0 THEN 0
                ELSE casino_turnover / (casino_turnover + sportsbook_turnover)
            END::numeric, 4)                                                         AS casino_ratio
        FROM combined
        ORDER BY total_turnover DESC
        LIMIT 20
        """,
    ),
    (
        "Sportsbook bet segmentation by type",
        """
        SELECT
            bet_type_id,
            COUNT(*)                           AS bet_count,
            ROUND(SUM(stake_euro)::numeric, 2) AS total_stake_euro,
            ROUND(AVG(stake_euro)::numeric, 2) AS avg_stake_euro
        FROM v_databricks_sportsbook_bets
        GROUP BY bet_type_id
        ORDER BY total_stake_euro DESC
        """,
    ),
    (
        "Properties bag exploration — casino transaction extended fields",
        """
        SELECT
            customer_id,
            ROUND(amount_abs::numeric, 2)                       AS amount,
            transaction_created_at,
            (properties::jsonb)->>'game_id'                     AS game_id,
            (properties::jsonb)->>'game_type'                   AS game_type,
            (properties::jsonb)->>'is_live'                     AS is_live,
            (properties::jsonb)->>'transaction_type_id'         AS transaction_type_id,
            (properties::jsonb)->>'casino_provider_id'          AS casino_provider_id
        FROM v_databricks_casino_transactions
        WHERE amount_abs IS NOT NULL AND amount_abs > 0
        ORDER BY transaction_created_at DESC
        LIMIT 10
        """,
    ),
]


def _rw_conn():
    return psycopg2.connect(
        host=RW_HOST, port=RW_PORT, user=RW_USER, dbname=RW_DB,
        connect_timeout=10,
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
    group_name="databricks_datafusion",
    deps=[AssetDep(AssetKey(["databricks_optimize"]))],
    description=(
        "Run OLAP-style queries through RisingWave's embedded DataFusion engine against "
        "Databricks Unity Catalog Iceberg tables. Sources and views are created once by "
        "databricks_iceberg_sources — this asset only runs the analytical queries."
    ),
)
def databricks_datafusion_demo(context: AssetExecutionContext):
    """Run DataFusion analytical queries against v_databricks_* views."""
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
