"""Dagster asset for syncing Iceberg countries data to RisingWave via Trino.

This provides a batch sync (full reload) approach that reads current snapshot
data from Iceberg via Trino and loads it into a RisingWave table.
"""

import os
from dagster import (
    AssetExecutionContext,
    asset,
    AssetKey,
)
from trino.dbapi import connect as trino_connect
import psycopg2


def _get_trino_connection():
    """Get connection to Trino."""
    return trino_connect(
        host="trino",
        port=8080,
        user="trino",
        catalog="datalake",
        schema="public",
    )


def _get_risingwave_connection():
    """Get connection to RisingWave."""
    return psycopg2.connect(
        host="frontend-node-0",
        port=4566,
        database="dev",
        user="root",
        password="",
    )


@asset(
    name="risingwave_countries_table",
    deps=[AssetKey("iceberg_countries")],
    description="Table in RisingWave synced from Iceberg via Trino (full refresh)",
    group_name="risingwave",
    compute_kind="python",
)
def risingwave_countries_table(context: AssetExecutionContext):
    """
    Syncs countries data from Iceberg to RisingWave using Trino.
    
    This asset:
    1. Reads current snapshot from Iceberg via Trino
    2. Creates/replaces table in RisingWave
    3. Supports full-refresh for clean data
    
    Unlike the native RisingWave Iceberg SOURCE which returns changelog data,
    this gives you the current snapshot only.
    """
    context.log.info("Starting sync from Iceberg to RisingWave via Trino...")
    
    # Step 1: Extract data from Iceberg via Trino
    context.log.info("Extracting data from Iceberg via Trino...")
    trino_conn = _get_trino_connection()
    trino_cur = trino_conn.cursor()
    
    try:
        trino_cur.execute("SELECT country, country_name FROM iceberg_countries")
        rows = trino_cur.fetchall()
        context.log.info(f"Extracted {len(rows)} rows from Iceberg")
    finally:
        trino_cur.close()
        trino_conn.close()
    
    # Step 2: Load data into RisingWave
    context.log.info("Loading data into RisingWave...")
    rw_conn = _get_risingwave_connection()
    rw_cur = rw_conn.cursor()
    
    try:
        # Drop and recreate table for full refresh
        rw_cur.execute("DROP TABLE IF EXISTS risingwave_countries_table")
        rw_cur.execute("""
            CREATE TABLE risingwave_countries_table (
                country VARCHAR,
                country_name VARCHAR
            )
        """)
        
        # Insert data
        if rows:
            # Use execute_values for bulk insert
            from psycopg2.extras import execute_values
            execute_values(
                rw_cur,
                "INSERT INTO risingwave_countries_table (country, country_name) VALUES %s",
                rows
            )
        
        rw_conn.commit()
        context.log.info(f"Successfully loaded {len(rows)} rows into RisingWave")
        
        # Verify
        rw_cur.execute("SELECT COUNT(*) FROM risingwave_countries_table")
        count = rw_cur.fetchone()[0]
        context.log.info(f"Table now contains {count} rows")
        
        # Sample data
        rw_cur.execute("SELECT * FROM risingwave_countries_table LIMIT 5")
        sample = rw_cur.fetchall()
        context.log.info(f"Sample data: {sample}")
        
    finally:
        rw_cur.close()
        rw_conn.close()
    
    return {"rows_synced": len(rows)}
