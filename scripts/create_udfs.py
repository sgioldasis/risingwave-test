#!/usr/bin/env python3
"""Create Python UDFs in RisingWave.

This script is called from docker-entrypoint-dagster.sh to ensure UDFs exist
before Dagster starts. It reads SQL from sql/create_udfs.sql and executes it.
"""
import os
import sys
import time
import psycopg2
from pathlib import Path


def wait_for_risingwave(max_retries=30, delay=2):
    """Wait for RisingWave to be ready."""
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host="frontend-node-0",
                port=4566,
                database="dev",
                user="root",
            )
            conn.close()
            print("✅ RisingWave is ready")
            return True
        except Exception as e:
            print(f"⏳ Waiting for RisingWave... ({i+1}/{max_retries})")
            time.sleep(delay)
    return False


def create_udfs():
    """Create Python UDFs from sql/create_udfs.sql."""
    # Find the SQL file
    sql_file = Path("/workspace/sql/create_udfs.sql")
    if not sql_file.exists():
        sql_file = Path("sql/create_udfs.sql")
    if not sql_file.exists():
        print(f"❌ Could not find sql/create_udfs.sql")
        return False
    
    sql_content = sql_file.read_text()
    print(f"📄 Read {len(sql_content)} characters from {sql_file}")
    
    # Connect to RisingWave
    conn = psycopg2.connect(
        host="frontend-node-0",
        port=4566,
        database="dev",
        user="root",
    )
    
    try:
        with conn.cursor() as cur:
            # Execute the entire SQL file
            cur.execute(sql_content)
            conn.commit()
            print("✅ Python UDFs created successfully")
            return True
    except Exception as e:
        print(f"⚠️ Error creating UDFs (may already exist): {e}")
        return True  # Don't fail if UDFs already exist
    finally:
        conn.close()


def main():
    """Main entry point."""
    print("=== Creating Python UDFs for RisingWave ===")
    
    if not wait_for_risingwave():
        print("❌ RisingWave is not ready, skipping UDF creation")
        return 1
    
    if create_udfs():
        print("✅ UDF setup complete")
        return 0
    else:
        print("❌ UDF setup failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
