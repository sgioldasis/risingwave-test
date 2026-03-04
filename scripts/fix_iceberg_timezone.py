#!/usr/bin/env python3
"""Fix Iceberg tables to use TIMESTAMPTZ instead of TIMESTAMP."""

import psycopg2

def main():
    conn = psycopg2.connect(
        host='localhost',
        port=4566,
        dbname='dev',
        user='root',
        password=''
    )
    cursor = conn.cursor()

    tables = ['iceberg_cart_events', 'iceberg_page_views', 'iceberg_purchases']

    for table in tables:
        print(f"Dropping {table}...")
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"  ✓ Dropped")

    conn.commit()
    cursor.close()
    conn.close()
    print("\nDone! Now run 'dbt run' to recreate the tables with TIMESTAMPTZ.")

if __name__ == "__main__":
    main()
