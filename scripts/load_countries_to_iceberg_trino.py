#!/usr/bin/env python3
"""
Load countries data directly into Iceberg using Trino.
This provides full SQL support including UPDATE/DELETE.
"""

import csv
import os

import trino
from trino.dbapi import connect


def get_trino_connection():
    """Create a connection to Trino."""
    return connect(
        host="localhost",
        port=9080,
        user="trino",
        catalog="iceberg",
        schema="analytics",
    )


def load_csv_data(filepath: str) -> list[tuple]:
    """Load country data from CSV file."""
    countries = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            countries.append((row['country_code'], row['country_name']))
    return countries


def create_table(conn):
    """Create the iceberg_countries table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg_countries (
            country VARCHAR,
            country_name VARCHAR
        )
    """)
    conn.commit()
    print("✓ Table iceberg_countries created/verified")


def load_data(conn, countries: list[tuple]):
    """Load countries data into Iceberg table."""
    cur = conn.cursor()
    
    # Clear existing data
    cur.execute("DELETE FROM iceberg_countries")
    
    # Insert data
    for country_code, country_name in countries:
        cur.execute(
            "INSERT INTO iceberg_countries (country, country_name) VALUES (?, ?)",
            (country_code, country_name)
        )
    
    conn.commit()
    print(f"✓ Loaded {len(countries)} countries into Iceberg")


def verify_data(conn):
    """Verify the data was loaded correctly."""
    cur = conn.cursor()
    cur.execute("SELECT country, country_name FROM iceberg_countries ORDER BY country")
    rows = cur.fetchall()
    
    print(f"\nLoaded {len(rows)} countries:")
    for row in rows:
        print(f"  {row[0]} - {row[1]}")
    
    return len(rows)


def main():
    print("Loading countries to Iceberg via Trino...")
    print("-" * 50)
    
    # Load data from CSV
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'countries.csv')
    print(f"Loading countries from {csv_path}...")
    countries = load_csv_data(csv_path)
    print(f"  Loaded {len(countries)} countries from CSV")
    
    # Connect to Trino
    print("\nConnecting to Trino...")
    conn = get_trino_connection()
    print("  Connected to Trino at localhost:9080")
    
    # Create table
    print("\nCreating table...")
    create_table(conn)
    
    # Load data
    print("\nLoading data...")
    load_data(conn, countries)
    
    # Verify
    count = verify_data(conn)
    
    conn.close()
    
    print("\n" + "=" * 50)
    print(f"✓ Successfully loaded {count} countries to Iceberg!")
    print("\nYou can now query the data:")
    print("  docker compose exec trino trino --catalog iceberg --schema analytics --execute 'SELECT * FROM iceberg_countries'")
    print("\nOr update data (works with Trino, not DuckDB):")
    print("  docker compose exec trino trino --catalog iceberg --schema analytics --execute \"UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'\"")


if __name__ == "__main__":
    main()
