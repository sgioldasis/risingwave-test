#!/usr/bin/env python3
"""
Load countries data directly into Iceberg using Trino.
This provides full SQL support including UPDATE/DELETE.
"""

import csv
import logging
import os

import trino
from trino.dbapi import connect

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_trino_connection():
    """Create a connection to Trino."""
    return connect(
        host="localhost",
        port=9080,
        user="trino",
        catalog="datalake",
        schema="public",
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
    # Create schema if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.commit()
    logger.info("✓ Schema public created/verified")
    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg_countries (
            country VARCHAR,
            country_name VARCHAR
        )
    """)
    conn.commit()
    logger.info("✓ Table iceberg_countries created/verified")


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
    logger.info(f"✓ Loaded {len(countries)} countries into Iceberg")


def verify_data(conn):
    """Verify the data was loaded correctly."""
    cur = conn.cursor()
    cur.execute("SELECT country, country_name FROM iceberg_countries ORDER BY country")
    rows = cur.fetchall()
    
    logger.info(f"\nLoaded {len(rows)} countries:")
    for row in rows:
        logger.info(f"  {row[0]} - {row[1]}")
    
    return len(rows)


def main():
    logger.info("Loading countries to Iceberg via Trino...")
    logger.info("-" * 50)
    
    # Load data from CSV
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'countries.csv')
    logger.info(f"Loading countries from {csv_path}...")
    countries = load_csv_data(csv_path)
    logger.info(f"  Loaded {len(countries)} countries from CSV")
    
    # Connect to Trino
    logger.info("\nConnecting to Trino...")
    conn = get_trino_connection()
    logger.info("  Connected to Trino at localhost:9080")
    
    # Create table
    logger.info("\nCreating table...")
    create_table(conn)
    
    # Load data
    logger.info("\nLoading data...")
    load_data(conn, countries)
    
    # Verify
    count = verify_data(conn)
    
    conn.close()
    
    logger.info("\n" + "=" * 50)
    logger.info(f"✓ Successfully loaded {count} countries to Iceberg!")
    logger.info("\nYou can now query the data:")
    logger.info("  docker compose exec trino trino --catalog datalake --schema public --execute 'SELECT * FROM iceberg_countries'")
    logger.info("\nOr update data (works with Trino, not DuckDB):")
    logger.info("  docker compose exec trino trino --catalog datalake --schema public --execute \"UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'\"")


if __name__ == "__main__":
    main()
