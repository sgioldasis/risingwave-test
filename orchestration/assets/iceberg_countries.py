"""Dagster asset for creating and populating Iceberg countries table using Trino."""
import csv
import os

from dagster import asset, AssetExecutionContext
from trino.dbapi import connect


def load_csv_data(filepath: str) -> list[tuple]:
    """Load country data from CSV file."""
    countries = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            countries.append((row['country_code'], row['country_name']))
    return countries


@asset(
    name="iceberg_countries",
    group_name="iceberg",
    description="Create and populate Iceberg countries table using Trino",
)
def iceberg_countries(context: AssetExecutionContext):
    """Creates the iceberg_countries table and populates it with country data via Trino."""
    context.log.info("Creating Iceberg countries table via Trino...")

    try:
        # Load data from CSV
        csv_path = os.path.join('/workspace', 'data', 'countries.csv')
        context.log.info(f"Loading countries from {csv_path}...")
        countries = load_csv_data(csv_path)
        context.log.info(f"Loaded {len(countries)} countries from CSV")

        # Connect to Trino (running in Docker, use 'trino' as host from within Dagster container)
        conn = connect(
            host="trino",
            port=8080,
            user="dagster",
            catalog="datalake",
            schema="public",
        )
        cur = conn.cursor()

        # Create schema if not exists
        context.log.info("Creating schema if not exists...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.commit()
        context.log.info("✓ Schema created/verified")

        # Create table
        context.log.info("Creating table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg_countries (
                country VARCHAR,
                country_name VARCHAR
            )
        """)
        conn.commit()
        context.log.info("✓ Table created/verified")

        # Clear existing data
        context.log.info("Clearing existing data...")
        cur.execute("DELETE FROM iceberg_countries")
        conn.commit()

        # Insert data
        context.log.info(f"Inserting {len(countries)} countries...")
        for country_code, country_name in countries:
            cur.execute(
                "INSERT INTO iceberg_countries (country, country_name) VALUES (?, ?)",
                (country_code, country_name)
            )
        conn.commit()

        # Verify
        cur.execute("SELECT COUNT(*) FROM iceberg_countries")
        count = cur.fetchone()[0]
        context.log.info(f"✓ Table now contains {count} countries")

        # Show sample data
        cur.execute("SELECT country, country_name FROM iceberg_countries ORDER BY country LIMIT 5")
        sample = cur.fetchall()
        context.log.info(f"Sample data: {sample}")

        conn.close()

        context.log.info(f"✓ Successfully created iceberg_countries table with {count} countries via Trino")
        return {"country_count": count}

    except Exception as e:
        context.log.error(f"Failed to create table: {e}")
        raise
