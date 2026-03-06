"""Dagster asset for creating and populating Iceberg countries table using Trino."""
from dagster import asset, AssetExecutionContext
from trino.dbapi import connect


# Country data
COUNTRIES_DATA = [
    ("US", "United States"),
    ("CA", "Canada"),
    ("GB", "United Kingdom"),
    ("DE", "Germany"),
    ("FR", "France"),
    ("IT", "Italy"),
    ("ES", "Spain"),
    ("NL", "Netherlands"),
    ("AU", "Australia"),
    ("JP", "Japan"),
    ("GR", "Greece"),
]


@asset(
    name="iceberg_countries",
    group_name="datalake",
    description="Create and populate Iceberg countries table using Trino",
)
def iceberg_countries(context: AssetExecutionContext):
    """Creates the iceberg_countries table and populates it with country data via Trino."""
    context.log.info("Creating Iceberg countries table via Trino...")

    try:
        # Connect to Trino (running in Docker, use 'trino' as host from within Dagster container)
        conn = connect(
            host="trino",
            port=8080,
            user="dagster",
            catalog="iceberg",
            schema="analytics",
        )
        cur = conn.cursor()

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
        context.log.info(f"Inserting {len(COUNTRIES_DATA)} countries...")
        for country_code, country_name in COUNTRIES_DATA:
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
