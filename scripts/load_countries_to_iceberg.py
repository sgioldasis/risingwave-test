#!/usr/bin/env python3
"""
Load countries CSV directly into Iceberg using DuckDB.
This bypasses RisingWave entirely.
"""

import duckdb


def main():
    conn = duckdb.connect(":memory:")
    
    # Load extensions
    print("Loading extensions...")
    conn.execute("INSTALL avro; LOAD avro;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("LOAD aws; LOAD httpfs;")
    
    # Configure S3/MinIO
    print("Configuring S3...")
    conn.execute("SET s3_endpoint = 'http://localhost:9301';")
    conn.execute("SET s3_access_key_id = 'hummockadmin';")
    conn.execute("SET s3_secret_access_key = 'hummockadmin';")
    conn.execute("SET s3_url_style = 'path';")
    conn.execute("SET s3_use_ssl = false;")
    
    # Attach Lakekeeper catalog
    print("Attaching to Lakekeeper...")
    conn.execute("""
        ATTACH 'risingwave-warehouse' AS lk (
            TYPE ICEBERG,
            ENDPOINT 'http://127.0.0.1:8181/catalog',
            AUTHORIZATION_TYPE 'none'
        );
    """)
    
    # Create table and load data
    print("Loading countries from CSV...")
    conn.execute("""
        CREATE OR REPLACE TABLE lk.public.iceberg_countries AS 
        SELECT * FROM read_csv_auto('dbt/seeds/countries.csv');
    """)
    
    # Verify
    result = conn.execute("SELECT * FROM lk.public.iceberg_countries;").fetchall()
    print(f"\nLoaded {len(result)} countries:")
    for row in result:
        print(f"  {row[0]} - {row[1]}")
    
    print("\n✓ Countries loaded to Iceberg successfully!")


if __name__ == "__main__":
    main()
