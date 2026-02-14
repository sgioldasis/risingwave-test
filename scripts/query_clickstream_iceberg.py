#!/usr/bin/env python3
"""
Query the latest rows from clickstream Iceberg tables via DuckDB

This script shows the latest data from the new clickstream tables
that are sinked to Iceberg.

Usage:
    python query_clickstream_iceberg.py           # Show latest rows from all tables
    python query_clickstream_iceberg.py --live    # Live monitoring mode
    python query_clickstream_iceberg.py --table clickstream_events  # Query specific table
"""

import duckdb
import sys
import time
import argparse
from datetime import datetime


def setup_duckdb() -> duckdb.DuckDBPyConnection:
    """Set up DuckDB connection with required extensions."""
    conn = duckdb.connect(":memory:")
    
    print("🔧 Installing and loading DuckDB extensions...")
    
    extensions = ["aws", "httpfs", "avro", "iceberg"]
    for ext in extensions:
        conn.execute(f"INSTALL {ext};")
        conn.execute(f"LOAD {ext};")
        print(f"  ✓ {ext}")
    
    return conn


def configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure S3 connection to MinIO."""
    print("\n🔧 Configuring S3 connection to MinIO...")
    
    conn.execute("SET s3_endpoint = 'http://localhost:9301';")
    conn.execute("SET s3_access_key_id = 'hummockadmin';")
    conn.execute("SET s3_secret_access_key = 'hummockadmin';")
    conn.execute("SET s3_url_style = 'path';")
    conn.execute("SET s3_use_ssl = false;")
    
    print("  ✓ S3 configured")


def attach_catalog(conn: duckdb.DuckDBPyConnection) -> None:
    """Attach to the Lakekeeper Iceberg catalog."""
    print("\n🔧 Attaching to Lakekeeper catalog...")
    
    conn.execute("""
        ATTACH 'risingwave-warehouse' AS lakekeeper_catalog (
            TYPE ICEBERG,
            ENDPOINT 'http://127.0.0.1:8181/catalog',
            AUTHORIZATION_TYPE 'none'
        );
    """)
    
    print("  ✓ Catalog attached")


def print_section_header(title: str, width: int = 80) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def query_table_stats(conn: duckdb.DuckDBPyConnection, table_name: str) -> dict:
    """Get statistics for a table."""
    try:
        result = conn.execute(f"""
            SELECT COUNT(*) as total_rows
            FROM lakekeeper_catalog.public.{table_name}
        """).fetchone()
        return {"total_rows": result[0]}
    except Exception as e:
        return {"error": str(e)}


def query_latest_clickstream_events(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest clickstream events."""
    print_section_header(f"Latest {limit} Clickstream Events")
    
    try:
        stats = query_table_stats(conn, "iceberg_clickstream_events")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT
                event_id,
                user_id,
                session_id,
                event_type,
                page_url,
                event_time,
                revenue_usd
            FROM lakekeeper_catalog.public.iceberg_clickstream_events
            ORDER BY event_time DESC
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'Event Type':<15} {'User':<8} {'Session':<12} {'Page URL':<30} {'Revenue':<10} {'Event Time'}")
        print("-" * 100)
        
        for row in results:
            event_id, user_id, session_id, event_type, page_url, event_time, revenue = row
            page_short = (page_url[:27] + '...') if len(page_url) > 30 else page_url
            print(f"{event_type:<15} {user_id:<8} {session_id[:10]:<12} {page_short:<30} ${revenue:<9.2f} {event_time}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying clickstream events: {e}")


def query_latest_users(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest users."""
    print_section_header(f"Latest {limit} Users")
    
    try:
        stats = query_table_stats(conn, "iceberg_users")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT
                user_id,
                full_name,
                email,
                country,
                signup_time,
                marketing_opt_in
            FROM lakekeeper_catalog.public.iceberg_users
            ORDER BY signup_time DESC
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'User ID':<10} {'Name':<20} {'Email':<30} {'Country':<10} {'Opt-in':<8} {'Signup Time'}")
        print("-" * 100)
        
        for row in results:
            user_id, full_name, email, country, signup_time, opt_in = row
            name_short = (full_name[:17] + '...') if len(full_name) > 20 else full_name
            email_short = (email[:27] + '...') if len(email) > 30 else email
            opt_str = "Yes" if opt_in else "No"
            print(f"{user_id:<10} {name_short:<20} {email_short:<30} {country:<10} {opt_str:<8} {signup_time}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying users: {e}")


def query_latest_sessions(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest sessions."""
    print_section_header(f"Latest {limit} Sessions")
    
    try:
        stats = query_table_stats(conn, "iceberg_sessions")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT
                session_id,
                user_id,
                device_id,
                session_start,
                geo_city,
                geo_region
            FROM lakekeeper_catalog.public.iceberg_sessions
            ORDER BY session_start DESC
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'Session ID':<15} {'User':<8} {'Device':<15} {'City':<15} {'Region':<15} {'Start Time'}")
        print("-" * 100)
        
        for row in results:
            session_id, user_id, device_id, session_start, city, region = row
            print(f"{session_id[:14]:<15} {user_id:<8} {device_id[:14]:<15} {city:<15} {region:<15} {session_start}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying sessions: {e}")


def query_latest_devices(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest devices."""
    print_section_header(f"Latest {limit} Devices")
    
    try:
        stats = query_table_stats(conn, "iceberg_devices")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT 
                device_id,
                device_type,
                os,
                browser
            FROM lakekeeper_catalog.public.iceberg_devices
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'Device ID':<15} {'Type':<12} {'OS':<12} {'Browser'}")
        print("-" * 60)
        
        for row in results:
            device_id, device_type, os, browser = row
            print(f"{device_id[:14]:<15} {device_type:<12} {os:<12} {browser}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying devices: {e}")


def query_latest_campaigns(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest campaigns."""
    print_section_header(f"Latest {limit} Campaigns")
    
    try:
        stats = query_table_stats(conn, "iceberg_campaigns")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT 
                campaign_id,
                source,
                medium,
                campaign,
                content
            FROM lakekeeper_catalog.public.iceberg_campaigns
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'Campaign ID':<15} {'Source':<12} {'Medium':<12} {'Campaign':<20} {'Content'}")
        print("-" * 80)
        
        for row in results:
            campaign_id, source, medium, campaign, content = row
            campaign_short = (campaign[:17] + '...') if len(campaign) > 20 else campaign
            print(f"{campaign_id[:14]:<15} {source:<12} {medium:<12} {campaign_short:<20} {content}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying campaigns: {e}")


def query_latest_page_catalog(conn: duckdb.DuckDBPyConnection, limit: int = 10):
    """Query latest page catalog."""
    print_section_header(f"Latest {limit} Page Catalog Entries")
    
    try:
        stats = query_table_stats(conn, "iceberg_page_catalog")
        if "error" in stats:
            print(f"  ⚠️  Table not found or error: {stats['error']}")
            return
        
        print(f"  Total rows in table: {stats['total_rows']:,}")
        print()
        
        results = conn.execute(f"""
            SELECT 
                page_url,
                page_category,
                product_id,
                product_category
            FROM lakekeeper_catalog.public.iceberg_page_catalog
            LIMIT {limit}
        """).fetchall()
        
        if not results:
            print("  No data found")
            return
        
        # Print header
        print(f"{'Page URL':<40} {'Page Category':<15} {'Product ID':<15} {'Product Category'}")
        print("-" * 90)
        
        for row in results:
            page_url, page_cat, product_id, product_cat = row
            url_short = (page_url[:37] + '...') if len(page_url) > 40 else page_url
            pid = product_id if product_id else "-"
            pcat = product_cat if product_cat else "-"
            print(f"{url_short:<40} {page_cat:<15} {pid:<15} {pcat}")
            
    except Exception as e:
        print(f"  ⚠️  Error querying page catalog: {e}")


def list_all_tables(conn: duckdb.DuckDBPyConnection):
    """List all available tables in the catalog."""
    print_section_header("Available Iceberg Tables")
    
    try:
        results = conn.execute("""
            SELECT table_name
            FROM lakekeeper_catalog.information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """).fetchall()
        
        if not results:
            print("  No tables found")
            return
        
        print(f"  Found {len(results)} table(s):")
        for row in results:
            print(f"    • {row[0]}")
            
    except Exception as e:
        print(f"  ⚠️  Error listing tables: {e}")


def show_table_summary(conn: duckdb.DuckDBPyConnection):
    """Show summary of all clickstream tables with row counts."""
    print_section_header("Table Summary")
    
    tables = [
        ("iceberg_clickstream_events", "Clickstream Events"),
        ("iceberg_users", "Users"),
        ("iceberg_sessions", "Sessions"),
        ("iceberg_devices", "Devices"),
        ("iceberg_campaigns", "Campaigns"),
        ("iceberg_page_catalog", "Page Catalog"),
    ]
    
    try:
        print("")
        total_rows = 0
        max_name_len = max(len(name) for _, name in tables)
        
        for table_name, display_name in tables:
            try:
                result = conn.execute(f"""
                    SELECT COUNT(*)
                    FROM lakekeeper_catalog.public.{table_name}
                """).fetchone()
                count = result[0] if result else 0
                total_rows += count
                # Pad the name to align the numbers
                padded_name = display_name.ljust(max_name_len)
                print(f"  {padded_name} : {count:>7,} rows")
            except Exception as e:
                print(f"  {display_name} : Error")
        
        separator = "-" * (max_name_len + 18)
        print(f"  {separator}")
        padded_total = "TOTAL".ljust(max_name_len)
        print(f"  {padded_total} : {total_rows:>7,} rows")
        
    except Exception as e:
        print(f"  ⚠️  Error generating summary: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Query latest rows from clickstream Iceberg tables"
    )
    parser.add_argument(
        "--table",
        choices=["clickstream_events", "users", "sessions", "devices", "campaigns", "page_catalog", "all"],
        default="all",
        help="Which table to query (default: all)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to show (default: 10)"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Live monitoring mode - continuously refresh"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Refresh interval in seconds for live mode (default: 5)"
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all available tables and exit"
    )
    
    args = parser.parse_args()
    
    # Setup connection
    conn = setup_duckdb()
    configure_s3(conn)
    attach_catalog(conn)
    
    if args.list_tables:
        list_all_tables(conn)
        return
    
    table_functions = {
        "clickstream_events": query_latest_clickstream_events,
        "users": query_latest_users,
        "sessions": query_latest_sessions,
        "devices": query_latest_devices,
        "campaigns": query_latest_campaigns,
        "page_catalog": query_latest_page_catalog,
    }
    
    def query_all():
        if args.table == "all":
            for table_name, func in table_functions.items():
                func(conn, args.limit)
        else:
            table_functions[args.table](conn, args.limit)
    
    if args.live:
        print(f"\n🔄 Live mode enabled (refreshing every {args.interval}s)")
        print("Press Ctrl+C to exit")
        try:
            while True:
                print("\033[2J\033[H")  # Clear screen
                print(f"\n📊 Clickstream Iceberg Tables - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                query_all()
                show_table_summary(conn)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n\n✅ Exiting live mode")
    else:
        print(f"\n📊 Clickstream Iceberg Tables - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        query_all()
        show_table_summary(conn)
    
    conn.close()


if __name__ == "__main__":
    main()
