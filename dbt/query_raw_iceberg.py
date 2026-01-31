#!/usr/bin/env python3
"""
Query funnel analytics from raw Iceberg events via DuckDB

This script computes funnel analytics (viewers, carters, purchasers, conversion rates)
from raw events stored in Iceberg tables.

Usage:
    python query_raw_iceberg.py           # Show funnel analytics
    python query_raw_iceberg.py --live    # Live monitoring mode
"""

import duckdb
import sys
import time
import argparse
from datetime import datetime, timedelta


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


def debug_raw_counts(conn: duckdb.DuckDBPyConnection) -> None:
    """Debug: Show raw counts and time ranges in Iceberg tables."""
    print("\n📊 DEBUG: Raw counts in Iceberg tables")
    print("-" * 60)
    
    try:
        # Page views
        page_stats = conn.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(event_time) as earliest,
                MAX(event_time) as latest
            FROM lakekeeper_catalog.public.iceberg_page_views
        """).fetchone()
        print(f"Page Views:  {page_stats[0]} events, {page_stats[1]} unique users")
        print(f"             Time range: {page_stats[2]} to {page_stats[3]}")
        
        # Cart events
        cart_stats = conn.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(event_time) as earliest,
                MAX(event_time) as latest
            FROM lakekeeper_catalog.public.iceberg_cart_events
        """).fetchone()
        print(f"Cart Events: {cart_stats[0]} events, {cart_stats[1]} unique users")
        print(f"             Time range: {cart_stats[2]} to {cart_stats[3]}")
        
        # Purchases
        purchase_stats = conn.execute("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(event_time) as earliest,
                MAX(event_time) as latest
            FROM lakekeeper_catalog.public.iceberg_purchases
        """).fetchone()
        print(f"Purchases:   {purchase_stats[0]} events, {purchase_stats[1]} unique users")
        print(f"             Time range: {purchase_stats[2]} to {purchase_stats[3]}")
        
        # Current time used in queries
        current_time = conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
        truncated = conn.execute("SELECT DATE_TRUNC('minute', CURRENT_TIMESTAMP)").fetchone()[0]
        print(f"\n⏰ Current time: {current_time}")
        print(f"   Truncated (excluded): {truncated}")
        print(f"   Query range: {truncated - timedelta(minutes=12)} to {truncated}")
        
    except Exception as e:
        print(f"❌ Debug error: {e}")


def query_latest_minute(conn: duckdb.DuckDBPyConnection) -> None:
    """Show events in the current (incomplete) minute for real-time monitoring."""
    print_section_header("⏱️  Current Minute (Real-time)")
    
    try:
        result = conn.execute("""
            SELECT
                DATE_TRUNC('minute', CURRENT_TIMESTAMP) as "Window Start",
                COUNT(DISTINCT p.user_id) as "Viewers",
                COUNT(DISTINCT c.user_id) as "Carters",
                COUNT(DISTINCT pur.user_id) as "Purchasers",
                COUNT(*) as "Total Events"
            FROM (
                SELECT user_id, event_time
                FROM lakekeeper_catalog.public.iceberg_page_views
                WHERE event_time >= DATE_TRUNC('minute', CURRENT_TIMESTAMP)
            ) p
            LEFT JOIN (
                SELECT user_id, event_time
                FROM lakekeeper_catalog.public.iceberg_cart_events
                WHERE event_time >= DATE_TRUNC('minute', CURRENT_TIMESTAMP)
            ) c ON p.user_id = c.user_id
            LEFT JOIN (
                SELECT user_id, event_time
                FROM lakekeeper_catalog.public.iceberg_purchases
                WHERE event_time >= DATE_TRUNC('minute', CURRENT_TIMESTAMP)
            ) pur ON p.user_id = pur.user_id;
        """).fetchdf()
        
        if result.empty or result["Total Events"].iloc[0] == 0:
            print("⚠️  No events yet in current minute.")
        else:
            print(result.to_string(index=False))
            print(f"\n🕐 Last updated: {datetime.now().strftime('%H:%M:%S')}")
            
    except Exception as e:
        print(f"❌ Error: {e}")


def query_funnel(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute and display funnel analytics from raw events."""
    print_section_header("🎯 Funnel Analytics (Last 12 Minutes)")
    
    try:
        # Compute funnel using tumbling windows like RisingWave
        # Include current incomplete minute for real-time monitoring
        result = conn.execute("""
            WITH page_windows AS (
                -- Create 1-minute tumbling windows like RisingWave TUMBLE()
                SELECT
                    DATE_TRUNC('minute', event_time) as window_start,
                    DATE_TRUNC('minute', event_time) + INTERVAL '1 minute' as window_end,
                    user_id
                FROM lakekeeper_catalog.public.iceberg_page_views
                WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '12 minutes'
            ),
            cart_events AS (
                -- Raw cart events with their exact timestamps
                SELECT
                    event_time,
                    user_id
                FROM lakekeeper_catalog.public.iceberg_cart_events
                WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '12 minutes'
            ),
            purchase_events AS (
                -- Raw purchase events with their exact timestamps
                SELECT
                    event_time,
                    user_id
                FROM lakekeeper_catalog.public.iceberg_purchases
                WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '12 minutes'
            ),
            funnel AS (
                -- Match RisingWave's join logic: cart/purchase events between window_start and window_end
                -- Note: RisingWave's BETWEEN is inclusive of both ends
                SELECT
                    p.window_start,
                    COUNT(DISTINCT p.user_id) as viewers,
                    COUNT(DISTINCT c.user_id) as carters,
                    COUNT(DISTINCT pur.user_id) as purchasers
                FROM page_windows p
                LEFT JOIN cart_events c
                    ON p.user_id = c.user_id
                    AND c.event_time >= p.window_start
                    AND c.event_time <= p.window_end
                LEFT JOIN purchase_events pur
                    ON p.user_id = pur.user_id
                    AND pur.event_time >= p.window_start
                    AND pur.event_time <= p.window_end
                GROUP BY p.window_start
            )
            SELECT
                window_start as "Time",
                viewers as "Viewers",
                carters as "Carters",
                purchasers as "Purchasers",
                ROUND(CASE WHEN viewers > 0 THEN carters::DECIMAL / viewers * 100 ELSE 0 END, 1) as "V→C %",
                ROUND(CASE WHEN carters > 0 THEN purchasers::DECIMAL / carters * 100 ELSE 0 END, 1) as "C→B %"
            FROM funnel
            ORDER BY window_start ASC
            LIMIT 10;
        """).fetchdf()
        
        if result.empty:
            print("⚠️  No funnel data available yet.")
            print("   Make sure events are being produced and written to Iceberg.")
        else:
            print("\n" + result.to_string(index=False))
            
    except Exception as e:
        print(f"❌ Error: {e}")


def live_mode(conn: duckdb.DuckDBPyConnection) -> None:
    """Run in live monitoring mode, refreshing every 5 seconds."""
    try:
        while True:
            # Clear screen (works on Unix-like systems)
            print("\033[2J\033[H")
            
            print("=" * 80)
            print(f"🔄 LIVE FUNNEL MONITORING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 80)
            print("(Press Ctrl+C to exit)")
            
            query_funnel(conn)
            
            print("\n⏳ Refreshing in 5 seconds...")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n👋 Live monitoring stopped.")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Query funnel analytics from raw Iceberg events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python query_raw_iceberg.py           # Show funnel analytics
  python query_raw_iceberg.py --debug   # Show debug info
  python query_raw_iceberg.py --live    # Live monitoring mode
        """
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Live monitoring mode (refreshes every 5 seconds)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug information about raw counts and time ranges"
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("🧊 Funnel Analytics from Iceberg Events")
    print("=" * 80)
    
    try:
        # Set up DuckDB
        conn = setup_duckdb()
        
        # Configure S3
        configure_s3(conn)
        
        # Attach to catalog
        attach_catalog(conn)
        
        # Run in appropriate mode
        if args.live:
            live_mode(conn)
        else:
            if args.debug:
                debug_raw_counts(conn)
            query_latest_minute(conn)
            query_funnel(conn)
        
        print("\n" + "=" * 80)
        print("✅ Done!")
        print("=" * 80)
        
        # Close connection
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())