# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.10.0",
#     "pyspark[connect]>=3.5.0,<4.0.0",
#     "pandas>=2.0.0",
#     "plotly>=5.0.0",
#     "nbformat>=5.0.0",
#     "requests>=2.31.0",
# ]
# ///

"""
User Activity Flow - Marimo Notebook
=====================================

This notebook queries Iceberg tables using Apache Spark and displays a User Activity Flow graph
showing how users move through different stages of the conversion funnel:
- Page Views
- Cart Events
- Purchases
"""

import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():

    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 🔄 User Activity Flow

    This notebook queries Iceberg tables using **Apache Spark** to visualize how users flow through
    the e-commerce conversion funnel: **Page Views → Cart Events → Purchases**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🔧 Setup Spark Session with Iceberg Support
    """)
    return


@app.cell
def _():
    import os

    # MUST set this BEFORE importing pyspark to fix Java 17+ compatibility
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_PYTHON'] = os.sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = os.sys.executable

    from pyspark.sql import SparkSession
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime

    # Iceberg version compatible with Spark 3.5
    ICEBERG_VERSION = "1.5.2"

    # Java 17+ compatibility flags
    java_opts = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.security=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.net.util=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--enable-native-access=ALL-UNNAMED"
    )

    # Build Spark session with Iceberg and S3 (MinIO) support
    spark = (SparkSession.builder
        .appName("IcebergUserActivityFlow")
        .config("spark.jars.packages",
                f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
                f"org.apache.iceberg:iceberg-aws:{ICEBERG_VERSION},"
                f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakekeeper.type", "rest")
        .config("spark.sql.catalog.lakekeeper.uri", "http://127.0.0.1:8181/catalog")
        .config("spark.sql.catalog.lakekeeper.warehouse", "risingwave-warehouse")
        .config("spark.sql.catalog.lakekeeper.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakekeeper.s3.endpoint", "http://127.0.0.1:9301")
        .config("spark.sql.catalog.lakekeeper.s3.access-key-id", "hummockadmin")
        .config("spark.sql.catalog.lakekeeper.s3.secret-access-key", "hummockadmin")
        .config("spark.sql.catalog.lakekeeper.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakekeeper.s3.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lakekeeper")
        .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    print("✅ Spark session created successfully with Iceberg support")
    print(f"   Spark version: {spark.version}")
    print(f"   Catalog: lakekeeper (REST)")
    return go, pd, spark


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Query Iceberg Tables
    """)
    return


@app.cell
def _(spark):

    # List available namespaces
    print("🔍 Available namespaces:")
    namespaces = spark.sql("SHOW NAMESPACES").collect()
    for ns in namespaces:
        print(f"  - {ns.namespace}")
    return


@app.cell
def _(spark):
    # List tables in public namespace
    print("📋 Tables in public namespace:")
    tables = spark.sql("SHOW TABLES IN lakekeeper.public").collect()
    for t in tables:
        print(f"  - {t.tableName}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Table Loading Status

    Spark is ready to query Iceberg tables. Charts will load data on demand.
    """)
    return


@app.cell
def _(spark):
    # Quick status check - count rows in each table
    try:
        page_count = spark.sql("SELECT COUNT(*) as cnt FROM lakekeeper.public.iceberg_page_views").collect()[0]["cnt"]
        cart_count = spark.sql("SELECT COUNT(*) as cnt FROM lakekeeper.public.iceberg_cart_events").collect()[0]["cnt"]
        purchase_count = spark.sql("SELECT COUNT(*) as cnt FROM lakekeeper.public.iceberg_purchases").collect()[0]["cnt"]
        print(f"✅ Page views: {page_count:,} rows")
        print(f"✅ Cart events: {cart_count:,} rows")
        print(f"✅ Purchases: {purchase_count:,} rows")
    except Exception as e:
        print(f"⚠️  Could not query tables: {e}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 User Activity Flow Visualization

    Below is a funnel chart showing how users flow through the conversion funnel.
    The width of each stage represents the number of users.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Funnel Conversion Metrics

    Detailed breakdown of user progression through the funnel.
    """)
    return


@app.cell(hide_code=True)
def _(go, spark):
    # Compute funnel metrics using same logic as DuckDB/RisingWave
    # This ensures consistency across all tools
    try:
        # Get the latest event time from page views
        funnel_latest_time_row = spark.sql("""
            SELECT MAX(event_time) as latest_time FROM lakekeeper.public.iceberg_page_views
        """).collect()
        funnel_latest_time = funnel_latest_time_row[0]['latest_time'] if funnel_latest_time_row else None
    except Exception:
        funnel_latest_time = None

    # Compute funnel metrics for the latest minute using proper JOIN logic
    if funnel_latest_time:
        # Format time label
        try:
            time_label = f"Latest minute: {funnel_latest_time.strftime('%Y-%m-%d %H:%M')}"
        except (AttributeError, TypeError):
            time_label = f"Latest minute: {str(funnel_latest_time)[:16]}"

        # Use same JOIN logic as DuckDB to compute funnel for latest minute
        funnel_result = spark.sql(f"""
            WITH page_events AS (
                SELECT user_id, event_time
                FROM lakekeeper.public.iceberg_page_views
                WHERE event_time >= DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}')
                AND event_time < DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}') + INTERVAL '1 minute'
            ),
            cart_events AS (
                SELECT user_id, event_time
                FROM lakekeeper.public.iceberg_cart_events
                WHERE event_time >= DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}')
                AND event_time < DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}') + INTERVAL '1 minute'
            ),
            purchase_events AS (
                SELECT user_id, event_time
                FROM lakekeeper.public.iceberg_purchases
                WHERE event_time >= DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}')
                AND event_time < DATE_TRUNC('minute', TIMESTAMP '{funnel_latest_time}') + INTERVAL '1 minute'
            )
            SELECT
                COUNT(DISTINCT p.user_id) as viewers,
                COUNT(DISTINCT c.user_id) as carters,
                COUNT(DISTINCT pur.user_id) as purchasers
            FROM page_events p
            LEFT JOIN cart_events c ON p.user_id = c.user_id
            LEFT JOIN purchase_events pur ON p.user_id = pur.user_id
        """).collect()[0]

        viewers = funnel_result["viewers"]
        carters = funnel_result["carters"]
        purchasers = funnel_result["purchasers"]

        # Calculate rates
        view_to_cart_rate = (carters / viewers * 100) if viewers > 0 else 0
        cart_to_buy_rate = (purchasers / carters * 100) if carters > 0 else 0
        overall_conversion = (purchasers / viewers * 100) if viewers > 0 else 0

        funnel_data = {
            "stages": ["Page Views", "Cart Events", "Purchases"],
            "users": [viewers, carters, purchasers],
            "rates": [100.0, view_to_cart_rate, cart_to_buy_rate],
        }
    else:
        time_label = "No data available"
        funnel_data = None

    if funnel_data and sum(funnel_data["users"]) > 0:
        # Create funnel chart
        funnel_fig = go.Figure(go.Funnel(
            y=funnel_data["stages"],
            x=funnel_data["users"],
            textposition="inside",
            textinfo="value+percent initial",
            opacity=0.8,
            marker={
                "color": ["#3b82f6", "#f59e0b", "#10b981"],
                "line": {"width": [4, 3, 2], "color": ["#1d4ed8", "#d97706", "#059669"]},
            },
            connector={"line": {"color": "#6b7280", "dash": "dot", "width": 3}},
        ))

        funnel_fig.update_layout(
            title=dict(
                text="User Conversion Funnel",
                subtitle=dict(text=time_label, font=dict(size=12, color="#666"))
            ),
            showlegend=False,
            height=500,
            template="plotly_white",
        )

        funnel_fig.show()

        # Print data freshness info below chart
        print(f"\n📅 {time_label}")
        print(f"   Viewers → Cart: {funnel_data['rates'][1]:.1f}%")
        print(f"   Cart → Purchase: {funnel_data['rates'][2]:.1f}%")
    else:
        print(f"📅 {time_label}")
        print("ℹ️  No data for the latest minute")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 Funnel Metrics Over Time

    This area chart shows how users progress through the funnel over time,
    similar to the dashboard visualization. The shaded areas represent the
    volume of users at each stage.
    """)
    return


@app.cell(hide_code=True)
def _(go, pd, spark):
    # Run fresh query for time series data (self-contained for rerun)
    # Use same logic as DuckDB and RisingWave funnel query

    # Get the latest event time to base our query on
    ts_latest_time_row = spark.sql("""
        SELECT MAX(event_time) as latest_time FROM lakekeeper.public.iceberg_page_views
    """).collect()
    ts_latest_time = ts_latest_time_row[0]['latest_time'] if ts_latest_time_row else None

    if ts_latest_time:
        # Query using same logic as DuckDB - last 30 minutes from latest data
        # with proper LEFT JOINs matching RisingWave funnel
        ts_data = spark.sql(f"""
            WITH page_windows AS (
                -- Create 1-minute tumbling windows from page views
                SELECT
                    DATE_TRUNC('minute', event_time) as window_start,
                    DATE_TRUNC('minute', event_time) + INTERVAL '1 minute' as window_end,
                    user_id
                FROM lakekeeper.public.iceberg_page_views
                WHERE event_time >= TIMESTAMP '{ts_latest_time}' - INTERVAL '30 minutes'
            ),
            cart_events AS (
                SELECT event_time, user_id
                FROM lakekeeper.public.iceberg_cart_events
                WHERE event_time >= TIMESTAMP '{ts_latest_time}' - INTERVAL '30 minutes'
            ),
            purchase_events AS (
                SELECT event_time, user_id
                FROM lakekeeper.public.iceberg_purchases
                WHERE event_time >= TIMESTAMP '{ts_latest_time}' - INTERVAL '30 minutes'
            ),
            funnel AS (
                -- Match RisingWave's join logic: cart/purchase events between window_start and window_end
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
                window_start,
                viewers,
                carters,
                purchasers
            FROM funnel
            ORDER BY window_start
        """).toPandas()
    else:
        # Fallback to simple query if no data
        ts_data = pd.DataFrame(columns=['window_start', 'viewers', 'carters', 'purchasers'])

    # Fill NaN values with 0
    ts_data = ts_data.fillna(0)

    if ts_data is not None and not ts_data.empty:
        # Convert window_start to datetime for better formatting
        ts_data['window_start'] = pd.to_datetime(ts_data['window_start'])

        # Get time range for the subtitle
        time_start = ts_data['window_start'].min()
        time_end = ts_data['window_start'].max()
        time_range_str = f"{time_start.strftime('%Y-%m-%d %H:%M')} to {time_end.strftime('%Y-%m-%d %H:%M')}"

        # Create area chart similar to the dashboard
        area_fig = go.Figure()

        # Add viewers area - only value in hover (time shown in x-axis label)
        area_fig.add_trace(go.Scatter(
            x=ts_data['window_start'],
            y=ts_data['viewers'],
            mode='lines',
            name='Viewers',
            line=dict(color='#636efa', width=3),
            fill='tozeroy',
            fillcolor='rgba(99, 110, 250, 0.3)',
            hovertemplate='%{y} viewers<extra></extra>'
        ))

        # Add carters area - only value in hover
        area_fig.add_trace(go.Scatter(
            x=ts_data['window_start'],
            y=ts_data['carters'],
            mode='lines',
            name='Carters',
            line=dict(color='#00cc96', width=3),
            fill='tozeroy',
            fillcolor='rgba(0, 204, 150, 0.3)',
            hovertemplate='%{y} carters<extra></extra>'
        ))

        # Add purchasers area - only value in hover
        area_fig.add_trace(go.Scatter(
            x=ts_data['window_start'],
            y=ts_data['purchasers'],
            mode='lines',
            name='Purchasers',
            line=dict(color='#ff6692', width=3),
            fill='tozeroy',
            fillcolor='rgba(255, 102, 146, 0.3)',
            hovertemplate='%{y} purchasers<extra></extra>'
        ))

        area_fig.update_layout(
            title=dict(
                text="User Activity Flow Over Time",
                subtitle=dict(text=f"Data range: {time_range_str}", font=dict(size=12))
            ),
            xaxis_title="Time (1-minute windows)",
            yaxis_title="Unique Users",
            height=500,
            template="plotly_white",
            hovermode='x unified',
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
            ),
            xaxis=dict(
                tickformat='%H:%M',
                tickmode='linear',
                dtick=60*1000,  # 1 minute in milliseconds
                showgrid=True,
                gridcolor='rgba(0,0,0,0.1)'
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        area_fig.show()

        # Also display a summary table
        print(f"\n📊 Time Series Summary ({len(ts_data)} data points):")
        print(f"   Time range: {time_range_str}")
        print(f"   Aggregation: 1-minute buckets")
        print(f"   Peak viewers: {int(ts_data['viewers'].max())} at {ts_data.loc[ts_data['viewers'].idxmax(), 'window_start'].strftime('%H:%M')}")
    else:
        print("❌ No time series data available")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📋 Sample Data Preview

    Let's look at some sample data from each table.
    """)
    return


@app.cell
def _(spark):
    # Show sample data from page views
    print("📄 Sample Page Views:")
    spark.table("lakekeeper.public.iceberg_page_views").show(5, truncate=False)
    return


@app.cell
def _(spark):
    # Show sample data from cart events
    print("🛒 Sample Cart Events:")
    spark.table("lakekeeper.public.iceberg_cart_events").show(5, truncate=False)
    return


@app.cell
def _(spark):
    # Show sample data from purchases
    print("💳 Sample Purchases:")
    spark.table("lakekeeper.public.iceberg_purchases").show(5, truncate=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🔍 SQL Queries

    You can also query the Iceberg tables directly using Spark SQL:
    """)
    return


@app.cell
def _(spark):
    # Example SQL query
    result = spark.sql("""
        SELECT
            'Page Views' as stage,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users
        FROM lakekeeper.public.iceberg_page_views

        UNION ALL

        SELECT
            'Cart Events' as stage,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users
        FROM lakekeeper.public.iceberg_cart_events

        UNION ALL

        SELECT
            'Purchases' as stage,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users
        FROM lakekeeper.public.iceberg_purchases
    """)

    print("📊 Event Summary by Stage:")
    result.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## ℹ️ About This Notebook

    This notebook demonstrates how to query Iceberg tables using Apache Spark with the Iceberg connector.

    **Key Features:**
    - ✅ Uses Spark SQL to query Iceberg tables
    - ✅ Connects to Lakekeeper REST catalog
    - ✅ Reads from MinIO S3 storage
    - ✅ Computes funnel metrics
    - ✅ Visualizes user flow with Plotly

    **Catalog Configuration:**
    - **Type:** REST (Lakekeeper)
    - **Endpoint:** http://127.0.0.1:8181/catalog
    - **Warehouse:** risingwave-warehouse
    - **S3 Endpoint:** http://127.0.0.1:9301

    Data source: Iceberg tables via Apache Spark (Lakekeeper catalog)
    """)
    return


@app.cell
def _():
    # Keep the notebook running
    print("✅ Notebook execution complete!")
    print("   Spark session is still active.")
    return


if __name__ == "__main__":
    app.run()
