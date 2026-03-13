# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.10.0",
#     "pyspark>=3.5.0,<4.0.0",
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
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.graph_objects as go
    import pandas as pd

    return (mo, go, pd)


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
    # CRITICAL: Set environment variables BEFORE importing pyspark
    # to prevent Spark Connect "Too large frame" protocol errors
    os.environ.pop("SPARK_REMOTE", None)
    os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
    # Bind Spark to localhost only to prevent external network interference
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    from pyspark.sql import SparkSession

    # Iceberg version - must match the runtime JAR version
    ICEBERG_VERSION = "1.6.1"

    # Java 17 compatibility - extended flags for Spark/Hadoop security classes
    java_opts = (
        "-XX:+IgnoreUnrecognizedVMOptions "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.fs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.util=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.x509=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.pkcs=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.jgss=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth.login=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth.kerberos=ALL-UNNAMED "
        "--add-opens=java.base/com.sun.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/com.sun.security.auth.module=ALL-UNNAMED "
        "--add-opens=java.base/sun.net.util=ALL-UNNAMED "
        "--add-opens=java.base/sun.net.dns=ALL-UNNAMED "
        "--add-opens=java.management/sun.management=ALL-UNNAMED "
        "-Djdk.reflect.useDirectMethodHandle=false"
    )

    # Initialize Spark with Iceberg support
    spark = (
        SparkSession.builder
        .appName("UserActivityFlow")
        .master("local[1]")
        # Bind to localhost to prevent external network interference
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
        # Explicitly disable Spark Connect
        .config("spark.sql.catalogImplementation", "in-memory")
        # Java 17 compatibility
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        # Add Iceberg packages (using Scala 2.12 which is standard for Spark 3.5)
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}")
        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Lakekeeper REST catalog configuration
        .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakekeeper.type", "rest")
        .config("spark.sql.catalog.lakekeeper.uri", "http://127.0.0.1:8181/catalog")
        .config("spark.sql.catalog.lakekeeper.warehouse", "risingwave-warehouse")
        .config("spark.sql.catalog.lakekeeper.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # S3/MinIO endpoint configuration
        .config("spark.sql.catalog.lakekeeper.s3.endpoint", "http://127.0.0.1:9301")
        .config("spark.sql.catalog.lakekeeper.s3.access-key-id", "hummockadmin")
        .config("spark.sql.catalog.lakekeeper.s3.secret-access-key", "hummockadmin")
        .config("spark.sql.catalog.lakekeeper.s3.path-style-access", "true")
        # S3 Connection Stability Settings - AGGRESSIVE fix for "Connection reset" errors
        # Connection timeouts (milliseconds) - INCREASED
        .config("spark.sql.catalog.lakekeeper.s3.connection-timeout-ms", "60000")  # 60 seconds
        .config("spark.sql.catalog.lakekeeper.s3.socket-timeout-ms", "60000")      # 60 seconds
        # Connection pooling - INCREASED for high parallel reads
        .config("spark.sql.catalog.lakekeeper.s3.connection-maximum-connections", "200")
        # Retry configuration - AGGRESSIVE
        .config("spark.sql.catalog.lakekeeper.s3.max-retries", "20")
        .config("spark.sql.catalog.lakekeeper.s3.retry-mode", "adaptive")
        # Disable SSL for local MinIO
        .config("spark.sql.catalog.lakekeeper.s3.ssl-enabled", "false")
        # Additional stability settings for Iceberg S3FileIO
        .config("spark.sql.catalog.lakekeeper.s3.checksum-enabled", "false")
        # AWS SDK v2 specific settings for Apache HTTP client
        .config("spark.sql.catalog.lakekeeper.s3.apahce-http-client.connection-timeout", "60000")
        .config("spark.sql.catalog.lakekeeper.s3.apache-http-client.socket-timeout", "60000")
        # Spark S3A fallback settings (in case S3FileIO uses S3A internally)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "20")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        # REDUCE parallelism to ease connection pressure on MinIO
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        # Additional Spark SQL settings for stability
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Reduce caching to ensure fresh data reads
        .config("spark.sql.files.openCostInBytes", "0")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session initialized with Iceberg support")
    logger.info(f"   Spark version: {spark.version}")
    logger.info("   S3 connection stability settings applied (AGGRESSIVE)")
    logger.info("   - Connection pool: 200")
    logger.info("   - Timeouts: 60s")
    logger.info("   - Max retries: 20")
    logger.info("   - Parallelism reduced to 4")
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🔧 Helper: Clear Spark Caches & Retry

    Run this cell if you encounter "Connection reset" errors. It clears all cached DataFrames
    and attempts a fresh connection to S3.
    """)
    return


@app.cell
@mo.capture_exception
@mo.capture_stderr
@mo.capture_stdout
def _(spark):
    import time

    def clear_and_retry_query(query_fn, max_retries=3, retry_delay=2):
        """
        Clear Spark caches and retry a query function with exponential backoff.
        Useful for recovering from S3 connection reset errors and stale data.
        """
        for attempt in range(max_retries):
            try:
                # Clear all cached DataFrames
                spark.catalog.clearCache()
                # Clear SQL cache
                spark.sql("CLEAR CACHE")

                # Refresh Iceberg table metadata to force fresh reads
                for table in ['iceberg_funnel', 'iceberg_page_views', 'iceberg_cart_events', 'iceberg_purchases']:
                    try:
                        spark.sql(f"REFRESH TABLE lakekeeper.public.{table}")
                    except:
                        pass

                # Small delay for cache propagation
                time.sleep(0.5)

                logger.info(f"🧹 Cache cleared (attempt {attempt + 1}/{max_retries})")

                # Execute the query function
                result = query_fn()
                logger.info("✅ Query succeeded")
                return result

            except Exception as e:
                error_msg = str(e)
                if "Connection reset" in error_msg or "SdkClientException" in error_msg:
                    logger.warning(f"⚠️  S3 connection error on attempt {attempt + 1}: {error_msg[:100]}...")
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.info(f"⏳ Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    else:
                        logger.error("❌ Max retries exceeded")
                        raise
                else:
                    # Not a connection error, re-raise immediately
                    raise

        return None

    # Test the helper with a simple count query
    def test_query():
        return spark.sql("SELECT COUNT(*) as cnt FROM lakekeeper.public.iceberg_funnel").collect()

    try:
        result = clear_and_retry_query(test_query)
        if result:
            logger.info(f"📊 Funnel table has {result[0]['cnt']:,} rows")
    except Exception as e:
        logger.warning(f"⚠️  Could not verify table: {e}")
        logger.info("   This is OK - the helper function is now available for other cells")

    return (clear_and_retry_query,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 Query Iceberg Tables
    """)
    return


@app.cell
def _(spark):

    # List available namespaces
    logger.info("🔍 Available namespaces:")
    namespaces = spark.sql("SHOW NAMESPACES").collect()
    for ns in namespaces:
        logger.info(f"  - {ns.namespace}")
    return


@app.cell
def _(spark):
    # List tables in public namespace
    logger.info("📋 Tables in public namespace:")
    tables = spark.sql("SHOW TABLES IN lakekeeper.public").collect()
    for t in tables:
        logger.info(f"  - {t.tableName}")
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
    # Quick status check - count rows in funnel table
    try:
        funnel_count = spark.sql("SELECT COUNT(*) as cnt FROM lakekeeper.public.iceberg_funnel").collect()[0]["cnt"]
        logger.info(f"✅ Funnel table: {funnel_count:,} rows")
        logger.info("   Ready to visualize user activity flow!")
    except Exception as e:
        logger.warning(f"⚠️  Could not query funnel table: {e}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 User Activity Flow Visualization

    Below is a funnel chart showing how users flow through the conversion funnel.
    The width of each stage represents the number of users.

    **Tip:** If you see "Connection reset" errors, the cells now automatically
    retry with cache clearing. You can also run the "Clear Spark Caches & Retry"
    cell above to manually reset connections.
    """)
    return


@app.cell(hide_code=True)
def _(clear_and_retry_query, go, spark):
    # Use pre-computed iceberg_funnel table for metrics
    # Uses clear_and_retry_query helper to handle S3 connection issues
    try:
        def get_latest_funnel():
            return spark.sql("""
                SELECT
                    window_start,
                    viewers,
                    carters,
                    purchasers
                FROM lakekeeper.public.iceberg_funnel
                ORDER BY window_start DESC
                LIMIT 1
            """).collect()

        funnel_latest_row = clear_and_retry_query(get_latest_funnel, max_retries=2)

        if funnel_latest_row:
            row = funnel_latest_row[0]
            viewers = row['viewers']
            carters = row['carters']
            purchasers = row['purchasers']
            window_start = row['window_start']

            # Format time label
            try:
                time_label = f"Latest minute: {window_start.strftime('%Y-%m-%d %H:%M')}"
            except (AttributeError, TypeError):
                time_label = f"Latest minute: {str(window_start)[:16]}"

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
    except Exception as e:
        logger.error(f"Error querying iceberg_funnel: {e}")
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
        logger.info(f"\n📅 {time_label}")
        logger.info(f"   Viewers → Cart: {funnel_data['rates'][1]:.1f}%")
        logger.info(f"   Cart → Purchase: {funnel_data['rates'][2]:.1f}%")
    else:
        logger.info(f"📅 {time_label}")
        logger.info("ℹ️  No data for the latest minute")
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
def _(clear_and_retry_query, go, pd, spark):
    # Use pre-computed iceberg_funnel table for time series data
    # Uses clear_and_retry_query helper to handle S3 connection issues

    def get_time_series_data():
        return spark.sql("""
            SELECT
                window_start,
                viewers,
                carters,
                purchasers
            FROM lakekeeper.public.iceberg_funnel
            WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '30 minutes'
            ORDER BY window_start
        """).toPandas()

    ts_data = clear_and_retry_query(get_time_series_data, max_retries=2)

    if ts_data is None or ts_data.empty:
        # Fallback to empty DataFrame if no data
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
        logger.info(f"\n📊 Time Series Summary ({len(ts_data)} data points):")
        logger.info(f"   Time range: {time_range_str}")
        logger.info(f"   Aggregation: 1-minute buckets")
        logger.info(f"   Peak viewers: {int(ts_data['viewers'].max())} at {ts_data.loc[ts_data['viewers'].idxmax(), 'window_start'].strftime('%H:%M')}")
    else:
        logger.error("❌ No time series data available")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📋 Sample Data Preview

    Let's look at some sample data from the funnel table.
    """)
    return


@app.cell
def _(clear_and_retry_query, spark):
    logger.info("📊 Sample Pre-computed Funnel:")

    def get_sample_data():
        return spark.table("lakekeeper.public.iceberg_funnel").show(5, truncate=False)

    clear_and_retry_query(get_sample_data, max_retries=2)
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
    - ✅ Uses pre-computed `iceberg_funnel` table for metrics
    - ✅ Visualizes user flow with Plotly

    **Tables Queried:**
    - `iceberg_funnel` - Pre-computed funnel metrics

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
    logger.info("✅ Notebook execution complete!")
    logger.info("   Spark session is still active.")
    return


if __name__ == "__main__":
    app.run()
