from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import pymysql
import psycopg2
from typing import Optional
import json
import threading
import time
import uvicorn
import os
import re
import asyncio
import logging
import httpx

from confluent_kafka import Consumer, KafkaError, TopicPartition

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ML Serving Service configuration
ML_SERVING_URL = os.environ.get("ML_SERVING_URL", "http://localhost:8001")

# Global event for notifying SSE clients of new data
data_updated_event = asyncio.Event()
main_loop = None

async def call_ml_serving(endpoint: str, method: str = "GET", json_data: dict = None) -> dict:
    """Call the ML serving service."""
    url = f"{ML_SERVING_URL}{endpoint}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            if method == "GET":
                response = await client.get(url)
            elif method == "POST":
                response = await client.post(url, json=json_data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
    except httpx.ConnectError:
        logger.error(f"Cannot connect to ML serving service at {ML_SERVING_URL}")
        return {"error": "ML serving service unavailable"}
    except httpx.HTTPStatusError as e:
        logger.error(f"ML serving error: {e.response.status_code} - {e.response.text}")
        return {"error": f"ML serving error: {e.response.status_code}"}
    except Exception as e:
        logger.error(f"Error calling ML serving: {e}")
        return {"error": str(e)}


# Lifespan context manager for startup/shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    global kafka_consumer_thread, consumer_running, main_loop
    # Startup
    main_loop = asyncio.get_running_loop()
    consumer_running = True
    kafka_consumer_thread = threading.Thread(target=_kafka_raw_funnel_consumer_loop, daemon=True)
    kafka_consumer_thread.start()
    
    logger.info(f"Dashboard API started. ML serving at: {ML_SERVING_URL}")
    
    yield
    # Shutdown
    consumer_running = False
    if kafka_consumer_thread:
        kafka_consumer_thread.join(timeout=2)

app = FastAPI(lifespan=lifespan)

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import os

# StarRocks serves ad hoc queries across the unified hot/cold funnel history.
STARROCKS_URL = os.getenv(
    'STARROCKS_URL',
    'mysql+pymysql://root@localhost:9030/sr_local_db_sr_local_db',
)
starrocks_engine = create_engine(
    STARROCKS_URL,
    connect_args={"connect_timeout": 5},
    pool_pre_ping=True,
)
logger.info(
    "[StarRocks] Ad hoc query connection: %s",
    starrocks_engine.url.render_as_string(hide_password=True),
)

# RisingWave wire pre-check (2026-11-14): with the RW containers STOPPED,
# Docker's port proxy still ACCEPTS TCP connections on the published port,
# so a plain "is the port open" check passes while no Postgres server ever
# answers. StarRocks's JDBC catalog then hangs through multiple 10s HikariCP
# timeouts before its analysis fails (~30s) -- that's why the degraded
# queries took 31s. This does a real, bounded wire handshake instead: if
# RisingWave doesn't answer SELECT 1 within ~2s, skip the live attempt and
# go straight to the cold fallback. The try/except around the live query
# stays as a safety net for RW dying between check and query.
RISINGWAVE_HOST = os.getenv("RISINGWAVE_HOST", "localhost")
RISINGWAVE_PORT = int(os.getenv("RISINGWAVE_PORT", "4566"))


def _risingwave_reachable(timeout: float = 2.0) -> bool:
    """Bounded handshake against RisingWave's Postgres wire port.

    psycopg2, not pymysql -- RW speaks the Postgres protocol; a MySQL
    handshake always fails and would report a healthy RW as down.
    """
    try:
        conn = psycopg2.connect(
            host=RISINGWAVE_HOST,
            port=RISINGWAVE_PORT,
            user="root",
            dbname="dev",
            connect_timeout=int(timeout),
        )
        # No set_session() here: readonly=True makes psycopg2 run
        # "SET default_transaction_read_only", which RW rejects as an
        # unknown config entry -- a healthy RW would look down.
        try:
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = %d" % int(timeout * 1000))
                cursor.execute("SELECT 1")
            return True
        finally:
            conn.close()
    except Exception:
        return False


def _starrocks_http_error(error: Exception) -> HTTPException:
    """Map StarRocks failures to an actionable API response."""
    message = str(error)
    status_code = 504 if "timeout" in message.lower() else 503
    return HTTPException(
        status_code=status_code,
        detail={"source": "starrocks", "error": message},
    )


def _cached_enriched_records(connection, limit: int) -> list[dict]:
    """Build enriched records from the RW-free Routine Load cache.

    Reads dashboard_funnel_serving_cached_direct (frozen
    mv_unified_funnel_summary + funnel_summary_direct Routine Load, deduped
    by construction) so the fallback stays hot+cold and fresh -- only the
    zero-copy RW catalog is out of the loop.
    """
    result = connection.execute(text("""
        WITH country_aggregated AS (
            SELECT
                window_start,
                window_end,
                SUM(viewers) AS viewers,
                SUM(carters) AS carters,
                SUM(purchasers) AS purchasers,
                SUM(carters) / NULLIF(SUM(viewers), 0) AS view_to_cart_rate,
                SUM(purchasers) / NULLIF(SUM(carters), 0) AS cart_to_buy_rate
            FROM dashboard_funnel_serving_cached_direct
            GROUP BY window_start, window_end
        )
        SELECT
            window_start,
            window_end,
            viewers,
            carters,
            purchasers,
            view_to_cart_rate,
            cart_to_buy_rate,
            CASE
                WHEN view_to_cart_rate IS NULL THEN 'unknown'
                WHEN view_to_cart_rate >= 0.5 THEN 'excellent'
                WHEN view_to_cart_rate >= 0.3 THEN 'good'
                WHEN view_to_cart_rate >= 0.1 THEN 'average'
                ELSE 'needs_improvement'
            END AS view_to_cart_category,
            CASE
                WHEN cart_to_buy_rate IS NULL THEN 'unknown'
                WHEN cart_to_buy_rate >= 0.5 THEN 'excellent'
                WHEN cart_to_buy_rate >= 0.3 THEN 'good'
                WHEN cart_to_buy_rate >= 0.1 THEN 'average'
                ELSE 'needs_improvement'
            END AS cart_to_buy_category,
            ROUND((carters / NULLIF(viewers, 0)) * 0.4
                + (purchasers / NULLIF(carters, 0)) * 0.6, 2) AS funnel_score,
            'N/A' AS view_to_cart_emoji,
            'N/A' AS cart_to_buy_emoji,
            CASE
                WHEN view_to_cart_rate IS NULL OR cart_to_buy_rate IS NULL THEN 'unknown'
                WHEN view_to_cart_rate >= 0.3 AND cart_to_buy_rate >= 0.3 THEN 'strong'
                WHEN view_to_cart_rate >= 0.2 OR cart_to_buy_rate >= 0.2 THEN 'moderate'
                ELSE 'weak'
            END AS funnel_health
        FROM country_aggregated
        ORDER BY window_start DESC
        LIMIT :limit
    """), {"limit": limit})
    return [
        {
            "window_start": row.window_start.isoformat() if row.window_start else None,
            "window_end": row.window_end.isoformat() if row.window_end else None,
            "viewers": int(row.viewers) if row.viewers else 0,
            "carters": int(row.carters) if row.carters else 0,
            "purchasers": int(row.purchasers) if row.purchasers else 0,
            "view_to_cart_rate": float(row.view_to_cart_rate) if row.view_to_cart_rate else 0.0,
            "cart_to_buy_rate": float(row.cart_to_buy_rate) if row.cart_to_buy_rate else 0.0,
            "view_to_cart_category": row.view_to_cart_category,
            "cart_to_buy_category": row.cart_to_buy_category,
            "funnel_score": float(row.funnel_score) if row.funnel_score else 0.0,
            "view_to_cart_emoji": row.view_to_cart_emoji,
            "cart_to_buy_emoji": row.cart_to_buy_emoji,
            "funnel_health": row.funnel_health,
        }
        for row in result
    ]


@app.get("/api/serving/status")
def get_serving_status():
    """Report StarRocks catalog availability and hot/cold serving freshness."""
    try:
        with starrocks_engine.connect() as conn:
            catalogs = {
                row[0]
                for row in conn.execute(text("SHOW CATALOGS"))
            }
            freshness = {}
            component_errors = {}
            for name, query in {
                "hot_window": "SELECT MAX(window_start) FROM risingwave.public.funnel_summary",
                "cold_window": "SELECT MAX(window_start) FROM mv_unified_funnel_summary",
                "serving_window": "SELECT MAX(window_start) FROM dashboard_funnel_serving",
            }.items():
                try:
                    value = conn.execute(text(query)).scalar()
                    freshness[name] = value.isoformat() if value else None
                except Exception as error:
                    freshness[name] = None
                    component_errors[name] = str(error)

        required_catalogs = {"risingwave", "databricks_uc", "lakekeeper_local"}
        missing_catalogs = sorted(required_catalogs - catalogs)
        status = "ready" if not missing_catalogs and not component_errors else "degraded"
        return {
            "status": status,
            "catalogs": {
                "required": sorted(required_catalogs),
                "missing": missing_catalogs,
            },
            "freshness": freshness,
            "component_errors": component_errors,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as error:
        logger.exception("Error checking StarRocks serving status")
        raise _starrocks_http_error(error) from error

# In-memory cache for the latest funnel data
latest_funnel_data = {
    "window_start": None,
    "window_end": None,
    "viewers": 0,
    "carters": 0,
    "purchasers": 0,
    "view_to_cart_rate": 0.0,
    "cart_to_buy_rate": 0.0
}
previous_funnel_data = None
funnel_data_history = []  # Keep last 1000 records for chart (deduplicated to ~50 unique minutes)
MAX_HISTORY_SIZE = 1000
kafka_consumer_thread = None
consumer_running = False


def parse_timestamp(ts):
    """Parse timestamp (Unix ms, string, or datetime) to ISO format in UTC with timezone, truncated to seconds."""
    if ts is None:
        return None
    from datetime import datetime, timezone
    # Handle Unix timestamp in milliseconds (from Kafka JSON)
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return dt.replace(microsecond=0).isoformat()
    # Handle string
    if isinstance(ts, str):
        # Check if it's already a number string
        if ts.isdigit():
            dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
            return dt.replace(microsecond=0).isoformat()
        # Parse string to datetime to handle microseconds and ensure consistent format
        try:
            # Remove timezone suffix for parsing
            ts_no_tz = ts.replace('Z', '+00:00')
            if ts_no_tz.endswith('+00:00'):
                ts_no_tz = ts_no_tz[:-6]
            # Parse the base timestamp
            dt = datetime.fromisoformat(ts_no_tz)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            # Truncate to seconds and return with timezone
            return dt.replace(microsecond=0).isoformat()
        except Exception:
            # Fallback: ensure string has timezone info
            if not ts.endswith('Z') and not re.search(r'[+-]\d{2}:\d{2}$', ts):
                ts = ts + '+00:00'
            return ts
    # Handle datetime object
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.replace(microsecond=0).isoformat()
    return str(ts)


# Raw-topic Kafka feed for the Dashboard (2026-11-14). RW was only ever
# in the loop as a stream processor: raw events land in Redpanda first,
# RW windowed them and sank them back to Kafka. The aggregation is trivial
# in-process -- per-minute distinct-user sets -- so the backend now
# consumes the raw topics directly and pushes SSE per event batch:
# event-driven, no RW, no polling, no waiting on any database refresh.
RAW_FUNNEL_TOPICS = ('page_views', 'cart_events', 'purchases')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
# Startup replay window: how far back the consumer rebuilds history from
# the topics (so a fresh backend shows recent data even with RW down --
# the old RW-path restart showed empty charts).
RAW_FUNNEL_REPLAY_MINUTES = float(os.getenv('RAW_FUNNEL_REPLAY_MINUTES', '180'))
# topic -> funnel stage the event counts toward
_RAW_EVENT_STAGE = {
    'page_views': 'viewers',
    'cart_events': 'carters',
    'purchases': 'purchasers',
}
# Per-minute distinct user sets: window_start iso -> {stage: set[user_id]}
_raw_window_sets: dict = {}


def _publish_window_record(record):
    """Upsert a window record into history (newest first) and SSE state."""
    global latest_funnel_data, previous_funnel_data, funnel_data_history

    funnel_data_history = [
        r for r in funnel_data_history
        if r.get("window_start") != record["window_start"]
    ]
    # Keep newest-first ordering even if events arrive out of order
    insert_at = len(funnel_data_history)
    for i, r in enumerate(funnel_data_history):
        if (r.get("window_start") or "") <= record["window_start"]:
            insert_at = i
            break
    funnel_data_history.insert(insert_at, record)
    if len(funnel_data_history) > MAX_HISTORY_SIZE:
        funnel_data_history.pop()

    previous_funnel_data = latest_funnel_data.copy()
    latest_funnel_data = record.copy()


def _process_raw_event(topic: str, payload: bytes) -> bool:
    """Fold one raw event into the window sets. Returns True when a window
    aggregate actually changed (only then does SSE need to push)."""
    stage = _RAW_EVENT_STAGE.get(topic)
    if stage is None:
        return False
    try:
        event = json.loads(payload)
        user_id = event.get('user_id')
        event_time = event.get('event_time')
        if user_id is None or not event_time:
            return False
        dt = datetime.fromisoformat(str(event_time).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning("Skipping malformed %s event: %s", topic, e)
        return False

    window = dt.replace(second=0, microsecond=0)
    sets = _raw_window_sets.setdefault(window.isoformat(), {
        'viewers': set(), 'carters': set(), 'purchasers': set(),
    })
    stage_set = sets[stage]
    if user_id in stage_set:
        return False
    stage_set.add(user_id)

    viewers = len(sets['viewers'])
    carters = len(sets['carters'])
    purchasers = len(sets['purchasers'])
    _publish_window_record({
        "window_start": parse_timestamp(window),
        "window_end": parse_timestamp(window + timedelta(minutes=1)),
        "viewers": viewers,
        "carters": carters,
        "purchasers": purchasers,
        "view_to_cart_rate": carters / viewers if viewers else 0.0,
        "cart_to_buy_rate": purchasers / carters if carters else 0.0,
    })
    return True


def _kafka_raw_funnel_consumer_loop():
    """Feed the Dashboard's in-memory funnel state from the raw topics.

    Supersedes, in order: the consumer of the RW-fed funnel topic (froze
    whenever RW paused -- RW is that topic's only producer) and the
    StarRocks poller (minute/20s grain + Routine Load batching made the
    dashboard visibly laggier than the old RW path). Subscribes to
    page_views/cart_events/purchases, aggregates distinct users per
    minute in memory, and fires the SSE event whenever an aggregate
    changes -- typically sub-second after an event is produced.

    On start it replays the last RAW_FUNNEL_REPLAY_MINUTES from the
    topics by timestamp, rebuilding history without any database.
    """
    global consumer_running

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'dashboard-raw-funnel',
        'auto.offset.reset': 'latest',   # position set explicitly in on_assign
        'enable.auto.commit': False,     # position comes from the timestamp replay, not offsets
    })

    def on_assign(consumer_, partitions):
        replay_ms = int((datetime.now(timezone.utc) - timedelta(
            minutes=RAW_FUNNEL_REPLAY_MINUTES)).timestamp() * 1000)
        resolved = consumer_.offsets_for_times(
            [TopicPartition(p.topic, p.partition, replay_ms) for p in partitions],
            timeout=10.0,
        )
        for p in resolved:
            if p.offset < 0:
                # No messages that old -- start from the earliest retained
                low, _ = consumer_.get_watermark_offsets(
                    TopicPartition(p.topic, p.partition), timeout=10.0, cached=False)
                p.offset = low
        consumer_.assign(resolved)
        logger.info("Raw funnel consumer assigned %s from %d minutes back",
                    [(p.topic, p.partition, p.offset) for p in resolved],
                    RAW_FUNNEL_REPLAY_MINUTES)

    consumer.subscribe(list(RAW_FUNNEL_TOPICS), on_assign=on_assign)

    while consumer_running:
        try:
            msg = consumer.poll(0.5)
        except Exception as e:
            logger.error(f"Raw funnel consumer error: {e}")
            time.sleep(1.0)
            continue

        changed = False
        while msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning(f"Raw funnel consumer: {msg.error()}")
            else:
                changed = _process_raw_event(msg.topic(), msg.value()) or changed
            msg = consumer.poll(0.0)  # drain the rest of the batch, no blocking

        if changed and main_loop:
            main_loop.call_soon_threadsafe(data_updated_event.set)

    consumer.close()



@app.get("/api/funnel")
def get_funnel_data():
    """Return funnel data from Kafka consumer cache, aggregated to per-minute points."""
    try:
        # Step 1: Deduplicate 20-second windows - keep only the latest for each window_start
        # Materialized views emit updates (retractions + new values), so we need to dedupe first
        latest_windows = {}
        for record in funnel_data_history:
            window_start = record.get('window_start')
            if not window_start:
                continue
            
            try:
                # Keep the record with the latest window_end for each window_start
                if window_start not in latest_windows:
                    latest_windows[window_start] = record
                else:
                    existing_end = latest_windows[window_start].get('window_end', '')
                    new_end = record.get('window_end', '')
                    if new_end > existing_end:
                        latest_windows[window_start] = record
            except Exception:
                continue
        
        # Step 2: Aggregate deduplicated 20-second windows to per-minute buckets
        minute_buckets = {}
        for record in latest_windows.values():
            window_start = record.get('window_start')
            if not window_start:
                continue
            
            try:
                # Truncate to minute for aggregation
                if isinstance(window_start, str):
                    minute_key = window_start[:16]  # "YYYY-MM-DDTHH:MM"
                    minute_start = minute_key + ":00+00:00"  # Include UTC timezone
                else:
                    continue
                
                # Sum values for all 20-second windows in the same minute
                if minute_key not in minute_buckets:
                    minute_buckets[minute_key] = {
                        'window_start': minute_start,
                        'window_end': minute_key + ":59+00:00",
                        'viewers': 0,
                        'carters': 0,
                        'purchasers': 0,
                        'view_to_cart_rate': 0.0,
                        'cart_to_buy_rate': 0.0
                    }
                
                # Accumulate the values from deduplicated records only
                bucket = minute_buckets[minute_key]
                bucket['viewers'] += record.get('viewers', 0)
                bucket['carters'] += record.get('carters', 0)
                bucket['purchasers'] += record.get('purchasers', 0)
            except Exception:
                continue
        
        # Step 3: Calculate conversion rates for each minute bucket
        for bucket in minute_buckets.values():
            viewers = bucket['viewers']
            carters = bucket['carters']
            purchasers = bucket['purchasers']
            
            if viewers > 0:
                bucket['view_to_cart_rate'] = round(carters / viewers, 4)
            if carters > 0:
                bucket['cart_to_buy_rate'] = round(purchasers / carters, 4)
        
        # Sort by window_start to ensure chronological order for charts
        sorted_data = sorted(minute_buckets.values(), key=lambda x: x.get('window_start') or '')
        return sorted_data
    except Exception as e:
        logger.error(f"Error getting funnel data: {e}")
        return {
            "error": str(e),
            "data": [],
            "message": "No funnel data available from Kafka. Waiting for data..."
        }


@app.get("/api/stats")
def get_stats():
    """Return latest stats and changes from Kafka consumer cache."""
    try:
        latest = latest_funnel_data
        previous = previous_funnel_data if previous_funnel_data else latest

        def calc_change(curr, prev):
            if prev == 0 or prev is None: 
                return 0
            return ((curr - prev) / prev) * 100

        return {
            "latest": {
                "viewers": latest["viewers"],
                "carters": latest["carters"],
                "purchasers": latest["purchasers"],
                "view_to_cart_rate": latest["view_to_cart_rate"],
                "cart_to_buy_rate": latest["cart_to_buy_rate"]
            },
            "changes": {
                "viewers": calc_change(latest["viewers"], previous["viewers"]),
                "carters": calc_change(latest["carters"], previous["carters"]),
                "purchasers": calc_change(latest["purchasers"], previous["purchasers"]),
                "view_to_cart_rate": calc_change(latest["view_to_cart_rate"], previous["view_to_cart_rate"]),
                "cart_to_buy_rate": calc_change(latest["cart_to_buy_rate"], previous["cart_to_buy_rate"]),
            }
        }
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {
            "latest": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "changes": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "error": str(e),
            "message": "No funnel data available from Kafka. Waiting for data..."
        }


# SSE endpoint for real-time funnel updates
@app.get("/api/funnel/stream")
async def funnel_stream():
    """Server-Sent Events endpoint for real-time funnel data updates.
    
    This endpoint provides a persistent connection that pushes updates
    to the client only when data changes, eliminating the need for polling.
    """
    async def event_generator():
        last_funnel_hash = None
        last_stats_hash = None
        
        while True:
            try:
                # Get current data (using the same logic as the polling endpoints)
                current_funnel = get_funnel_data()
                current_stats = get_stats()
                
                # Create hashes to detect changes
                funnel_json = json.dumps(current_funnel, sort_keys=True) if isinstance(current_funnel, list) else str(current_funnel)
                stats_json = json.dumps(current_stats, sort_keys=True) if isinstance(current_stats, dict) else str(current_stats)
                
                current_funnel_hash = hash(funnel_json)
                current_stats_hash = hash(stats_json)
                
                # Only send if data changed
                if current_funnel_hash != last_funnel_hash or current_stats_hash != last_stats_hash:
                    event_data = {
                        "funnel": current_funnel if isinstance(current_funnel, list) else [],
                        "stats": current_stats if isinstance(current_stats, dict) else {},
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    
                    # SSE format: data: <json>\n\n
                    yield f"data: {json.dumps(event_data)}\n\n"
                    
                    last_funnel_hash = current_funnel_hash
                    last_stats_hash = current_stats_hash
                else:
                    # Send heartbeat comment to keep connection alive
                    yield ":heartbeat\n\n"
                
                # Wait for next update notice or timeout (for heartbeat)
                try:
                    await asyncio.wait_for(data_updated_event.wait(), timeout=15.0)
                    data_updated_event.clear()
                except asyncio.TimeoutError:
                    # Just continue and send heartbeat
                    pass
                
            except Exception as e:
                logger.error(f"Error in SSE stream: {e}")
                error_data = {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}
                yield f"data: {json.dumps(error_data)}\n\n"
                await asyncio.sleep(1)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering if using nginx
        }
    )


# Producer management
import psutil
import os
import signal
import subprocess

# Log file for the producer
PRODUCER_LOG = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "producer.log")


def find_producer_process():
    """Find the external producer process."""
    for proc in psutil.process_iter(['cmdline']):
        try:
            cmdline = proc.info.get('cmdline')
            if cmdline and any("scripts/producer.py" in arg for arg in cmdline):
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


@app.post("/api/producer/start")
def start_producer():
    proc = find_producer_process()
    if not proc:
        # Start the script runner's producer script
        # We run it from the project root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        # Clear old log
        if os.path.exists(PRODUCER_LOG):
            os.remove(PRODUCER_LOG)
            
        subprocess.Popen(["bash", "bin/3_run_producer.sh", "1"], cwd=project_root)
        return {"status": "started"}
    return {"status": "already running"}


@app.post("/api/producer/stop")
def stop_producer():
    proc = find_producer_process()
    if proc:
        # Kill the producer process and its children
        try:
            parent = psutil.Process(proc.pid)
            for child in parent.children(recursive=True):
                child.send_signal(signal.SIGTERM)
            parent.send_signal(signal.SIGTERM)
            return {"status": "stopping"}
        except Exception as e:
            logger.error(f"Error stopping producer: {e}")
            return {"status": "error", "message": str(e)}
    return {"status": "not running"}


@app.get("/api/producer/status")
def get_producer_status():
    proc = find_producer_process()
    is_running = proc is not None
    
    logs = []
    if os.path.exists(PRODUCER_LOG):
        try:
            with open(PRODUCER_LOG, "r") as f:
                # Read last 30 lines
                logs = f.readlines()[-30:]
                logs = [line.strip() for line in logs]
        except Exception as e:
            logger.error(f"Error reading producer log: {e}")
            
    return {
        "running": is_running,
        "output": logs
    }


@app.get("/api/last-event-time")
def get_last_event_time():
    """Return the timestamp of the latest funnel data from Kafka."""
    try:
        latest = latest_funnel_data
        return {"last_event_time": latest.get("window_end")}
    except Exception as e:
        logger.error(f"Error getting last event time: {e}")
        return {"error": str(e), "last_event_time": None}


# =============================================================================
# ML Prediction Endpoints (via ML Serving Service)
# =============================================================================

# Note: ML training is now handled by Dagster, serving by separate service

@app.get("/api/predictions/health")
async def get_predictions_health():
    """Check if ML prediction service is healthy."""
    result = await call_ml_serving("/health")
    if "error" in result:
        return {
            "healthy": False,
            "message": result["error"],
            "engine": "scikit-learn",
            "error": "Service unavailable"
        }
    return {
        "healthy": True,
        "message": "ML serving service is available",
        "engine": "scikit-learn",
        "models_available": result.get("models_loaded", False),
        "metrics": result.get("available_metrics", [])
    }


@app.post("/api/predictions/train")
async def train_predictions_models():
    """
    Trigger model reload from ML serving service.
    
    Note: Actual training is now handled by Dagster.
    This endpoint just reloads models from MinIO.
    """
    try:
        logger.info("Model reload triggered via API")
        result = await call_ml_serving("/reload", method="POST")
        
        if "error" in result:
            return {
                "success": False,
                "message": f"Failed to reload models: {result['error']}",
                "trained_at": datetime.now(timezone.utc).isoformat()
            }
        
        return {
            "success": True,
            "message": f"Reloaded {result.get('models_loaded', 0)} models",
            "models": result.get("available_metrics", []),
            "trained_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error reloading models: {e}")
        return {
            "success": False,
            "error": str(e),
            "trained_at": datetime.now(timezone.utc).isoformat()
        }


@app.get("/api/predictions/next")
async def get_next_predictions():
    """
    Get predictions for the next minute from ML models.

    Returns predicted values for viewers, carters, purchasers,
    and conversion rates for the upcoming minute.

    ML serving reads from funnel_summary (1-minute windows) so values are
    already at minute-level granularity - no scaling needed.
    """
    try:
        result = await call_ml_serving("/predict")

        if "error" in result:
            logger.error(f"ML serving error: {result['error']}")
            return {
                "error": result["error"],
                "predicted_at": datetime.now(timezone.utc).isoformat()
            }

        # Extract model_version and detailed model_type from first metric that has it
        # ML serving now returns per-metric model_type and uses live moving average
        model_version = "unknown"
        detailed_model_type = "unknown"
        is_heuristic = False
        is_online_learning = False
        for metric in ['viewers', 'carters', 'purchasers']:
            metric_data = result.get(metric)
            if metric_data and isinstance(metric_data, dict):
                # Get detailed model type from ML serving response
                detailed_model_type = metric_data.get("model_type", "unknown")
                model_version = metric_data.get("model_version", "unknown")
                # Check if this is a heuristic/live prediction
                if model_version in ['moving_average', 'heuristic', 'live_moving_average', 'unknown', None]:
                    is_heuristic = True
                # Check for online learning models (including fallback when not ready yet)
                if detailed_model_type in ['river_kafka_online', 'river_starrocks_online', 'river_risingwave_online', 'moving_average_fallback'] \
                        or detailed_model_type.startswith('recent_window_'):
                    is_online_learning = True
                break
        
        # Handle model version display
        # If version looks like a timestamp (vYYYYMMDD_HHMMSS), it's from MinIO trained model
        if is_online_learning:
            # Online learning models - show as ML model
            is_heuristic = False
            model_version = "online_learning"
        elif model_version in ['moving_average', 'heuristic', 'live_moving_average', 'unknown', None]:
            is_heuristic = True
            if detailed_model_type == "unknown":
                detailed_model_type = "MovingAverage"
            model_version = "heuristic"
        elif model_version == "rate_proportional":
            # Rate-proportional scaling prediction
            is_heuristic = True
            detailed_model_type = "RateProportional"
        elif model_version == "ema":
            # Exponential Moving Average prediction
            is_heuristic = True
            detailed_model_type = "EMA"
        elif model_version.startswith('v') and len(model_version) >= 8:
            # It's a trained model version from MinIO (e.g., v20260309_125613)
            is_heuristic = True  # MovingAverage is heuristic
            detailed_model_type = "MovingAverage"
        
        # Build response with flat structure for frontend compatibility
        # Include mode and source from ML serving for mode indicator badge
        predictions = {
            "predicted_at": result.get("predicted_at", datetime.now(timezone.utc).isoformat()),
            "timestamp": result.get("timestamp", (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()),
            "model_version": model_version,
            "model_type": detailed_model_type,  # Detailed type: RandomForestRegressor, LinearRegression, MovingAverage
            "is_heuristic": is_heuristic,
            "mode": result.get("mode", "unknown"),  # "online" or "batch"
            "source": result.get("source", "unknown")  # "minio", "risingwave", or "kafka"
        }
        
        # Add predictions for each metric at the root level
        # ML serving now reads from funnel_summary (1-minute windows) - no scaling needed
        # ML serving returns format: {"viewers": {"value": x, "confidence": y, ...}, ...}
        metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
        for metric in metrics:
            metric_data = result.get(metric)
            if metric_data and isinstance(metric_data, dict):
                raw_value = metric_data.get("value")
                predictions[metric] = raw_value
                predictions[f"{metric}_confidence"] = metric_data.get("confidence", 0)
            else:
                predictions[metric] = None
                predictions[f"{metric}_confidence"] = 0
        
        # Add historical context (last actual values for comparison).
        # Compare against the last CLOSED window: latest_funnel_data is the
        # in-progress minute's partial aggregate (e.g. 25 viewers halfway
        # into a minute that ends at 60), which makes every prediction look
        # "way off" against an actual that is still accumulating
        # (2026-11-14).
        last_actual_record = None
        now_utc = datetime.now(timezone.utc)
        for record in funnel_data_history:  # newest first
            window_end = record.get("window_end")
            if isinstance(window_end, str):
                try:
                    if datetime.fromisoformat(window_end) <= now_utc:
                        last_actual_record = record
                        break
                except ValueError:
                    continue
        if last_actual_record is None:
            last_actual_record = latest_funnel_data

        if last_actual_record and last_actual_record.get("window_start"):
            # Use window_start for consistency with aggregated minute data
            # and truncate to minute level to avoid duplicates
            window_start = last_actual_record.get("window_start")
            if isinstance(window_start, str):
                # Ensure timezone
                if not window_start.endswith('Z') and not re.search(r'[+-]\d{2}:\d{2}$', window_start):
                    window_start = window_start + '+00:00'
                # Truncate to minute (remove seconds)
                window_start = window_start[:16] + ':00+00:00'
            predictions["last_actual"] = {
                "timestamp": window_start,
                "viewers": last_actual_record.get("viewers"),
                "carters": last_actual_record.get("carters"),
                "purchasers": last_actual_record.get("purchasers"),
                "view_to_cart_rate": last_actual_record.get("view_to_cart_rate"),
                "cart_to_buy_rate": last_actual_record.get("cart_to_buy_rate")
            }
        
        return predictions
    except Exception as e:
        logger.error(f"Error getting predictions: {e}")
        return {
            "error": str(e),
            "predicted_at": datetime.now(timezone.utc).isoformat()
        }


@app.get("/api/predictions/history")
async def get_predictions_history(limit: int = 20):
    """
    Get historical predictions for analysis and comparison.
    
    Note: History is now maintained in the ML serving service.
    """
    try:
        result = await call_ml_serving(f"/predict?include_details=true")
        
        if "error" in result:
            return {
                "error": result["error"],
                "data": [],
                "count": 0
            }
        
        # Build predictions array from individual metric results
        # ML serving returns: {"viewers": {"value": x, ...}, ...}
        predictions_list = []
        metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
        for metric in metrics:
            metric_data = result.get(metric)
            if metric_data and isinstance(metric_data, dict):
                predictions_list.append({
                    "metric": metric,
                    "predicted_value": metric_data.get("value"),
                    "confidence": metric_data.get("confidence", 0),
                    "model_version": metric_data.get("model_version", "unknown")
                })
        
        return {
            "data": [{
                "timestamp": result.get("timestamp"),
                "model_version": result.get("model_version", "unknown"),
                "predictions": predictions_list
            }],
            "count": 1,
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting prediction history: {e}")
        return {
            "error": str(e),
            "data": [],
            "count": 0
        }


@app.get("/api/predictions/status")
async def get_predictions_status():
    """Get the status of all ML models including version info."""
    try:
        result = await call_ml_serving("/models")
        
        if "error" in result:
            return {
                "error": result["error"],
                "models": {},
                "checked_at": datetime.now(timezone.utc).isoformat()
            }
        
        # Build model status with version info
        # ML serving returns: {"models": {metric: {...}}, "manifest": {...}, "last_reload": ...}
        models_data = result.get("models", {})
        manifest = result.get("manifest", {})
        
        models_status = {}
        current_versions = {}
        available_metrics = []
        
        for metric, model_info in models_data.items():
            available_metrics.append(metric)
            version = model_info.get("version", "unknown")
            current_versions[metric] = version
            models_status[metric] = {
                "trained": model_info.get("loaded", False),
                "version": version,
                "model_type": model_info.get("model_type", "unknown"),
                "loaded_at": model_info.get("loaded_at")
            }
        
        return {
            "engine": "scikit-learn",
            "models": models_status,
            "total_models": len(available_metrics),
            "metrics": available_metrics,
            "current_versions": current_versions,
            "manifest": manifest,
            "checked_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting model status: {e}")
        return {
            "error": str(e),
            "models": {},
            "checked_at": datetime.now(timezone.utc).isoformat()
        }


@app.get("/api/predictions/comparison")
async def get_predictions_comparison():
    """
    Get a comparison of recent predictions vs actual values.
    
    This endpoint is useful for evaluating model accuracy.
    Includes time-adjusted predictions based on elapsed seconds in current minute.
    """
    try:
        from datetime import datetime, timezone
        
        # Get current time info
        now = datetime.now(timezone.utc)
        elapsed_seconds = now.second + now.microsecond / 1_000_000
        progress_percent = elapsed_seconds / 60.0  # 0.0 to 1.0
        
        # Get latest predictions
        next_pred = await get_next_predictions()
        
        # Calculate time-adjusted predictions (prorated for elapsed time)
        time_adjusted = None
        if "error" not in next_pred:
            time_adjusted = {
                "viewers": round(next_pred["viewers"] * progress_percent, 1),
                "carters": round(next_pred["carters"] * progress_percent, 1),
                "purchasers": round(next_pred["purchasers"] * progress_percent, 1),
                "view_to_cart_rate": next_pred["view_to_cart_rate"],
                "cart_to_buy_rate": next_pred["cart_to_buy_rate"],
                "progress_percent": round(progress_percent * 100, 1),
                "elapsed_seconds": round(elapsed_seconds, 1)
            }
        
        # Get recent actual data from history - dedupe then aggregate to minute buckets
        # Step 1: Deduplicate - keep only latest for each 20-second window
        latest_windows = {}
        for record in funnel_data_history[:100]:
            window_start = record.get('window_start')
            if not window_start:
                continue
            
            if window_start not in latest_windows:
                latest_windows[window_start] = record
            else:
                existing_end = latest_windows[window_start].get('window_end', '')
                new_end = record.get('window_end', '')
                if new_end > existing_end:
                    latest_windows[window_start] = record
        
        # Step 2: Aggregate to minute buckets
        minute_buckets = {}
        for record in latest_windows.values():
            window_start = record.get('window_start')
            if not window_start:
                continue
            
            try:
                if isinstance(window_start, str):
                    minute_key = window_start[:16]  # "YYYY-MM-DDTHH:MM"
                    minute_start = minute_key + ":00+00:00"  # Include UTC timezone
                else:
                    continue
                
                if minute_key not in minute_buckets:
                    minute_buckets[minute_key] = {
                        'window_start': minute_start,
                        'viewers': 0,
                        'carters': 0,
                        'purchasers': 0,
                        'view_to_cart_rate': 0.0,
                        'cart_to_buy_rate': 0.0
                    }
                
                minute_buckets[minute_key]['viewers'] += record.get('viewers', 0)
                minute_buckets[minute_key]['carters'] += record.get('carters', 0)
                minute_buckets[minute_key]['purchasers'] += record.get('purchasers', 0)
            except Exception:
                continue
        
        # Calculate conversion rates for minute buckets
        recent_actuals = []
        for bucket in minute_buckets.values():
            viewers = bucket['viewers']
            carters = bucket['carters']
            purchasers = bucket['purchasers']
            
            if viewers > 0:
                bucket['view_to_cart_rate'] = round(carters / viewers, 4)
            if carters > 0:
                bucket['cart_to_buy_rate'] = round(purchasers / carters, 4)
            
            recent_actuals.append(bucket)
        
        # Sort chronologically
        recent_actuals.sort(key=lambda x: x.get('window_start') or '')
        
        return {
            "predictions": next_pred if "error" not in next_pred else None,
            "time_adjusted_predictions": time_adjusted,
            "recent_actuals": recent_actuals[-10:],  # Last 10 actual values
            "comparison_available": "error" not in next_pred and len(recent_actuals) > 0,
            "current_minute_progress": {
                "elapsed_seconds": round(elapsed_seconds, 1),
                "progress_percent": round(progress_percent * 100, 1),
                "remaining_seconds": round(60 - elapsed_seconds, 1)
            },
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting comparison: {e}")
        return {
            "error": str(e),
            "predictions": None,
            "time_adjusted_predictions": None,
            "recent_actuals": [],
            "comparison_available": False,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }


# =============================================================================
# Query Endpoints - SQLAlchemy StarRocks Integration
# =============================================================================

@app.get("/api/query/funnel")
def query_funnel_data(
    start_time: str = Query(..., description="Start time in ISO format"),
    end_time: str = Query(..., description="End time in ISO format")
):
    """
    Query the StarRocks unified hot/cold funnel history.
    
    Returns per-minute records with country breakdown from the pre-aggregated 1-minute windows.
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                SELECT
                    f.window_start,
                    f.window_end,
                    f.country,
                    c.country_name,
                    f.viewers,
                    f.carters,
                    f.purchasers,
                    f.view_to_cart_rate,
                    f.cart_to_buy_rate
                FROM dashboard_funnel_serving f
                LEFT JOIN databricks_uc.sr_poc_external.iceberg_countries c
                  ON f.country = c.country
                WHERE f.window_start >= :start_time
                  AND f.window_end <= :end_time
                ORDER BY f.window_start DESC, f.country
                LIMIT 1000
            """)

            try:
                if not _risingwave_reachable():
                    raise RuntimeError("RisingWave unreachable (pre-check)")
                result = conn.execute(query, {
                    "start_time": start_time,
                    "end_time": end_time
                })
                degraded = False
            except Exception:
                logger.warning("Hot catalog unavailable; serving via the RW-free Routine Load cache")
                # Fall back to the RW-free cached view (frozen
                # mv_unified_funnel_summary + funnel_summary_direct Routine
                # Load, deduped by construction via its cutoff predicate):
                # still hot+cold and fresh -- only the zero-copy RW catalog
                # is out of the loop. Country names come from the local
                # Iceberg-countries cache, so the fallback needs no
                # Databricks access either.
                cached_query = text("""
                    SELECT
                        f.window_start,
                        f.window_end,
                        f.country,
                        c.country_name,
                        f.viewers,
                        f.carters,
                        f.purchasers,
                        f.view_to_cart_rate,
                        f.cart_to_buy_rate
                    FROM dashboard_funnel_serving_cached_direct f
                    LEFT JOIN mv_iceberg_countries_cache c
                      ON f.country = c.country
                    WHERE f.window_start >= :start_time
                      AND f.window_end <= :end_time
                    ORDER BY f.window_start DESC, f.country
                    LIMIT 1000
                """)
                result = conn.execute(cached_query, {
                    "start_time": start_time,
                    "end_time": end_time
                })
                degraded = True

            records = []
            for row in result:
                records.append({
                    "window_start": row.window_start.isoformat() if row.window_start else None,
                    "window_end": row.window_end.isoformat() if row.window_end else None,
                    "country": row.country,
                    "country_name": row.country_name,
                    "viewers": int(row.viewers),
                    "carters": int(row.carters),
                    "purchasers": int(row.purchasers),
                    "view_to_cart_rate": float(row.view_to_cart_rate) if row.view_to_cart_rate else 0.0,
                    "cart_to_buy_rate": float(row.cart_to_buy_rate) if row.cart_to_buy_rate else 0.0
                })

            return {
                "data": records,
                "count": len(records),
                "query": {
                    "start_time": start_time,
                    "end_time": end_time
                },
                "degraded": degraded,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying funnel data: {e}")
        raise _starrocks_http_error(e) from e


@app.get("/api/query/funnel/aggregate")
def query_funnel_aggregate(
    start_time: str = Query(..., description="Start time in ISO format"),
    end_time: str = Query(..., description="End time in ISO format")
):
    """
    Query aggregated funnel data from the StarRocks unified hot/cold history.
    
    Returns sum of viewers, carters, and purchasers for the specified time range
    from the per-minute unified materialized view.
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                SELECT
                    COALESCE(SUM(f.viewers), 0) as total_viewers,
                    COALESCE(SUM(f.carters), 0) as total_carters,
                    COALESCE(SUM(f.purchasers), 0) as total_purchasers,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT f.country) as country_count
                FROM dashboard_funnel_serving f
                WHERE f.window_start >= :start_time
                  AND f.window_end <= :end_time
            """)
            
            try:
                if not _risingwave_reachable():
                    raise RuntimeError("RisingWave unreachable (pre-check)")
                result = conn.execute(query, {
                    "start_time": start_time,
                    "end_time": end_time
                }).one()
                degraded = False
            except Exception:
                logger.warning("Hot catalog unavailable; aggregating via the RW-free Routine Load cache")
                # Fall back to the RW-free cached view (frozen
                # mv_unified_funnel_summary + funnel_summary_direct Routine
                # Load): no per-(window_start, country) dedup needed here --
                # the view is deduped by construction via its cutoff
                # predicate, unlike the raw historical stream.
                cached_query = text("""
                    SELECT
                        COALESCE(SUM(viewers), 0) AS total_viewers,
                        COALESCE(SUM(carters), 0) AS total_carters,
                        COALESCE(SUM(purchasers), 0) AS total_purchasers,
                        COUNT(*) AS record_count,
                        COUNT(DISTINCT country) AS country_count
                    FROM dashboard_funnel_serving_cached_direct
                    WHERE window_start >= :start_time
                      AND window_end <= :end_time
                """)
                result = conn.execute(cached_query, {
                    "start_time": start_time,
                    "end_time": end_time
                }).one()
                degraded = True
            
            return {
                "data": {
                    "total_viewers": int(result.total_viewers),
                    "total_carters": int(result.total_carters),
                    "total_purchasers": int(result.total_purchasers),
                    "record_count": int(result.record_count),
                    "country_count": int(result.country_count)
                },
                "query": {
                    "start_time": start_time,
                    "end_time": end_time
                },
                "degraded": degraded,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying funnel aggregate: {e}")
        raise _starrocks_http_error(e) from e


# --- Cached-architecture endpoints (2026-09-08) ---
# Mirror /api/query/funnel and /api/query/funnel/aggregate exactly, but read
# from dashboard_funnel_serving_cached (backed by mv_unified_funnel_summary,
# REFRESH MANUAL, and mv_iceberg_countries_cache, 20s refresh) instead of
# the zero-copy default. Added for a side-by-side demo comparing both
# architectures this project tried -- same query, same params, run through
# both endpoints to see the real timing difference directly. See
# docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md's twelfth follow-up.
@app.get("/api/query/funnel/cached")
def query_funnel_data_cached(
    start_time: str = Query(..., description="Start time in ISO format"),
    end_time: str = Query(..., description="End time in ISO format")
):
    """
    Same as /api/query/funnel, but reads the cached architecture
    (dashboard_funnel_serving_cached) instead of the zero-copy default.
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                SELECT
                    f.window_start,
                    f.window_end,
                    f.country,
                    c.country_name,
                    f.viewers,
                    f.carters,
                    f.purchasers,
                    f.view_to_cart_rate,
                    f.cart_to_buy_rate
                FROM dashboard_funnel_serving_cached f
                LEFT JOIN mv_iceberg_countries_cache c
                  ON f.country = c.country
                WHERE f.window_start >= :start_time
                  AND f.window_end <= :end_time
                ORDER BY f.window_start DESC, f.country
                LIMIT 1000
            """)

            try:
                if not _risingwave_reachable():
                    raise RuntimeError("RisingWave unreachable (pre-check)")
                result = conn.execute(query, {
                    "start_time": start_time,
                    "end_time": end_time
                })
                degraded = False
            except Exception:
                logger.warning("Cached view unavailable; serving via the RW-free Routine Load cache")
                # dashboard_funnel_serving_cached itself reads the RW hot
                # branch through its cutoff (SELECT MIN ... FROM
                # risingwave.public.funnel_summary), so when RW is down even
                # this "cached" primary fails. Fall back to the fully
                # RW-free view: frozen mv_unified_funnel_summary +
                # funnel_summary_direct Routine Load -- still hot+cold.
                cached_query = text("""
                    SELECT
                        f.window_start,
                        f.window_end,
                        f.country,
                        c.country_name,
                        f.viewers,
                        f.carters,
                        f.purchasers,
                        f.view_to_cart_rate,
                        f.cart_to_buy_rate
                    FROM dashboard_funnel_serving_cached_direct f
                    LEFT JOIN mv_iceberg_countries_cache c
                      ON f.country = c.country
                    WHERE f.window_start >= :start_time
                      AND f.window_end <= :end_time
                    ORDER BY f.window_start DESC, f.country
                    LIMIT 1000
                """)
                result = conn.execute(cached_query, {
                    "start_time": start_time,
                    "end_time": end_time
                })
                degraded = True

            records = []
            for row in result:
                records.append({
                    "window_start": row.window_start.isoformat() if row.window_start else None,
                    "window_end": row.window_end.isoformat() if row.window_end else None,
                    "country": row.country,
                    "country_name": row.country_name,
                    "viewers": int(row.viewers),
                    "carters": int(row.carters),
                    "purchasers": int(row.purchasers),
                    "view_to_cart_rate": float(row.view_to_cart_rate) if row.view_to_cart_rate else 0.0,
                    "cart_to_buy_rate": float(row.cart_to_buy_rate) if row.cart_to_buy_rate else 0.0
                })

            return {
                "data": records,
                "count": len(records),
                "query": {
                    "start_time": start_time,
                    "end_time": end_time
                },
                "degraded": degraded,
                "architecture": "cached",
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying cached funnel data: {e}")
        raise _starrocks_http_error(e) from e


@app.get("/api/query/funnel/cached/aggregate")
def query_funnel_aggregate_cached(
    start_time: str = Query(..., description="Start time in ISO format"),
    end_time: str = Query(..., description="End time in ISO format")
):
    """
    Same as /api/query/funnel/aggregate, but reads the cached architecture
    (dashboard_funnel_serving_cached) instead of the zero-copy default.
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                SELECT
                    COALESCE(SUM(f.viewers), 0) as total_viewers,
                    COALESCE(SUM(f.carters), 0) as total_carters,
                    COALESCE(SUM(f.purchasers), 0) as total_purchasers,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT f.country) as country_count
                FROM dashboard_funnel_serving_cached f
                WHERE f.window_start >= :start_time
                  AND f.window_end <= :end_time
            """)

            try:
                if not _risingwave_reachable():
                    raise RuntimeError("RisingWave unreachable (pre-check)")
                result = conn.execute(query, {
                    "start_time": start_time,
                    "end_time": end_time
                }).one()
                degraded = False
            except Exception:
                logger.warning("Cached view unavailable; aggregating via the RW-free Routine Load cache")
                # Same as the row fallback: the RW-free view (frozen MV +
                # Routine Load direct branch) instead of the frozen MV alone.
                cached_query = text("""
                    SELECT
                        COALESCE(SUM(viewers), 0) AS total_viewers,
                        COALESCE(SUM(carters), 0) AS total_carters,
                        COALESCE(SUM(purchasers), 0) AS total_purchasers,
                        COUNT(*) AS record_count,
                        COUNT(DISTINCT country) AS country_count
                    FROM dashboard_funnel_serving_cached_direct
                    WHERE window_start >= :start_time
                      AND window_end <= :end_time
                """)
                result = conn.execute(cached_query, {
                    "start_time": start_time,
                    "end_time": end_time
                }).one()
                degraded = True

            return {
                "data": {
                    "total_viewers": int(result.total_viewers),
                    "total_carters": int(result.total_carters),
                    "total_purchasers": int(result.total_purchasers),
                    "record_count": int(result.record_count),
                    "country_count": int(result.country_count)
                },
                "query": {
                    "start_time": start_time,
                    "end_time": end_time
                },
                "degraded": degraded,
                "architecture": "cached",
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying cached funnel aggregate: {e}")
        raise _starrocks_http_error(e) from e


@app.get("/api/funnel/enriched")
def get_enriched_funnel_data(
    limit: int = Query(100, description="Number of records to return")
):
    """
    Query the StarRocks unified hot/cold view with SQL-derived metrics.
    
    Returns funnel data with Python UDF categorizations:
    - view_to_cart_category: conversion_category(view_to_cart_rate)
    - cart_to_buy_category: conversion_category(cart_to_buy_rate)
    - funnel_score: calculate_funnel_score(viewers, carters, purchasers)
    - view_to_cart_emoji: format_rate_with_emoji(view_to_cart_rate)
    - cart_to_buy_emoji: format_rate_with_emoji(cart_to_buy_rate)
    - funnel_health: overall funnel health indicator
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                SELECT
                    window_start,
                    window_end,
                    viewers,
                    carters,
                    purchasers,
                    view_to_cart_rate,
                    cart_to_buy_rate,
                    view_to_cart_category,
                    cart_to_buy_category,
                    funnel_score,
                    view_to_cart_emoji,
                    cart_to_buy_emoji,
                    funnel_health
                FROM dashboard_funnel_enriched
                ORDER BY window_start DESC
                LIMIT :limit
            """)
            
            if not _risingwave_reachable():
                logger.warning("Hot catalog unavailable; serving via the RW-free Routine Load cache")
                records = _cached_enriched_records(conn, limit)
                return {
                    "data": records,
                    "count": len(records),
                    "degraded": True,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            result = conn.execute(query, {"limit": limit})
            
            records = []
            for row in result:
                records.append({
                    "window_start": row.window_start.isoformat() if row.window_start else None,
                    "window_end": row.window_end.isoformat() if row.window_end else None,
                    "viewers": int(row.viewers) if row.viewers else 0,
                    "carters": int(row.carters) if row.carters else 0,
                    "purchasers": int(row.purchasers) if row.purchasers else 0,
                    "view_to_cart_rate": float(row.view_to_cart_rate) if row.view_to_cart_rate else 0.0,
                    "cart_to_buy_rate": float(row.cart_to_buy_rate) if row.cart_to_buy_rate else 0.0,
                    "view_to_cart_category": row.view_to_cart_category,
                    "cart_to_buy_category": row.cart_to_buy_category,
                    "funnel_score": float(row.funnel_score) if row.funnel_score else 0.0,
                    "view_to_cart_emoji": row.view_to_cart_emoji,
                    "cart_to_buy_emoji": row.cart_to_buy_emoji,
                    "funnel_health": row.funnel_health
                })
            
            return {
                "data": records,
                "count": len(records),
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying enriched funnel data: {e}")
        try:
            with starrocks_engine.connect() as conn:
                records = _cached_enriched_records(conn, limit)
            return {
                "data": records,
                "count": len(records),
                "degraded": True,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as fallback_error:
            raise _starrocks_http_error(fallback_error) from fallback_error


@app.get("/api/funnel/health")
def get_funnel_health():
    """
    Get current funnel health summary from StarRocks-derived metrics.
    """
    try:
        with starrocks_engine.connect() as conn:
            query = text("""
                WITH latest_5_minutes AS (
                    SELECT funnel_health, funnel_score, view_to_cart_rate, cart_to_buy_rate
                    FROM dashboard_funnel_enriched
                    WHERE window_start >= NOW() - INTERVAL 15 MINUTE
                    ORDER BY window_start DESC
                    LIMIT 5
                )
                SELECT
                    funnel_health,
                    COUNT(*) AS count,
                    AVG(funnel_score) AS avg_score,
                    AVG(view_to_cart_rate) AS avg_view_to_cart,
                    AVG(cart_to_buy_rate) AS avg_cart_to_buy
                FROM latest_5_minutes
                GROUP BY funnel_health
            """)
            
            if not _risingwave_reachable():
                logger.warning("Hot catalog unavailable; serving via the RW-free Routine Load cache")
                fallback_rows = _cached_enriched_records(conn, 5)
                health_summary = {}
                for row in fallback_rows:
                    health = row["funnel_health"]
                    summary = health_summary.setdefault(
                        health,
                        {
                            "count": 0,
                            "avg_score": 0.0,
                            "avg_view_to_cart_rate": 0.0,
                            "avg_cart_to_buy_rate": 0.0,
                        },
                    )
                    summary["count"] += 1
                    summary["avg_score"] += row["funnel_score"]
                    summary["avg_view_to_cart_rate"] += row["view_to_cart_rate"]
                    summary["avg_cart_to_buy_rate"] += row["cart_to_buy_rate"]
                for summary in health_summary.values():
                    count = summary["count"]
                    summary["avg_score"] /= count
                    summary["avg_view_to_cart_rate"] /= count
                    summary["avg_cart_to_buy_rate"] /= count
                return {
                    "health_summary": health_summary,
                    "degraded": True,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
            result = conn.execute(query)

            health_summary = {}
            for row in result:
                health_summary[row.funnel_health] = {
                    "count": int(row.count),
                    "avg_score": float(row.avg_score) if row.avg_score else 0.0,
                    "avg_view_to_cart_rate": float(row.avg_view_to_cart) if row.avg_view_to_cart else 0.0,
                    "avg_cart_to_buy_rate": float(row.avg_cart_to_buy) if row.avg_cart_to_buy else 0.0
                }
            
            return {
                "health_summary": health_summary,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error querying funnel health: {e}")
        try:
            with starrocks_engine.connect() as conn:
                fallback_rows = _cached_enriched_records(conn, 5)
            health_summary = {}
            for row in fallback_rows:
                health = row["funnel_health"]
                summary = health_summary.setdefault(
                    health,
                    {
                        "count": 0,
                        "avg_score": 0.0,
                        "avg_view_to_cart_rate": 0.0,
                        "avg_cart_to_buy_rate": 0.0,
                    },
                )
                summary["count"] += 1
                summary["avg_score"] += row["funnel_score"]
                summary["avg_view_to_cart_rate"] += row["view_to_cart_rate"]
                summary["avg_cart_to_buy_rate"] += row["cart_to_buy_rate"]
            for summary in health_summary.values():
                count = summary["count"]
                summary["avg_score"] /= count
                summary["avg_view_to_cart_rate"] /= count
                summary["avg_cart_to_buy_rate"] /= count
            return {
                "health_summary": health_summary,
                "degraded": True,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as fallback_error:
            raise _starrocks_http_error(fallback_error) from fallback_error


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
