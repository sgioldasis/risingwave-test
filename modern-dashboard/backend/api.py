from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import uvicorn
import json

app = FastAPI()

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DATABASE_URL = "postgresql://root:root@localhost:4566/dev"
engine = create_engine(DATABASE_URL)

@app.get("/api/funnel")
def get_funnel_data():
    try:
        query = """
            SELECT
                window_start,
                viewers,
                carters,
                purchasers,
                view_to_cart_rate,
                cart_to_buy_rate
            FROM public.funnel
            ORDER BY window_start DESC
            LIMIT 100
        """
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        
        # Convert to JSON-friendly format
        return df.to_dict(orient="records")
    except Exception as e:
        # Return empty data with a flag indicating no data available
        return {
            "error": str(e),
            "data": [],
            "message": "No funnel data available. Please run dbt to create the funnel materialized view."
        }

@app.get("/api/stats")
def get_stats():
    try:
        query = """
            SELECT
                viewers,
                carters,
                purchasers,
                view_to_cart_rate,
                cart_to_buy_rate
            FROM public.funnel
            ORDER BY window_start DESC
            LIMIT 2
        """
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        
        if len(df) < 1:
            return {
                "latest": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
                "changes": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0}
            }

        latest = df.iloc[0].to_dict()
        previous = df.iloc[1].to_dict() if len(df) > 1 else latest

        def calc_change(curr, prev):
            if prev == 0: return 0
            return ((curr - prev) / prev) * 100

        return {
            "latest": latest,
            "changes": {
                "viewers": calc_change(latest["viewers"], previous["viewers"]),
                "carters": calc_change(latest["carters"], previous["carters"]),
                "purchasers": calc_change(latest["purchasers"], previous["purchasers"]),
                "view_to_cart_rate": calc_change(latest["view_to_cart_rate"], previous["view_to_cart_rate"]),
                "cart_to_buy_rate": calc_change(latest["cart_to_buy_rate"], previous["cart_to_buy_rate"]),
            }
        }
    except Exception as e:
        # Return default values when table doesn't exist
        return {
            "latest": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "changes": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "error": str(e),
            "message": "No funnel data available. Please run dbt to create the funnel materialized view."
        }

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
        except Exception:
            pass
            
    return {
        "running": is_running,
        "output": logs
    }

@app.get("/api/last-event-time")
def get_last_event_time():
    try:
        # Query the maximum timestamp from all raw event tables
        query = """
            SELECT MAX(ts) as last_timestamp FROM (
                SELECT event_time as ts FROM src_page
                UNION ALL
                SELECT event_time as ts FROM src_cart
                UNION ALL
                SELECT event_time as ts FROM src_purchase
            ) all_events
        """
        with engine.connect() as connection:
            result = connection.execute(text(query))
            row = result.fetchone()
            last_timestamp = row[0] if row and row[0] else None
        
        return {"last_event_time": last_timestamp.isoformat() if last_timestamp else None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/clickstream/events-by-time")
def get_clickstream_events_by_time():
    """Get clickstream event counts grouped by minute for the last 30 minutes."""
    try:
        # Use substring to extract timestamp part before Z and cast directly
        query = """
            SELECT
                DATE_TRUNC('minute', substring(event_time from 1 for 19)::timestamp) as time_bucket,
                event_type,
                COUNT(*) as count
            FROM clickstream_events
            WHERE substring(event_time from 1 for 19)::timestamp >= NOW() - INTERVAL '60 minutes'
              AND substring(event_time from 1 for 19)::timestamp <= NOW() + INTERVAL '5 minutes'
            GROUP BY time_bucket, event_type
            ORDER BY time_bucket DESC, event_type
            LIMIT 100
        """
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        
        # Pivot the data to have time_bucket as rows and event_types as columns
        if not df.empty:
            pivot_df = df.pivot(index='time_bucket', columns='event_type', values='count').fillna(0)
            pivot_df = pivot_df.reset_index()
            # Convert to list of dicts for JSON
            return pivot_df.to_dict(orient='records')
        return []
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/api/clickstream/recent-sessions")
def get_recent_sessions():
    """Get recent sessions with revenue statistics (most recent per city)."""
    try:
        # Get all recent sessions
        query = """
            SELECT
                s.session_id,
                s.geo_city,
                s.geo_region,
                s.session_start,
                COUNT(e.event_id) as event_count,
                COALESCE(SUM(e.revenue_usd), 0) as total_revenue
            FROM sessions s
            LEFT JOIN clickstream_events e ON s.session_id = e.session_id
            WHERE substring(s.session_start from 1 for 19)::timestamp >= NOW() - INTERVAL '30 minutes'
            GROUP BY s.session_id, s.geo_city, s.geo_region, s.session_start
            ORDER BY s.session_start DESC
            LIMIT 100
        """
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
        
        # Keep only most recent session per city with revenue > 0
        seen_cities = set()
        unique_sessions = []
        for _, row in df.iterrows():
            city = row['geo_city']
            revenue = row['total_revenue']
            # Skip cities with 0 revenue
            if revenue <= 0:
                continue
            if city not in seen_cities:
                seen_cities.add(city)
                unique_sessions.append(row.to_dict())
            if len(unique_sessions) >= 20:
                break
        
        return unique_sessions
    except Exception as e:
        return {"error": str(e), "data": []}


@app.get("/api/clickstream/event-summary")
def get_clickstream_event_summary():
    """Get summary stats for clickstream events in the last minute."""
    try:
        query = """
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
                COUNT(*) FILTER (WHERE event_type = 'click') as clicks,
                COUNT(*) FILTER (WHERE event_type = 'add_to_cart') as add_to_carts,
                COUNT(*) FILTER (WHERE event_type = 'checkout_start') as checkouts,
                COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
                COALESCE(SUM(revenue_usd) FILTER (WHERE event_type = 'purchase'), 0) as revenue
            FROM clickstream_events
            WHERE substring(event_time from 1 for 19)::timestamp >= NOW() - INTERVAL '5 minutes'
        """
        with engine.connect() as connection:
            result = connection.execute(text(query))
            row = result.fetchone()
            
        return {
            "page_views": row[0] or 0,
            "clicks": row[1] or 0,
            "add_to_carts": row[2] or 0,
            "checkouts": row[3] or 0,
            "purchases": row[4] or 0,
            "revenue": float(row[5]) if row[5] else 0
        }
    except Exception as e:
        return {"error": str(e), "page_views": 0, "clicks": 0, "add_to_carts": 0, "checkouts": 0, "purchases": 0, "revenue": 0}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
