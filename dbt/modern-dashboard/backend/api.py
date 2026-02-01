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

import threading
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Global producer state
producer_running = False
producer_output = []

def run_producer():
    global producer_running, producer_output
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:19092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        
        producer_output.append(f"[{datetime.now().strftime('%H:%M:%S')}] Producer started successfully")
        
        while producer_running:
            user_id = random.randint(1, 100)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 1. Page Views
            view_data = {
                "user_id": user_id,
                "page_id": f"page_{random.randint(1, 20)}",
                "event_time": current_time,
            }
            producer.send("page_views", value=view_data)

            # 2. Add to Cart
            if random.random() < 0.3:
                cart_data = {
                    "user_id": user_id,
                    "item_id": f"item_{random.randint(100, 200)}",
                    "event_time": current_time,
                }
                producer.send("cart_events", value=cart_data)

            # 3. Purchases
            if random.random() < 0.1:
                purchase_data = {
                    "user_id": user_id,
                    "amount": round(random.uniform(10, 500), 2),
                    "event_time": current_time,
                }
                producer.send("purchases", value=purchase_data)

            msg = f"Sent events for User {user_id} at {current_time}"
            producer_output.append(msg)
            # Keep only last 100 messages
            if len(producer_output) > 100:
                producer_output.pop(0)
            
            time.sleep(1)
            
        producer.close()
        producer_output.append(f"[{datetime.now().strftime('%H:%M:%S')}] Producer stopped")
    except Exception as e:
        producer_output.append(f"[ERROR] {str(e)}")
        producer_running = False

@app.post("/api/producer/start")
def start_producer():
    global producer_running
    if not producer_running:
        producer_running = True
        thread = threading.Thread(target=run_producer)
        thread.daemon = True
        thread.start()
        return {"status": "started"}
    return {"status": "already running"}

@app.post("/api/producer/stop")
def stop_producer():
    global producer_running
    producer_running = False
    return {"status": "stopping"}

@app.get("/api/producer/status")
def get_producer_status():
    return {
        "running": producer_running,
        "output": producer_output[-30:] # Last 30 lines
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
