from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from confluent_kafka import Consumer, KafkaException
from datetime import datetime, timedelta, timezone
import json
import threading
import time
import uvicorn
import os
import re
import asyncio
import logging
import httpx

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
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
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

# Kafka configuration - use localhost when running outside Docker
import os
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
FUNNEL_TOPIC = 'funnel'
logger.info(f"[Kafka] Using bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

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


def kafka_consumer_loop():
    """Background thread that consumes funnel data from Kafka."""
    global latest_funnel_data, previous_funnel_data, funnel_data_history, consumer_running
    
    consumer = None
    while consumer_running:
        try:
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'dashboard-consumer',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }
            consumer = Consumer(conf)
            consumer.subscribe([FUNNEL_TOPIC])
            
            while consumer_running:
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Deserialize message
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    # Parse timestamps
                    if 'window_start' in data:
                        data['window_start'] = parse_timestamp(data['window_start'])
                    if 'window_end' in data:
                        data['window_end'] = parse_timestamp(data['window_end'])
                    
                    # Store previous data before updating
                    previous_funnel_data = latest_funnel_data.copy()
                    
                    # Update latest data
                    latest_funnel_data.update({
                        "window_start": data.get('window_start'),
                        "window_end": data.get('window_end'),
                        "viewers": int(data.get('viewers', 0)),
                        "carters": int(data.get('carters', 0)),
                        "purchasers": int(data.get('purchasers', 0)),
                        "view_to_cart_rate": float(data.get('view_to_cart_rate', 0.0)),
                        "cart_to_buy_rate": float(data.get('cart_to_buy_rate', 0.0))
                    })
                    
                    # Add to history
                    funnel_data_history.insert(0, latest_funnel_data.copy())
                    if len(funnel_data_history) > MAX_HISTORY_SIZE:
                        funnel_data_history.pop()
                    
                    # Manually commit offset
                    consumer.commit(msg)
                    
                    # Notify SSE clients that new data is available
                    if main_loop:
                        main_loop.call_soon_threadsafe(data_updated_event.set)
                            
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
            time.sleep(5)  # Wait before reconnecting
        finally:
            if consumer:
                try:
                    consumer.close()
                except:
                    pass


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
    
    Note: ML models predict for 20-second windows. We scale up by 3x
    to provide minute-level predictions that match the dashboard aggregation.
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
                if detailed_model_type in ['river_kafka_online', 'river_risingwave_online', 'moving_average_fallback']:
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
        predictions = {
            "predicted_at": result.get("predicted_at", datetime.now(timezone.utc).isoformat()),
            "timestamp": result.get("timestamp", (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()),
            "model_version": model_version,
            "model_type": detailed_model_type,  # Detailed type: RandomForestRegressor, LinearRegression, MovingAverage
            "is_heuristic": is_heuristic
        }
        
        # Add predictions for each metric at the root level
        # ML models predict for 20-second windows, scale up by 3x for minute-level display
        # ML serving returns format: {"viewers": {"value": x, "confidence": y, ...}, ...}
        WINDOW_SCALE = 3  # 3 x 20-second windows = 1 minute
        metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
        for metric in metrics:
            metric_data = result.get(metric)
            if metric_data and isinstance(metric_data, dict):
                raw_value = metric_data.get("value")
                # Scale up absolute counts (viewers, carters, purchasers) but not rates
                if raw_value is not None and metric in ['viewers', 'carters', 'purchasers']:
                    predictions[metric] = raw_value * WINDOW_SCALE
                else:
                    predictions[metric] = raw_value
                predictions[f"{metric}_confidence"] = metric_data.get("confidence", 0)
            else:
                predictions[metric] = None
                predictions[f"{metric}_confidence"] = 0
        
        # Add historical context (last actual values for comparison)
        if latest_funnel_data and latest_funnel_data.get("window_start"):
            # Use window_start for consistency with aggregated minute data
            # and truncate to minute level to avoid duplicates
            window_start = latest_funnel_data.get("window_start")
            if isinstance(window_start, str):
                # Ensure timezone
                if not window_start.endswith('Z') and not re.search(r'[+-]\d{2}:\d{2}$', window_start):
                    window_start = window_start + '+00:00'
                # Truncate to minute (remove seconds)
                window_start = window_start[:16] + ':00+00:00'
            predictions["last_actual"] = {
                "timestamp": window_start,
                "viewers": latest_funnel_data.get("viewers"),
                "carters": latest_funnel_data.get("carters"),
                "purchasers": latest_funnel_data.get("purchasers"),
                "view_to_cart_rate": latest_funnel_data.get("view_to_cart_rate"),
                "cart_to_buy_rate": latest_funnel_data.get("cart_to_buy_rate")
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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
