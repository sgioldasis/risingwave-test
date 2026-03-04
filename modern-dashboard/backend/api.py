from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta, timezone
import json
import threading
import time
import uvicorn
import asyncio
import logging

logger = logging.getLogger(__name__)

# Global ML predictor instance
_ml_predictor = None
_ml_training_task = None

def _get_ml_predictor():
    """Get or create the ML predictor instance."""
    global _ml_predictor
    if _ml_predictor is None:
        from services.ml_predictor import FunnelMLPredictor
        _ml_predictor = FunnelMLPredictor()
    return _ml_predictor


async def auto_train_loop():
    """Background task to auto-train models every 30 seconds."""
    while True:
        try:
            await asyncio.sleep(30)  # Train every 30 seconds for faster startup
            predictor = _get_ml_predictor()
            # Only train if models aren't already trained
            status = predictor.get_model_status()
            if not status.get('is_trained', False):
                logger.info("Auto-training: models not trained, attempting training...")
                success = predictor.train_models()
                if success:
                    logger.info("Auto-training completed successfully")
                else:
                    logger.debug("Auto-training skipped - insufficient data")
            else:
                # Still train periodically to update models with new data
                logger.debug("Models already trained, retraining with latest data...")
                predictor.train_models()
        except Exception as e:
            logger.error(f"Auto-training error: {e}")


def start_auto_training():
    """Start the background auto-training task."""
    global _ml_training_task
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            _ml_training_task = asyncio.create_task(auto_train_loop())
            logger.info("Auto-training loop started")
    except Exception as e:
        logger.error(f"Failed to start auto-training: {e}")


# Lifespan context manager for startup/shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    global kafka_consumer_thread, consumer_running
    # Startup
    consumer_running = True
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    kafka_consumer_thread.start()
    
    # Start auto-training loop for live demo
    start_auto_training()
    
    # Do an initial training attempt after a short delay to allow data to be available
    async def initial_training():
        await asyncio.sleep(5)  # Wait 5 seconds for startup
        try:
            logger.info("Attempting initial model training...")
            predictor = _get_ml_predictor()
            success = predictor.train_models()
            if success:
                logger.info("Initial training completed successfully")
            else:
                logger.warning("Initial training failed - will retry in auto-train loop")
        except Exception as e:
            logger.error(f"Initial training error: {e}")
    
    asyncio.create_task(initial_training())
    
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
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092').split(',')
FUNNEL_TOPIC = 'funnel'
print(f"[Kafka] Using bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

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
    """Parse timestamp (Unix ms, string, or datetime) to ISO format in UTC."""
    if ts is None:
        return None
    from datetime import datetime, timezone
    # Handle Unix timestamp in milliseconds (from Kafka JSON)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    # Handle string
    if isinstance(ts, str):
        # Check if it's already a number string
        if ts.isdigit():
            return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
        return ts
    # Handle datetime object
    if isinstance(ts, datetime):
        return ts.isoformat()
    return str(ts)


def kafka_consumer_loop():
    """Background thread that consumes funnel data from Kafka."""
    global latest_funnel_data, previous_funnel_data, funnel_data_history, consumer_running
    
    consumer = None
    while consumer_running:
        try:
            consumer = KafkaConsumer(
                FUNNEL_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )
            
            while consumer_running:
                try:
                    for message in consumer:
                        if not consumer_running:
                            break
                        
                        data = message.value
                        
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
                            
                except Exception as e:
                    print(f"Error processing Kafka message: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            print(f"Kafka connection error: {e}")
            time.sleep(5)  # Wait before reconnecting
        finally:
            if consumer:
                try:
                    consumer.close()
                except:
                    pass


@app.get("/api/funnel")
def get_funnel_data():
    """Return funnel data from Kafka consumer cache."""
    try:
        # Deduplicate by window_start, keeping only the latest for each minute
        seen_windows = {}
        for record in funnel_data_history:
            window_start = record.get('window_start')
            if window_start:
                # Keep the record with the latest window_end for each window_start
                if window_start not in seen_windows:
                    seen_windows[window_start] = record
                else:
                    # Keep the one with later window_end (more recent update)
                    existing_end = seen_windows[window_start].get('window_end', '')
                    new_end = record.get('window_end', '')
                    if new_end > existing_end:
                        seen_windows[window_start] = record
        
        # Sort by window_start to ensure chronological order for charts
        sorted_data = sorted(seen_windows.values(), key=lambda x: x.get('window_start') or '')
        return sorted_data
    except Exception as e:
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
        return {
            "latest": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "changes": {"viewers": 0, "carters": 0, "purchasers": 0, "view_to_cart_rate": 0, "cart_to_buy_rate": 0},
            "error": str(e),
            "message": "No funnel data available from Kafka. Waiting for data..."
        }


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
    """Return the timestamp of the latest funnel data from Kafka."""
    try:
        latest = latest_funnel_data
        return {"last_event_time": latest.get("window_end")}
    except Exception as e:
        return {"error": str(e), "last_event_time": None}


# =============================================================================
# ML Prediction Endpoints (scikit-learn based)
# =============================================================================

# Note: ML predictor instance and auto-training functions are defined at top of file

@app.get("/api/predictions/health")
def get_predictions_health():
    """Check if ML prediction service is healthy."""
    try:
        predictor = _get_ml_predictor()
        return {
            "healthy": True,
            "message": "ML prediction service is available",
            "engine": "scikit-learn",
            "models_trained": len(predictor.models) > 0,
            "metrics": predictor.metrics
        }
    except Exception as e:
        logger.error(f"ML health check failed: {e}")
        return {
            "healthy": False,
            "message": str(e),
            "engine": "scikit-learn",
            "error": "Service unavailable"
        }


@app.post("/api/predictions/train")
def train_predictions_models():
    """
    Train ML models on the funnel data.
    
    This endpoint trains RandomForest/LinearRegression models
    for each metric (viewers, carters, purchasers, etc.)
    """
    try:
        logger.info("Manual training triggered via API")
        predictor = _get_ml_predictor()
        success = predictor.train_models()
        
        if success:
            return {
                "success": True,
                "message": f"Trained {len(predictor.models)} models successfully",
                "models": list(predictor.models.keys()),
                "trained_at": datetime.now(timezone.utc).isoformat()
            }
        else:
            # Get status for debugging
            status = predictor.get_model_status()
            return {
                "success": False,
                "message": "Training failed - insufficient data or error occurred",
                "debug_info": status,
                "trained_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        logger.error(f"Error training models: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e),
            "trained_at": datetime.now(timezone.utc).isoformat()
        }


@app.get("/api/predictions/next")
def get_next_predictions():
    """
    Get predictions for the next minute from ML models.
    
    Returns predicted values for viewers, carters, purchasers,
    and conversion rates for the upcoming minute.
    """
    try:
        predictor = _get_ml_predictor()
        
        # Ensure models are trained
        if not predictor.models:
            predictor.train_models()
        
        # Build response with flat structure for frontend compatibility
        predictions = {
            "predicted_at": datetime.now(timezone.utc).isoformat(),
            "timestamp": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        }
        
        # Add predictions for each metric at the root level
        for metric in predictor.metrics:
            result = predictor.predict_next(metric)
            if result:
                predictions[metric] = result.predicted_value
                predictions[f"{metric}_confidence"] = result.confidence
            else:
                predictions[metric] = None
                predictions[f"{metric}_confidence"] = 0
        
        # Add historical context (last actual values for comparison)
        if latest_funnel_data and latest_funnel_data.get("window_end"):
            predictions["last_actual"] = {
                "timestamp": latest_funnel_data.get("window_end"),
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
def get_predictions_history(limit: int = 20):
    """
    Get historical predictions for analysis and comparison.
    
    Args:
        limit: Maximum number of historical predictions to return
    """
    try:
        predictor = _get_ml_predictor()
        history = predictor.get_prediction_history(limit=limit)
        return {
            "data": history,
            "count": len(history),
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
def get_predictions_status():
    """Get the status of all ML models."""
    try:
        predictor = _get_ml_predictor()
        return {
            "engine": "scikit-learn",
            "models": {
                metric: {
                    "trained": metric in predictor.models,
                    "scaler_trained": metric in predictor.scalers
                }
                for metric in predictor.metrics
            },
            "total_models": len(predictor.models),
            "metrics": predictor.metrics,
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
def get_predictions_comparison():
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
        next_pred = get_next_predictions()
        
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
        
        # Get recent actual data from history
        recent_actuals = []
        seen_windows = {}
        for record in funnel_data_history[:50]:  # Last 50 records
            window_start = record.get('window_start')
            if window_start and window_start not in seen_windows:
                seen_windows[window_start] = record
                recent_actuals.append(record)
        
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
