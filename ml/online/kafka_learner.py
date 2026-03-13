"""Online learner with Kafka streaming and lag feature engineering."""

import logging
import os
import time
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable

from .models import RiverModelManager
from .kafka_streamer import KafkaDataStreamer
from .checkpoints import CheckpointManager

logger = logging.getLogger(__name__)


class KafkaOnlineLearner:
    """
    Continuous learning service using Kafka as the data source.
    
    This learner consumes from the funnel Kafka topic and maintains
    a rolling window of history to compute lag features (like RisingWave does).
    """
    
    # Keep last N records for computing lag features
    HISTORY_WINDOW_SIZE = 20
    
    def __init__(
        self,
        checkpoint_interval: float = 60.0,
        auto_start: bool = False
    ):
        """
        Initialize the Kafka-based online learner.
        
        Args:
            checkpoint_interval: Seconds between checkpoints
            auto_start: Whether to start immediately
        """
        self.checkpoint_interval = checkpoint_interval
        
        # Initialize components
        self.models = RiverModelManager()
        self.streamer = KafkaDataStreamer(auto_start=False)
        self.checkpointer = CheckpointManager()
        
        # History for computing lag features
        self._history: deque[Dict[str, Any]] = deque(maxlen=self.HISTORY_WINDOW_SIZE)
        
        # State
        self._running = False
        self._checkpoint_thread: Optional[threading.Thread] = None
        self._last_checkpoint = time.time()
        self._records_learned = 0
        self._start_time: Optional[datetime] = None
        
        # Callbacks for prediction integration
        self._prediction_callbacks: list[Callable[[str, float], None]] = []
        
        # Register callback for new records
        self.streamer.add_callback(self._on_new_record)
        
        if auto_start:
            self.start()
    
    def _compute_lag_features(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute lag features from history.
        
        This mirrors what RisingWave's funnel_training view does:
        - lag_1: 1 window ago (20 seconds)
        - lag_2: 4 windows ago (80 seconds ~ 1 minute)
        - lag_3: 7 windows ago (140 seconds ~ 2.5 minutes)
        """
        # Start with base record
        enriched = record.copy()
        
        # Get historical values (history is ordered oldest to newest)
        history_list = list(self._history)
        n = len(history_list)
        
        # Lag features (with 20-second windows, these approximate 1-minute intervals)
        if n >= 1:
            enriched['viewers_lag_1'] = history_list[-1].get('viewers', 0)
            enriched['carters_lag_1'] = history_list[-1].get('carters', 0)
            enriched['purchasers_lag_1'] = history_list[-1].get('purchasers', 0)
        
        if n >= 4:  # ~1 minute back
            enriched['viewers_lag_2'] = history_list[-4].get('viewers', 0)
            enriched['carters_lag_2'] = history_list[-4].get('carters', 0)
            enriched['purchasers_lag_2'] = history_list[-4].get('purchasers', 0)
        
        if n >= 7:  # ~2 minutes back
            enriched['viewers_lag_3'] = history_list[-7].get('viewers', 0)
            enriched['carters_lag_3'] = history_list[-7].get('carters', 0)
            enriched['purchasers_lag_3'] = history_list[-7].get('purchasers', 0)
        
        # TPS (transactions per second) features
        if n >= 2:
            # Current rate based on last window
            v_curr = record.get('viewers', 0)
            v_prev = history_list[-1].get('viewers', 0)
            enriched['tps_viewers'] = abs(v_curr - v_prev) / 20.0  # 20-second window
            
            c_curr = record.get('carters', 0)
            c_prev = history_list[-1].get('carters', 0)
            enriched['tps_carters'] = abs(c_curr - c_prev) / 20.0
        
        if n >= 4:
            # Smoothed TPS over 3 windows (60 seconds)
            v_curr = record.get('viewers', 0)
            v_1 = history_list[-1].get('viewers', 0)
            v_2 = history_list[-2].get('viewers', 0)
            v_3 = history_list[-3].get('viewers', 0)
            
            change_1 = abs(v_curr - v_1)
            change_2 = abs(v_1 - v_2)
            change_3 = abs(v_2 - v_3)
            enriched['tps_smoothed'] = (change_1 + change_2 + change_3) / 60.0
        
        return enriched
    
    def _on_new_record(self, record: Dict[str, Any]) -> None:
        """Process a new record from the Kafka streamer."""
        # Compute lag features before learning
        enriched_record = self._compute_lag_features(record)
        
        # Add to history for future lag calculations
        self._history.append(record)
        
        # Extract features and targets
        features = {
            'hour_of_day': float(enriched_record.get('hour_of_day', 0)),
            'minute_of_hour': float(enriched_record.get('minute_of_hour', 0)),
            'day_of_week': float(enriched_record.get('day_of_week', 0)),
            'viewers_lag_1': float(enriched_record.get('viewers_lag_1', 0)),
            'viewers_lag_2': float(enriched_record.get('viewers_lag_2', 0)),
            'viewers_lag_3': float(enriched_record.get('viewers_lag_3', 0)),
            'carters_lag_1': float(enriched_record.get('carters_lag_1', 0)),
            'carters_lag_2': float(enriched_record.get('carters_lag_2', 0)),
            'carters_lag_3': float(enriched_record.get('carters_lag_3', 0)),
            'purchasers_lag_1': float(enriched_record.get('purchasers_lag_1', 0)),
            'purchasers_lag_2': float(enriched_record.get('purchasers_lag_2', 0)),
            'purchasers_lag_3': float(enriched_record.get('purchasers_lag_3', 0)),
            'tps_viewers': float(enriched_record.get('tps_viewers', 0)),
            'tps_carters': float(enriched_record.get('tps_carters', 0)),
            'tps_smoothed': float(enriched_record.get('tps_smoothed', 0)),
        }
        
        targets = {
            'viewers': float(enriched_record.get('viewers', 0)),
            'carters': float(enriched_record.get('carters', 0)),
            'purchasers': float(enriched_record.get('purchasers', 0)),
            'view_to_cart_rate': float(enriched_record.get('view_to_cart_rate', 0)),
            'cart_to_buy_rate': float(enriched_record.get('cart_to_buy_rate', 0)),
        }
        
        # Learn from each metric
        for metric, target in targets.items():
            self.models.learn_one(metric, features, target)
        
        self._records_learned += 1
        
        # Trigger prediction callbacks for real-time updates
        for metric in targets:
            prediction = self.models.predict_one(metric, features)
            if prediction is not None:
                for callback in self._prediction_callbacks:
                    try:
                        callback(metric, prediction)
                    except Exception as e:
                        logger.error(f"Prediction callback error: {e}")
    
    def start(self) -> None:
        """Start the online learning service."""
        if self._running:
            return
        
        # Try to load from checkpoint first
        logger.info("Attempting to load from checkpoint...")
        if self.checkpointer.load_checkpoint(self.models):
            logger.info("Checkpoint loaded successfully")
        else:
            logger.info("No checkpoint found, starting fresh models")
        
        self._running = True
        self._start_time = datetime.now(timezone.utc)
        
        # Start the Kafka streamer
        self.streamer.start()
        
        # Start checkpoint thread
        self._checkpoint_thread = threading.Thread(target=self._checkpoint_loop, daemon=True)
        self._checkpoint_thread.start()
        
        logger.info(f"KafkaOnlineLearner started (checkpoint_interval={self.checkpoint_interval}s)")
    
    def stop(self) -> None:
        """Stop the online learning service."""
        self._running = False
        
        # Stop streamer
        self.streamer.stop()
        
        # Wait for checkpoint thread
        if self._checkpoint_thread:
            self._checkpoint_thread.join(timeout=5.0)
        
        # Final checkpoint
        self._save_checkpoint()
        
        logger.info("KafkaOnlineLearner stopped")
    
    def _checkpoint_loop(self) -> None:
        """Background loop for periodic checkpointing."""
        while self._running:
            time.sleep(1.0)
            
            if not self._running:
                break
            
            elapsed = time.time() - self._last_checkpoint
            if elapsed >= self.checkpoint_interval:
                self._save_checkpoint()
                self._last_checkpoint = time.time()
    
    def _save_checkpoint(self) -> None:
        """Save a checkpoint of current models."""
        stats = self.get_stats()
        self.checkpointer.save_checkpoint(self.models, stats)
    
    def predict(self, metric: str, features: Dict[str, float]) -> Optional[float]:
        """Make a prediction using the current models."""
        return self.models.predict_one(metric, features)
    
    def predict_all(self, features: Dict[str, float]) -> Dict[str, float]:
        """Make predictions for all metrics."""
        predictions = {}
        for metric in self.models.METRICS:
            pred = self.predict(metric, features)
            if pred is not None:
                predictions[metric] = pred
        return predictions
    
    def get_stats(self) -> Dict[str, Any]:
        """Get learning statistics."""
        model_stats = self.models.get_all_stats()
        
        runtime_seconds = 0
        if self._start_time:
            runtime_seconds = (datetime.now(timezone.utc) - self._start_time).total_seconds()
        
        return {
            'running': self._running,
            'started_at': self._start_time.isoformat() if self._start_time else None,
            'runtime_seconds': runtime_seconds,
            'records_learned': self._records_learned,
            'history_size': len(self._history),
            'kafka_connected': self.streamer.is_connected(),
            'checkpoint_interval': self.checkpoint_interval,
            'models': model_stats
        }
    
    def is_ready(self, min_samples: int = 5) -> bool:
        """Check if models have enough training data."""
        return all(
            self.models.is_ready(metric, min_samples)
            for metric in self.models.METRICS
        )
    
    def add_prediction_callback(self, callback: Callable[[str, float], None]) -> None:
        """Add a callback for real-time prediction updates."""
        self._prediction_callbacks.append(callback)
    
    def remove_prediction_callback(self, callback: Callable[[str, float], None]) -> None:
        """Remove a prediction callback."""
        if callback in self._prediction_callbacks:
            self._prediction_callbacks.remove(callback)


# Global singleton
_global_kafka_learner: Optional[KafkaOnlineLearner] = None


def get_kafka_learner(
    checkpoint_interval: Optional[float] = None,
    auto_start: bool = False
) -> KafkaOnlineLearner:
    """Get or create the global KafkaOnlineLearner instance."""
    global _global_kafka_learner
    
    if _global_kafka_learner is None:
        checkpoint = checkpoint_interval or float(os.getenv('CHECKPOINT_INTERVAL', '60.0'))
        _global_kafka_learner = KafkaOnlineLearner(
            checkpoint_interval=checkpoint,
            auto_start=auto_start
        )
    
    return _global_kafka_learner


def reset_kafka_learner() -> None:
    """Reset the global learner."""
    global _global_kafka_learner
    if _global_kafka_learner and _global_kafka_learner._running:
        _global_kafka_learner.stop()
    _global_kafka_learner = None