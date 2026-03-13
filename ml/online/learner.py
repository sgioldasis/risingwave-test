"""Online learning service that continuously trains models from streaming data."""

import logging
import os
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable

from .models import RiverModelManager
from .streamer import DataStreamer, record_to_features_target
from .checkpoints import CheckpointManager

logger = logging.getLogger(__name__)


class OnlineLearner:
    """
    Continuous learning service that trains River models from RisingWave data.
    
    This service runs in the background, fetching new records and updating
    models incrementally. It also periodically checkpoints models to MinIO.
    """
    
    def __init__(
        self,
        poll_interval: float = 5.0,
        checkpoint_interval: float = 60.0,
        auto_start: bool = False
    ):
        """
        Initialize the online learner.
        
        Args:
            poll_interval: Seconds between fetching new data
            checkpoint_interval: Seconds between checkpoints
            auto_start: Whether to start streaming immediately
        """
        self.poll_interval = poll_interval
        self.checkpoint_interval = checkpoint_interval
        
        # Initialize components
        self.models = RiverModelManager()
        self.streamer = DataStreamer(poll_interval=poll_interval)
        self.checkpointer = CheckpointManager()
        
        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_checkpoint = time.time()
        self._records_learned = 0
        self._start_time: Optional[datetime] = None
        
        # Callbacks for prediction integration
        self._prediction_callbacks: list[Callable[[str, float], None]] = []
        
        # Register callback for new records
        self.streamer.add_callback(self._on_new_record)
        
        if auto_start:
            self.start()
    
    def _on_new_record(self, record: Dict[str, Any]) -> None:
        """Process a new record from the streamer."""
        features, targets = record_to_features_target(record)
        
        # Learn from each metric
        for metric, target in targets.items():
            self.models.learn_one(metric, features, target)
        
        self._records_learned += 1
        
        # Log progress every 10 records
        if self._records_learned % 10 == 0:
            stats = self.models.get_all_stats()
            total_samples = sum(s.get('learning_count', 0) for s in stats.values())
            logger.info(f"[Learner] Learned {self._records_learned} records, total samples: {total_samples}")
        
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
        
        # Start the streamer
        self.streamer.start_streaming()
        
        # Start checkpoint thread
        self._thread = threading.Thread(target=self._checkpoint_loop, daemon=True)
        self._thread.start()
        
        logger.info(f"OnlineLearner started (poll={self.poll_interval}s, checkpoint={self.checkpoint_interval}s)")
    
    def stop(self) -> None:
        """Stop the online learning service."""
        self._running = False
        
        # Stop streamer
        self.streamer.stop_streaming()
        
        # Wait for checkpoint thread
        if self._thread:
            self._thread.join(timeout=5.0)
        
        # Final checkpoint
        self._save_checkpoint()
        
        logger.info("OnlineLearner stopped")
    
    def _checkpoint_loop(self) -> None:
        """Background loop for periodic checkpointing."""
        while self._running:
            time.sleep(1.0)  # Check every second
            
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
        """
        Make a prediction using the current models.
        
        Args:
            metric: Metric to predict
            features: Feature dictionary
            
        Returns:
            Predicted value or None if model not ready
        """
        return self.models.predict_one(metric, features)
    
    def predict_all(self, features: Dict[str, float]) -> Dict[str, float]:
        """
        Make predictions for all metrics.
        
        Args:
            features: Feature dictionary
            
        Returns:
            Dictionary of metric -> prediction
        """
        predictions = {}
        for metric in self.models.METRICS:
            pred = self.predict(metric, features)
            if pred is not None:
                predictions[metric] = pred
        return predictions
    
    def learn_manual(self, features: Dict[str, float], targets: Dict[str, float]) -> None:
        """
        Manually trigger learning from features and targets.
        
        Useful for testing or batch updates.
        
        Args:
            features: Feature dictionary
            targets: Target values by metric
        """
        for metric, target in targets.items():
            self.models.learn_one(metric, features, target)
        self._records_learned += 1
    
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
            'poll_interval': self.poll_interval,
            'checkpoint_interval': self.checkpoint_interval,
            'models': model_stats
        }
    
    def is_ready(self, min_samples: int = 5) -> bool:
        """Check if models have enough training data to make predictions."""
        return all(
            self.models.is_ready(metric, min_samples)
            for metric in self.models.METRICS
        )
    
    def add_prediction_callback(self, callback: Callable[[str, float], None]) -> None:
        """Add a callback to receive real-time prediction updates."""
        self._prediction_callbacks.append(callback)
    
    def remove_prediction_callback(self, callback: Callable[[str, float], None]) -> None:
        """Remove a prediction callback."""
        if callback in self._prediction_callbacks:
            self._prediction_callbacks.remove(callback)


# Global singleton instance for sharing across modules
_global_learner: Optional[OnlineLearner] = None


def get_learner(
    poll_interval: Optional[float] = None,
    checkpoint_interval: Optional[float] = None,
    auto_start: bool = False
) -> OnlineLearner:
    """
    Get or create the global OnlineLearner instance.
    
    Args:
        poll_interval: Seconds between data fetches (default: from env or 5.0)
        checkpoint_interval: Seconds between checkpoints (default: from env or 60.0)
        auto_start: Whether to start immediately
        
    Returns:
        OnlineLearner instance
    """
    global _global_learner
    
    if _global_learner is None:
        poll = poll_interval or float(os.getenv('ONLINE_LEARNING_INTERVAL', '5.0'))
        checkpoint = checkpoint_interval or float(os.getenv('CHECKPOINT_INTERVAL', '60.0'))
        _global_learner = OnlineLearner(
            poll_interval=poll,
            checkpoint_interval=checkpoint,
            auto_start=auto_start
        )
    
    return _global_learner


def reset_learner() -> None:
    """Reset the global learner (useful for testing)."""
    global _global_learner
    if _global_learner and _global_learner._running:
        _global_learner.stop()
    _global_learner = None