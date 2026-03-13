"""Kafka-based data streaming for River online learning."""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, Callable, Optional

from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)


class KafkaDataStreamer:
    """Streams training data from Kafka funnel topic for incremental learning."""
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
    FUNNEL_TOPIC = 'funnel'
    
    def __init__(self, auto_start: bool = False):
        """
        Initialize the Kafka data streamer.
        
        Args:
            auto_start: Whether to start consuming immediately
        """
        self.consumer: Optional[Consumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: list[Callable[[Dict[str, Any]], None]] = []
        
        # Track seen windows to avoid duplicates (RisingWave may emit updates)
        self._seen_windows: set[str] = set()
        self._max_seen_windows = 1000
        
        if auto_start:
            self.start()
    
    def add_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Add a callback to be called for each new record."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Remove a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def start(self) -> None:
        """Start the Kafka consumer in a background thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info(f"KafkaDataStreamer started (topic: {self.FUNNEL_TOPIC})")
    
    def stop(self) -> None:
        """Stop the Kafka consumer."""
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("KafkaDataStreamer stopped")
    
    def _consume_loop(self) -> None:
        """Main consumption loop running in background thread."""
        conf = {
            'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'river-online-learning',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        
        while self._running:
            try:
                self.consumer = Consumer(conf)
                self.consumer.subscribe([self.FUNNEL_TOPIC])
                
                logger.info(f"Connected to Kafka: {self.KAFKA_BOOTSTRAP_SERVERS}")
                
                while self._running:
                    try:
                        msg = self.consumer.poll(timeout=1.0)
                        if msg is None:
                            continue
                        if msg.error():
                            logger.error(f"Consumer error: {msg.error()}")
                            continue
                        
                        # Deserialize message
                        data = json.loads(msg.value().decode('utf-8'))
                        self._process_message(data)
                            
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        time.sleep(1)
                        
            except Exception as e:
                logger.error(f"Kafka connection error: {e}")
                time.sleep(5)  # Wait before reconnecting
            finally:
                if self.consumer:
                    try:
                        self.consumer.close()
                    except:
                        pass
                    self.consumer = None
    
    def _process_message(self, data: Dict[str, Any]) -> None:
        """Process a funnel message from Kafka."""
        try:
            # Parse window_start for deduplication
            window_start = data.get('window_start')
            if window_start:
                # Handle different timestamp formats
                if isinstance(window_start, (int, float)):
                    window_start = datetime.fromtimestamp(
                        window_start / 1000, tz=timezone.utc
                    ).isoformat()
                window_start = str(window_start)
                
                # Skip duplicates
                if window_start in self._seen_windows:
                    return
                
                self._seen_windows.add(window_start)
                
                # Clean up old window IDs to prevent memory growth
                if len(self._seen_windows) > self._max_seen_windows:
                    # Remove oldest entries (simple approach: clear half)
                    self._seen_windows = set(list(self._seen_windows)[-self._max_seen_windows//2:])
            
            # Convert Kafka message to training record format
            record = self._convert_to_record(data)
            if record:
                # Notify all callbacks
                for callback in self._callbacks:
                    try:
                        callback(record)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                        
        except Exception as e:
            logger.error(f"Error processing funnel data: {e}")
    
    def _convert_to_record(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert Kafka funnel message to training record format.
        
        The funnel topic contains aggregated window data from RisingWave.
        We need to extract features and targets for online learning.
        """
        try:
            # Parse timestamps
            window_start = data.get('window_start')
            window_end = data.get('window_end')
            
            if isinstance(window_start, (int, float)):
                window_start = datetime.fromtimestamp(
                    window_start / 1000, tz=timezone.utc
                ).isoformat()
            if isinstance(window_end, (int, float)):
                window_end = datetime.fromtimestamp(
                    window_end / 1000, tz=timezone.utc
                ).isoformat()
            
            # Parse timestamp to extract time features
            dt = datetime.fromisoformat(str(window_start).replace('Z', '+00:00'))
            hour_of_day = dt.hour
            minute_of_hour = dt.minute
            day_of_week = dt.weekday()
            
            # Extract metrics
            viewers = float(data.get('viewers', 0))
            carters = float(data.get('carters', 0))
            purchasers = float(data.get('purchasers', 0))
            view_to_cart_rate = float(data.get('view_to_cart_rate', 0))
            cart_to_buy_rate = float(data.get('cart_to_buy_rate', 0))
            
            # Build record in the same format as RisingWave funnel_training
            # Note: Lag features are calculated from historical context
            record = {
                'window_start': window_start,
                'window_end': window_end,
                'viewers': viewers,
                'carters': carters,
                'purchasers': purchasers,
                'view_to_cart_rate': view_to_cart_rate,
                'cart_to_buy_rate': cart_to_buy_rate,
                'hour_of_day': hour_of_day,
                'minute_of_hour': minute_of_hour,
                'day_of_week': day_of_week,
                # Lag features - will be populated by feature engineering
                'viewers_lag_1': 0.0,
                'viewers_lag_2': 0.0,
                'viewers_lag_3': 0.0,
                'carters_lag_1': 0.0,
                'carters_lag_2': 0.0,
                'carters_lag_3': 0.0,
                'purchasers_lag_1': 0.0,
                'purchasers_lag_2': 0.0,
                'purchasers_lag_3': 0.0,
                'tps_viewers': 0.0,
                'tps_carters': 0.0,
                'tps_smoothed': 0.0,
            }
            
            return record
            
        except Exception as e:
            logger.error(f"Error converting record: {e}")
            return None
    
    def is_connected(self) -> bool:
        """Check if connected to Kafka."""
        return self.consumer is not None
