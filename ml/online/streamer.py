"""Data streaming from RisingWave for online learning."""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Iterator, Optional, Callable
import threading

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DataStreamer:
    """Streams training data from RisingWave for incremental learning."""
    
    # Database connection settings
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')
    
    def __init__(self, poll_interval: float = 5.0):
        """
        Initialize the data streamer.
        
        Args:
            poll_interval: Seconds between polling RisingWave for new data
        """
        self.poll_interval = poll_interval
        self.connection_params = {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'database': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD
        }
        self._last_seen_timestamp: Optional[str] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: list[Callable[[Dict[str, Any]], None]] = []
    
    def _get_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self.connection_params)
    
    def fetch_new_records(self, limit: int = 100) -> list[Dict[str, Any]]:
        """
        Fetch new records since last check.
        
        Args:
            limit: Maximum records to fetch per poll
            
        Returns:
            List of new training records
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            if self._last_seen_timestamp:
                # Fetch records newer than last seen
                query = """
                    SELECT
                        window_start,
                        window_end,
                        viewers,
                        carters,
                        purchasers,
                        view_to_cart_rate,
                        cart_to_buy_rate,
                        hour_of_day,
                        minute_of_hour,
                        day_of_week,
                        COALESCE(viewers_lag_1, 0) as viewers_lag_1,
                        COALESCE(viewers_lag_2, 0) as viewers_lag_2,
                        COALESCE(viewers_lag_3, 0) as viewers_lag_3,
                        COALESCE(carters_lag_1, 0) as carters_lag_1,
                        COALESCE(carters_lag_2, 0) as carters_lag_2,
                        COALESCE(carters_lag_3, 0) as carters_lag_3,
                        COALESCE(purchasers_lag_1, 0) as purchasers_lag_1,
                        COALESCE(purchasers_lag_2, 0) as purchasers_lag_2,
                        COALESCE(purchasers_lag_3, 0) as purchasers_lag_3,
                        COALESCE(tps_viewers, 0) as tps_viewers,
                        COALESCE(tps_carters, 0) as tps_carters,
                        COALESCE(tps_smoothed, 0) as tps_smoothed
                    FROM funnel_training
                    WHERE window_end < NOW()
                      AND window_start > %s
                    ORDER BY window_start ASC
                    LIMIT %s
                """
                cursor.execute(query, (self._last_seen_timestamp, limit))
            else:
                # First fetch - get recent records for bootstrapping
                query = """
                    SELECT
                        window_start,
                        window_end,
                        viewers,
                        carters,
                        purchasers,
                        view_to_cart_rate,
                        cart_to_buy_rate,
                        hour_of_day,
                        minute_of_hour,
                        day_of_week,
                        COALESCE(viewers_lag_1, 0) as viewers_lag_1,
                        COALESCE(viewers_lag_2, 0) as viewers_lag_2,
                        COALESCE(viewers_lag_3, 0) as viewers_lag_3,
                        COALESCE(carters_lag_1, 0) as carters_lag_1,
                        COALESCE(carters_lag_2, 0) as carters_lag_2,
                        COALESCE(carters_lag_3, 0) as carters_lag_3,
                        COALESCE(purchasers_lag_1, 0) as purchasers_lag_1,
                        COALESCE(purchasers_lag_2, 0) as purchasers_lag_2,
                        COALESCE(purchasers_lag_3, 0) as purchasers_lag_3,
                        COALESCE(tps_viewers, 0) as tps_viewers,
                        COALESCE(tps_carters, 0) as tps_carters,
                        COALESCE(tps_smoothed, 0) as tps_smoothed
                    FROM funnel_training
                    WHERE window_end < NOW()
                    ORDER BY window_start DESC
                    LIMIT %s
                """
                cursor.execute(query, (limit,))
            
            rows = cursor.fetchall()
            
            # Convert to list of dicts
            records = []
            for row in rows:
                record = dict(row)
                # Convert datetime objects to strings
                if record.get('window_start'):
                    record['window_start'] = record['window_start'].isoformat()
                if record.get('window_end'):
                    record['window_end'] = record['window_end'].isoformat()
                records.append(record)
            
            # Reverse if this was initial fetch (DESC order -> ASC order for chronological learning)
            # Initial fetch uses DESC, subsequent fetches use ASC
            is_initial_fetch = self._last_seen_timestamp is None
            if is_initial_fetch:
                records.reverse()
            
            # Update last seen timestamp (most recent record)
            if records:
                self._last_seen_timestamp = records[-1]['window_start']
            
            cursor.close()
            conn.close()
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching new records: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def add_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Add a callback to be called for each new record."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Remove a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def start_streaming(self) -> None:
        """Start the background streaming thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._stream_loop, daemon=True)
        self._thread.start()
        logger.info(f"DataStreamer started with {self.poll_interval}s interval")
    
    def stop_streaming(self) -> None:
        """Stop the background streaming thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("DataStreamer stopped")
    
    def _stream_loop(self) -> None:
        """Main streaming loop running in background thread."""
        is_first_fetch = True
        while self._running:
            try:
                records = self.fetch_new_records()
                if records:
                    if is_first_fetch:
                        logger.info(f"[Streamer] Initial fetch: {len(records)} records from RisingWave")
                        is_first_fetch = False
                    else:
                        logger.info(f"[Streamer] Fetched {len(records)} new records")
                    for record in records:
                        for callback in self._callbacks:
                            try:
                                callback(record)
                            except Exception as e:
                                logger.error(f"Callback error: {e}")
            except Exception as e:
                logger.error(f"Streaming error: {e}")
            
            # Sleep until next poll
            time.sleep(self.poll_interval)
    
    def iter_records(self) -> Iterator[Dict[str, Any]]:
        """
        Iterator that yields records as they arrive.
        
        Blocks until records are available. Use in foreground thread.
        """
        while True:
            records = self.fetch_new_records()
            for record in records:
                yield record
            
            if not records:
                time.sleep(self.poll_interval)


def record_to_features_target(record: Dict[str, Any]) -> tuple[Dict[str, float], Dict[str, float]]:
    """
    Convert a record to features and targets for learning.
    
    Args:
        record: Training record from RisingWave
        
    Returns:
        Tuple of (features_dict, targets_dict)
    """
    features = {
        'hour_of_day': float(record.get('hour_of_day', 0)),
        'minute_of_hour': float(record.get('minute_of_hour', 0)),
        'day_of_week': float(record.get('day_of_week', 0)),
        'viewers_lag_1': float(record.get('viewers_lag_1', 0)),
        'viewers_lag_2': float(record.get('viewers_lag_2', 0)),
        'viewers_lag_3': float(record.get('viewers_lag_3', 0)),
        'carters_lag_1': float(record.get('carters_lag_1', 0)),
        'carters_lag_2': float(record.get('carters_lag_2', 0)),
        'carters_lag_3': float(record.get('carters_lag_3', 0)),
        'purchasers_lag_1': float(record.get('purchasers_lag_1', 0)),
        'purchasers_lag_2': float(record.get('purchasers_lag_2', 0)),
        'purchasers_lag_3': float(record.get('purchasers_lag_3', 0)),
        'tps_viewers': float(record.get('tps_viewers', 0)),
        'tps_carters': float(record.get('tps_carters', 0)),
        'tps_smoothed': float(record.get('tps_smoothed', 0)),
    }
    
    targets = {
        'viewers': float(record.get('viewers', 0)),
        'carters': float(record.get('carters', 0)),
        'purchasers': float(record.get('purchasers', 0)),
        'view_to_cart_rate': float(record.get('view_to_cart_rate', 0)),
        'cart_to_buy_rate': float(record.get('cart_to_buy_rate', 0)),
    }
    
    return features, targets