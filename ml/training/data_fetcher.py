"""Data fetching from RisingWave for ML training."""

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches training data from RisingWave."""
    
    # Database connection settings
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')
    
    def __init__(self):
        self.connection_params = {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'database': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD
        }
    
    def _get_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self.connection_params)
    
    def fetch_training_data(
        self,
        minutes_back: int = 1,
        min_samples: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Fetch training data from RisingWave funnel_training view.
        
        Args:
            minutes_back: Number of minutes of historical data to fetch
            min_samples: Minimum number of samples required
            
        Returns:
            List of dictionaries containing training data
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Query to fetch recent training data with lag features
            # Only use completed windows (window_end < NOW()) to avoid training on partial data
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
                FROM (
                    SELECT *
                    FROM funnel_training
                    WHERE window_end < NOW()
                    ORDER BY window_start DESC
                    LIMIT %s
                ) sub
                ORDER BY window_start ASC
            """
            
            # Fetch more records to account for the lag features needing 3 prior minutes
            # With 20-second windows, we need 3x more records
            cursor.execute(query, (minutes_back * 3 + 5,))
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert to list of dicts and handle datetime serialization
            data = []
            for row in rows:
                record = dict(row)
                # Convert datetime objects to strings for JSON serialization
                if record.get('window_start'):
                    record['window_start'] = record['window_start'].isoformat()
                if record.get('window_end'):
                    record['window_end'] = record['window_end'].isoformat()
                data.append(record)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Fetched {len(data)} training records from RisingWave (last {minutes_back} minutes)")
            
            if len(data) < min_samples:
                logger.warning(f"Only {len(data)} samples found (need at least {min_samples})")
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch training data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_latest_data_timestamp(self) -> Optional[str]:
        """Get the timestamp of the latest available training data."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MAX(window_end) as latest_time
                FROM funnel_training
                WHERE window_end < NOW()
            """)
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0]:
                return result[0].isoformat()
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            return None
    
    def has_new_data(self, since_timestamp: Optional[str]) -> bool:
        """
        Check if new data is available since the given timestamp.
        
        Args:
            since_timestamp: ISO format timestamp to check against
            
        Returns:
            True if new data is available
        """
        latest = self.get_latest_data_timestamp()
        if not latest:
            return False
        
        if not since_timestamp:
            return True
        
        # Compare timestamps
        try:
            latest_dt = datetime.fromisoformat(latest.replace('Z', '+00:00'))
            since_dt = datetime.fromisoformat(since_timestamp.replace('Z', '+00:00'))
            return latest_dt > since_dt
        except Exception:
            return True  # If we can't parse, assume new data exists
