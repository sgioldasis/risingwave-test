"""StarRocks-backed training streamer for the online learner.

Polls funnel windows from StarRocks instead of the RisingWave-fed funnel
Kafka topic, so online learning keeps working while RisingWave is paused.
StarRocks's own pipeline (Routine Load from the raw topics + async MV)
maintains those windows with no RW in the loop -- verified 2026-11-14:
funnel_summary_direct's newest window advanced in real time with RW stopped.

Same callback contract as KafkaDataStreamer: one record dict per window
(same shape, incl. time features), each window learned exactly once.
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pymysql

logger = logging.getLogger(__name__)


class StarRocksDataStreamer:
    """Polls new funnel windows from StarRocks and invokes callbacks."""

    HOST = os.getenv('STARROCKS_HOST', 'localhost')
    PORT = int(os.getenv('STARROCKS_PORT', '9030'))
    USER = os.getenv('STARROCKS_USER', 'root')
    PASSWORD = os.getenv('STARROCKS_PASSWORD', '')
    DB = os.getenv('STARROCKS_DB', 'sr_local_db_sr_local_db')
    # The RW-free hot+cold view: frozen mv_unified_funnel_summary + the
    # Routine Load direct branch (deduped by construction via its cutoff).
    SOURCE_TABLE = os.getenv('STARROCKS_FUNNEL_SOURCE', 'dashboard_funnel_serving_cached_direct')
    POLL_INTERVAL = float(os.getenv('SR_POLL_INTERVAL', '5.0'))
    # First poll backfills this many hours of history so a fresh service
    # trains on recent data immediately instead of waiting for new windows.
    BACKFILL_HOURS = int(os.getenv('SR_BACKFILL_HOURS', '2'))

    QUERY = """
        SELECT
            window_start,
            window_end,
            SUM(viewers) AS viewers,
            SUM(carters) AS carters,
            SUM(purchasers) AS purchasers,
            CAST(SUM(carters) AS DOUBLE) / NULLIF(SUM(viewers), 0) AS view_to_cart_rate,
            CAST(SUM(purchasers) AS DOUBLE) / NULLIF(SUM(carters), 0) AS cart_to_buy_rate
        FROM {table}
        WHERE window_start >= (
            SELECT MAX(window_start) FROM {table}
        ) - INTERVAL {backfill} HOUR
        GROUP BY window_start, window_end
        ORDER BY window_start ASC
    """

    INCREMENTAL_QUERY = """
        SELECT
            window_start,
            window_end,
            SUM(viewers) AS viewers,
            SUM(carters) AS carters,
            SUM(purchasers) AS purchasers,
            CAST(SUM(carters) AS DOUBLE) / NULLIF(SUM(viewers), 0) AS view_to_cart_rate,
            CAST(SUM(purchasers) AS DOUBLE) / NULLIF(SUM(carters), 0) AS cart_to_buy_rate
        FROM {table}
        WHERE window_start > %(last_window)s
        GROUP BY window_start, window_end
        ORDER BY window_start ASC
    """

    def __init__(self, auto_start: bool = False):
        self._callbacks: list[Callable[[Dict[str, Any]], None]] = []
        self._last_window: Optional[str] = None  # watermark, ISO string
        self._seen_windows: set[str] = set()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_poll_ok = False
        if auto_start:
            self.start()

    def add_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info(
            "StarRocksDataStreamer started (table: %s, poll_interval: %ss)",
            self.SOURCE_TABLE, self.POLL_INTERVAL,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("StarRocksDataStreamer stopped")

    def is_connected(self) -> bool:
        """True while the last poll against StarRocks succeeded."""
        return self._last_poll_ok

    def _get_connection(self):
        return pymysql.connect(
            host=self.HOST,
            port=self.PORT,
            user=self.USER,
            password=self.PASSWORD,
            database=self.DB,
            connect_timeout=3,
        )

    def _poll_loop(self) -> None:
        while self._running:
            try:
                self._poll_once()
                self._last_poll_ok = True
            except Exception as e:
                self._last_poll_ok = False
                logger.error("StarRocks poll error: %s", e)
            time.sleep(self.POLL_INTERVAL)

    @staticmethod
    def _now() -> datetime:
        """UTC clock used for the closed-window cutoff (overridable in tests)."""
        return datetime.now(timezone.utc)

    def _poll_once(self) -> None:
        """Fetch windows newer than the watermark and feed callbacks."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                if self._last_window is None:
                    cursor.execute(self.QUERY.format(
                        table=self.SOURCE_TABLE,
                        backfill=self.BACKFILL_HOURS,
                    ))
                else:
                    cursor.execute(
                        self.INCREMENTAL_QUERY.format(table=self.SOURCE_TABLE),
                        {"last_window": self._last_window},
                    )
                rows = cursor.fetchall()
        finally:
            conn.close()

        now = self._now()
        for row in rows:
            # Learn only CLOSED windows: the in-progress window's first
            # sight carries partial aggregates (e.g. 13 viewers ten seconds
            # into the minute that ends at 60), and with the per-window
            # dedup it would never be re-learned at its final value -- that
            # poisoned the model with systematically low targets
            # (2026-11-14). Skip without dedup or watermark; the window is
            # picked up on a later poll once it has closed.
            window_end = row[1]
            if isinstance(window_end, datetime):
                if window_end.tzinfo is None:
                    window_end = window_end.replace(tzinfo=timezone.utc)
                if window_end > now:
                    continue
            record = self._convert_row(row)
            if record is None:
                continue
            # Each window is learned once (same as the Kafka streamer's
            # _seen_windows dedup): the in-progress window's growing
            # aggregate would otherwise be learned many times.
            if record['window_start'] in self._seen_windows:
                continue
            self._seen_windows.add(record['window_start'])
            self._last_window = record['window_start']
            for callback in self._callbacks:
                try:
                    callback(record)
                except Exception as e:
                    logger.error("Callback error: %s", e)

    def _convert_row(self, row) -> Optional[Dict[str, Any]]:
        """Convert a StarRocks row to the learner's record shape."""
        try:
            window_start, window_end, viewers, carters, purchasers, vtc, ctb = row
            if isinstance(window_start, datetime):
                window_start = window_start.replace(tzinfo=timezone.utc).isoformat()
            if isinstance(window_end, datetime):
                window_end = window_end.replace(tzinfo=timezone.utc).isoformat()
            dt = datetime.fromisoformat(str(window_start).replace('Z', '+00:00'))
            return {
                'window_start': window_start,
                'window_end': window_end,
                'viewers': float(viewers or 0),
                'carters': float(carters or 0),
                'purchasers': float(purchasers or 0),
                'view_to_cart_rate': float(vtc) if vtc is not None else 0.0,
                'cart_to_buy_rate': float(ctb) if ctb is not None else 0.0,
                'hour_of_day': dt.hour,
                'minute_of_hour': dt.minute,
                'day_of_week': dt.weekday(),
                # Lag features - populated by the learner's feature engineering
                'viewers_lag_1': 0.0, 'viewers_lag_2': 0.0, 'viewers_lag_3': 0.0,
                'carters_lag_1': 0.0, 'carters_lag_2': 0.0, 'carters_lag_3': 0.0,
                'purchasers_lag_1': 0.0, 'purchasers_lag_2': 0.0, 'purchasers_lag_3': 0.0,
                'tps_viewers': 0.0, 'tps_carters': 0.0, 'tps_smoothed': 0.0,
            }
        except Exception as e:
            logger.error("Error converting StarRocks row: %s", e)
            return None
