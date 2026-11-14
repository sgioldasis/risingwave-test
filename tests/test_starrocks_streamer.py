"""Tests for the StarRocks training streamer (ml/online/starrocks_streamer.py).

The streamer feeds the online learner from StarRocks's RW-free funnel
windows instead of the RW-fed Kafka funnel topic. These tests pin the
record contract (the learner's feature/target extraction depends on it),
the watermark behavior (backfill first, then incremental), and the
per-window dedup.
"""

from datetime import datetime, timezone
from unittest import mock

from ml.online.starrocks_streamer import StarRocksDataStreamer


def _row(window_start, viewers=60, carters=20, purchasers=7):
    """A StarRocks result row: (window_start, window_end, viewers, carters,
    purchasers, view_to_cart_rate, cart_to_buy_rate) with datetimes, as
    pymysql returns them. Dates passed in must already be safely in the
    past (the streamer skips unclosed windows)."""
    start = datetime.fromisoformat(window_start)
    end = start.replace(minute=59)
    return (start, end, viewers, carters, purchasers, carters / viewers, purchasers / carters)


def _streamer_with_rows(rows):
    """A streamer whose _get_connection returns a fake pymysql connection
    yielding `rows` from every query."""
    streamer = StarRocksDataStreamer()
    cursor = mock.MagicMock()
    cursor.fetchall.return_value = rows
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.close.return_value = None
    streamer._get_connection = mock.MagicMock(return_value=conn)
    return streamer, cursor


class TestRecordConversion:
    def test_record_shape_matches_the_learner_contract(self):
        """The learner's _on_new_record reads these exact keys; a missing
        one silently trains on zero features."""
        streamer, _ = _streamer_with_rows([_row("2020-06-15T12:00:00")])
        record = streamer._convert_row(_row("2020-06-15T12:00:00"))

        assert record["viewers"] == 60.0
        assert record["carters"] == 20.0
        assert record["purchasers"] == 7.0
        assert record["view_to_cart_rate"] == 20 / 60
        assert record["cart_to_buy_rate"] == 7 / 20
        # time features extracted from window_start
        assert record["hour_of_day"] == 12
        assert record["minute_of_hour"] == 0
        assert record["day_of_week"] == datetime(2020, 6, 15).weekday()
        # lag placeholders -- the learner's lag engineering overwrites these
        for key in ("viewers_lag_1", "carters_lag_2", "purchasers_lag_3", "tps_smoothed"):
            assert record[key] == 0.0

    def test_timestamps_are_utc_iso_strings(self):
        streamer, _ = _streamer_with_rows([])
        record = streamer._convert_row(_row("2020-06-15T12:00:00"))
        assert record["window_start"] == "2020-06-15T12:00:00+00:00"
        assert record["window_end"].startswith("2020-06-15T12:59")

    def test_null_rates_become_zero(self):
        streamer, _ = _streamer_with_rows([])
        row = (datetime(2026, 11, 14, 12, 0), datetime(2026, 11, 14, 12, 59),
               0, 0, 0, None, None)
        record = streamer._convert_row(row)
        assert record["view_to_cart_rate"] == 0.0
        assert record["cart_to_buy_rate"] == 0.0


class TestWatermark:
    def test_first_poll_backfills_relative_to_newest_window(self):
        """Backfill must anchor on MAX(window_start), not now() -- the
        cached view's frozen-MV history has months-old windows, and
        anchoring on now() would return nothing after a long pause."""
        streamer, cursor = _streamer_with_rows(
            [_row("2020-06-15T11:00:00"), _row("2020-06-15T11:01:00")]
        )
        received = []
        streamer.add_callback(received.append)
        streamer._poll_once()

        sql = " ".join(cursor.execute.call_args[0][0].split())
        assert "MAX(window_start)" in sql
        assert "- INTERVAL 2 HOUR" in sql

    def test_incremental_polls_use_the_watermark(self):
        streamer, cursor = _streamer_with_rows([])
        streamer._last_window = "2020-06-15T12:00:00+00:00"
        streamer._seen_windows = {"2020-06-15T12:00:00+00:00"}
        streamer._poll_once()

        sql = " ".join(cursor.execute.call_args[0][0].split())
        assert "window_start > %(last_window)s" in sql
        assert cursor.execute.call_args[0][1] == {"last_window": "2020-06-15T12:00:00+00:00"}

    def test_watermark_advances_to_newest_fetched_window(self):
        streamer, _ = _streamer_with_rows(
            [_row("2020-06-15T12:00:00"), _row("2020-06-15T12:01:00")]
        )
        streamer.add_callback(lambda record: None)
        streamer._poll_once()
        assert streamer._last_window == "2020-06-15T12:01:00+00:00"


class TestClosedWindowsOnly:
    def test_unclosed_window_is_skipped_without_watermark(self):
        """The in-progress window carries partial aggregates; learning it
        (and deduping it) would train the model on opening values that
        never get corrected. It must be skipped entirely so it is learned
        once, closed, on a later poll."""
        streamer, _ = _streamer_with_rows(
            [_row("2020-06-15T12:00:00"), _row("2099-01-01T00:00:00")]
        )
        received = []
        streamer.add_callback(received.append)
        streamer._poll_once()

        assert len(received) == 1
        assert received[0]["window_start"] == "2020-06-15T12:00:00+00:00"
        # The unclosed window must not watermark or dedup-block itself
        assert streamer._last_window == "2020-06-15T12:00:00+00:00"
        assert "2099-01-01T00:00:00+00:00" not in streamer._seen_windows

    def test_unclosed_window_is_learned_once_it_closes(self):
        streamer, cursor = _streamer_with_rows(
            [_row("2020-06-15T12:00:00"), _row("2099-01-01T00:00:00")]
        )
        received = []
        streamer.add_callback(received.append)
        streamer._poll_once()
        assert len(received) == 1

        # The window is now closed (clock advanced past its window_end) and
        # the query returns it again -- it must be learned exactly once.
        streamer._now = lambda: datetime(2099, 1, 1, 2, 0, tzinfo=timezone.utc)
        streamer._poll_once()

        assert [r["window_start"] for r in received] == [
            "2020-06-15T12:00:00+00:00",
            "2099-01-01T00:00:00+00:00",
        ]


class TestDedupAndCallbacks:
    def test_each_window_is_learned_once(self):
        """Same dedup contract as the Kafka streamer's _seen_windows: the
        in-progress window's growing aggregate must not be re-learned."""
        streamer, _ = _streamer_with_rows(
            [_row("2020-06-15T12:00:00"), _row("2020-06-15T12:00:00", viewers=80)]
        )
        received = []
        streamer.add_callback(received.append)
        streamer._poll_once()
        assert len(received) == 1

    def test_callback_error_does_not_stop_the_batch(self):
        streamer, _ = _streamer_with_rows(
            [_row("2020-06-15T12:00:00"), _row("2020-06-15T12:01:00")]
        )
        received = []

        def broken(record):
            raise RuntimeError("boom")

        streamer.add_callback(broken)
        streamer.add_callback(received.append)
        streamer._poll_once()
        assert len(received) == 2

    def test_poll_error_marks_disconnected(self):
        """is_connected() mirrors the last poll outcome -- the predictor's
        kafka_connected badge reads it every request."""
        streamer = StarRocksDataStreamer()
        streamer._get_connection = mock.MagicMock(
            side_effect=ConnectionError("starrocks down")
        )

        def stop_after_one(_seconds):
            streamer._running = False

        with mock.patch("ml.online.starrocks_streamer.time.sleep",
                        side_effect=stop_after_one):
            streamer._poll_loop()
        assert streamer.is_connected() is False
