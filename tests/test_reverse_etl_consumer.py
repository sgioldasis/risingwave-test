"""The delta sync's Postgres target bring-up (orchestration/assets/reverse_etl_consumer.py):
the table the JDBC sink never creates, and the consumer report (connector state + row count).

Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest
from dagster import build_asset_context

from orchestration.assets import reverse_etl_consumer as consumer


class FakeConnection:
    """The schema_postgres seam: records executed SQL, answers the column probe and one count."""

    def __init__(self, row_count=0):
        self.sql, self.row_count, self.closed = [], row_count, False

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))
        if "COUNT(*)" in sql:
            self._result = [(self.row_count,)]
        elif "information_schema.columns" in sql:
            self._result = [
                ("rid", "bigint"),
                ("id", "bigint"),
                ("description", "text"),
                ("updated_at", "timestamp with time zone"),
            ]
        else:
            self._result = []

    def fetchone(self):
        return self._result[0]

    def fetchall(self):
        return self._result

    def commit(self):
        pass

    def close(self):
        self.closed = True


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected status {self.status_code}")


@pytest.fixture
def fake_db(monkeypatch):
    conn = FakeConnection()
    monkeypatch.setattr(consumer.connections, "schema_postgres", lambda: _connectable(conn))
    return conn


def _connectable(conn):
    class _Connectable:
        def connect(self):
            return conn

    return _Connectable()


class TestPostgresTarget:
    def test_it_creates_the_target_table_with_rid_as_the_key(self, fake_db):
        consumer.reverse_etl_delta_postgres_target(build_asset_context())
        (ddl,) = [sql for sql in fake_db.sql if "CREATE TABLE" in sql]
        assert "CREATE TABLE IF NOT EXISTS reverse_etl_delta_target" in ddl
        assert "rid BIGINT NOT NULL" in ddl and "PRIMARY KEY (rid)" in ddl

    def test_it_is_idempotent(self, fake_db):
        consumer.reverse_etl_delta_postgres_target(build_asset_context())
        (ddl,) = [sql for sql in fake_db.sql if "CREATE TABLE" in sql]
        assert "IF NOT EXISTS" in ddl

    def test_it_closes_the_connection_even_on_failure(self, fake_db):
        def boom(sql, params=None):
            raise RuntimeError("boom")

        fake_db.execute = boom
        with pytest.raises(RuntimeError, match="boom"):
            consumer.reverse_etl_delta_postgres_target(build_asset_context())
        assert fake_db.closed


class TestConsumerReport:
    def test_running_connector_and_row_count(self, fake_db, monkeypatch):
        fake_db.row_count = 3
        body = {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]}
        monkeypatch.setattr(consumer.requests, "get", lambda url, timeout: FakeResponse(200, body))
        result = consumer.reverse_etl_delta_consumer_report(build_asset_context())
        assert result == "connector=RUNNING, rows=3"
        (count_sql,) = [sql for sql in fake_db.sql if "COUNT(*)" in sql]
        assert "reverse_etl_delta_target" in count_sql

    def test_a_missing_connector_is_reported_not_raised(self, fake_db, monkeypatch):
        monkeypatch.setattr(consumer.requests, "get", lambda url, timeout: FakeResponse(404, {}))
        result = consumer.reverse_etl_delta_consumer_report(build_asset_context())
        assert result == "connector=not created, rows=0"

    def test_an_unreachable_connect_raises_with_the_fix(self, fake_db, monkeypatch):
        def boom(url, timeout):
            raise consumer.requests.ConnectionError("refused")

        monkeypatch.setattr(consumer.requests, "get", boom)
        with pytest.raises(RuntimeError, match="KAFKA_CONNECT_URL"):
            consumer.reverse_etl_delta_consumer_report(build_asset_context())
