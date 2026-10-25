"""Fake resources for the reverse-ETL tests, bound under the keys the project registers (postgres, risingwave,
databricks, kafka, http, registry; see orchestration/assets/reverse_etl_resources.py). Each fake records what it
was asked to do in one shared `calls` list of (kind, detail) tuples, so a test can assert what happened and in
which order."""

import dataclasses
import types
from unittest import mock

from orchestration.assets.reverse_etl_config import ReverseEtlSyncConfig


def make_config(source_columns=("description STRING",), key_column="rid", **overrides) -> ReverseEtlSyncConfig:
    cfg = ReverseEtlSyncConfig.for_name(
        "unittest", catalog="de_dev", schema="sr_poc_external", source_columns=source_columns, key_column=key_column
    )
    return dataclasses.replace(cfg, **overrides) if overrides else cfg


class FakeDatabase:
    """Stands in for PostgresResource and RisingWaveResource: records each statement under `kind`.
    `columns` is what an information_schema.columns query returns (the table's current columns)."""

    def __init__(self, calls: list, kind: str, columns: list | None = None):
        self.calls, self.kind, self.columns = calls, kind, columns or []

    def connect_with_timeout(self):
        return self.connect()

    def connect(self):
        cursor = mock.MagicMock()
        cursor.execute.side_effect = lambda sql, params=None: self.calls.append((self.kind, " ".join(str(sql).split())))
        cursor.fetchall.side_effect = lambda: [(name, "character varying") for name in self.columns]
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        return connection


class FakeDatabricks:
    def __init__(self, calls: list, sql_error=None, location_error=None):
        self.calls, self.sql_error, self.location_error = calls, sql_error, location_error

    def require_env(self):
        self.calls.append(("env", ""))

    def token(self):
        return "token"

    def cached_token(self):
        return "token"

    def reset_meter(self):
        pass

    def run_sql(self, token, statement):
        self.calls.append(("databricks", " ".join(statement.split())))
        if self.sql_error:
            raise self.sql_error

    def table_location(self, token, cfg, fqn):
        if self.location_error:
            raise self.location_error
        self.calls.append(("location", fqn))
        return "abfss://x"

    def table_location_cached(self, token, cfg, fqn):
        self.calls.append(("location", fqn))
        return "abfss://x"

    def clear_location_cache(self):
        self.calls.append(("cache", "cleared"))


class FakeSchemaRegistry:
    def client(self, url):
        return mock.MagicMock()


class FakeKafka:
    def __init__(self, calls: list, lag: int = 0):
        self.calls, self.lag = calls, lag

    def consumer(self, group_id, extra=None):
        topic_meta = lambda topic: types.SimpleNamespace(topics={topic: types.SimpleNamespace(partitions={0: None})})
        return types.SimpleNamespace(
            list_topics=lambda topic, timeout=0: topic_meta(topic),
            committed=lambda partitions, timeout=0: [types.SimpleNamespace(offset=0) for _ in partitions],
            get_watermark_offsets=lambda tp, timeout=0: (0, self.lag),
            close=lambda: None,
        )

    def admin_client(self):
        self.calls.append(("kafka", "admin"))
        return mock.MagicMock()  # no group, no topic listed: nothing to wait for

    def producer(self):
        self.calls.append(("kafka", "producer"))
        return mock.MagicMock()


class FakeHttp:
    """Kafka Connect and the registry: every call succeeds; Connect reports `connector_state`."""

    def __init__(self, calls: list, connector_state: str = "RUNNING"):
        self.calls, self.connector_state = calls, connector_state

    def _response(self, payload=None):
        return mock.MagicMock(status_code=200, ok=True, json=lambda: payload or {}, text="")

    def get(self, url, timeout=10, **kwargs):
        self.calls.append(("http", f"GET {url}"))
        state = self.connector_state
        status = {"connector": {"state": state}, "tasks": [{"id": 0, "state": state, "trace": "boom" if state == "FAILED" else ""}]}
        return self._response(status if url.endswith("/status") else {})

    def put(self, url, timeout=30, **kwargs):
        self.calls.append(("http", f"PUT {url}"))
        return self._response()

    def post(self, url, timeout=30, **kwargs):
        self.calls.append(("http", f"POST {url}"))
        return self._response()

    def delete(self, url, params=None, timeout=30):
        self.calls.append(("http", url))
        return self._response()


def fake_resources(
    calls: list, connector_state="RUNNING", lag=0, postgres_columns=None, risingwave_columns=None, **databricks_errors
) -> dict:
    return {
        "postgres": FakeDatabase(calls, "postgres", postgres_columns),
        "risingwave": FakeDatabase(calls, "risingwave", risingwave_columns),
        "databricks": FakeDatabricks(calls, **databricks_errors),
        "kafka": FakeKafka(calls, lag),
        "registry": FakeSchemaRegistry(),
        "http": FakeHttp(calls, connector_state),
    }


def kinds(calls: list) -> list:
    return [kind for kind, _ in calls]


def error_text(result) -> str:
    """The text of every step failure of a Dagster run result (including the causes)."""
    return " ".join(str(event.step_failure_data.error) for event in result.get_step_failure_events())
