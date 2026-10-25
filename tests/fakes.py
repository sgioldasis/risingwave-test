"""Fake resources for the reverse-ETL tests, bound under the keys the project registers (postgres, risingwave,
databricks, kafka, http, registry; see orchestration/assets/reverse_etl_resources.py). Each fake records what it
was asked to do in one shared `calls` list of (kind, detail) tuples, so a test can assert what happened and in
which order."""

import dataclasses
import types
from unittest import mock

import pyarrow.fs as pafs
from confluent_kafka.schema_registry import SchemaRegistryClient
from deltalake import DeltaTable

from orchestration.assets.reverse_etl_config import ReverseEtlSyncConfig


def make_config(source_columns=("description STRING",), key_column="rid", **overrides) -> ReverseEtlSyncConfig:
    cfg = ReverseEtlSyncConfig.for_name(
        "unittest", catalog="de_dev", schema="sr_poc_external", source_columns=source_columns, key_column=key_column
    )
    return dataclasses.replace(cfg, **overrides) if overrides else cfg


class FakeDatabase:
    """Stands in for PostgresResource and RisingWaveResource: records each statement under `kind`.
    `columns` is what an information_schema.columns query returns (the table's current columns).
    `state_rows` is the sync's watermark table, a list of (last_commit_version, last_schema): the sync's state
    read returns it freshest first and its state write appends to it."""

    def __init__(self, calls: list, kind: str, columns: list | None = None, state_rows: list | None = None):
        self.calls, self.kind, self.columns = calls, kind, columns or []
        self.state_rows = state_rows if state_rows is not None else []

    def connect_with_timeout(self):
        return self.connect()

    def connect(self):
        cursor = mock.MagicMock()
        result: list = []

        def execute(sql, params=None):
            text = " ".join(str(sql).split())
            self.calls.append((self.kind, text))
            result.clear()
            if text.startswith("SELECT last_commit_version, last_schema FROM reverse_etl_sync_state"):
                result.extend(sorted(self.state_rows, key=lambda row: -row[0]))
            elif text.startswith("INSERT INTO reverse_etl_sync_state"):
                self.state_rows.append((params[1], params[2]))
            else:
                result.extend((name, "character varying") for name in self.columns)

        cursor.execute.side_effect = execute
        cursor.fetchall.side_effect = lambda: list(result)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        return connection


class FakeDatabricks:
    def __init__(self, calls: list, sql_error=None, location_error=None, table_location="abfss://x", table_columns=()):
        self.calls, self.sql_error, self.location_error = calls, sql_error, location_error
        self.location = table_location
        self.table_columns = table_columns  # what DESCRIBE TABLE reports

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
        if statement.startswith("DESCRIBE TABLE"):
            return {
                "manifest": {"schema": {"columns": [{"name": "col_name"}]}},
                "result": {"data_array": [[name] for name in self.table_columns]},
            }
        return {}

    def table_location(self, token, cfg, fqn):
        if self.location_error:
            raise self.location_error
        self.calls.append(("location", fqn))
        return self.location

    def table_location_cached(self, token, cfg, fqn):
        self.calls.append(("location", fqn))
        return self.location

    def clear_location_cache(self):
        self.calls.append(("cache", "cleared"))


class FakeSchemaRegistry:
    """An in-memory registry (confluent-kafka's mock client), so Avro serializers and deserializers really work
    against it. Hand the same instance to several runs to keep its subjects between them. With `calls`, every
    registration request is recorded as ("registry", subject), in order with the other fakes' calls."""

    def __init__(self, calls: list | None = None):
        self.mock_client = SchemaRegistryClient.new_client({"url": "mock://reverse-etl-tests"})
        if calls is not None:
            register = self.mock_client.register_schema_full_response

            def recording(subject_name, schema, normalize_schemas=False):
                calls.append(("registry", subject_name))
                return register(subject_name, schema, normalize_schemas)

            self.mock_client.register_schema_full_response = recording

    def client(self, url):
        return self.mock_client


class RecordingProducer:
    """A Kafka producer that keeps what it was given. With `fail`, every delivery is reported as rejected."""

    def __init__(self, fail: bool = False):
        self.fail, self.messages, self.flushes = fail, [], 0

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, value))
        if on_delivery is not None and self.fail:
            on_delivery("broker unavailable", None)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flushes += 1
        return 0


class FakeAdmin:
    """A Kafka admin client with topics and consumer groups. `group_failures` is used up one per attempt to delete a
    consumer group (an exception to raise, or None to succeed); once it runs out, deletes succeed. With
    `topics_linger`, a deleted topic stays listed, as one still being removed on the broker does."""

    def __init__(self, topics=(), group_failures=(), topics_linger=False):
        self.topics, self.topics_linger = set(topics), topics_linger
        self.created, self.deleted = [], []
        self.group_failures, self.group_attempts = list(group_failures), 0

    def list_topics(self, timeout=0):
        return types.SimpleNamespace(topics={topic: None for topic in self.topics})

    def create_topics(self, new_topics):
        self.created += [new_topic.topic for new_topic in new_topics]
        self.topics |= {new_topic.topic for new_topic in new_topics}
        return {}

    def delete_topics(self, topics, operation_timeout=0):
        self.deleted += topics
        if not self.topics_linger:
            self.topics -= set(topics)
        return {topic: types.SimpleNamespace(result=lambda: None) for topic in topics}

    def delete_consumer_groups(self, groups):
        self.group_attempts += 1
        failure = self.group_failures.pop(0) if self.group_failures else None

        def result():
            if failure is not None:
                raise failure

        return {group: types.SimpleNamespace(result=result) for group in groups}


class FakeKafka:
    """With `admin`, admin_client() returns it; without, a mock that finds no group and no topic."""

    def __init__(self, calls: list, lag: int = 0, producer_fails: bool = False, admin: FakeAdmin | None = None):
        self.calls, self.lag, self.admin = calls, lag, admin
        self.recording_producer = RecordingProducer(fail=producer_fails)

    def consumer(self, group_id, extra=None):
        def topic_meta(topic):
            return types.SimpleNamespace(topics={topic: types.SimpleNamespace(partitions={0: None})})

        return types.SimpleNamespace(
            list_topics=lambda topic, timeout=0: topic_meta(topic),
            committed=lambda partitions, timeout=0: [types.SimpleNamespace(offset=0) for _ in partitions],
            get_watermark_offsets=lambda tp, timeout=0: (0, self.lag),
            close=lambda: None,
        )

    def admin_client(self):
        self.calls.append(("kafka", "admin"))
        return self.admin if self.admin is not None else mock.MagicMock()  # no group, no topic listed

    def producer(self):
        self.calls.append(("kafka", "producer"))
        return self.recording_producer


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
    calls: list,
    connector_state="RUNNING",
    lag=0,
    postgres_columns=None,
    risingwave_columns=None,
    state_rows=None,
    producer_fails=False,
    table_location="abfss://x",
    registry=None,
    kafka=None,
    **databricks_errors,
) -> dict:
    return {
        "postgres": FakeDatabase(calls, "postgres", postgres_columns, state_rows),
        "risingwave": FakeDatabase(calls, "risingwave", risingwave_columns),
        "databricks": FakeDatabricks(calls, table_location=table_location, **databricks_errors),
        "kafka": kafka if kafka is not None else FakeKafka(calls, lag, producer_fails),
        "registry": registry or FakeSchemaRegistry(),
        "http": FakeHttp(calls, connector_state),
    }


def kinds(calls: list) -> list:
    return [kind for kind, _ in calls]


def error_text(result) -> str:
    """The text of every step failure of a Dagster run result (including the causes)."""
    return " ".join(str(event.step_failure_data.error) for event in result.get_step_failure_events())


# --- Delta storage ----------------------------------------------------------------------------------------------------

AZURE_CREDENTIALS = {"account_name": "testaccount", "client_id": "id", "tenant_id": "tenant", "client_secret": "secret"}


def use_local_paths(monkeypatch, modules) -> None:
    """Read Delta tables from plain local paths: no storage options, so the reader finds no Azure filesystem and takes
    its snapshot-diff fallback."""
    for module in modules:
        monkeypatch.setattr(module, "_azure_storage_options", lambda location: {})


def use_local_azure(monkeypatch, root, modules) -> None:
    """Make abfss:// locations resolve to `root`/testcontainer/<path> on local disk, with fake credentials and a local
    stand-in for pyarrow's AzureFileSystem: the reader's production fast path, without Azure. The caller creates the
    `testcontainer` directory under `root`."""

    def local_table(location, storage_options=None, version=None):
        return DeltaTable(str(root / "testcontainer" / location.split(".dfs.core.windows.net/", 1)[1]), version=version)

    for module in modules:
        monkeypatch.setattr(module, "DeltaTable", local_table)
        monkeypatch.setattr(module, "_azure_storage_options", lambda location: AZURE_CREDENTIALS)
    monkeypatch.setattr(pafs, "AzureFileSystem", lambda **options: pafs.SubTreeFileSystem(str(root), pafs.LocalFileSystem()))
