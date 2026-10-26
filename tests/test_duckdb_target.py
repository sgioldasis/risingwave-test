"""The demo's DuckDB target: the topic's latest event per key mirrored into a local DuckDB file —
upserts land, tombstones delete, a field the table never saw adds a column, and a re-run (the
consumer is stateless) rebuilds the same mirror. Kafka and the registry are fakes (the Jobotron's
consume-asset pattern); the DuckDB file is real, in a tmp dir.

Run from the repo root:  .venv/bin/python -m pytest
"""

import json
import types

import dagster as dg
import duckdb
import pytest

from orchestration.assets.reverse_etl_config import duckdb_target_config
from orchestration.assets.reverse_etl_duckdb_target import build_duckdb_target_asset
from tests.fakes import FakeKafka, FakeSchemaRegistry, make_config


def event(key, op, after):
    """A Debezium envelope as the sync's json encoding writes it: key payload + change payload."""
    return (
        json.dumps({"payload": key}),
        json.dumps({"payload": {"op": op, "before": None, "after": after, "source": {"table": "t"}}}),
    )


class FakeConsumer:
    """Serves the topic's messages once; with `quiet_first` the first read comes back empty although
    the broker holds them — the cold-consumer race the drain loop guards against with watermarks."""

    def __init__(self, messages, quiet_first=False):
        self.messages, self.quiet_first, self.delivered = messages, quiet_first, False

    def assign(self, partitions):
        self.assigned = partitions

    def consume(self, num_messages=1, timeout=0):
        if self.quiet_first:
            self.quiet_first = False
            return []
        if self.delivered:
            return []
        self.delivered = True
        return [
            types.SimpleNamespace(
                error=lambda: None,
                key=lambda key=key: key,
                value=lambda value=value: value,
            )
            for key, value in self.messages
        ]

    def get_watermark_offsets(self, partition, timeout=0, cached=True):
        return (0, len(self.messages))

    def close(self):
        pass


class TargetKafka(FakeKafka):
    """The topic's messages are what the target's consumer reads back."""

    def consumer(self, group_id, extra=None):
        messages = [(key, value) for _, key, value in self.recording_producer.messages]
        return FakeConsumer(messages, self.quiet_first)


class Topic:
    """A topic with Debezium messages on it, plus the asset run against it."""

    def __init__(self, tmp_path, calls):
        self.cfg = duckdb_target_config(
            make_config(source_columns=(), key_column="id", duckdb_target_path=str(tmp_path / "mirror.duckdb")).targets_config()
        )
        self.kafka = TargetKafka(calls)
        self.kafka.quiet_first = False
        self.registry = FakeSchemaRegistry(calls)

    def produce(self, key, op, after):
        key_json, value_json = event(key, op, after)
        self.kafka.recording_producer.messages.append((self.cfg.kafka_topic, key_json, value_json))

    def run(self):
        return dg.materialize(
            [build_duckdb_target_asset(self.cfg)],
            resources={"kafka": self.kafka, "registry": self.registry},
            raise_on_error=False,
        )

    def rows(self, columns="*"):
        connection = duckdb.connect(self.cfg.duckdb_target_path, read_only=True)
        try:
            return connection.execute(f"SELECT {columns} FROM {self.cfg.kafka_topic} ORDER BY 1").fetchall()
        finally:
            connection.close()


@pytest.fixture
def topic(tmp_path, calls):
    return Topic(tmp_path, calls)


def test_the_mirror_holds_the_latest_event_per_key(topic):
    topic.produce({"id": 1}, "c", {"id": 1, "name": "a"})
    topic.produce({"id": 1}, "u", {"id": 1, "name": "a2"})
    topic.produce({"id": 2}, "c", {"id": 2, "name": "b"})
    assert topic.run().success
    assert topic.rows('"id", "name"') == [("1", "a2"), ("2", "b")]


def test_a_tombstone_removes_the_key(topic):
    topic.produce({"id": 1}, "c", {"id": 1, "name": "a"})
    topic.produce(1, "d", None)  # a scalar key payload is the key itself
    topic.produce({"id": 2}, "c", {"id": 2, "name": "b"})
    assert topic.run().success
    assert topic.rows('"id", "name"') == [("2", "b")]


def test_a_field_no_event_has_seen_yet_adds_a_column(topic):
    topic.produce({"id": 1}, "c", {"id": 1, "name": "a"})
    assert topic.run().success
    topic.produce({"id": 2}, "c", {"id": 2, "name": "b", "born": "2020"})
    assert topic.run().success
    # The new column exists; a row whose last event did not carry it stays NULL.
    assert topic.rows('"id", "name", "born"') == [("1", "a", None), ("2", "b", "2020")]


def test_a_second_run_rebuilds_the_same_mirror(topic):
    topic.produce({"id": 1}, "c", {"id": 1, "name": "a"})
    topic.produce({"id": 1}, "u", {"id": 1, "name": "a2"})
    assert topic.run().success
    first = topic.rows('"id", "name"')
    assert topic.run().success
    assert topic.rows('"id", "name"') == first == [("1", "a2")]


def test_a_quiet_first_read_reads_again_instead_of_mirroring_nothing(topic):
    topic.produce({"id": 1}, "c", {"id": 1, "name": "a"})
    topic.kafka.quiet_first = True  # the cold-consumer race: empty first read, messages exist
    assert topic.run().success
    assert topic.rows('"id", "name"') == [("1", "a")]
