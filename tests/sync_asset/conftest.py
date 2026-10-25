"""Fixtures for the sync asset tests: the storage mode and encoding to run under, the source table, the fake
watermark table and registry, and `sync`, which runs the real sync asset once and decodes what it produced.

See support.py for the two storage modes.
"""

import json

import dagster as dg
import pytest
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from orchestration.assets import reverse_etl_delta_reader as reader
from orchestration.assets.reverse_etl_demo_hooks import DemoSyncHooks
from tests.fakes import FakeSchemaRegistry, make_config, use_local_azure, use_local_paths
from tests.sync_asset.support import (
    ABFS_LOCATION,
    FAST_PATH,
    SNAPSHOT_DIFF,
    Produced,
    SourceTable,
)


@pytest.fixture(autouse=True)
def no_backfill_override(monkeypatch):
    monkeypatch.delenv("REVERSE_ETL_BACKFILL_FROM_VERSION", raising=False)


@pytest.fixture(params=[FAST_PATH, SNAPSHOT_DIFF])
def storage(request) -> str:
    return request.param


@pytest.fixture(params=["json", "avro"])
def encoding(request) -> str:
    return request.param


@pytest.fixture
def source(tmp_path, storage, monkeypatch) -> SourceTable:
    """The source table, with the reader pointed at it: a local path, or an abfss:// location backed by a local
    directory."""
    if storage == SNAPSHOT_DIFF:
        use_local_paths(monkeypatch, (cdf_setup, reader))
        path = tmp_path / "source_table"
        return SourceTable(path, str(path), column_mapping=False)

    path = tmp_path / "testcontainer" / "table"
    path.parent.mkdir()
    use_local_azure(monkeypatch, tmp_path, (cdf_setup, reader))
    return SourceTable(path, ABFS_LOCATION, column_mapping=True)


@pytest.fixture
def state_rows() -> list:
    """The sync's watermark table in the fake Postgres; it outlives a run, like the real one."""
    return []


@pytest.fixture
def registry(calls) -> FakeSchemaRegistry:
    """One registry per test, shared by its runs, so subjects registered by one sync are there for the next."""
    return FakeSchemaRegistry(calls)


@pytest.fixture
def sync(source, calls, resources, state_rows, registry, encoding):
    """Run the sync asset once; returns (result, the fake resources, the messages produced, decoded).
    Keyword options go to the fakes; `propagate_drops` turns on schema-change propagation to the targets."""
    deserializer = AvroDeserializer(registry.mock_client)

    def decode(topic, key, value) -> Produced:
        if encoding == "json":
            value_json = json.loads(value)
            return Produced(topic, json.loads(key)["payload"], value_json["payload"], value, value_json["schema"])
        return Produced(
            topic,
            deserializer(key, SerializationContext(topic, MessageField.KEY)),
            deserializer(value, SerializationContext(topic, MessageField.VALUE)),
            value,
            None,
        )

    def run(
        propagate_drops=False, target_columns=("id", "name", "born"), hooks=DemoSyncHooks(),
        state_backend="postgres", state_path="", **options,
    ):
        """`hooks=None` runs the bare core (what a team's sync would be); the default is the demo wiring."""
        cfg = make_config(
            source_columns=(), key_column="id", encoding=encoding, schema_registry_url="http://registry.invalid",
            propagate_drops=propagate_drops, state_backend=state_backend, state_path=state_path,
        )
        # Mirror the demo's wiring (reverse_etl_defs.py): the demo hooks reach risingwave/http and
        # the Postgres target, so those must be declared required; the bare core and test hooks
        # touch none of them and run without them.
        extra = ("risingwave", "http", "postgres") if isinstance(hooks, DemoSyncHooks) else ()
        # The sync's precondition asks the admin for the sync's topic; a test overrides
        # kafka_admin_topics=() to get the missing-topic failure.
        options.setdefault("kafka_admin_topics", (cfg.kafka_topic,))
        res = resources(
            table_location=source.location,
            state_rows=state_rows,
            risingwave_columns=list(target_columns),
            postgres_columns=list(target_columns),
            registry=registry,
            **options,
        )
        result = dg.materialize(
            [cdf_setup.build_sync_asset(cfg, hooks=hooks, extra_resource_keys=extra)], resources=res, raise_on_error=False
        )
        produced = [decode(*message) for message in res["kafka"].recording_producer.messages]
        return result, res, produced

    return run
