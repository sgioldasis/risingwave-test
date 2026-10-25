"""The sync asset against a real Delta table, for both encodings and both of the reader's read paths.

The source is a Delta table written with delta-rs into a temp directory. The sync asset, the delta reader, the event
building and the Kafka serialization (Connect JSON, or Avro in the Confluent wire format) are the real code. Postgres
(the watermark), Kafka and RisingWave are fakes, and the schema registry is confluent-kafka's in-memory mock, so the
produced Avro bytes are decoded back with the real deserializer (tests/fakes.py).

Two storage modes, because the reader has two paths:

- `azure_fast_path` is the production path. The table is column-mapped, like one Databricks writes, and has an
  abfss:// location, so the reader reads the parquet files itself and renames physical to logical columns. Azure is
  replaced by a local directory: the location is mapped to a temp path and pyarrow's AzureFileSystem by a local one.
- `snapshot_diff` is the reader's fallback (partitioned tables, or no Azure filesystem): a plain table on a local path,
  diffed snapshot against snapshot.

delta-rs cannot rename, drop or add columns on a column-mapped table, so those are emulated the way Spark records them:
a commit holding only a new metaData action, with each column keeping its mapping id and physical name.

Run from the repo root:  .venv/bin/python -m pytest
"""

import datetime
import json
import os
import struct
import time
import uuid
from typing import NamedTuple

import dagster as dg
import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import DeltaTable, write_deltalake

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from orchestration.assets import reverse_etl_delta_reader as reader
from tests.fakes import FakeSchemaRegistry, error_text, make_config

FAST_PATH, SNAPSHOT_DIFF = "azure_fast_path", "snapshot_diff"
SCHEMA = pa.schema(
    [pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string()), pa.field("born", pa.date32())]
)
DAY = datetime.date(2012, 1, 1)
DAY_NUMBER = 15340  # days from 1970-01-01 to 2012-01-01
TOPIC = "reverse_etl_unittest_topic"
ABFS_LOCATION = "abfss://testcontainer@testaccount.dfs.core.windows.net/table"
MAPPING = {"delta.columnMapping.mode": "name", "delta.enableDeletionVectors": "false"}


class SourceTable:
    """A local Delta table; every call is one commit."""

    def __init__(self, path, location: str, column_mapping: bool):
        self.path, self.location, self.column_mapping = str(path), location, column_mapping

    def _arrow_schema(self) -> pa.Schema:
        return pa.schema(DeltaTable(self.path).schema().to_arrow())

    def create(self, *rows):
        write_deltalake(
            self.path, pa.Table.from_pylist(list(rows), schema=SCHEMA), configuration=MAPPING if self.column_mapping else None
        )

    def insert(self, *rows):
        write_deltalake(self.path, pa.Table.from_pylist(list(rows), schema=self._arrow_schema()), mode="append")

    def update(self, row_id, **values):
        updates = {column: f"'{value}'" for column, value in values.items()}
        DeltaTable(self.path).update(updates=updates, predicate=f"id = {row_id}")

    def delete(self, row_id):
        DeltaTable(self.path).delete(f"id = {row_id}")

    def _schema_commit(self, operation: str, change, max_column_id: int | None = None):
        """One commit holding only a metaData action with the changed schema (what Spark writes for ALTER TABLE)."""
        table = DeltaTable(self.path)
        metadata = table.metadata()
        schema = json.loads(table.schema().to_json())
        change(schema["fields"])
        configuration = dict(metadata.configuration)
        if max_column_id is not None:
            configuration["delta.columnMapping.maxColumnId"] = str(max_column_id)
        actions = [
            {"commitInfo": {"timestamp": int(time.time() * 1000), "operation": operation, "isBlindAppend": False}},
            {
                "metaData": {
                    "id": metadata.id,
                    "name": metadata.name,
                    "description": metadata.description,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": json.dumps(schema),
                    "partitionColumns": list(metadata.partition_columns),
                    "createdTime": metadata.created_time,
                    "configuration": configuration,
                }
            },
        ]
        log = os.path.join(self.path, "_delta_log", f"{table.version() + 1:020d}.json")
        with open(log, "w") as handle:
            handle.write("\n".join(json.dumps(action) for action in actions) + "\n")

    def add_column(self, name: str, type_name: str = "string"):
        max_id = int(DeltaTable(self.path).metadata().configuration["delta.columnMapping.maxColumnId"])

        def change(fields):
            fields.append(
                {
                    "name": name,
                    "type": type_name,
                    "nullable": True,
                    "metadata": {
                        "delta.columnMapping.id": max_id + 1,
                        "delta.columnMapping.physicalName": f"col-{uuid.uuid4()}",
                    },
                }
            )

        self._schema_commit("ADD COLUMNS", change, max_column_id=max_id + 1)

    def rename_column(self, old: str, new: str):
        def change(fields):
            for field in fields:
                if field["name"] == old:
                    field["name"] = new

        self._schema_commit("RENAME COLUMN", change)

    def drop_column(self, name: str):
        self._schema_commit("DROP COLUMN", lambda fields: fields.__setitem__(slice(None), [f for f in fields if f["name"] != name]))

    def expire_history(self):
        """What log retention does: a checkpoint at the current version, then the older commit files are gone, so
        only the current version can still be loaded."""
        table = DeltaTable(self.path)
        table.create_checkpoint()
        for version in range(table.version()):
            os.remove(os.path.join(self.path, "_delta_log", f"{version:020d}.json"))

    def insert_with_new_column(self, column: str, *rows):
        """Add a column to the table, then append rows that carry it (two commits)."""
        if self.column_mapping:
            self.add_column(column)
            self.insert(*rows)
        else:
            write_deltalake(self.path, pa.Table.from_pylist(list(rows)), mode="append", schema_mode="merge")


def row(row_id, name, born=None, **more):
    return {"id": row_id, "name": name, "born": born, **more}


class Produced(NamedTuple):
    topic: str
    key: dict  # the key's fields
    value: dict  # the Debezium envelope: op, before, after, source
    raw_value: bytes
    schema: dict | None  # the embedded Connect schema (JSON only)


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
        monkeypatch.setattr(cdf_setup, "_azure_storage_options", lambda location: {})
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})
        path = tmp_path / "source_table"
        return SourceTable(path, str(path), column_mapping=False)

    path = tmp_path / "testcontainer" / "table"
    path.parent.mkdir()
    credentials = {"account_name": "testaccount", "client_id": "id", "tenant_id": "tenant", "client_secret": "secret"}

    def local_table(location, storage_options=None, version=None):
        return DeltaTable(str(tmp_path / "testcontainer" / location.split(".dfs.core.windows.net/", 1)[1]), version=version)

    for module in (cdf_setup, reader):
        monkeypatch.setattr(module, "DeltaTable", local_table)
        monkeypatch.setattr(module, "_azure_storage_options", lambda location: credentials)
    monkeypatch.setattr(pafs, "AzureFileSystem", lambda **options: pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem()))
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

    def run(propagate_drops=False, target_columns=("id", "name", "born"), **options):
        cfg = make_config(
            source_columns=(), key_column="id", encoding=encoding, schema_registry_url="http://registry.invalid",
            propagate_drops=propagate_drops,
        )
        res = resources(
            table_location=source.location,
            state_rows=state_rows,
            risingwave_columns=list(target_columns),
            postgres_columns=list(target_columns),
            registry=registry,
            **options,
        )
        result = dg.materialize([cdf_setup.build_sync_asset(cfg)], resources=res, raise_on_error=False)
        produced = [decode(*message) for message in res["kafka"].recording_producer.messages]
        return result, res, produced

    return run


def events(produced) -> list:
    """(op, key, before name, after name, commit version) per message, in the order produced."""
    return [
        (
            p.value["op"],
            p.key["id"],
            (p.value["before"] or {}).get("name"),
            (p.value["after"] or {}).get("name"),
            p.value["source"]["commit_version"],
        )
        for p in produced
    ]


def metadata(result, name):
    return result.get_asset_materialization_events()[0].materialization.metadata[name].value


def statements(calls, kind) -> list:
    return [detail for k, detail in calls if k == kind]


# --- what is sent: every storage mode, both encodings -------------------------------------------------------------


def test_the_first_run_sends_every_row_as_a_create_and_records_the_watermark(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"), row(3, "c"))
    result, _, produced = sync()
    assert result.success
    assert sorted(events(produced)) == [("c", 1, None, "a", 0), ("c", 2, None, "b", 0), ("c", 3, None, "c", 0)]
    assert {p.topic for p in produced} == {TOPIC}
    assert [version for version, _ in state_rows] == [0]
    assert metadata(result, "events_synced") == 3


def test_later_commits_arrive_as_insert_update_and_delete_in_commit_order(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"))
    sync()
    source.insert(row(4, "d"))  # version 1
    source.update(1, name="a2")  # version 2
    source.delete(2)  # version 3
    result, _, produced = sync()
    assert result.success
    assert events(produced) == [("c", 4, None, "d", 1), ("u", 1, "a", "a2", 2), ("d", 2, "b", None, 3)]
    assert state_rows[-1][0] == 3
    assert metadata(result, "op_counts") == {"c": 1, "u": 1, "d": 1}


def test_a_run_with_no_new_commits_produces_nothing_and_leaves_the_watermark(source, sync, calls, state_rows):
    source.create(row(1, "a"))
    sync()
    watermark, producers = list(state_rows), calls.count(("kafka", "producer"))
    result, _, produced = sync()
    assert result.success
    assert produced == []
    assert calls.count(("kafka", "producer")) == producers  # not even a producer was created
    assert state_rows == watermark
    assert metadata(result, "events_synced") == 0


def test_a_sync_resumes_after_the_stored_watermark(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"))  # version 0
    source.update(1, name="a2")  # version 1
    source.delete(2)  # version 2
    state_rows.append((1, None))  # versions 0 and 1 were already synced
    result, _, produced = sync()
    assert result.success
    assert events(produced) == [("d", 2, "b", None, 2)]
    assert state_rows[-1][0] == 2


def test_dates_arrive_as_dates_in_both_encodings(source, sync, encoding):
    source.create(row(1, "a", DAY))
    result, _, produced = sync()
    assert result.success
    born = produced[0].value["after"]["born"]
    if encoding == "json":
        # Connect JSON: epoch days with the Debezium date schema
        after_fields = next(f for f in produced[0].schema["fields"] if f["field"] == "after")["fields"]
        field = next(f for f in after_fields if f["field"] == "born")
        assert (field["type"], field["name"]) == ("int32", "io.debezium.time.Date")
        assert born == DAY_NUMBER
    else:
        # Avro: an int with the date logical type, which the deserializer turns back into a date
        assert born == DAY


def test_a_rejected_delivery_fails_the_run_before_the_watermark_moves(source, sync, state_rows):
    source.create(row(1, "a"))
    result, _, _ = sync(producer_fails=True)
    assert not result.success
    assert "Kafka delivery" in error_text(result)
    assert state_rows == []  # the next run replays the same versions

    result, _, produced = sync()  # the broker is back
    assert result.success
    assert events(produced) == [("c", 1, None, "a", 0)]
    assert [version for version, _ in state_rows] == [0]


def test_a_new_source_column_is_added_to_risingwave_and_carried_in_the_messages(source, sync, calls, registry, encoding):
    source.create(row(1, "a"))
    sync()
    source.insert_with_new_column("email", {"id": 5, "name": "e", "born": None, "email": "e@example.com"})
    result, _, produced = sync()
    assert result.success
    assert [p.value["after"]["email"] for p in produced] == ["e@example.com"]
    added = [sql for kind, sql in calls if kind == "risingwave" and "ADD COLUMN" in sql]
    assert len(added) == 1 and "email" in added[0]
    assert metadata(result, "risingwave_columns_added") == ["email"]
    if encoding == "avro":  # the registry's latest value schema has the column RisingWave was just told about
        assert "email" in registry.mock_client.get_latest_version(f"{TOPIC}-value").schema.schema_str


# --- Avro only -----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("encoding", ["avro"])
def test_avro_messages_use_the_confluent_wire_format(source, sync, registry, encoding):
    source.create(row(1, "a"))
    _, _, produced = sync()
    magic, schema_id = struct.unpack(">bI", produced[0].raw_value[:5])
    assert magic == 0
    assert schema_id == registry.mock_client.get_latest_version(f"{TOPIC}-value").schema_id


@pytest.mark.parametrize("encoding", ["avro"])
def test_each_avro_schema_is_registered_once_across_runs_without_a_schema_change(source, sync, registry, encoding):
    source.create(row(1, "a"))
    sync()
    source.update(1, name="a2")
    sync()
    for subject in (f"{TOPIC}-key", f"{TOPIC}-value"):
        assert registry.mock_client.get_versions(subject) == [1]


@pytest.mark.parametrize("encoding", ["avro"])
def test_the_new_avro_schema_is_registered_before_risingwave_gets_the_column(source, sync, calls, encoding):
    """RisingWave checks a new column against the registry's latest schema, so the registry must come first."""
    source.create(row(1, "a"))
    sync()
    start = len(calls)
    source.insert_with_new_column("email", {"id": 5, "name": "e", "born": None, "email": "e@example.com"})
    sync()
    later = calls[start:]
    registered = next(i for i, c in enumerate(later) if c == ("registry", f"{TOPIC}-value"))
    altered = next(i for i, c in enumerate(later) if c[0] == "risingwave" and "ADD COLUMN" in c[1])
    assert registered < altered


# --- schema changes: column-mapped tables on the production fast path -----------------------------------------------


@pytest.mark.parametrize("storage", [FAST_PATH])
class TestSchemaChangesOnAColumnMappedTable:
    """A column keeps its mapping id through a rename, which is how the sync tells a rename from a drop."""

    def test_a_renamed_source_column_is_renamed_on_both_targets(self, source, sync, calls, state_rows, storage):
        source.create(row(1, "a"), row(2, "b"))
        sync(propagate_drops=True)
        source.rename_column("name", "title")  # version 1: a schema commit, no data change
        result, _, produced = sync(propagate_drops=True)
        assert result.success
        assert produced == []  # no row changed
        assert metadata(result, "columns_renamed") == ["name -> title"]
        assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "name" TO "title"' in s]
        assert [s for s in statements(calls, "risingwave") if "UPDATE" in s and "title" in s and "name" in s]
        assert [s for s in statements(calls, "http") if s.startswith("POST") and "/tasks/0/restart" in s]
        assert state_rows[-1][0] == 1
        assert [column["name"] for column in json.loads(state_rows[-1][1])] == ["id", "title", "born"]

    def test_a_dropped_source_column_is_dropped_from_both_targets(self, source, sync, calls, state_rows, storage):
        source.create(row(1, "a", DAY))
        sync(propagate_drops=True)
        source.drop_column("born")  # version 1
        result, _, produced = sync(propagate_drops=True)
        assert result.success
        assert produced == []
        assert metadata(result, "columns_dropped") == {"risingwave": ["born"], "postgres": ["born"]}
        assert [s for s in statements(calls, "postgres") if 'DROP COLUMN "born"' in s]
        assert [s for s in statements(calls, "risingwave") if "DROP COLUMN" in s and "born" in s]
        assert state_rows[-1][0] == 1
        assert [column["name"] for column in json.loads(state_rows[-1][1])] == ["id", "name"]

    def test_nothing_is_changed_on_the_targets_when_drop_propagation_is_off(self, source, sync, calls, storage):
        source.create(row(1, "a"))
        sync()
        source.drop_column("born")
        result, _, _ = sync()  # propagate_drops is off by default
        assert result.success
        assert not [s for s in statements(calls, "postgres") + statements(calls, "risingwave") if "DROP COLUMN" in s]

    def test_rows_inserted_after_a_rename_carry_the_new_column_name(self, source, sync, calls, encoding, storage):
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.rename_column("name", "title")  # version 1
        source.insert({"id": 2, "title": "b", "born": None})  # version 2, in the same unsynced window
        result, _, produced = sync(propagate_drops=True)
        assert result.success
        assert [(p.key["id"], p.value["after"]["title"]) for p in produced] == [(2, "b")]
        assert "name" not in produced[0].value["after"]
        assert metadata(result, "columns_renamed") == ["name -> title"]

    def test_a_rename_keeps_the_data_instead_of_dropping_the_old_column(self, source, sync, calls, storage):
        """A rename taken for a drop would lose the column's values: Postgres must only rename, and RisingWave
        (which cannot rename) must copy the values into the new column before it drops the old one."""
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.rename_column("name", "title")
        result, _, _ = sync(propagate_drops=True)
        assert result.success
        assert not [s for s in statements(calls, "postgres") if "DROP COLUMN" in s]
        risingwave = statements(calls, "risingwave")
        copied = next(i for i, s in enumerate(risingwave) if "UPDATE" in s and "title" in s)
        dropped = next(i for i, s in enumerate(risingwave) if "DROP COLUMN" in s and "name" in s)
        assert copied < dropped

    @pytest.mark.parametrize("encoding", ["avro"])
    def test_the_avro_schema_registered_for_a_rename_keeps_every_column_the_table_still_has(
        self, source, sync, registry, encoding, storage
    ):
        """RisingWave rebinds the table to the registry's latest schema while it still has the old column, so the
        schema registered for a rename must carry both the old and the new name."""
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.rename_column("name", "title")
        sync(propagate_drops=True)
        latest = registry.mock_client.get_latest_version(f"{TOPIC}-value").schema.schema_str
        assert "title" in latest and '"name"' in latest


# --- several schema changes inside one unsynced window ---------------------------------------------------------------
#
# The deployment reads one version per chunk (REVERSE_ETL_CDF_CHUNK_VERSIONS=1); the code default is 10. Each of
# these was a bug found live on 2026-10-19 (see the comments in the sync asset).


@pytest.fixture
def one_version_per_chunk(monkeypatch):
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", 1)


def recorded_schema(state_rows) -> list:
    """(name, mapping id) per column in the newest watermark row: the schema the next sync diffs against."""
    return [(column["name"], column["id"]) for column in json.loads(state_rows[-1][1])]


@pytest.mark.usefixtures("one_version_per_chunk")
@pytest.mark.parametrize("storage", [FAST_PATH])
class TestSeveralSchemaChangesInOneWindow:
    WITH_EMAIL = ("id", "name", "born", "email")

    def test_a_column_added_populated_and_renamed_in_one_window_keeps_its_value_and_is_renamed(
        self, source, sync, calls, state_rows, storage
    ):
        source.create(row(1, "a"))
        sync(propagate_drops=True, target_columns=self.WITH_EMAIL)
        source.add_column("email")  # version 1
        source.insert(row(5, "e", email="e@example.com"))  # version 2
        source.rename_column("email", "contact")  # version 3
        result, _, produced = sync(propagate_drops=True, target_columns=self.WITH_EMAIL)
        assert result.success
        assert [(p.key["id"], p.value["after"]["email"]) for p in produced] == [(5, "e@example.com")]  # not lost
        assert metadata(result, "columns_renamed") == ["email -> contact"]
        assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "email" TO "contact"' in s]
        assert state_rows[-1][0] == 3
        assert recorded_schema(state_rows) == [("id", 1), ("name", 2), ("born", 3), ("contact", 4)]

    def test_a_rename_after_the_last_data_event_still_reaches_the_targets(self, source, sync, calls, state_rows, storage):
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.insert(row(2, "b"))  # version 1: the last data event
        source.rename_column("name", "title")  # version 2: after it
        result, _, produced = sync(propagate_drops=True)
        assert result.success
        assert [(p.key["id"], p.value["after"]["name"]) for p in produced] == [(2, "b")]
        assert metadata(result, "columns_renamed") == ["name -> title"]
        assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "name" TO "title"' in s]
        assert state_rows[-1][0] == 2

    def test_the_recorded_schema_pairs_names_and_ids_from_the_same_commit(self, source, sync, state_rows, storage):
        """An id of None next to a name would read as a drop at the next sync."""
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.insert(row(2, "b"))
        source.rename_column("name", "title")
        sync(propagate_drops=True)
        assert recorded_schema(state_rows) == [("id", 1), ("title", 2), ("born", 3)]

    def test_a_column_added_and_dropped_inside_one_window_leaves_the_targets_alone(
        self, source, sync, calls, state_rows, storage
    ):
        source.create(row(1, "a"))
        sync(propagate_drops=True)
        source.add_column("scratch")  # version 1
        source.drop_column("scratch")  # version 2
        result, _, produced = sync(propagate_drops=True)
        assert result.success
        assert produced == []
        assert not [s for s in statements(calls, "postgres") + statements(calls, "risingwave") if "DROP COLUMN" in s or "RENAME" in s]
        assert state_rows[-1][0] == 2

    def test_a_rename_and_a_drop_in_one_window_both_reach_the_targets(self, source, sync, calls, state_rows, storage):
        source.create(row(1, "a", DAY))
        sync(propagate_drops=True)
        source.rename_column("name", "title")  # version 1
        source.drop_column("born")  # version 2
        result, _, _ = sync(propagate_drops=True)
        assert result.success
        assert metadata(result, "columns_renamed") == ["name -> title"]
        assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "name" TO "title"' in s]
        assert [s for s in statements(calls, "postgres") if 'DROP COLUMN "born"' in s]
        assert [s for s in statements(calls, "risingwave") if "DROP COLUMN" in s and "born" in s]
        assert recorded_schema(state_rows) == [("id", 1), ("title", 2)]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Known limitation at the code-default chunk size (10): a chunk spanning several versions is read with the schema "
        "of its top version, so a row written before a rename in the same chunk is produced with its value dropped "
        "(title: None). The deployment sets REVERSE_ETL_CDF_CHUNK_VERSIONS=1, where this is covered above."
    ),
)
@pytest.mark.parametrize("storage", [FAST_PATH])
def test_a_rename_in_the_same_chunk_as_earlier_rows_keeps_their_values(source, sync, monkeypatch, storage):
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", 10)
    source.create(row(1, "a"))
    sync(propagate_drops=True)
    source.insert(row(2, "b"))  # version 1
    source.rename_column("name", "title")  # version 2, same chunk
    _, _, produced = sync(propagate_drops=True)
    assert [p.value["after"].get("title", p.value["after"].get("name")) for p in produced] == ["b"]


# --- first run when the log history has aged out ----------------------------------------------------------------------


def test_a_first_run_after_the_history_aged_out_baselines_instead_of_backfilling(source, sync, calls, state_rows):
    source.create(row(1, "a"))  # version 0
    source.insert(row(2, "b"))  # version 1
    source.insert(row(3, "c"))  # version 2
    source.expire_history()  # only version 2 can still be loaded
    result, _, produced = sync()
    assert result.success
    assert produced == []  # a silently incomplete picture is never backfilled
    assert ("kafka", "producer") not in calls
    assert [version for version, _ in state_rows] == [2]

    source.insert(row(4, "d"))  # version 3
    result, _, produced = sync()
    assert result.success
    assert events(produced) == [("c", 4, None, "d", 3)]


# --- reading a backlog in chunks ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("chunk_versions", [1, 2, 3, 10])
def test_a_backlog_is_sent_in_commit_order_whatever_the_chunk_size_and_flushed_once(
    source, sync, state_rows, monkeypatch, chunk_versions
):
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", chunk_versions)
    source.create(row(1, "a"))
    sync()
    for row_id in range(2, 7):  # versions 1 to 5
        source.insert(row(row_id, f"n{row_id}"))
    result, res, produced = sync()
    assert result.success
    assert events(produced) == [("c", row_id, None, f"n{row_id}", row_id - 1) for row_id in range(2, 7)]
    assert res["kafka"].recording_producer.flushes == 1  # one delivery checkpoint for the whole backlog
    assert state_rows[-1][0] == 5
    assert metadata(result, "op_counts") == {"c": 5}


@pytest.mark.parametrize("storage", [FAST_PATH])
@pytest.mark.parametrize("chunk_versions", [1, 2])
def test_a_schema_only_chunk_at_the_end_still_advances_the_watermark(source, sync, calls, state_rows, monkeypatch, storage, chunk_versions):
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", chunk_versions)
    source.create(row(1, "a"))
    sync(propagate_drops=True)
    source.insert(row(2, "b"))  # version 1
    source.insert(row(3, "c"))  # version 2
    source.rename_column("name", "title")  # version 3: a chunk with no rows at all
    result, _, produced = sync(propagate_drops=True)
    assert result.success
    assert [p.key["id"] for p in produced] == [2, 3]
    assert state_rows[-1][0] == 3  # not stranded on the last chunk that had events
    assert metadata(result, "columns_renamed") == ["name -> title"]
    assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "name" TO "title"' in s]
