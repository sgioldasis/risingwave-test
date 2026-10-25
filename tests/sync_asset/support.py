"""Shared pieces of the sync asset tests: the source table helper and the helpers that read results.

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
import time
import uuid
from typing import NamedTuple

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

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


def recorded_schema(state_rows) -> list:
    """(name, mapping id) per column in the newest watermark row: the schema the next sync diffs against."""
    return [(column["name"], column["id"]) for column in json.loads(state_rows[-1][1])]

