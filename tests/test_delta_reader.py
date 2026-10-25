"""The Delta reader (orchestration/assets/reverse_etl_delta_reader.py) on its own, against real delta-rs tables in tmp
dirs: the reconstructed change rows for both read paths, the version functions, the state read and the helpers that
only deletion-vector, partitioned and column-mapped tables reach.

Two storage modes, as in test_sync_asset.py: `azure` is the production fast path (an abfss:// location backed by a
local directory, with pyarrow's AzureFileSystem replaced by a local one), `local` is a plain path, which makes the reader
diff snapshots. Reads that both paths can serve must agree, so those tests run on both.

delta-rs cannot write deletion vectors, so those code paths are covered through their helpers and guards.

Run from the repo root:  .venv/bin/python -m pytest
"""

import datetime
import json
import os
import re
import types

import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from deltalake import DeltaTable, write_deltalake

from orchestration.assets import reverse_etl_delta_reader as reader
from tests.fakes import make_config

ABFS_ROOT = "abfss://testcontainer@testaccount.dfs.core.windows.net/table"
SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("born", pa.date32()),
        pa.field("seen", pa.timestamp("us", tz="UTC")),
        pa.field("operator", pa.string()),
    ]
)
MOMENT = datetime.datetime(2026, 10, 18, 17, 26, 36, 779212, tzinfo=datetime.timezone.utc)
DAY = datetime.date(2012, 1, 1)
CFG = make_config(source_columns=(), key_column="id")


def row(row_id, name, operator="gr", born=None, seen=None):
    return {"id": row_id, "name": name, "born": born, "seen": seen, "operator": operator}


class Lake:
    """A local Delta table; every call is one commit."""

    def __init__(self, path, location, partition_by=None, configuration=None):
        self.path, self.location, self.partition_by, self.configuration = str(path), location, partition_by, configuration

    def create(self, *rows):
        write_deltalake(
            self.path, pa.Table.from_pylist(list(rows), schema=SCHEMA), partition_by=self.partition_by, configuration=self.configuration
        )

    def insert(self, *rows):
        write_deltalake(self.path, pa.Table.from_pylist(list(rows), schema=SCHEMA), mode="append")

    def update(self, row_id, name):
        DeltaTable(self.path).update(updates={"name": f"'{name}'"}, predicate=f"id = {row_id}")

    def delete(self, row_id):
        DeltaTable(self.path).delete(f"id = {row_id}")

    def compact(self):
        DeltaTable(self.path).optimize.compact()

    def read(self, since=0, until=None):
        return reader.read_changes(CFG, self.location, since, until)


@pytest.fixture(params=["azure", "local"])
def storage(request):
    return request.param


@pytest.fixture
def make_lake(tmp_path, storage, monkeypatch):
    """Builds a Lake in the chosen storage mode, with the reader pointed at it."""
    if storage == "local":
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})

        def build(**options):
            return Lake(tmp_path / "table", str(tmp_path / "table"), **options)

        return build

    credentials = {"account_name": "testaccount", "client_id": "id", "tenant_id": "tenant", "client_secret": "secret"}

    def local_table(location, storage_options=None, version=None):
        return DeltaTable(str(tmp_path / "testcontainer" / location.split(".dfs.core.windows.net/", 1)[1]), version=version)

    monkeypatch.setattr(reader, "DeltaTable", local_table)
    monkeypatch.setattr(reader, "_azure_storage_options", lambda location: credentials)
    monkeypatch.setattr(pafs, "AzureFileSystem", lambda **options: pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem()))
    (tmp_path / "testcontainer").mkdir()

    def build_azure(**options):
        return Lake(tmp_path / "testcontainer" / "table", ABFS_ROOT, **options)

    return build_azure


@pytest.fixture
def lake(make_lake):
    return make_lake()


def summary(rows):
    """(change type, id, name, commit version) per row, ordered by version then type then id (the order within a version
    is not part of the contract)."""
    return sorted(
        ((r["_change_type"], r["id"], r["name"], r["_commit_version"]) for r in rows), key=lambda item: (item[3], item[0], item[1])
    )


# --- change rows, on both read paths -------------------------------------------------------------------------------------


class TestReadChanges:
    def test_inserts_updates_and_deletes_are_reconstructed_per_commit(self, lake):
        lake.create(row(1, "a"), row(2, "b"))  # v0
        lake.insert(row(3, "c"))  # v1
        lake.update(2, "b2")  # v2
        lake.delete(1)  # v3
        assert summary(lake.read()) == [
            ("insert", 1, "a", 0),
            ("insert", 2, "b", 0),
            ("insert", 3, "c", 1),
            ("update_postimage", 2, "b2", 2),
            ("update_preimage", 2, "b", 2),
            ("delete", 1, "a", 3),
        ]

    def test_a_delete_carries_the_whole_preimage(self, lake):
        lake.create(row(1, "a", born=DAY, seen=MOMENT))
        lake.delete(1)
        (deleted,) = [r for r in lake.read() if r["_change_type"] == "delete"]
        assert (deleted["name"], deleted["born"], deleted["seen"]) == ("a", "2012-01-01", "2026-10-18T17:26:36.779212Z")

    def test_a_key_deleted_and_reinserted_in_the_next_commit_emits_both(self, lake):
        lake.create(row(1, "a"), row(2, "b"))
        lake.delete(1)
        lake.insert(row(1, "again"))
        assert summary(lake.read(1)) == [("delete", 1, "a", 1), ("insert", 1, "again", 2)]

    def test_a_compaction_changes_no_rows_so_it_emits_nothing(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        lake.insert(row(3, "c"))
        lake.compact()  # v3 rewrites three files into one
        assert summary(lake.read(3)) == []

    def test_only_the_requested_range_is_read(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        lake.insert(row(3, "c"))
        lake.insert(row(4, "d"))
        assert summary(lake.read(since=2, until=2)) == [("insert", 3, "c", 2)]
        assert summary(lake.read(since=2)) == [("insert", 3, "c", 2), ("insert", 4, "d", 3)]

    def test_an_empty_range_reads_nothing(self, lake):
        lake.create(row(1, "a"))
        assert lake.read(since=5) == []
        assert lake.read(since=1, until=0) == []

    def test_rows_come_out_ordered_by_commit_version(self, lake):
        lake.create(row(1, "a"))
        for n in range(2, 6):
            lake.insert(row(n, f"n{n}"))
        versions = [r["_commit_version"] for r in lake.read()]
        assert versions == sorted(versions)

    def test_timestamps_and_dates_are_the_contracts_strings(self, lake):
        lake.create(row(1, "a", born=DAY, seen=MOMENT))
        lake.update(1, "a2")
        for r in lake.read():
            assert (r["born"], r["seen"]) == ("2012-01-01", "2026-10-18T17:26:36.779212Z")

    def test_every_commit_carries_its_own_timestamp(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        stamps = {r["_commit_version"]: r["_commit_timestamp"] for r in lake.read()}
        assert set(stamps) == {0, 1}
        assert all(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", stamp) for stamp in stamps.values())

    def test_a_null_value_stays_null(self, lake):
        lake.create(row(1, None))
        (only,) = lake.read()
        assert only["name"] is None and only["born"] is None and only["seen"] is None


class TestPartitionedTables:
    """Partition values live in the file paths, so these always use the snapshot diff, on either storage."""

    @pytest.fixture
    def lake(self, make_lake):
        return make_lake(partition_by=["operator"])

    def test_partition_values_are_in_every_row(self, lake):
        lake.create(row(1, "a", "gr"), row(2, "b", "br"))
        assert sorted((r["id"], r["operator"]) for r in lake.read()) == [(1, "gr"), (2, "br")]

    def test_an_update_and_a_delete_are_found_across_partitions(self, lake):
        lake.create(row(1, "a", "gr"), row(2, "b", "br"))
        lake.update(2, "b2")
        lake.delete(1)
        assert summary(lake.read(1)) == [
            ("update_postimage", 2, "b2", 1),
            ("update_preimage", 2, "b", 1),
            ("delete", 1, "a", 2),
        ]


# --- version functions ---------------------------------------------------------------------------------------------------


class TestVersions:
    def test_the_current_version_is_the_latest_commit(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        lake.insert(row(3, "c"))
        assert reader.get_current_version(lake.location) == 2

    def test_a_table_with_its_full_history_starts_at_zero(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        lake.insert(row(3, "c"))
        assert reader.get_earliest_available_version(lake.location) == 0

    def test_a_table_with_one_commit_starts_at_zero(self, lake):
        lake.create(row(1, "a"))
        assert reader.get_earliest_available_version(lake.location) == 0

    def test_aged_out_history_starts_at_the_checkpoint(self, lake):
        lake.create(row(1, "a"))
        lake.insert(row(2, "b"))
        lake.insert(row(3, "c"))
        table = DeltaTable(lake.path)
        table.create_checkpoint()
        for version in range(table.version()):
            os.remove(os.path.join(lake.path, "_delta_log", f"{version:020d}.json"))
        assert reader.get_earliest_available_version(lake.location) == 2


class StubTable:
    """A table whose versions below `first` fail to load with `message`."""

    def __init__(self, latest, first, message):
        self.latest, self.first, self.message, self.loaded = latest, first, message, []

    def version(self):
        return self.latest

    def load_as_version(self, version):
        self.loaded.append(version)
        if version < self.first:
            raise RuntimeError(self.message)


class TestEarliestVersionSearch:
    @pytest.mark.parametrize(
        "message",
        [
            "Generic delta kernel error: No files in log segment",
            "ObjectStore error: status 404 Not Found",
            "The specified path does not exist",
            "Cannot find the commit file",
            "version 3 has been truncated",
            "log entry expired",
        ],
    )
    def test_a_missing_log_reads_as_not_available_and_the_search_continues(self, monkeypatch, message):
        stub = StubTable(latest=10, first=4, message=message)
        monkeypatch.setattr(reader, "_open", lambda location: stub)
        assert reader.get_earliest_available_version("anywhere") == 4
        assert stub.loaded[-1] == 10  # the handle is left on the current snapshot

    def test_any_other_failure_is_not_swallowed(self, monkeypatch):
        monkeypatch.setattr(reader, "_open", lambda location: StubTable(latest=10, first=4, message="403 permission denied"))
        with pytest.raises(RuntimeError, match="permission denied"):
            reader.get_earliest_available_version("anywhere")

    def test_a_table_at_version_zero_is_not_searched(self, monkeypatch):
        stub = StubTable(latest=0, first=0, message="")
        monkeypatch.setattr(reader, "_open", lambda location: stub)
        assert reader.get_earliest_available_version("anywhere") == 0 and stub.loaded == []


# --- the watermark read --------------------------------------------------------------------------------------------------


class TestReadState:
    def write(self, tmp_path, rows):
        path = str(tmp_path / "state")
        schema = pa.schema([("sync_name", pa.string()), ("last_commit_version", pa.int64()), ("last_schema", pa.string())])
        write_deltalake(path, pa.Table.from_pylist(rows, schema=schema))
        return path

    @pytest.fixture(autouse=True)
    def local(self, monkeypatch):
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})

    def test_the_freshest_row_of_this_sync_wins(self, tmp_path):
        path = self.write(
            tmp_path,
            [
                {"sync_name": CFG.sync_name, "last_commit_version": 3, "last_schema": "old"},
                {"sync_name": CFG.sync_name, "last_commit_version": 9, "last_schema": "new"},
                {"sync_name": "someone_else", "last_commit_version": 99, "last_schema": "x"},
            ],
        )
        assert reader.read_state(CFG, path) == (9, "new")

    def test_a_sync_with_no_row_has_no_state(self, tmp_path):
        path = self.write(tmp_path, [{"sync_name": "someone_else", "last_commit_version": 1, "last_schema": "x"}])
        assert reader.read_state(CFG, path) == (None, None)

    def test_a_row_with_no_version_has_no_watermark_but_keeps_its_schema(self, tmp_path):
        path = self.write(tmp_path, [{"sync_name": CFG.sync_name, "last_commit_version": None, "last_schema": "s"}])
        assert reader.read_state(CFG, path) == (None, "s")


# --- the schema ----------------------------------------------------------------------------------------------------------


class TestRowFields:
    def test_delta_types_map_to_connect_types_and_anything_compound_is_a_string(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})
        table = pa.table(
            {
                "tiny": pa.array([1], pa.int8()),
                "small": pa.array([1], pa.int16()),
                "normal": pa.array([1], pa.int32()),
                "big": pa.array([1], pa.int64()),
                "single": pa.array([1.0], pa.float32()),
                "double": pa.array([1.0], pa.float64()),
                "flag": pa.array([True]),
                "text": pa.array(["x"]),
                "moment": pa.array([MOMENT], pa.timestamp("us", tz="UTC")),
                "day": pa.array([DAY], pa.date32()),
                "money": pa.array([1], pa.decimal128(10, 2)),
                "nested": pa.array([{"a": 1}], pa.struct([("a", pa.int64())])),
            }
        )
        write_deltalake(str(tmp_path / "t"), table)
        fields = {name: connect_type for name, connect_type, _ in reader.get_row_fields(str(tmp_path / "t"))}
        assert fields == {
            "tiny": "int8",
            "small": "int16",
            "normal": "int32",
            "big": "int64",
            "single": "float",
            "double": "double",
            "flag": "boolean",
            "text": "string",
            "moment": reader.ZONED_TIMESTAMP,
            "day": reader.DATE,
            "money": "string",
            "nested": "string",
        }

    def test_the_schema_can_be_read_at_an_earlier_version(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})
        path = str(tmp_path / "t")
        write_deltalake(path, pa.table({"id": pa.array([1], pa.int64())}))
        write_deltalake(path, pa.table({"id": pa.array([2], pa.int64()), "extra": pa.array(["x"])}), mode="append", schema_mode="merge")
        assert [name for name, _, _ in reader.get_row_fields(path, version=0)] == ["id"]
        assert [name for name, _, _ in reader.get_row_fields(path)] == ["id", "extra"]

    def test_nullability_is_reported(self, lake):
        lake.create(row(1, "a"))
        nullable = {name: nullable for name, _, nullable in reader.get_row_fields(lake.location)}
        assert nullable["id"] is False and nullable["name"] is True


# --- pure helpers --------------------------------------------------------------------------------------------------------


class TestFormatting:
    def test_a_naive_timestamp_is_taken_as_utc(self):
        assert reader._format_timestamp(datetime.datetime(2026, 10, 18, 17, 26, 36, 779212)) == "2026-10-18T17:26:36.779212Z"

    def test_an_aware_timestamp_is_converted_to_utc(self):
        local = datetime.datetime(2026, 10, 18, 19, 26, 36, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        assert reader._format_timestamp(local) == "2026-10-18T17:26:36.000000Z"

    def test_anything_that_is_not_a_timestamp_is_stringified(self):
        assert reader._format_timestamp("already") == "already" and reader._format_timestamp(5) == "5"

    def test_a_commit_timestamp_is_epoch_milliseconds_in_the_same_format(self):
        assert reader._format_commit_timestamp(0) == "1970-01-01T00:00:00.000000Z"
        assert reader._format_commit_timestamp(1_760_808_396_779) == "2025-10-18T17:26:36.779000Z"

    def test_rows_are_formatted_in_place_and_formatting_twice_changes_nothing(self):
        rows = [{"at": MOMENT, "day": DAY, "none": None}]
        reader._format_rows(rows, ["at", "none"], ["day"])
        expected = [{"at": "2026-10-18T17:26:36.779212Z", "day": "2012-01-01", "none": None}]
        assert rows == expected
        reader._format_rows(rows, ["at", "none"], ["day"])
        assert rows == expected

    def test_a_datetime_in_a_date_column_is_left_alone(self):
        rows = [{"day": MOMENT}]
        reader._format_rows(rows, [], ["day"])
        assert rows == [{"day": MOMENT}]


class TestChangedPayloadColumns:
    def test_a_difference_in_a_payload_column_is_a_change(self):
        assert reader._changed_payload_columns({"id": 1, "name": "a"}, {"id": 1, "name": "b"}, {"id"})

    def test_a_difference_in_a_skipped_column_is_not(self):
        assert not reader._changed_payload_columns({"id": 1, "name": "a"}, {"id": 2, "name": "a"}, {"id"})

    def test_a_column_present_on_one_side_only_reads_as_null_so_schema_drift_alone_is_no_change(self):
        assert not reader._changed_payload_columns({"id": 1, "name": "a"}, {"id": 1, "name": "a", "extra": None}, {"id"})
        assert reader._changed_payload_columns({"id": 1, "name": "a"}, {"id": 1, "name": "a", "extra": "x"}, {"id"})


# --- raw file reads (what the fast path does) ----------------------------------------------------------------------------


@pytest.fixture
def raw(tmp_path):
    """A column-mapped table, as Databricks writes: parquet files hold physical column names. Returns helpers to read its
    files through a local stand-in for the Azure filesystem."""
    (tmp_path / "testcontainer").mkdir()
    path = tmp_path / "testcontainer" / "table"
    write_deltalake(
        str(path),
        pa.Table.from_pylist([row(1, "a", "gr"), row(2, "b", "br"), row(3, "c", "gr")], schema=SCHEMA),
        configuration={"delta.columnMapping.mode": "name", "delta.enableDeletionVectors": "false"},
    )
    table = DeltaTable(str(path))
    uris = [f"{ABFS_ROOT}/{p}" for p in sorted(r["path"] for r in pa.table(table.get_add_actions(flatten=True)).to_pylist())]
    filesystem = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
    return types.SimpleNamespace(table=table, uris=uris, fs=filesystem, path=path)


class TestRawFileReads:
    def test_physical_column_names_are_renamed_to_logical_ones(self, raw):
        rows = reader._read_files_as_rows(raw.table, raw.fs, "testcontainer", raw.uris, [])
        assert sorted((r["id"], r["name"]) for r in rows) == [(1, "a"), (2, "b"), (3, "c")]

    def test_a_snapshot_delta_rs_returns_all_null_is_read_from_the_files(self, raw):
        proxy = types.SimpleNamespace(
            to_pyarrow_table=raw.table.to_pyarrow_table,
            file_uris=lambda: raw.uris,
            schema=raw.table.schema,
            version=raw.table.version,
        )
        snapshot = reader._snapshot_rows(proxy, ["seen"], ["id"], raw.fs, "testcontainer")
        assert sorted(key for key in snapshot) == [(1,), (2,), (3,)]
        assert snapshot[(2,)]["name"] == "b"

    def test_an_all_null_snapshot_with_no_filesystem_is_refused(self, raw):
        with pytest.raises(RuntimeError, match="no Azure filesystem is available"):
            reader._snapshot_rows(raw.table, [], ["id"], None, "")

    def test_a_deletion_vector_mask_drops_the_dead_rows(self, raw):
        (uri,) = raw.uris
        rows = reader._read_files_as_rows(raw.table, raw.fs, "testcontainer", [uri], [], masks={uri: [True, False, True]})
        assert sorted(r["id"] for r in rows) == [1, 3]

    def test_a_file_without_a_mask_is_read_whole(self, raw):
        (uri,) = raw.uris
        rows = reader._read_files_as_rows(raw.table, raw.fs, "testcontainer", [uri], [], masks={"another file": [False]})
        assert len(rows) == 3

    def test_a_mask_of_the_wrong_length_is_refused(self, raw):
        (uri,) = raw.uris
        with pytest.raises(RuntimeError, match="3 rows|has 2 entries"):
            reader._read_files_as_rows(raw.table, raw.fs, "testcontainer", [uri], [], masks={uri: [True, False]})

    def test_partition_values_are_injected_when_the_file_lacks_the_column(self, raw):
        (uri,) = raw.uris
        rows = reader._read_files_as_rows(
            raw.table, raw.fs, "testcontainer", [uri], [], partition_values={uri: {"region": "emea"}}
        )
        assert {r["region"] for r in rows} == {"emea"}

    def test_no_files_means_no_rows(self, raw):
        assert reader._read_files_as_rows(raw.table, raw.fs, "testcontainer", [], []) == []


class TestLogHelpers:
    def test_a_commits_adds_and_removes_are_read_from_its_log_entry(self, tmp_path):
        log = tmp_path / "testcontainer" / "table" / "_delta_log"
        log.mkdir(parents=True)
        lines = [
            {"commitInfo": {"operation": "UPDATE"}},
            {"remove": {"path": "old.parquet"}},
            {"add": {"path": "same.parquet", "deletionVector": {"storageType": "i"}}},
            {"add": {"path": "new.parquet"}},
        ]
        (log / f"{7:020d}.json").write_text("\n".join(json.dumps(line) for line in lines) + "\n")
        filesystem = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
        adds, removes = reader._commit_actions(filesystem, "testcontainer/table/_delta_log", 7)
        assert [a["path"] for a in adds] == ["same.parquet", "new.parquet"]
        assert adds[0]["deletionVector"] == {"storageType": "i"}
        assert [r["path"] for r in removes] == ["old.parquet"]

    def test_partition_values_are_restored_from_the_add_actions(self, tmp_path):
        path = str(tmp_path / "t")
        write_deltalake(path, pa.Table.from_pylist([row(1, "a", "gr"), row(2, "b", "br")], schema=SCHEMA), partition_by=["operator"])
        values = reader._partition_values(DeltaTable(path), "root")
        assert sorted(v["operator"] for v in values.values()) == ["br", "gr"]
        assert all(uri.startswith("root/operator=") for uri in values)

    def test_an_unpartitioned_table_has_no_partition_values(self, tmp_path):
        path = str(tmp_path / "t")
        write_deltalake(path, pa.Table.from_pylist([row(1, "a")], schema=SCHEMA))
        assert reader._partition_values(DeltaTable(path), "root") == {}

    def test_deletion_vector_masks_are_keyed_by_file_and_files_without_one_are_left_out(self):
        class Reader:
            def read_all(self):
                return pa.table({"filepath": ["a", "b"], "selection_vector": pa.array([[True, False], None], pa.list_(pa.bool_()))})

        handle = types.SimpleNamespace(deletion_vectors=lambda: Reader())
        assert reader._dv_masks(handle) == {"a": [True, False]}

    def test_a_deltalake_that_cannot_read_deletion_vectors_is_reported(self):
        def missing():
            raise AttributeError("deletion_vectors")

        with pytest.raises(RuntimeError, match="cannot read deletion vectors"):
            reader._dv_masks(types.SimpleNamespace(deletion_vectors=missing))

    def test_the_physical_name_map_is_empty_without_column_mapping(self, tmp_path):
        path = str(tmp_path / "t")
        write_deltalake(path, pa.Table.from_pylist([row(1, "a")], schema=SCHEMA))
        assert reader._physical_rename_map(DeltaTable(path)) == {}

    def test_the_physical_name_map_goes_from_physical_to_logical(self, raw):
        mapping = reader._physical_rename_map(raw.table)
        assert sorted(mapping.values()) == sorted(["id", "name", "born", "seen", "operator"])
        assert all(physical.startswith("col-") for physical in mapping)


# --- deletion-vector guards in read_changes ------------------------------------------------------------------------------


class DeletionVectorTable:
    """A real table whose protocol claims the deletionVectors feature."""

    def __init__(self, real, can_read=True):
        self.real, self.can_read = real, can_read

    def protocol(self):
        return types.SimpleNamespace(writer_features=["deletionVectors"], reader_features=[])

    def __getattr__(self, name):
        if name == "deletion_vectors" and not self.can_read:
            raise AttributeError(name)
        return getattr(self.real, name)


class TestDeletionVectorGuards:
    def test_a_deltalake_that_cannot_read_them_is_refused_before_any_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})
        path = str(tmp_path / "t")
        write_deltalake(path, pa.Table.from_pylist([row(1, "a")], schema=SCHEMA))
        monkeypatch.setattr(reader, "_open", lambda location: DeletionVectorTable(DeltaTable(path), can_read=False))
        with pytest.raises(RuntimeError, match="cannot read them"):
            reader.read_changes(CFG, path, 0, 0)

    def test_without_an_azure_filesystem_they_are_refused_with_the_reason(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reader, "_azure_storage_options", lambda location: {})
        path = str(tmp_path / "t")
        write_deltalake(path, pa.Table.from_pylist([row(1, "a")], schema=SCHEMA))
        monkeypatch.setattr(reader, "_open", lambda location: DeletionVectorTable(DeltaTable(path)))
        with pytest.raises(RuntimeError, match="no Azure filesystem is available"):
            reader.read_changes(CFG, path, 0, 0)
