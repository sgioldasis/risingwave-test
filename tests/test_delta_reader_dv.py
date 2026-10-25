"""The Delta reader on deletion-vector tables, against real Databricks-written commits.

delta-rs cannot write deletion vectors (1.6.6 rewrites the file instead), so these tests read two small tables that were
written once on DEV Databricks and committed under tests/fixtures/delta_dv/ (synthetic rows; the user, statement and
transaction ids in their logs are replaced by placeholders, and the CRC files are left out). Both hold the same history,
`plain` unpartitioned and `part` partitioned by operator:

    v0 create   v1 insert (1 a, 2 b, 3 c)   v2 delete id 2   v3 update id 1 to a2   v4 insert (4 d)

`plain` has the three shapes a DV table commits: v2 re-adds the SAME data file with a deletion vector (nothing else
changes, which a path-set comparison cannot see), v3 writes the updated row to a new file and marks the old row dead in
the old file's vector. `part` deletes id 2 by removing its whole partition file (no vector) and updates id 1 like `plain`.

Run from the repo root:  .venv/bin/python -m pytest
"""

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError

from orchestration.assets import reverse_etl_delta_reader as reader
from tests.fakes import make_config, use_local_azure, use_local_paths

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "delta_dv"
ABFS_ROOT = "abfss://testcontainer@testaccount.dfs.core.windows.net/table"
CFG = make_config(source_columns=(), key_column="id")


@pytest.fixture(params=["plain", "part"])
def kind(request):
    return request.param


@pytest.fixture
def table_path(tmp_path, kind):
    """The fixture table copied under a fake container, so the reader's abfss:// location resolves to it."""
    destination = tmp_path / "testcontainer" / "table"
    shutil.copytree(FIXTURES / kind, destination)
    return destination


@pytest.fixture
def azure(tmp_path, table_path, monkeypatch):
    """The reader's fast path on local disk. A local table reports its deletion-vector masks under file:// URIs, where
    a table in storage reports the abfss:// URIs the reader builds for the same files, so the masks are re-keyed."""
    use_local_azure(monkeypatch, tmp_path, (reader,))
    decode_masks = reader._dv_masks

    def abfss_masks(delta):
        return {f"{ABFS_ROOT}/{uri.split('/testcontainer/table/', 1)[1]}": mask for uri, mask in decode_masks(delta).items()}

    monkeypatch.setattr(reader, "_dv_masks", abfss_masks)


def read(since=0, until=None):
    return reader.read_changes(CFG, ABFS_ROOT, since, until)


def summary(rows):
    return sorted(
        ((r["_change_type"], r["id"], r["name"], r["_commit_version"]) for r in rows), key=lambda item: (item[3], item[0], item[1])
    )


@pytest.mark.usefixtures("azure")
class TestChangeRows:
    def test_every_commit_of_the_history_is_reconstructed(self):
        assert summary(read()) == [
            ("insert", 1, "a", 1),
            ("insert", 2, "b", 1),
            ("insert", 3, "c", 1),
            ("delete", 2, "b", 2),
            ("update_postimage", 1, "a2", 3),
            ("update_preimage", 1, "a", 3),
            ("insert", 4, "d", 4),
        ]

    def test_a_delete_carries_the_full_preimage(self):
        (deleted,) = [r for r in read(2, 2) if r["_change_type"] == "delete"]
        assert (deleted["id"], deleted["name"]) == (2, "b")

    def test_an_update_reads_its_preimage_through_the_vector_in_force_one_version_earlier(self):
        """Reading v3 alone must find id 1 alive in the old file (the v2 vector keeps it) and dead after the update."""
        assert summary(read(3, 3)) == [("update_postimage", 1, "a2", 3), ("update_preimage", 1, "a", 3)]

    def test_a_row_a_vector_killed_is_never_read_as_live(self):
        """After v3 the old file's vector marks id 1 (and, in `plain`, id 2) dead: only the new file's row for id 1 and
        later inserts are live, so nothing at v4 touches them."""
        assert summary(read(4, 4)) == [("insert", 4, "d", 4)]

    def test_an_empty_range_reads_nothing(self):
        assert read(since=9) == []

    def test_every_row_carries_its_commits_timestamp(self):
        stamps = {r["_commit_version"]: r["_commit_timestamp"] for r in read()}
        assert set(stamps) == {1, 2, 3, 4} and all(stamp.endswith("Z") for stamp in stamps.values())


class TestPartitionedTable:
    pytestmark = pytest.mark.usefixtures("azure")

    @pytest.fixture
    def kind(self):
        return "part"

    def test_partition_values_are_injected_into_every_row_the_raw_read_returns(self):
        operators = {(r["id"], r["operator"]) for r in read() if r["_change_type"] in ("insert", "update_postimage")}
        assert operators == {(1, "gr"), (2, "br"), (3, "gr"), (4, "br")}

    def test_a_delete_that_removes_a_whole_partition_file_still_reads_its_row(self):
        (deleted,) = read(2, 2)
        assert (deleted["_change_type"], deleted["id"], deleted["operator"]) == ("delete", 2, "br")


class TestPartitionValuesKeptOnlyInThePath:
    """Databricks writes the partition column into the data files of a column-mapped table, so the reader finds it there.
    Tables from other writers keep partition values only in the add actions, and the reader injects them. This derives
    such a table from the fixture by stripping the partition column from every file of a temporary copy."""

    pytestmark = pytest.mark.usefixtures("azure")

    @pytest.fixture
    def kind(self):
        return "part"

    @pytest.fixture(autouse=True)
    def stripped_files(self, table_path):
        physical = next(
            field.metadata["delta.columnMapping.physicalName"]
            for field in DeltaTable(str(table_path)).schema().fields
            if field.name == "operator"
        )
        for file in table_path.rglob("*.parquet"):
            pq.write_table(pq.read_table(file).drop_columns([physical]), file)
        return physical

    def test_the_files_really_lack_the_column(self, table_path, stripped_files):
        assert all(stripped_files not in pq.read_table(file).column_names for file in table_path.rglob("*.parquet"))

    def test_every_row_still_gets_its_partition_value_from_the_add_actions(self):
        operators = {(r["id"], r["operator"]) for r in read() if r["_change_type"] in ("insert", "update_postimage")}
        assert operators == {(1, "gr"), (2, "br"), (3, "gr"), (4, "br")}

    def test_the_removed_files_rows_do_too(self):
        deletes = [r for r in read() if r["_change_type"] == "delete"]
        assert [(r["id"], r["operator"]) for r in deletes] == [(2, "br")]
        preimages = [r for r in read() if r["_change_type"] == "update_preimage"]
        assert [(r["id"], r["operator"]) for r in preimages] == [(1, "gr")]


# --- the helpers on real data ------------------------------------------------------------------------------------------


class TestHelpersOnRealCommits:
    def test_the_vector_in_force_at_each_version_is_decoded_into_a_keep_mask(self, table_path, kind):
        masks = {v: list(reader._dv_masks(DeltaTable(str(table_path), version=v)).values()) for v in range(5)}
        expected = {
            "plain": {0: [], 1: [], 2: [[True, False, True]], 3: [[False, False, True]], 4: [[False, False, True]]},
            "part": {0: [], 1: [], 2: [], 3: [[False, True]], 4: [[False, True]]},
        }[kind]
        assert masks == expected

    def test_a_dv_only_commit_is_visible_in_the_log_but_not_in_the_file_set(self, tmp_path, table_path, kind):
        if kind != "plain":
            pytest.skip("only the unpartitioned table has a DV-only commit")
        filesystem = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
        adds, removes = reader._commit_actions(filesystem, "testcontainer/table/_delta_log", 2)
        assert [a["path"] for a in adds] == [r["path"] for r in removes]  # the same data file is removed and re-added
        assert adds[0]["deletionVector"]["storageType"] == "u"
        files = {v: reader._file_paths(DeltaTable(str(table_path), version=v)) for v in (1, 2)}
        assert files[1] == files[2]  # which is why a path-set diff sees nothing

    def test_every_add_of_an_update_commit_says_whether_it_carries_a_vector(self, tmp_path, table_path):
        filesystem = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
        adds, removes = reader._commit_actions(filesystem, "testcontainer/table/_delta_log", 3)
        assert len(removes) >= 1 and sorted(bool(a.get("deletionVector")) for a in adds) == [False, True]

    def test_partition_values_are_restored_from_the_add_actions_by_logical_name(self, table_path, kind):
        if kind != "part":
            pytest.skip("only the partitioned table has partition values")
        values = reader._partition_values(DeltaTable(str(table_path), version=1), "root")
        assert sorted(v["operator"] for v in values.values()) == ["br", "gr"]


# --- what makes the fast path necessary ---------------------------------------------------------------------------------


class TestWhyTheFastPathIsRequired:
    def test_delta_rs_refuses_to_scan_a_table_with_deletion_vectors(self, table_path):
        """A canary for upgrades: if this stops raising, delta-rs can scan DV tables itself and the reader's rule that DV
        tables need raw file reads (and its refusal without an Azure filesystem) can be revisited."""
        with pytest.raises(DeltaProtocolError, match="deletionVectors"):
            DeltaTable(str(table_path)).to_pyarrow_table()

    def test_without_an_azure_filesystem_the_table_is_refused_with_the_reason(self, table_path, monkeypatch):
        use_local_paths(monkeypatch, (reader,))
        with pytest.raises(RuntimeError, match="no Azure filesystem is available"):
            reader.read_changes(CFG, str(table_path), 0, 4)

    def test_the_reader_sees_the_protocol_feature(self, table_path):
        assert "deletionVectors" in DeltaTable(str(table_path)).protocol().writer_features
        assert isinstance(pa.table(DeltaTable(str(table_path)).deletion_vectors().read_all()), pa.Table)
