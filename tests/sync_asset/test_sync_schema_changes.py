"""The sync asset when the source schema changes: add, rename and drop on column-mapped tables (the production fast path), several changes inside one unsynced window, and at both 1 and the default 10 versions per chunk.

Run from the repo root:  .venv/bin/python -m pytest
"""

import json

import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from tests.sync_asset.support import (
    DAY,
    FAST_PATH,
    TOPIC,
    metadata,
    recorded_schema,
    row,
    statements,
)


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


@pytest.fixture(params=[1, 10], ids=["chunk1", "chunk10"])
def one_version_per_chunk(request, monkeypatch):
    """Despite the name, also run at the code default of 10: chunks are split at schema changes, so both agree."""
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", request.param)


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


@pytest.mark.parametrize("storage", [FAST_PATH])
def test_a_rename_in_the_same_chunk_as_earlier_rows_keeps_their_values_at_the_default_chunk_size(source, sync, monkeypatch, storage):
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", 10)
    source.create(row(1, "a"))
    sync(propagate_drops=True)
    source.insert(row(2, "b"))  # version 1
    source.rename_column("name", "title")  # version 2, same chunk
    _, _, produced = sync(propagate_drops=True)
    assert [p.value["after"].get("title", p.value["after"].get("name")) for p in produced] == ["b"]

