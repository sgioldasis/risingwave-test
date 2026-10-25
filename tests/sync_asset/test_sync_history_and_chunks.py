"""The sync asset's first run when the log history has aged out, and reading a backlog in chunks.

Run from the repo root:  .venv/bin/python -m pytest
"""


import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from tests.sync_asset.support import (
    FAST_PATH,
    events,
    metadata,
    row,
    statements,
)


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
