"""The watermark state backends: Postgres (the shared table) and DuckDB (a local file) behind one
interface, with identical replay semantics (plan section 12). The Postgres runs use the fake; the
DuckDB runs a real file in the tmp directory.

Run from the repo root:  .venv/bin/python -m pytest
"""

import json

import pytest

from orchestration.assets.reverse_etl_state import build_sync_state
from tests.fakes import make_config
from tests.sync_asset.support import row


@pytest.fixture(params=["postgres", "duckdb"], ids=["postgres", "duckdb"])
def backend(request) -> str:
    return request.param


def state_options(backend: str, tmp_path) -> dict:
    """The sync options that select the backend: a real file for DuckDB, the fake's default for
    Postgres."""
    if backend == "duckdb":
        return {"state_backend": "duckdb", "state_path": str(tmp_path / "state.duckdb")}
    return {}


def read_back(backend: str, res, tmp_path):
    """The watermark rows, freshest first, read through the same backend interface the sync uses —
    never the fakes' internals, so both backends are proven to behave the same."""
    cfg = make_config(**state_options(backend, tmp_path))
    state = build_sync_state(cfg, res["postgres"] if backend == "postgres" else None)
    return state.read(cfg.sync_name)


def watermark(res, backend: str, cfg) -> list[tuple[int | None, str | None]]:
    """The sync's state rows, freshest first, read back from wherever the backend put them."""
    if backend == "duckdb":
        from orchestration.assets.reverse_etl_state import DuckDBSyncState

        return DuckDBSyncState(cfg.state_path).read(cfg.sync_name)
    return sorted(res["postgres"].state_rows, key=lambda row: -(row[0] or 0))


class TestBothBackends:
    def test_the_first_run_records_the_watermark_and_the_schema(self, backend, sync, source, tmp_path):
        source.create(row(1, "a"))
        result, res, produced = sync(**state_options(backend, tmp_path))
        assert result.success and len(produced) == 1
        rows = read_back(backend, res, tmp_path)
        assert [v for v, _ in rows] == [0]  # one commit: the create
        assert [column["name"] for column in json.loads(rows[0][1])] == ["id", "name", "born"]

    def test_a_failed_run_leaves_the_watermark_unmoved_and_the_next_run_replays(
        self, backend, sync, source, tmp_path
    ):
        source.create(row(1, "a"))
        sync(**state_options(backend, tmp_path))
        source.insert(row(2, "b"))
        result, res, produced = sync(producer_fails=True, **state_options(backend, tmp_path))
        # The messages were enqueued, but the flush failed: delivery is NOT confirmed, so the
        # watermark must stay unmoved and the next run replays the same versions.
        assert not result.success
        assert [v for v, _ in read_back(backend, res, tmp_path)] == [0]  # unmoved by the failed run
        result, res, produced = sync(**state_options(backend, tmp_path))
        assert result.success and [p.value["after"]["name"] for p in produced] == ["b"]  # replayed
        # Append-only: the replay's row on top of the first run's, freshest first.
        assert [v for v, _ in read_back(backend, res, tmp_path)] == [1, 0]


class TestDuckDBOnly:
    def test_the_state_file_is_created_on_first_run(self, sync, source, tmp_path):
        path = tmp_path / "state.duckdb"
        source.create(row(1, "a"))
        assert not path.exists()
        result, _, produced = sync(state_backend="duckdb", state_path=str(path))
        assert result.success and len(produced) == 1
        assert path.exists(), "D15: the local state file appears when the sync first needs it"

    def test_the_duckdb_backend_needs_a_path(self):
        with pytest.raises(ValueError, match="state_path is required"):
            make_config(state_backend="duckdb")

    def test_only_postgres_and_duckdb_are_backends(self):
        with pytest.raises(ValueError, match="state_backend must be"):
            make_config(state_backend="sqlite")
