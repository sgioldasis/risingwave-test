"""The watermark state backends: Postgres (the shared table) and DuckDB (a local file) behind one
interface, with identical replay semantics (plan section 12). The Postgres runs use the fake; the
DuckDB runs a real file in the tmp directory. The watermark guard's identity columns (source_fqn,
label) and its decision table live here too — both backends must behave the same (release design).

Run from the repo root:  .venv/bin/python -m pytest
"""

import json
from dataclasses import replace

import pytest

from orchestration.assets.reverse_etl_cdf_setup import check_watermark_identity
from orchestration.assets.reverse_etl_state import DuckDBSyncState, PostgresSyncState, build_sync_state
from tests.fakes import FakeDatabase, make_config
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

    def test_the_run_records_the_source_and_label_for_the_guard(self, backend, sync, source, tmp_path):
        """The watermark guard's anchor is written by the run itself: after a sync, the state
        knows which source and label the row belongs to (the rename check reads this)."""
        source.create(row(1, "a"))
        result, res, produced = sync(**state_options(backend, tmp_path))
        assert result.success and len(produced) == 1
        cfg = make_config(**state_options(backend, tmp_path))
        state = build_sync_state(cfg, res["postgres"] if backend == "postgres" else None)
        assert state.rows_for_source(cfg.source_fqn) == [(cfg.sync_name, cfg.label)]


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


class Log:
    """Records what the guard told the run log (Dagster's manager in production)."""

    def __init__(self):
        self.lines: list[str] = []

    def info(self, message: str) -> None:
        self.lines.append(message)

    def warning(self, message: str) -> None:
        self.lines.append(message)


def guard_state(backend: str, tmp_path):
    """(state, cfg) for the guard tests: the fake Postgres, or a real DuckDB file."""
    cfg = make_config(**state_options(backend, tmp_path))
    return build_sync_state(cfg, FakeDatabase([], "postgres") if backend == "postgres" else None), cfg


class TestTheWatermarkIdentity:
    """Every write records source_fqn and label beside the watermark (the guard's stable anchor:
    both sync_name and label move on a rename, the source does not), and the guard's decision
    table behaves identically on both backends."""

    def test_the_write_records_where_the_row_came_from(self, backend, tmp_path):
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        state.write(cfg.sync_name, 7, "[]", source_fqn=cfg.source_fqn, label=cfg.label)
        assert state.read(cfg.sync_name) == [(7, "[]")]
        assert state.rows_for_source(cfg.source_fqn) == [(cfg.sync_name, cfg.label)]

    def test_a_legacy_row_without_identity_matches_no_source(self, backend, tmp_path):
        """Legacy rows (pre-guard, NULL source) are invisible to the guard and repopulated by
        the next write — no false positives for a table that predates the guard."""
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        state.write(cfg.sync_name, 7, "[]")
        assert state.rows_for_source(cfg.source_fqn) == []

    def test_a_fresh_source_proceeds_silently(self, backend, tmp_path):
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        log = Log()
        check_watermark_identity(state, cfg, log)
        assert log.lines == []

    def test_a_renamed_label_is_an_error_naming_the_pin_fix(self, backend, tmp_path):
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        state.write(cfg.sync_name, 5, None, source_fqn=cfg.source_fqn, label=cfg.label)
        renamed = replace(cfg, sync_name=f"{cfg.sync_name}_v2")
        with pytest.raises(RuntimeError, match="the label was renamed") as error:
            check_watermark_identity(state, renamed, Log())
        message = str(error.value)
        assert f"sync_name='{cfg.sync_name}' (label '{cfg.label}')" in message
        assert "Pin sync_name in the YAML to keep the watermark, or delete the old rows to start fresh." in message

    def test_a_derived_name_colliding_with_a_deliberate_sync_is_an_error(self, backend, tmp_path):
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        state.write("orders_watermark", 5, None, source_fqn=cfg.source_fqn, label="orders")
        with pytest.raises(RuntimeError, match="collides with a deliberate sync; set sync_name explicitly"):
            check_watermark_identity(state, cfg, Log())

    def test_an_explicitly_pinned_sync_warns_and_proceeds(self, backend, tmp_path):
        """A pinned identity is deliberate: a pinned rename, or a legitimate second sync on one
        table — the guard warns and lets it start fresh."""
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        state.write(cfg.sync_name, 5, None, source_fqn=cfg.source_fqn, label=cfg.label)
        pinned = replace(cfg, sync_name="pinned_key", sync_name_explicit=True)
        log = Log()
        check_watermark_identity(state, pinned, log)
        assert len(log.lines) == 1 and "explicitly pinned" in log.lines[0]

    def test_an_explicit_sync_with_no_prior_rows_is_silent(self, backend, tmp_path):
        state, cfg = guard_state(backend, tmp_path)
        state.require()
        pinned = replace(cfg, sync_name="pinned_key", sync_name_explicit=True)
        log = Log()
        check_watermark_identity(state, pinned, log)
        assert log.lines == []


class TestTheGuardMigration:
    """Check-only plus migrate (release design): require() refuses a pre-guard table with the
    exact ALTER statements in hand; create() performs the migration, so the demo's next setup
    run upgrades its table."""

    def test_a_pre_guard_table_fails_the_check_carrying_the_alter_statements(self):
        postgres = FakeDatabase([], "postgres", state_guard_columns=False)
        with pytest.raises(RuntimeError) as error:
            PostgresSyncState(postgres).require()
        message = str(error.value)
        assert "predates the watermark guard" in message
        assert "ALTER TABLE reverse_etl_sync_state ADD COLUMN IF NOT EXISTS source_fqn TEXT" in message
        assert "ALTER TABLE reverse_etl_sync_state ADD COLUMN IF NOT EXISTS label TEXT" in message

    def test_create_migrates_a_pre_guard_table_in_place(self):
        postgres = FakeDatabase([], "postgres", state_guard_columns=False)
        state = PostgresSyncState(postgres)
        with pytest.raises(RuntimeError):
            state.require()
        state.create()
        state.require()  # the migration ran; the check passes now

    def test_a_fresh_duckdb_file_carries_the_guard_columns(self, tmp_path):
        state = DuckDBSyncState(str(tmp_path / "state.duckdb"))
        state.require()  # created fresh with the new DDL — old files are recreated, not migrated
        state.write("reverse_etl_x", 1, "[]", source_fqn="c.s.t", label="x")
        assert state.rows_for_source("c.s.t") == [("reverse_etl_x", "x")]
