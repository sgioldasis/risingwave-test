"""The demo change asset: the SQL it builds for each change, and what the asset does with it.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets import reverse_etl_change as change_module
from orchestration.assets.reverse_etl_change import DemoChangeSkip, change_statements
from tests.fakes import bind_connections, error_text, make_config

TABLE = "de_dev.sr_poc_external.reverse_etl_unittest_source"


@pytest.fixture(autouse=True)
def fixed_values(monkeypatch):
    """Statements carry a clock time and a random number; pin both so the SQL can be compared exactly."""
    monkeypatch.setattr(change_module.time, "strftime", lambda fmt: "12:00:00")
    monkeypatch.setattr(change_module.random, "uniform", lambda low, high: 42.0)


@pytest.fixture
def cfg():
    return make_config(
        source_columns=("id BIGINT", "description STRING", "score DOUBLE", "updated_at TIMESTAMP"), key_column="id"
    )


def delta_cfg():
    return make_config(source_columns=("id BIGINT", "description STRING"), key_column="id", sync_name="reverse_etl_delta")


class TestRowChanges:
    def test_an_insert_takes_the_next_id_and_a_value_per_column(self, cfg):
        assert change_statements(cfg, "insert") == [
            f"INSERT INTO {TABLE} (id, description, score, updated_at) "
            f"SELECT coalesce(max(id), 0) + 1, 'demo 12:00:00', 42.0, current_timestamp() FROM {TABLE}"
        ]

    def test_an_update_changes_every_column_of_the_latest_row(self, cfg):
        assert change_statements(cfg, "update") == [
            f"UPDATE {TABLE} SET description = 'demo 12:00:00', score = 42.0, updated_at = current_timestamp() "
            f"WHERE id = (SELECT max(id) FROM {TABLE})"
        ]

    def test_a_delete_removes_the_latest_row(self, cfg):
        assert change_statements(cfg, "delete") == [f"DELETE FROM {TABLE} WHERE id = (SELECT max(id) FROM {TABLE})"]

    def test_an_unknown_change_is_refused(self, cfg):
        with pytest.raises(ValueError, match="got 'truncate'"):
            change_statements(cfg, "truncate")


class TestColumnChanges:
    def test_add_column_takes_the_first_free_number_and_populates_the_latest_row(self, cfg):
        statements = change_statements(cfg, "add_column", {"new_col_1", "new_col_3"})
        assert statements == [
            f"ALTER TABLE {TABLE} ADD COLUMN new_col_2 STRING",
            f"UPDATE {TABLE} SET new_col_2 = 'added 12:00:00' WHERE id = (SELECT max(id) FROM {TABLE})",
        ]

    def test_rename_flips_the_highest_new_column_to_renamed(self, cfg):
        assert change_statements(cfg, "rename_column", {"new_col_1", "new_col_2"}) == [
            f"ALTER TABLE {TABLE} RENAME COLUMN new_col_2 TO renamed_col_2"
        ]

    def test_rename_flips_a_renamed_column_back_when_no_new_column_is_left(self, cfg):
        assert change_statements(cfg, "rename_column", {"renamed_col_4"}) == [
            f"ALTER TABLE {TABLE} RENAME COLUMN renamed_col_4 TO new_col_4"
        ]

    def test_rename_refuses_when_the_target_name_is_taken(self, cfg):
        with pytest.raises(DemoChangeSkip, match="both new_col_1 and renamed_col_1 exist"):
            change_statements(cfg, "rename_column", {"new_col_1", "renamed_col_1"})

    def test_rename_with_no_demo_column_is_a_skip(self, cfg):
        with pytest.raises(DemoChangeSkip, match="run add_column first"):
            change_statements(cfg, "rename_column", {"description"})

    def test_drop_removes_the_highest_demo_column_whichever_prefix(self, cfg):
        assert change_statements(cfg, "drop_column", {"new_col_2", "renamed_col_5"}) == [
            f"ALTER TABLE {TABLE} DROP COLUMN renamed_col_5"
        ]

    def test_drop_never_touches_a_configured_column(self, cfg):
        with pytest.raises(DemoChangeSkip, match="run add_column first"):
            change_statements(cfg, "drop_column", {"description", "score"})

    def test_a_skip_is_a_value_error_so_other_callers_still_see_it_as_one(self):
        assert issubclass(DemoChangeSkip, ValueError)


class TestBulkLoad:
    def test_rows_are_split_into_commits_of_at_most_the_ceiling(self):
        statements = change_statements(delta_cfg(), "bulk_1m", rows=250_000)
        assert [s.count("sequence(0, ") for s in statements] == [1, 1, 1]
        assert [s.split("sequence(0, ")[1].split(")")[0] for s in statements] == ["99999", "99999", "49999"]

    def test_each_commit_chains_off_the_tables_current_max_id(self):
        (statement,) = change_statements(delta_cfg(), "bulk_1m", rows=10)
        assert f"SELECT coalesce(max(id), 0) AS base FROM {TABLE}" in statement
        assert "'bulk-' || CAST(base + 1 + j AS STRING)" in statement

    @pytest.mark.parametrize("rows", [0, -1, change_module.BULK_MAX_ROWS + 1])
    def test_a_row_count_out_of_range_is_refused(self, rows):
        with pytest.raises(ValueError, match="rows must be between"):
            change_statements(delta_cfg(), "bulk_1m", rows=rows)

    def test_only_the_delta_sync_has_a_bulk_load(self, cfg):
        with pytest.raises(ValueError, match="delta sync's bulk load generator"):
            change_statements(cfg, "bulk_1m", rows=10)


def run_change(cfg, monkeypatch, resources, change, **options):
    config = {"ops": {f"{cfg.sync_name}_make_change": {"config": {"change": change}}}}
    bind_connections(monkeypatch, resources(**options))
    return dg.materialize(
        [change_module.build_change_asset(cfg)], run_config=config, raise_on_error=False
    )


def change_sql(result):
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata["databricks_change_sql"].value


class TestChangeAsset:
    def test_the_statements_run_in_order_and_are_reported_for_the_page(self, cfg, calls, monkeypatch, resources):
        result = run_change(cfg, monkeypatch, resources, "add_column", table_columns=("id", "description"))
        assert result.success
        sent = [detail for kind, detail in calls if kind == "databricks" and not detail.startswith("DESCRIBE")]
        assert sent[0] == f"ALTER TABLE {TABLE} ADD COLUMN new_col_1 STRING"
        assert change_sql(result)["sql"] == sent

    def test_the_tables_columns_are_read_only_for_column_changes(self, cfg, calls, monkeypatch, resources):
        run_change(cfg, monkeypatch, resources, "update", table_columns=("id",))
        assert not any(detail.startswith("DESCRIBE") for _, detail in calls)

    def test_a_rename_with_nothing_to_rename_ends_green_and_says_why(self, cfg, calls, monkeypatch, resources):
        result = run_change(cfg, monkeypatch, resources, "rename_column", table_columns=("id", "description"))
        assert result.success
        report = change_sql(result)
        assert report["sql"] == [] and "run add_column first" in report["skipped"]
        assert not [detail for kind, detail in calls if kind == "databricks" and not detail.startswith("DESCRIBE")]

    def test_a_missing_configured_column_points_at_the_stale_sync_config(self, cfg, monkeypatch, resources):
        result = run_change(cfg, monkeypatch, resources, "update", sql_error=RuntimeError("[UNRESOLVED_COLUMN] score"))
        assert not result.success
        assert "source_columns" in error_text(result)

    def test_any_other_statement_failure_is_not_reworded(self, cfg, monkeypatch, resources):
        result = run_change(cfg, monkeypatch, resources, "update", sql_error=RuntimeError("PERMISSION_DENIED"))
        assert not result.success
        text = error_text(result)
        assert "PERMISSION_DENIED" in text and "source_columns" not in text

    def test_a_bulk_load_warns_unless_the_sync_reads_one_commit_per_chunk(self, calls, monkeypatch, resources):
        monkeypatch.delenv("REVERSE_ETL_CDF_CHUNK_VERSIONS", raising=False)
        cfg = delta_cfg()
        warnings = []
        monkeypatch.setattr(
            dg.DagsterLogManager, "warning", lambda self, message, *a, **k: warnings.append(message), raising=False
        )
        assert run_change(cfg, monkeypatch, resources, "bulk_1m").success
        assert any("REVERSE_ETL_CDF_CHUNK_VERSIONS=1" in message for message in warnings)
