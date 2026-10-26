"""The table-setup asset: an existing table is checked, never created from the key alone.

Runs through Dagster (materialize) with fake resources. Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg

from orchestration.assets import reverse_etl_demo_assets as demo_assets
from tests.fakes import bind_connections, error_text, make_config


def run_setup(cfg, monkeypatch, resources, **options):
    bind_connections(monkeypatch, resources(**options))
    return dg.materialize([demo_assets.build_table_setup_asset(cfg)], raise_on_error=False)


def creates(calls) -> list:
    return [c for c in calls if c[0] == "databricks" and "CREATE TABLE" in c[1]]


def test_without_source_columns_an_existing_table_is_checked_not_created(calls, monkeypatch, resources):
    cfg = make_config(source_columns=())
    assert run_setup(cfg, monkeypatch, resources).success
    assert ("location", cfg.source_fqn) in calls
    assert not creates(calls)


def test_without_source_columns_a_missing_table_fails_instead_of_being_created(calls, monkeypatch, resources):
    result = run_setup(
        make_config(source_columns=()), monkeypatch, resources, location_error=RuntimeError("UC REST lookup failed (404)")
    )
    assert not result.success
    assert "404" in error_text(result)
    assert not creates(calls)


def test_with_source_columns_the_table_is_created_if_absent(calls, monkeypatch, resources):
    cfg = make_config(source_columns=("description STRING",))
    assert run_setup(cfg, monkeypatch, resources).success
    statements = [c for c in creates(calls) if "CREATE TABLE IF NOT EXISTS" in c[1]]
    assert len(statements) == 1
    assert cfg.source_fqn in statements[0][1]
    assert "description STRING" in statements[0][1]
    assert ("location", cfg.source_fqn) not in calls


def test_the_watermark_table_is_created_in_postgres(calls, monkeypatch, resources):
    assert run_setup(make_config(), monkeypatch, resources).success
    assert [c for c in calls if c[0] == "postgres" and "CREATE TABLE IF NOT EXISTS reverse_etl_sync_state" in c[1]]
