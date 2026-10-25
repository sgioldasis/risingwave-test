"""The reset job runs op by op through Dagster with fake resources. The watermark and the location cache are
cleared BEFORE any Databricks call, so a failed drop cannot leave a stale watermark behind (the recreated source
restarts at version 0).

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets import reverse_etl_reset as reset_module
from tests.fakes import error_text, kinds, make_config


@pytest.fixture
def run_reset(resources):
    def run(cfg, **options):
        job = reset_module.build_reset_job(cfg)
        job = dg.Definitions(jobs=[job], resources=resources(**options)).get_job_def(cfg.reset_job)
        return job.execute_in_process(raise_on_error=False)

    return run


def watermark_delete_index(calls) -> int:
    return next(i for i, c in enumerate(calls) if c[0] == "postgres" and "DELETE FROM reverse_etl_sync_state" in c[1])


def test_the_ops_run_in_dependency_order(calls, run_reset):
    assert run_reset(make_config()).success
    # connector, RisingWave, Postgres table, Kafka, then the Databricks op (watermark delete first)
    order = [kind for kind in kinds(calls) if kind in ("http", "risingwave", "postgres", "kafka", "databricks")]
    assert order[0] == "http"
    assert order.index("risingwave") < order.index("postgres") < order.index("kafka") < order.index("databricks")


def test_the_registry_subjects_are_not_deleted_for_json(calls, run_reset):
    assert run_reset(make_config()).success
    assert not [c for c in calls if c[0] == "http" and "/subjects/" in c[1]]


def test_the_registry_subjects_are_deleted_for_avro_before_the_databricks_drops(calls, run_reset):
    cfg = make_config(encoding="avro", schema_registry_url="http://registry.invalid")
    assert run_reset(cfg).success
    subject_calls = [i for i, c in enumerate(calls) if c[0] == "http" and "/subjects/" in c[1]]
    assert len(subject_calls) == 4  # key and value, soft then permanent
    assert max(subject_calls) < kinds(calls).index("databricks")


def test_watermark_and_cache_are_cleared_before_the_first_databricks_call(calls, run_reset):
    assert run_reset(make_config()).success
    order = kinds(calls)
    assert watermark_delete_index(calls) < order.index("databricks")
    assert order.index("cache") < order.index("databricks")


def test_both_databricks_tables_are_dropped_on_success(calls, run_reset):
    cfg = make_config()
    run_reset(cfg)
    drops = [sql for kind, sql in calls if kind == "databricks"]
    assert drops == [f"DROP TABLE IF EXISTS {cfg.source_fqn}", f"DROP TABLE IF EXISTS {cfg.state_fqn}"]


def test_a_failed_drop_still_leaves_the_watermark_cleared_and_fails_the_run(calls, run_reset):
    result = run_reset(make_config(), sql_error=RuntimeError("PERMISSION_DENIED: does not have MANAGE"))
    assert not result.success
    assert "MANAGE" in error_text(result)
    watermark_delete_index(calls)  # raises StopIteration if the watermark was not cleared
    assert "cache" in kinds(calls)
    assert kinds(calls).count("databricks") == 1  # stopped at the first failing drop


def test_the_guard_refuses_a_name_outside_the_allowlist(calls, run_reset):
    result = run_reset(make_config(kafka_topic="production_orders"))
    assert not result.success
    assert kinds(calls) == []  # nothing was touched
    assert "Refusing to reset" in error_text(result)
