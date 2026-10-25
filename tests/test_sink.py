"""The Debezium sink asset and its health check, run through Dagster with fake resources.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets import reverse_etl_debezium_sink as sink_module
from tests.fakes import error_text, make_config


@pytest.fixture(autouse=True)
def kafka_env(monkeypatch):
    """The connector config needs the Kafka bootstrap; do not depend on a .env another test happened to load."""
    monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")


def run_sink(cfg, resources, **options):
    return dg.materialize(
        [sink_module.build_debezium_sink_asset(cfg)], resources=resources(**options), raise_on_error=False
    )


def test_the_connector_config_is_put_and_awaited_until_running(calls, resources):
    cfg = make_config()
    assert run_sink(cfg, resources).success
    url = f"{sink_module.CONNECT_URL}/connectors/{cfg.connector_name}"
    assert calls.count(("http", f"PUT {url}/config")) == 1
    assert ("http", f"GET {url}/status") in calls


def test_a_failed_connector_fails_the_asset_with_its_trace(resources):
    result = run_sink(make_config(), resources, connector_state="FAILED")
    assert not result.success
    assert "FAILED" in error_text(result)


@pytest.fixture
def run_check(defs_with_fakes):
    def run(**options):
        cfg = make_config()
        job = defs_with_fakes(cfg, **options).resolve_job_def(f"{cfg.sync_name}_sink_health_job")
        return job.execute_in_process(raise_on_error=False).get_asset_check_evaluations()[0]

    return run


def test_a_running_connector_with_no_lag_passes(run_check):
    assert run_check().passed


def test_a_failed_connector_fails_the_check(run_check):
    evaluation = run_check(connector_state="FAILED")
    assert not evaluation.passed
    assert "FAILED" in evaluation.description


def test_a_large_lag_warns(run_check):
    evaluation = run_check(lag=50_000)
    assert not evaluation.passed
    assert "lag" in evaluation.description
