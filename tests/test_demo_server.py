"""The demo server's Jobotron launch path (scripts/reverse_etl_demo_server.py):
the shapes of the GraphQL params it builds for the demo UI's buttons.

Found live (2026-11-11): the publish run config was keyed by the BARE asset
name, but the library asset's op name folds the prefix in with a double
underscore (jobotron__unverified_customers_<market>) — Dagster accepted the
run anyway and silently ignored the per-run dry_run switch, so a "real"
publish ran as a dry run.

Run from the repo root:  .venv/bin/python -m pytest
"""

from types import SimpleNamespace

import pytest

from scripts import reverse_etl_demo_server as server


@pytest.fixture
def launched(monkeypatch):
    """Capture the params launch_jobotron hands to _launch_run."""
    calls: dict = {}
    monkeypatch.setattr(
        server,
        "gql",
        lambda *a, **k: {
            "repositoriesOrError": {"nodes": [{"name": "__repository__", "location": {"name": "orchestration.definitions"}}]}
        },
    )

    def record(params, _retried=False):
        calls["params"] = params
        return {"runId": "run-1"}

    monkeypatch.setattr(server, "_launch_run", record)
    return calls


@pytest.fixture
def topic_factory(monkeypatch):
    """Fake the publish preflight's Kafka admin; parameterize topic presence."""

    def factory(*topics: str):
        monkeypatch.setattr(server, "_kafka_inspect_conf", lambda: {"bootstrap.servers": "x:9092"})

        class FakeAdmin:
            def __init__(self, conf):
                pass

            def list_topics(self, timeout=None):
                return SimpleNamespace(topics=set(topics))

        monkeypatch.setattr("confluent_kafka.admin.AdminClient", FakeAdmin)

    return factory


@pytest.fixture
def topic_exists(topic_factory):
    return topic_factory(*[f"jobotron_unverified_customers_{m}" for m in server.MARKETS])


class TestLaunchJobotron:
    def test_publish_config_is_keyed_by_the_full_op_name(self, launched, topic_exists):
        server.launch_jobotron("publish", "cy", False)
        selector = launched["params"]["selector"]
        assert selector["assetSelection"] == [{"path": ["jobotron", "unverified_customers_cy"]}]
        # The prefix folds into the op name with a DOUBLE underscore; keyed by
        # the bare name, Dagster silently drops the config.
        assert launched["params"]["runConfigData"]["ops"] == {"jobotron__unverified_customers_cy": {"config": {"dry_run": False}}}

    def test_publish_with_an_omitted_mode_is_a_dry_run(self, launched, topic_exists):
        # bool(None) was False: a bare publish published FOR REAL while the
        # docstring promises the YAML default (true). Omission now means dry.
        server.launch_jobotron("publish", "cy", None)
        assert launched["params"]["runConfigData"]["ops"] == {"jobotron__unverified_customers_cy": {"config": {"dry_run": True}}}

    def test_real_publish_preflights_the_topic(self, launched, topic_factory):
        # The library's producer would retry UNKNOWN_TOPIC_OR_PART for 120s;
        # the server fails fast instead, before any launch.
        topic_factory()  # no topics exist
        with pytest.raises(ValueError, match="run setup first"):
            server.launch_jobotron("publish", "cy", False)
        assert "params" not in launched

    def test_publish_honours_dry_run_true(self, launched):
        server.launch_jobotron("publish", "cy", True)
        assert launched["params"]["runConfigData"]["ops"] == {"jobotron__unverified_customers_cy": {"config": {"dry_run": True}}}

    def test_setup_config_keyed_by_its_own_asset_name(self, launched):
        # The local plumbing assets have no prefix: the op key is the name itself.
        server.launch_jobotron("setup", "cy", False)
        assert launched["params"]["runConfigData"]["ops"] == {"jobotron_unverified_customers_cy_setup": {"config": {"dry_run": False}}}

    def test_consume_sends_no_config(self, launched):
        server.launch_jobotron("consume", "cy", None)
        assert "runConfigData" not in launched["params"]

    def test_reset_runs_the_instance_job(self, launched):
        server.launch_jobotron("reset", "cy", None)
        params = launched["params"]
        assert params["selector"]["jobName"] == "jobotron_unverified_customers_cy_reset_job"
        assert "runConfigData" not in params
