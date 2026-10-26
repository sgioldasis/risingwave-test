"""What the reusable ReverseEtlCdfSync builds for Dagster: the sync asset -- plus, by default,
the topic asset and the bootstrap job that runs topic then sync.

The definitions come from the component's own build_defs(), the call Dagster makes when it loads a defs.yaml. No
services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg

from orchestration.components.reverse_etl_cdf_sync import ReverseEtlCdfSync
from tests.fakes import FakeAdmin, FakeKafka

REQUIRED = {
    "name": "orders",
    "catalog": "de_dev",
    "schema_name": "sr_poc_external",
    "source_table": "existing_table",
    "key_column": "id",
    "state_backend": "postgres",
    # Given -> verbatim (the derivation path has its own tests in test_cdf_sync_attributes.py).
    "kafka_topic": "orders_cdc",
    "sync_name": "orders_watermark",
}

SYNC_ASSET = "reverse_etl_orders_to_kafka"
TOPIC_ASSET = "reverse_etl_orders_topic_setup"
BOOTSTRAP_JOB = "reverse_etl_orders_bootstrap_job"


def build(**attributes) -> dg.Definitions:
    component = ReverseEtlCdfSync.from_attributes_dict(attributes={**REQUIRED, **attributes})
    return component.build_defs(dg.ComponentTree.for_test().load_context)


def sync_asset(defs: dg.Definitions, name: str = SYNC_ASSET):
    """The sync asset itself, selected by name: the defs also carry the topic asset by default."""
    (asset,) = [a for a in defs.assets if name in {s.key.to_user_string() for s in a.specs}]
    return asset


def spec_names(defs: dg.Definitions) -> set[str]:
    return {s.key.to_user_string() for a in defs.assets for s in a.specs}


class TestTheSync:
    def test_the_component_builds_the_sync_with_the_documented_names(self):
        (spec,) = sync_asset(build()).specs
        assert spec.key.to_user_string() == SYNC_ASSET
        assert spec.group_name == "reverse_etl_orders"

    def test_it_builds_no_checks_schedules_or_sensors(self):
        defs = build()
        assert list(defs.asset_checks or []) == []
        assert list(defs.schedules or []) == [] and list(defs.sensors or []) == []

    def test_the_name_overrides_follow_the_component(self):
        (spec,) = sync_asset(build(group_name="team_group", sync_asset="team_sync"), name="team_sync").specs
        assert spec.key.to_user_string() == "team_sync"
        assert spec.group_name == "team_group"

    def test_declared_deps_become_the_assets_upstream_keys(self):
        (spec,) = sync_asset(build(create_topic=False, deps=["upstream_asset", "other"])).specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == {"upstream_asset", "other"}

    def test_without_deps_the_asset_declares_none(self):
        (spec,) = sync_asset(build(create_topic=False)).specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == set()

    def test_the_asset_needs_only_the_four_resources(self):
        required = set(sync_asset(build()).required_resource_keys)
        assert {"postgres", "databricks", "kafka", "registry"} <= required
        assert {"risingwave", "http"} & required == set()

    def test_a_duckdb_state_sync_does_not_need_the_postgres_resource(self):
        """The postgres resource belongs to the postgres state backend (plan section 12): a
        duckdb-state sync needs nothing but its file path."""
        asset = sync_asset(build(state_backend="duckdb", state_path="/tmp/orders/state.duckdb"))
        assert "postgres" not in set(asset.required_resource_keys)
        assert {"databricks", "kafka", "registry"} <= set(asset.required_resource_keys)

    def test_the_definitions_load_with_only_the_four_fakes(self, resources):
        four = {k: v for k, v in resources().items() if k not in ("risingwave", "http")}
        dg.Definitions.validate_loadable(dg.Definitions.merge(build(), dg.Definitions(resources=four)))

    def test_two_syncs_merge_without_a_name_clash(self):
        merged = dg.Definitions.merge(build(name="one"), build(name="two"))
        assert len(list(merged.assets)) == 4  # two syncs, two topics


class TestTheTopicBootstrap:
    def test_by_default_the_component_builds_the_topic_asset_and_the_bootstrap_job(self):
        defs = build()
        assert spec_names(defs) == {SYNC_ASSET, TOPIC_ASSET}
        (job,) = defs.jobs
        assert job.name == BOOTSTRAP_JOB

    def test_create_topic_false_builds_neither(self):
        """The topic is the team's platform's: the component goes back to building only the sync."""
        defs = build(create_topic=False)
        assert spec_names(defs) == {SYNC_ASSET}
        assert list(defs.jobs or []) == []

    def test_the_topic_asset_sits_in_the_syncs_group(self):
        (topic,) = [a for a in build().assets if TOPIC_ASSET in {s.key.to_user_string() for s in a.specs}]
        (spec,) = topic.specs
        assert spec.group_name == "reverse_etl_orders"

    def test_the_sync_runs_after_the_topic_exists(self):
        """The bootstrap job's topic -> sync edge is this dependency; the team's own deps ride along."""
        (spec,) = sync_asset(build(deps=["team_upstream"])).specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == {TOPIC_ASSET, "team_upstream"}

    def test_the_bootstrap_job_runs_both_assets_in_one_process(self):
        (job,) = build().jobs
        assert job.executor_def is dg.in_process_executor
        assert {key.to_user_string() for key in job.selection.selected_keys} == {TOPIC_ASSET, SYNC_ASSET}

    def test_the_topic_asset_creates_the_topic_compacted(self, calls):
        """A Debezium-style change topic wants cleanup.policy=compact: the broker default (delete)
        risks retention dropping events a lagging consumer has not read."""
        (topic,) = [a for a in build().assets if TOPIC_ASSET in {s.key.to_user_string() for s in a.specs}]
        admin = FakeAdmin()
        result = dg.materialize([topic], resources={"kafka": FakeKafka(calls, admin=admin)})
        assert result.success
        assert admin.created == ["orders_cdc"]
        assert admin.created_configs == [{"cleanup.policy": "compact"}]
