"""What the reusable ReverseEtlCdfSync builds for Dagster: exactly one asset, the sync — and nothing else.

The definitions come from the component's own build_defs(), the call Dagster makes when it loads a defs.yaml. No
services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg

from orchestration.components.reverse_etl_cdf_sync import ReverseEtlCdfSync

REQUIRED = {
    "name": "orders",
    "catalog": "de_dev",
    "schema_name": "sr_poc_external",
    "source_table": "existing_table",
    "key_column": "id",
    "kafka_topic": "orders_cdc",
    "sync_name": "orders_watermark",
}


def build(**attributes) -> dg.Definitions:
    component = ReverseEtlCdfSync.from_attributes_dict(attributes={**REQUIRED, **attributes})
    return component.build_defs(dg.ComponentTree.for_test().load_context)


class TestOneAsset:
    def test_the_component_builds_exactly_one_asset_the_sync(self):
        (asset,) = build().assets
        (spec,) = asset.specs
        assert spec.key.to_user_string() == "reverse_etl_orders_to_kafka"
        assert spec.group_name == "reverse_etl_orders"

    def test_it_builds_no_jobs_checks_schedules_or_sensors(self):
        defs = build()
        assert list(defs.jobs or []) == [] and list(defs.asset_checks or []) == []
        assert list(defs.schedules or []) == [] and list(defs.sensors or []) == []

    def test_the_name_overrides_follow_the_component(self):
        (asset,) = build(group_name="team_group", sync_asset="team_sync").assets
        (spec,) = asset.specs
        assert spec.key.to_user_string() == "team_sync"
        assert spec.group_name == "team_group"

    def test_declared_deps_become_the_assets_upstream_keys(self):
        (asset,) = build(deps=["upstream_asset", "other"]).assets
        (spec,) = asset.specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == {"upstream_asset", "other"}

    def test_without_deps_the_asset_declares_none(self):
        (asset,) = build().assets
        (spec,) = asset.specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == set()

    def test_the_asset_needs_only_the_four_resources(self):
        (asset,) = build().assets
        required = set(asset.required_resource_keys)
        assert {"postgres", "databricks", "kafka", "registry"} <= required
        assert {"risingwave", "http"} & required == set()

    def test_the_definitions_load_with_only_the_four_fakes(self, resources):
        four = {k: v for k, v in resources().items() if k not in ("risingwave", "http")}
        dg.Definitions.validate_loadable(dg.Definitions.merge(build(), dg.Definitions(resources=four)))

    def test_two_syncs_merge_without_a_name_clash(self):
        merged = dg.Definitions.merge(build(name="one"), build(name="two"))
        assert len(list(merged.assets)) == 2
