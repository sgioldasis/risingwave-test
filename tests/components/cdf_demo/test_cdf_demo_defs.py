"""What ReverseEtlCdfDemo builds for Dagster: its assets, jobs, check and schedule, and how the options change them.

The definitions come from the component's own build_defs(), the call Dagster makes when it loads a defs.yaml. No
services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.components.reverse_etl_cdf_demo import ReverseEtlCdfDemo

COLUMNS = ["id BIGINT NOT NULL", "value STRING"]


def build(**attributes) -> dg.Definitions:
    component = ReverseEtlCdfDemo.from_attributes_dict(
        attributes={"name": "unittest", "catalog": "de_dev", "schema_name": "sr_poc_external", **attributes}
    )
    return component.build_defs(dg.ComponentTree.for_test().load_context)


def asset_names(defs) -> set:
    return {key.to_user_string() for asset in defs.assets for key in asset.keys}


def job_names(defs) -> set:
    return {job.name for job in defs.jobs}


def with_fakes(defs, resources) -> dg.Definitions:
    return dg.Definitions.merge(defs, dg.Definitions(resources=resources()))


BASE = {
    "reverse_etl_unittest_table_setup",
    "reverse_etl_unittest_to_kafka",
    "reverse_etl_unittest_risingwave_target",
    "reverse_etl_unittest_jdbc_sink",
    "reverse_etl_unittest_topic_setup",
}


class TestAssets:
    def test_a_sync_on_an_existing_table_has_the_four_pipeline_assets_and_the_topic_asset(self):
        assert asset_names(build()) == BASE

    def test_source_columns_add_the_make_change_asset(self):
        assert asset_names(build(source_columns=COLUMNS)) == BASE | {"reverse_etl_unittest_make_change"}

    def test_seed_statements_add_the_seed_asset(self):
        assert asset_names(build(seed_statements=["INSERT INTO {source_table} VALUES (1)"])) == BASE | {"reverse_etl_unittest_seed"}

    def test_a_shared_topic_asset_is_not_built_a_second_time(self):
        assert asset_names(build(create_topic_asset=False)) == BASE - {"reverse_etl_unittest_topic_setup"}

    def groups(self, defs) -> dict:
        return {spec.key.to_user_string(): spec.group_name for asset in defs.assets for spec in asset.specs}

    def test_the_pipeline_seed_and_topic_assets_are_in_the_syncs_group(self):
        groups = self.groups(build(seed_statements=["SELECT 1"]))
        assert set(groups) == BASE | {"reverse_etl_unittest_seed"}
        assert set(groups.values()) == {"reverse_etl_unittest"}

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Known gap: build_change_asset sets no group_name, so make_change shows in Dagster's 'default' group instead of "
            "the sync's own. Passing group_name=cfg.group_name would fix it (it can also change group-based selections); "
            "remove this marker with that fix."
        ),
    )
    def test_the_make_change_asset_is_in_the_syncs_group_too(self):
        groups = self.groups(build(source_columns=COLUMNS))
        assert groups["reverse_etl_unittest_make_change"] == "reverse_etl_unittest"

    def test_the_group_can_be_overridden(self):
        assert set(self.groups(build(group_name="a_group")).values()) == {"a_group"}

    def test_asset_names_follow_the_overrides(self):
        defs = build(sync_asset="my_sync", sink_asset="my_sink")
        assert {"my_sync", "my_sink"} <= asset_names(defs)
        assert "reverse_etl_unittest_to_kafka" not in asset_names(defs)


class TestJobs:
    def test_every_sync_gets_a_setup_job_a_reset_job_and_a_health_check_job(self):
        names = job_names(build())
        assert {"reverse_etl_unittest_setup_job", "reverse_etl_unittest_reset_job"} <= names
        assert len(names) == 3

    def test_source_columns_add_the_make_change_job(self):
        assert "reverse_etl_unittest_make_change_job" in job_names(build(source_columns=COLUMNS))

    def test_a_sync_without_source_columns_gets_no_make_change_job_or_asset(self):
        defs = build()
        assert "reverse_etl_unittest_make_change_job" not in job_names(defs)
        assert "reverse_etl_unittest_make_change" not in asset_names(defs)

    def test_job_names_follow_the_overrides(self):
        names = job_names(build(setup_job="my_setup", reset_job="my_reset"))
        assert {"my_setup", "my_reset"} <= names

    def test_there_is_one_asset_check_and_one_schedule_for_the_sink_health(self):
        defs = build()
        assert len(defs.asset_checks) == 1
        assert len(defs.schedules) == 1


class TestSetupJobSelection:
    def selected(self, defs, resources, job):
        resolved = with_fakes(defs, resources).resolve_job_def(job)
        return {key.to_user_string() for key in resolved.asset_layer.executable_asset_keys}

    def test_the_sync_asset_depends_on_the_demo_setup_and_topic_assets(self):
        """The bare core declares no deps; the demo wires its own assets in as upstream ones."""
        sync = next(
            asset for asset in build().assets if "reverse_etl_unittest_to_kafka" in {k.to_user_string() for k in asset.keys}
        )
        (spec,) = sync.specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == {
            "reverse_etl_unittest_table_setup",
            "reverse_etl_unittest_topic_setup",
        }
        assert {"risingwave", "http"} <= set(sync.required_resource_keys)

    def test_the_setup_job_runs_the_pipeline_assets_and_the_topic_asset(self, resources):
        assert self.selected(build(), resources, "reverse_etl_unittest_setup_job") == BASE

    def test_seed_runs_in_the_setup_job_too(self, resources):
        defs = build(seed_statements=["SELECT 1"])
        assert self.selected(defs, resources, "reverse_etl_unittest_setup_job") == BASE | {"reverse_etl_unittest_seed"}

    def test_make_change_is_not_part_of_the_setup_job(self, resources):
        defs = build(source_columns=COLUMNS)
        assert "reverse_etl_unittest_make_change" not in self.selected(defs, resources, "reverse_etl_unittest_setup_job")

    def test_a_shared_topic_asset_defined_elsewhere_is_still_part_of_the_job(self, resources):
        @dg.asset(name="reverse_etl_unittest_topic_setup")
        def shared_topic_setup():
            return None

        defs = dg.Definitions.merge(build(create_topic_asset=False), dg.Definitions(assets=[shared_topic_setup]))
        assert self.selected(defs, resources, "reverse_etl_unittest_setup_job") == BASE


class TestLoadable:
    @pytest.mark.parametrize(
        "attributes",
        [
            {},
            {"source_columns": COLUMNS},
            {"source_columns": COLUMNS, "seed_statements": ["SELECT 1"]},
            {"encoding": "avro"},
            {"create_topic_asset": False},
        ],
        ids=["existing-table", "demo-table", "demo-table-seeded", "avro", "shared-topic"],
    )
    def test_the_definitions_validate_and_every_job_resolves(self, resources, attributes):
        defs = build(**attributes)
        if attributes.get("create_topic_asset") is False:
            # the shared asset belongs to the project, not to the component
            @dg.asset(name="reverse_etl_unittest_topic_setup")
            def shared_topic_setup():
                return None

            defs = dg.Definitions.merge(defs, dg.Definitions(assets=[shared_topic_setup]))
        loaded = with_fakes(defs, resources)
        dg.Definitions.validate_loadable(loaded)
        for job in loaded.resolve_all_job_defs():
            assert loaded.resolve_job_def(job.name) is not None

    def test_two_syncs_with_different_labels_merge_without_a_name_clash(self):
        merged = dg.Definitions.merge(build(name="one"), build(name="two"))
        assert len(asset_names(merged)) == 2 * len(BASE)
        assert len(job_names(merged)) == 6
