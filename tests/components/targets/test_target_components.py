"""The released target components (orchestration/components/reverse_etl_targets.py): what each
builds for Dagster, what each requires, and what each derives from the label — the released
components' surface, one YAML per concern. The definitions come from the components' own
build_defs(), the call Dagster makes when it loads a defs.yaml. No services needed.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.components.reverse_etl_targets import (
    ReverseEtlDuckdbTarget,
    ReverseEtlPostgresTarget,
    ReverseEtlRisingwaveTarget,
)

SYNC_ASSET = "reverse_etl_company_to_kafka"
TOPIC_ASSET = "reverse_etl_company_topic_setup"


def build(component_class, **attributes) -> dg.Definitions:
    component = component_class.from_attributes_dict(attributes=attributes)
    return component.build_defs(dg.ComponentTree.for_test().load_context)


def asset_names(defs: dg.Definitions) -> set:
    return {key.to_user_string() for asset in defs.assets for key in asset.keys}


def spec_names(defs: dg.Definitions) -> set:
    return {s.key.to_user_string() for a in defs.assets for s in a.specs}


def deps_of(defs: dg.Definitions, name: str) -> set:
    (asset,) = [a for a in defs.assets if name in {s.key.to_user_string() for s in a.specs}]
    (spec,) = asset.specs
    return {dep.asset_key.to_user_string() for dep in spec.deps}


class TestRisingwave:
    REQUIRED = {"name": "company", "source_fqn": "de_dev.their_schema.dim_company", "key_column": "Company_ID"}

    def build(self, **attributes):
        return build(ReverseEtlRisingwaveTarget, **{**self.REQUIRED, **attributes})

    @pytest.mark.parametrize("missing", sorted(REQUIRED))
    def test_each_required_attribute_must_be_given(self, missing):
        attributes = {key: value for key, value in self.REQUIRED.items() if key != missing}
        with pytest.raises(Exception, match=missing):
            ReverseEtlRisingwaveTarget.from_attributes_dict(attributes=attributes)

    def test_it_builds_exactly_the_risingwave_table_asset(self):
        defs = self.build()
        assert spec_names(defs) == {"reverse_etl_company_risingwave_target"}
        assert list(defs.asset_checks or []) == [] and list(defs.jobs or []) == []

    def test_the_golden_names_derive_from_the_label(self):
        cfg = ReverseEtlRisingwaveTarget.from_attributes_dict(attributes=self.REQUIRED).to_config()
        assert (cfg.sync_name, cfg.kafka_topic) == ("reverse_etl_company", "reverse_etl_company_topic")
        assert (cfg.risingwave_table, cfg.risingwave_asset, cfg.group_name) == (
            "reverse_etl_company_target", "reverse_etl_company_risingwave_target", "reverse_etl_company",
        )
        # The envelope: the sync's formula over the same inputs — the wire contract's arithmetic.
        assert cfg.envelope_schema_name == "reverse_etl_company.their_schema.dim_company"

    def test_the_ordering_is_the_deps_field(self):
        assert deps_of(self.build(), "reverse_etl_company_risingwave_target") == set()
        assert deps_of(
            self.build(deps=[SYNC_ASSET, TOPIC_ASSET]), "reverse_etl_company_risingwave_target"
        ) == {SYNC_ASSET, TOPIC_ASSET}


class TestPostgres:
    REQUIRED = {"name": "company", "key_column": "Company_ID"}

    def build(self, **attributes):
        return build(ReverseEtlPostgresTarget, **{**self.REQUIRED, **attributes})

    @pytest.mark.parametrize("missing", sorted(REQUIRED))
    def test_each_required_attribute_must_be_given(self, missing):
        attributes = {key: value for key, value in self.REQUIRED.items() if key != missing}
        with pytest.raises(Exception, match=missing):
            ReverseEtlPostgresTarget.from_attributes_dict(attributes=attributes)

    def test_it_builds_the_sink_with_the_health_check_job_and_schedule(self):
        defs = self.build()
        assert spec_names(defs) == {"reverse_etl_company_jdbc_sink"}
        (check,) = defs.asset_checks
        assert [(spec.name, spec.asset_key.to_user_string()) for spec in check.check_specs] == [
            ("sink_healthy", "reverse_etl_company_jdbc_sink")
        ]
        assert {job.name for job in defs.jobs} == {"reverse_etl_company_sink_health_job"}
        (schedule,) = defs.schedules
        assert schedule.name == "reverse_etl_company_sink_health_schedule"
        assert schedule.cron_schedule == "*/5 * * * *"  # the library's default cadence

    def test_the_health_cron_is_overridable(self):
        (schedule,) = self.build(health_cron="*/15 * * * *").schedules
        assert schedule.cron_schedule == "*/15 * * * *"

    def test_the_golden_names_derive_from_the_label(self):
        cfg = ReverseEtlPostgresTarget.from_attributes_dict(attributes=self.REQUIRED).to_config()
        assert (cfg.connector_name, cfg.postgres_table) == ("reverse_etl_company_sink", "reverse_etl_company_target")
        assert (cfg.kafka_topic, cfg.sink_asset, cfg.consumer_group) == (
            "reverse_etl_company_topic", "reverse_etl_company_jdbc_sink", "connect-reverse_etl_company_sink",
        )
        assert cfg.sink_tasks_max == 1

    def test_the_ordering_is_the_deps_field(self):
        assert deps_of(self.build(), "reverse_etl_company_jdbc_sink") == set()
        assert deps_of(self.build(deps=[SYNC_ASSET]), "reverse_etl_company_jdbc_sink") == {SYNC_ASSET}


class TestDuckdb:
    REQUIRED = {"name": "company", "key_column": "Company_ID", "duckdb_target_path": "/workspace/mirror.duckdb"}

    def build(self, **attributes):
        return build(ReverseEtlDuckdbTarget, **{**self.REQUIRED, **attributes})

    @pytest.mark.parametrize("missing", sorted(REQUIRED))
    def test_each_required_attribute_must_be_given(self, missing):
        attributes = {key: value for key, value in self.REQUIRED.items() if key != missing}
        with pytest.raises(Exception, match=missing):
            ReverseEtlDuckdbTarget.from_attributes_dict(attributes=attributes)

    def test_it_builds_exactly_the_mirror_asset(self):
        defs = self.build()
        assert spec_names(defs) == {"reverse_etl_company_duckdb_target"}
        assert list(defs.asset_checks or []) == [] and list(defs.jobs or []) == [] and list(defs.schedules or []) == []

    def test_the_golden_names_derive_from_the_label(self):
        cfg = ReverseEtlDuckdbTarget.from_attributes_dict(attributes=self.REQUIRED).to_config()
        assert (cfg.kafka_topic, cfg.duckdb_asset) == ("reverse_etl_company_topic", "reverse_etl_company_duckdb_target")
        # Rename-safe: the mirror reads from earliest and never commits offsets.
        assert cfg.consumer_group == "company-duckdb-target"

    def test_the_ordering_is_the_deps_field(self):
        assert deps_of(self.build(), "reverse_etl_company_duckdb_target") == set()
        assert deps_of(self.build(deps=[SYNC_ASSET]), "reverse_etl_company_duckdb_target") == {SYNC_ASSET}


class TestTogether:
    def test_the_three_targets_merge_without_a_name_clash(self):
        merged = dg.Definitions.merge(
            build(ReverseEtlRisingwaveTarget, **TestRisingwave.REQUIRED),
            build(ReverseEtlPostgresTarget, **TestPostgres.REQUIRED),
            build(ReverseEtlDuckdbTarget, **TestDuckdb.REQUIRED),
        )
        assert spec_names(merged) == {
            "reverse_etl_company_risingwave_target",
            "reverse_etl_company_jdbc_sink",
            "reverse_etl_company_duckdb_target",
        }

    def test_the_assets_are_loadable_with_no_resources(self):
        """The env contract: the connections come from the env-fed functions, so the merged
        definitions load with nothing bound at all."""
        merged = dg.Definitions.merge(
            build(ReverseEtlRisingwaveTarget, **TestRisingwave.REQUIRED),
            build(ReverseEtlPostgresTarget, **TestPostgres.REQUIRED),
            build(ReverseEtlDuckdbTarget, **TestDuckdb.REQUIRED),
        )
        dg.Definitions.validate_loadable(merged)

    def test_two_labels_never_share_a_name(self):
        first = build(ReverseEtlPostgresTarget, name="one", key_column="id")
        second = build(ReverseEtlPostgresTarget, name="two", key_column="id")
        assert spec_names(first).isdisjoint(spec_names(second))
