"""The targets side of the sync/targets seam (decoupling plan section 13): build_target_defs takes
only a ReverseEtlTargetsConfig — no field of it names the source table — and builds exactly the
selected target assets plus the sink health check's check, job and schedule (D19). The demo's
builder composes it with the targets half of its config (cfg.targets_config()).

Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets.reverse_etl_config import ReverseEtlTargetsConfig
from orchestration.assets.reverse_etl_defs import build_target_defs
from orchestration.assets.reverse_etl_risingwave_setup import build_risingwave_table_asset
from tests.fakes import fake_resources, make_config

LOCATION = "abfss://x"  # the RisingWave CREATE's one composition input: where the source lives


def targets_cfg(**overrides) -> ReverseEtlTargetsConfig:
    """A targets config with every name explicit, as a YAML would state them (section 11)."""
    return ReverseEtlTargetsConfig(
        sync_name="reverse_etl_unittest",
        kafka_topic="reverse_etl_unittest_topic",
        key_column="id",
        envelope_schema_name="reverse_etl_unittest.sr_poc.reverse_etl_unittest_source",
        schema_registry_url="http://registry.invalid",
        group_name="reverse_etl_unittest",
        sync_asset="reverse_etl_unittest_to_kafka",
        topic_asset="reverse_etl_unittest_topic_setup",
        risingwave_asset="reverse_etl_unittest_risingwave_target",
        sink_asset="reverse_etl_unittest_jdbc_sink",
        connector_name="reverse_etl_unittest_sink",
        postgres_table="reverse_etl_unittest_target",
        risingwave_table="reverse_etl_unittest_target",
        **overrides,
    )


def upstream_assets():
    """The target assets are consumers: they declare deps on the sync's (and the topic's) assets,
    which live on the other side of the seam — stubs so the definitions alone are loadable."""

    @dg.asset(name="reverse_etl_unittest_to_kafka")
    def sync_asset():
        return None

    @dg.asset(name="reverse_etl_unittest_topic_setup")
    def topic_asset():
        return None

    return [sync_asset, topic_asset]


def load(defs, calls) -> dg.Definitions:
    merged = dg.Definitions.merge(
        defs,
        dg.Definitions(assets=upstream_assets(), resources=fake_resources(calls)),
    )
    dg.Definitions.validate_loadable(merged)
    return merged


def asset_names(defs) -> set:
    return {key.to_user_string() for asset in defs.assets for key in asset.keys}


UPSTREAM = {"reverse_etl_unittest_to_kafka", "reverse_etl_unittest_topic_setup"}

ALL_TARGETS = {
    "reverse_etl_unittest_risingwave_target",
    "reverse_etl_unittest_jdbc_sink",
    "reverse_etl_unittest_duckdb_target",
}


class TestTheSeam:
    def test_the_targets_config_names_nothing_of_the_source(self):
        """The seam (plan section 13.2): a target cannot know where the events came from. The
        field set is golden — a new field must be seam-relevant or this fails and says so."""
        assert {field.name for field in ReverseEtlTargetsConfig.__dataclass_fields__.values()} == {
            # the seam: what the sync puts on the topic
            "sync_name", "kafka_topic", "key_column", "envelope_schema_name", "encoding", "schema_registry_url",
            # the Dagster names and the target names
            "group_name", "sync_asset", "topic_asset", "risingwave_asset", "sink_asset",
            "connector_name", "postgres_table", "risingwave_table",
            # the selection (section 12)
            "sink_tasks_max", "targets", "duckdb_target_path",
        }
        assert not any("source" in name or name in ("catalog", "schema") for name in map(str, dir(ReverseEtlTargetsConfig)))

    def test_the_demo_config_splits_at_the_seam(self):
        cfg = make_config(source_columns=(), key_column="id", targets=("postgres", "duckdb"), duckdb_target_path="/tmp/t.duckdb")
        targets = cfg.targets_config()
        assert (targets.sync_name, targets.kafka_topic, targets.key_column) == (cfg.sync_name, cfg.kafka_topic, cfg.key_column)
        assert targets.envelope_schema_name == cfg.envelope_schema_name
        assert (targets.key_subject, targets.value_subject) == (cfg.key_subject, cfg.value_subject) == (
            f"{cfg.kafka_topic}-key", f"{cfg.kafka_topic}-value",
        )
        assert (targets.encoding, targets.targets, targets.duckdb_target_path) == (cfg.encoding, ("postgres", "duckdb"), "/tmp/t.duckdb")
        assert (targets.connector_name, targets.risingwave_table, targets.sink_tasks_max) == (
            cfg.connector_name, cfg.risingwave_table, cfg.sink_tasks_max,
        )
        for absent in ("source_table", "catalog", "schema", "state_table", "reset_name_prefixes"):
            assert not hasattr(targets, absent)

    def test_the_risingwave_builder_refuses_a_missing_source_location(self):
        with pytest.raises(ValueError, match="source_fqn is required"):
            build_risingwave_table_asset(targets_cfg(), "")


class TestBuildTargetDefs:
    def test_all_three_targets_build_three_assets_and_the_health_side(self, calls):
        defs = load(build_target_defs(targets_cfg(targets=("risingwave", "postgres", "duckdb"), duckdb_target_path="/tmp/t.duckdb"), LOCATION), calls)
        assert asset_names(defs) - UPSTREAM == ALL_TARGETS
        assert [(spec.name, spec.asset_key.to_user_string()) for check in defs.asset_checks for spec in check.check_specs] == [
            ("sink_healthy", "reverse_etl_unittest_jdbc_sink")
        ]
        assert {job.name for job in defs.jobs} == {"reverse_etl_unittest_sink_health_job"}
        assert [schedule.name for schedule in defs.schedules] == ["reverse_etl_unittest_sink_health_schedule"]

    def test_a_selection_builds_only_its_own_targets(self, calls):
        for selection, expected in (
            (("risingwave",), {"reverse_etl_unittest_risingwave_target"}),
            (("postgres",), {"reverse_etl_unittest_jdbc_sink"}),
            (("duckdb",), {"reverse_etl_unittest_duckdb_target"}),
        ):
            defs = load(build_target_defs(targets_cfg(targets=selection, duckdb_target_path="/tmp/t.duckdb"), LOCATION), calls)
            assert asset_names(defs) - UPSTREAM == expected, selection
        # The health side follows the postgres target only (D19, plan section 12).
        with_health = build_target_defs(targets_cfg(targets=("duckdb",), duckdb_target_path="/tmp/t.duckdb"), LOCATION)
        assert with_health.asset_checks == [] and with_health.jobs == [] and with_health.schedules == []

    def test_the_health_check_watches_the_sink_and_nothing_else_is_selected(self, calls):
        """The health side of a targets-only build: one check on the sink asset, its job and its
        schedule -- no other job exists to run anything downstream (D19)."""
        defs = load(build_target_defs(targets_cfg(), LOCATION), calls)
        assert {job.name for job in defs.jobs} == {"reverse_etl_unittest_sink_health_job"}

    def test_the_targets_validate_like_the_syncs(self):
        with pytest.raises(ValueError, match="unknown target"):
            targets_cfg(targets=["elasticsearch"])
        with pytest.raises(ValueError, match="duckdb_target_path is required"):
            targets_cfg(targets=["duckdb"])
        with pytest.raises(ValueError, match="encoding must be"):
            targets_cfg(encoding="xml")
        with pytest.raises(ValueError, match="sink_tasks_max must be at least 1"):
            targets_cfg(sink_tasks_max=0)
