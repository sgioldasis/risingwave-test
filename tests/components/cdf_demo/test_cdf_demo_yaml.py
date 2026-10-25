"""The defs.yaml files that use ReverseEtlCdfDemo, the real inputs the component is deployed with.

Each one is read the way Dagster reads it (the `attributes` block goes to the component), so a typo in a field name, a
wrong type or an invalid value fails here instead of when the code location loads. Together they must not collide on
any name, and each must pass the reset job's allowlist. No services and no dbt manifests needed. Run from the repo
root:  .venv/bin/python -m pytest
"""

from collections import Counter
from pathlib import Path

import dagster as dg
import pytest
import yaml

from orchestration.assets import reverse_etl_reset
from orchestration.components.reverse_etl_cdf_demo import ReverseEtlCdfDemo

DEFS = Path(__file__).resolve().parents[3] / "orchestration" / "defs"
TYPE = "orchestration.components.reverse_etl_cdf_demo.ReverseEtlCdfDemo"


def cdf_demo_files() -> list[Path]:
    return sorted(path for path in DEFS.glob("*/defs.yaml") if yaml.safe_load(path.read_text()).get("type") == TYPE)


FILES = cdf_demo_files()
IDS = [path.parent.name for path in FILES]


# The infrastructure names each file must state, pinned so a rename or a typo fails here (plan
# section 11): these are the objects that exist in Databricks, Kafka, Kafka Connect, Postgres and
# RisingWave -- none of them may move silently.
GOLDEN_INFRA_NAMES = {
    folder: {
        "sync_name": f"reverse_etl_{label}",
        "source_table": f"reverse_etl_{label}_source",
        "state_table": f"reverse_etl_{label}_state",
        "kafka_topic": f"reverse_etl_{label}_topic",
        "connector_name": f"reverse_etl_{label}_sink",
        "postgres_table": f"reverse_etl_{label}_target",
        "risingwave_table": f"reverse_etl_{label}_target",
    }
    for folder, label in (
        ("reverse_etl_avro", "avro"),
        ("reverse_etl_cdf", "cdf"),
        ("reverse_etl_company", "company"),
        ("reverse_etl_delta", "delta"),
        ("reverse_etl_orders", "orders"),
    )
}


def load(path: Path) -> ReverseEtlCdfDemo:
    return ReverseEtlCdfDemo.from_attributes_dict(attributes=yaml.safe_load(path.read_text())["attributes"])


def build(path: Path) -> dg.Definitions:
    return load(path).build_defs(dg.ComponentTree.for_test().load_context)


def test_the_cdf_demo_files_are_found():
    """If the glob or the type string stops matching, the tests below would silently check nothing."""
    assert len(FILES) >= 5, f"found only {IDS}"


@pytest.mark.parametrize("path", FILES, ids=IDS)
class TestEachFile:
    def test_the_attributes_are_valid_for_the_component(self, path):
        assert load(path).to_config().sync_name.startswith("reverse_etl_")

    def test_the_file_has_nothing_besides_type_and_attributes(self, path):
        assert set(yaml.safe_load(path.read_text())) <= {"type", "attributes"}

    def test_every_name_the_reset_job_drops_is_inside_its_allowlist(self, path):
        reverse_etl_reset._assert_reset_allowed(load(path).to_config())

    def test_the_definitions_validate(self, path, resources):
        dg.Definitions.validate_loadable(dg.Definitions.merge(build(path), dg.Definitions(resources=resources())))


class TestNoCollisions:
    FIELDS = ("kafka_topic", "connector_name", "postgres_table", "risingwave_table", "source_table", "setup_job", "reset_job", "sync_name")

    def configs(self):
        return [load(path).to_config() for path in FILES]

    @pytest.mark.parametrize("field", FIELDS)
    def test_no_two_syncs_share_the_same_value(self, field):
        counts = Counter(getattr(cfg, field) for cfg in self.configs())
        assert {value: n for value, n in counts.items() if n > 1} == {}

    def test_no_two_syncs_define_the_same_asset_or_job(self):
        defs = [build(path) for path in FILES]
        assets = Counter(key.to_user_string() for d in defs for asset in d.assets for key in asset.keys)
        jobs = Counter(job.name for d in defs for job in d.jobs)
        assert {name: n for name, n in assets.items() if n > 1} == {}
        assert {name: n for name, n in jobs.items() if n > 1} == {}

    def test_all_the_syncs_merge_into_one_loadable_set_of_definitions(self, resources):
        merged = dg.Definitions.merge(*[build(path) for path in FILES], dg.Definitions(resources=resources()))
        dg.Definitions.validate_loadable(merged)


class TestTheInfrastructureNamesAreExplicit:
    @pytest.mark.parametrize("path", FILES, ids=IDS)
    def test_each_file_states_its_golden_infrastructure_names(self, path):
        attributes = yaml.safe_load(path.read_text())["attributes"]
        expected = GOLDEN_INFRA_NAMES[path.parent.name]
        assert {field: attributes[field] for field in expected} == expected
