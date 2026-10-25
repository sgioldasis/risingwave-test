"""ReverseEtlCdfDemo's attributes: what it requires, what it defaults, and how each one reaches the sync's config
(orchestration/components/reverse_etl_cdf_demo.py).

The attributes split at the sync/targets seam (plan section 13): the `sync:` block is the sync's
own configuration, the `targets:` block the downstream's, and the top level keeps what spans both
sides (the `name` label, the Dagster-name overrides, the reset). Tests address block fields with
dotted keys (`sync_key=value` builds `{"sync": {"key": value}}`).

No services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

from copy import deepcopy

from pydantic import BaseModel, ValidationError
import pytest

from orchestration.components.reverse_etl_cdf_demo import ReverseEtlCdfDemo

REQUIRED = {
    "name": "orders",
    "sync": {
        "catalog": "de_dev",
        "schema_name": "sr_poc_external",
        "sync_name": "reverse_etl_orders",
        "source_table": "reverse_etl_orders_source",
        "state_table": "reverse_etl_orders_state",
        "kafka_topic": "reverse_etl_orders_topic",
    },
    "targets": {
        "connector_name": "reverse_etl_orders_sink",
        "postgres_table": "reverse_etl_orders_target",
        "risingwave_table": "reverse_etl_orders_target",
    },
}


def component(**attributes) -> ReverseEtlCdfDemo:
    """Built the way Dagster builds it from a defs.yaml: from a dict of attributes, validated.
    A double-underscore keyword (`sync__key=value`) sets a field inside a block."""
    out = deepcopy(REQUIRED)
    for dotted, value in attributes.items():
        block, _, key = dotted.partition("__")
        if key:
            out[block][key] = value
        else:
            out[dotted] = value
    return ReverseEtlCdfDemo.from_attributes_dict(attributes=out)


class TestRequiredAndUnknownAttributes:
    @pytest.mark.parametrize("block, field", sorted((block, field) for block, fields in REQUIRED.items() if isinstance(fields, dict) for field in fields))
    def test_each_required_attribute_must_be_given(self, block, field):
        attributes = deepcopy(REQUIRED)
        del attributes[block][field]
        with pytest.raises(ValidationError, match=field):
            ReverseEtlCdfDemo.from_attributes_dict(attributes=attributes)

    def test_an_unknown_attribute_is_rejected_so_a_typo_in_defs_yaml_cannot_pass_silently(self):
        with pytest.raises(ValidationError, match="propagate_drop"):
            component(sync__propagate_drop=True)  # missing the s

    @pytest.mark.parametrize(
        "attribute, value",
        [("sync__source_columns", "id BIGINT"), ("targets__sink_tasks_max", "many"), ("sync__uniform", [])],
    )
    def test_an_attribute_of_the_wrong_type_is_rejected(self, attribute, value):
        with pytest.raises(ValidationError):
            component(**{attribute: value})


class TestDefaults:
    def test_a_sync_with_only_the_required_attributes_gets_the_documented_defaults(self):
        cfg = component().to_config()
        assert cfg.sync_name == REQUIRED["sync"]["sync_name"]  # explicit, not derived from the label
        assert cfg.key_column == "rid"
        assert cfg.source_columns == () and cfg.seed_statements == ()
        assert cfg.sink_tasks_max == 1
        assert cfg.encoding == "json"
        assert cfg.uniform is True and cfg.propagate_drops is False
        assert cfg.create_topic_asset is True

    def test_the_catalog_and_schema_come_from_the_sync_block(self):
        cfg = component(sync__catalog="other_catalog", sync__schema_name="scratch").to_config()
        assert (cfg.catalog, cfg.schema, cfg.reset_schema) == ("other_catalog", "scratch", "scratch")

    def test_the_dagster_names_derive_from_the_label_and_the_infrastructure_names_do_not(self):
        cfg = component().to_config()
        assert {
            "group_name": cfg.group_name,
            "table_setup_asset": cfg.table_setup_asset,
            "sync_asset": cfg.sync_asset,
            "risingwave_asset": cfg.risingwave_asset,
            "sink_asset": cfg.sink_asset,
            "topic_asset": cfg.topic_asset,
            "setup_job": cfg.setup_job,
            "reset_job": cfg.reset_job,
        } == {
            "group_name": "reverse_etl_orders",
            "table_setup_asset": "reverse_etl_orders_table_setup",
            "sync_asset": "reverse_etl_orders_to_kafka",
            "risingwave_asset": "reverse_etl_orders_risingwave_target",
            "sink_asset": "reverse_etl_orders_jdbc_sink",
            "topic_asset": "reverse_etl_orders_topic_setup",
            "setup_job": "reverse_etl_orders_setup_job",
            "reset_job": "reverse_etl_orders_reset_job",
        }
        # The infrastructure names are the YAML's, whatever the label says -- the sync block's
        # reach the sync side, the targets block's the downstream side.
        assert (cfg.sync_name, cfg.source_table, cfg.state_table, cfg.kafka_topic) == (
            "reverse_etl_orders",
            "reverse_etl_orders_source",
            "reverse_etl_orders_state",
            "reverse_etl_orders_topic",
        )
        assert (cfg.connector_name, cfg.postgres_table, cfg.risingwave_table) == (
            "reverse_etl_orders_sink",
            "reverse_etl_orders_target",
            "reverse_etl_orders_target",
        )
        cfg_other = component(name="other").to_config()
        assert cfg_other.sync_name == cfg.sync_name  # unchanged: explicit
        assert cfg_other.sync_asset == "reverse_etl_other_to_kafka"  # derived: follows the label

    def test_two_labels_never_share_a_dagster_name(self):
        first, second = component(name="a").to_config(), component(name="b").to_config()
        shared = {
            field
            for field in ("group_name", "table_setup_asset", "sync_asset", "risingwave_asset", "sink_asset", "topic_asset", "setup_job", "reset_job")
            if getattr(first, field) == getattr(second, field)
        }
        assert shared == set()


class TestSourceColumnsAndSeed:
    def test_source_columns_are_optional_and_default_to_empty(self):
        assert component().to_config().source_columns == ()

    def test_source_columns_and_seed_statements_become_tuples(self):
        cfg = component(
            sync__source_columns=["id BIGINT NOT NULL", "total DOUBLE"],
            sync__seed_statements=["INSERT INTO {source_table} VALUES (1)"],
        ).to_config()
        assert cfg.source_columns == ("id BIGINT NOT NULL", "total DOUBLE")
        assert cfg.seed_statements == ("INSERT INTO {source_table} VALUES (1)",)

    def test_the_key_column_is_passed_through(self):
        assert component(sync__key_column="surrogate").to_config().key_column == "surrogate"


OVERRIDES = {
    "group_name": "a_group",
    "table_setup_asset": "my_table_setup",
    "sync_asset": "my_sync",
    "risingwave_asset": "my_rw",
    "sink_asset": "my_sink",
    "topic_asset": "shared_topic_setup",
    "create_topic_asset": False,
    "setup_job": "my_setup_job",
    "reset_job": "my_reset_job",
    "reset_schema": "another_scratch_schema",
    "targets__sink_tasks_max": 4,
    "sync__uniform": False,
    "sync__propagate_drops": True,
    "sync__encoding": "avro",
    "sync__schema_registry_url": "http://registry.invalid/apis/ccompat/v7",
    # "duckdb" would need its state_path alongside (one override at a time here), so the row
    # pins the default; the duckdb selection is covered in tests/sync_asset/test_state_backends.py.
    "sync__state_backend": "postgres",
    "sync__state_path": "/tmp/demo/state.duckdb",
    # "duckdb" would need its duckdb_target_path alongside, so the row selects the risingwave
    # target; the duckdb-only shape is covered in tests/components/cdf_demo/test_cdf_demo_defs.py.
    "targets__targets": ("risingwave",),
    "targets__duckdb_target_path": "/tmp/demo/target.duckdb",
}


class TestOverrides:
    @pytest.mark.parametrize("field", sorted(OVERRIDES))
    def test_each_override_replaces_only_its_own_field(self, field):
        derived = component().to_config()
        overridden = component(**{field: OVERRIDES[field]}).to_config()
        name = field.split("__")[-1]
        assert getattr(overridden, name) == OVERRIDES[field]
        others = {other for other in vars(derived) if other != name and getattr(derived, other) != getattr(overridden, other)}
        assert others == set()

    def test_every_override_the_component_offers_is_covered_here(self):
        """A new optional attribute must get a row in OVERRIDES, or this fails and says which."""

        def optional_paths(model, prefix=""):
            paths = set()
            for name, field in model.model_fields.items():
                if field.is_required() and isinstance(field.annotation, type) and issubclass(field.annotation, BaseModel):
                    paths |= optional_paths(field.annotation, prefix=f"{prefix}{name}.")
                elif field.default is None and name not in {"source_columns", "seed_statements", "reset_name_prefixes"}:
                    paths.add(f"{prefix}{name}")
            return paths

        assert optional_paths(ReverseEtlCdfDemo) == {field.replace("__", ".") for field in OVERRIDES}

    def test_reset_name_prefixes_become_a_tuple(self):
        assert component(reset_name_prefixes=["reverse_etl_", "scratch_"]).to_config().reset_name_prefixes == ("reverse_etl_", "scratch_")

    def test_the_default_reset_prefix_covers_every_derived_name(self):
        assert component().to_config().reset_name_prefixes == ("reverse_etl_",)


class TestInvalidCombinations:
    """The component builds fine from these; the config refuses them when to_config() runs."""

    @pytest.mark.parametrize("tasks", [0, -1])
    def test_a_sink_needs_at_least_one_task(self, tasks):
        with pytest.raises(ValueError, match="sink_tasks_max must be at least 1"):
            component(targets__sink_tasks_max=tasks).to_config()

    def test_only_json_and_avro_are_encodings(self):
        with pytest.raises(ValueError, match="encoding must be 'json' or 'avro'"):
            component(sync__encoding="xml").to_config()

    def test_only_risingwave_postgres_and_duckdb_are_targets(self):
        with pytest.raises(ValueError, match="unknown target"):
            component(targets__targets=["elasticsearch"]).to_config()

    def test_the_duckdb_target_needs_a_path(self):
        with pytest.raises(ValueError, match="duckdb_target_path is required"):
            component(targets__targets=["duckdb"]).to_config()
