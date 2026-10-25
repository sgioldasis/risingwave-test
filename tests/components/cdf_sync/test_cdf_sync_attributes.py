"""ReverseEtlCdfSync's attributes: what it requires, what it defaults, and how each one reaches the sync's config
(orchestration/components/reverse_etl_cdf_sync.py).

The reusable component's attribute set is deliberately smaller than the demo's: the table, the key and the topic are
given, never derived or created. No services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest
from pydantic import ValidationError

from orchestration.components.reverse_etl_cdf_sync import ReverseEtlCdfSync

REQUIRED = {
    "name": "orders",
    "catalog": "de_dev",
    "schema_name": "sr_poc_external",
    "source_table": "existing_table",
    "key_column": "id",
    "kafka_topic": "orders_cdc",
}


def component(**attributes) -> ReverseEtlCdfSync:
    """Built the way Dagster builds it from a defs.yaml: from a dict of attributes, validated."""
    return ReverseEtlCdfSync.from_attributes_dict(attributes={**REQUIRED, **attributes})


class TestRequiredAndUnknownAttributes:
    @pytest.mark.parametrize("missing", sorted(REQUIRED))
    def test_each_required_attribute_must_be_given(self, missing):
        attributes = {key: value for key, value in REQUIRED.items() if key != missing}
        with pytest.raises(ValidationError, match=missing):
            ReverseEtlCdfSync.from_attributes_dict(attributes=attributes)

    def test_an_unknown_attribute_is_rejected_so_a_typo_in_defs_yaml_cannot_pass_silently(self):
        with pytest.raises(ValidationError, match="key_colum"):
            component(key_colum="id")  # missing the n

    def test_a_demo_attribute_is_unknown_here_too(self):
        """The demo's attributes are not the reusable component's: pointing the core at demo
        infrastructure it must not touch has to fail loudly."""
        with pytest.raises(ValidationError):
            component(seed_statements=["SELECT 1"])
        with pytest.raises(ValidationError):
            component(propagate_drops=True)


class TestDefaults:
    def test_a_sync_with_only_the_required_attributes_gets_the_documented_defaults(self):
        cfg = component().to_config()
        assert cfg.encoding == "json"
        assert cfg.sync_name == "reverse_etl_orders"
        assert cfg.group_name == "reverse_etl_orders"
        assert cfg.sync_asset == "reverse_etl_orders_to_kafka"
        assert (cfg.catalog, cfg.schema, cfg.source_table) == ("de_dev", "sr_poc_external", "existing_table")
        assert (cfg.key_column, cfg.kafka_topic) == ("id", "orders_cdc")

    def test_the_source_table_and_topic_are_taken_as_given_not_derived(self):
        cfg = component(name="company", source_table="dim_company", kafka_topic="company_cdc").to_config()
        assert (cfg.sync_name, cfg.source_table, cfg.kafka_topic) == (
            "reverse_etl_company",
            "dim_company",
            "company_cdc",
        )

    def test_two_labels_never_share_a_name(self):
        first, second = component(name="a").to_config(), component(name="b").to_config()
        assert {first.sync_name, first.sync_asset, first.group_name}.isdisjoint(
            {second.sync_name, second.sync_asset, second.group_name}
        )


class TestOverrides:
    def test_the_group_and_asset_names_can_be_overridden(self):
        cfg = component(group_name="team_group", sync_asset="team_sync").to_config()
        assert (cfg.group_name, cfg.sync_asset) == ("team_group", "team_sync")
        assert cfg.sync_name == "reverse_etl_orders"  # the watermark key stays the label's

    @pytest.mark.parametrize(
        "attribute, value",
        [("encoding", "avro"), ("schema_registry_url", "http://registry.invalid/apis/ccompat/v7")],
    )
    def test_the_optional_encoding_fields_pass_through(self, attribute, value):
        assert getattr(component(**{attribute: value}).to_config(), attribute) == value

    def test_only_json_and_avro_are_encodings(self):
        with pytest.raises(ValueError, match="encoding must be 'json' or 'avro'"):
            component(encoding="xml").to_config()

    def test_deps_stay_a_list_of_asset_key_names(self):
        assert component(deps=["upstream_asset", "other"]).deps == ["upstream_asset", "other"]

    def test_every_optional_attribute_the_component_offers_is_covered_here(self):
        """A new optional attribute must get a test above, or this fails and says which."""
        optional = {name for name, field in ReverseEtlCdfSync.model_fields.items() if field.default is None}
        assert optional == {"encoding", "schema_registry_url", "group_name", "sync_asset", "deps"}
