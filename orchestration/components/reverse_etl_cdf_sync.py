import dataclasses

import dagster as dg

from ..assets.reverse_etl_cdf_setup import build_sync_asset
from ..assets.reverse_etl_config import CdfSyncConfig, dagster_names
from ..assets.reverse_etl_topic import build_topic_asset


class ReverseEtlCdfSync(dg.Component, dg.Model, dg.Resolvable):
    """One Databricks Delta table's changes, read and written to Kafka as Debezium-style events.

    The reusable reverse-ETL component (docs/poc/REVERSE_ETL_COMPONENT_DECOUPLING_PLAN.md, section
    3.2): it builds exactly one Dagster asset, the sync. The sync reads an existing Delta table with
    delta-rs (no Spark, no Databricks compute), keeps its watermark in the Postgres table
    reverse_etl_sync_state, and produces Debezium-style events to an existing Kafka topic in Connect
    JSON or Confluent Avro.

    With create_topic (the default) it also builds an asset that creates the topic -- compact
    cleanup policy, a Debezium-style change topic's -- and a bootstrap job running topic then
    sync. The state table itself is still never created: a missing one fails the sync with what
    to create. The sync asset touches only the postgres, databricks, kafka and registry
    resources. Schema changes on the target side — column adds, drop/rename propagation —
    are the demo's hooks (ReverseEtlCdfDemo); this component runs without them.

    `name` is a short label deriving the default asset and group names, as
    reverse_etl_<name>_<...>, and — unless overridden — the watermark key and topic with it.
    The watermark key (`sync_name`) is the one name a replay silently depends on: changing it
    for an existing sync restarts that sync from the beginning, which is why the sync refuses
    to do that silently (the watermark guard) and why an explicit override exists.
    """

    name: str
    catalog: str
    schema_name: str
    # The existing table. The component never creates it.
    source_table: str
    # The column that identifies a row downstream. No default: the demo's surrogate
    # key `rid` means nothing for a real table.
    key_column: str
    # The topic to produce to. Omitted, it derives as reverse_etl_<name>_topic.
    kafka_topic: str | None = None
    # The watermark key and the Debezium envelope prefix. Omitted, it derives as
    # reverse_etl_<name>, so the whole derived name family stays consistent by
    # construction. Given, it is verbatim: pin it to keep the watermark across a
    # label rename -- the sync refuses to restart silently on a rename (step 3's guard).
    sync_name: str | None = None
    # The watermark state (reverse_etl_state.py): "postgres" (`state_table`, the shared
    # table, which must exist -- the sync fails with the DDL otherwise) or "duckdb" (a
    # local file at `state_path`, created on first run, needing no postgres resource).
    # Required, no default: it is the author's decision -- restart semantics, a postgres
    # dependency, or the DuckDB single-writer rule -- not something to forget about.
    state_backend: str
    state_table: str | None = None
    state_path: str | None = None

    # "json" (default: Kafka Connect JSON with the schema embedded in every message) or "avro"
    # (Confluent wire format, schemas in the registry under <topic>-key and <topic>-value).
    encoding: str | None = None
    schema_registry_url: str | None = None

    # Overrides for the derived Dagster names.
    group_name: str | None = None
    sync_asset: str | None = None

    # Upstream asset keys, so a team can order the sync after its own pipeline's assets.
    deps: list[str] | None = None

    # Build the topic asset and the bootstrap job (topic -> sync) for this sync's
    # topic. False: the topic is the team's platform's (or another component's) and
    # must already exist, as before.
    create_topic: bool = True

    def to_config(self) -> CdfSyncConfig:
        derived = dagster_names(self.name)
        # The state fields go into the initial construction so the config validates the
        # real backend+path combination (duckdb without a path fails here, at load).
        state_fields = {field: getattr(self, field) for field in ("state_table", "state_path") if getattr(self, field) is not None}
        config = CdfSyncConfig(
            sync_name=self.sync_name or f"reverse_etl_{self.name}",
            catalog=self.catalog,
            schema=self.schema_name,
            source_table=self.source_table,
            key_column=self.key_column,
            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",
            group_name=derived["group_name"],
            sync_asset=derived["sync_asset"],
            label=self.name,
            sync_name_explicit=self.sync_name is not None,
            state_backend=self.state_backend,
            **state_fields,
        )
        overrides = {
            field: getattr(self, field)
            for field in ("encoding", "schema_registry_url", "group_name", "sync_asset")
            if getattr(self, field) is not None
        }
        return dataclasses.replace(config, **overrides)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        team_deps = tuple(dg.AssetKey(dep) for dep in self.deps or ())
        if not self.create_topic:
            return dg.Definitions(assets=[build_sync_asset(self.to_config(), deps=team_deps)])
        derived = dagster_names(self.name)
        topic_key = dg.AssetKey(derived["topic_asset"])
        cfg = self.to_config()
        return dg.Definitions(
            assets=[
                build_topic_asset(cfg, derived["topic_asset"]),
                # The sync runs after the topic exists: the bootstrap job's topic -> sync
                # edge is this dependency.
                build_sync_asset(cfg, deps=(topic_key, *team_deps)),
            ],
            jobs=[
                dg.define_asset_job(
                    name=derived["bootstrap_job"],
                    selection=dg.AssetSelection.assets(topic_key, dg.AssetKey(cfg.sync_asset)),
                    executor_def=dg.in_process_executor,
                )
            ],
        )
