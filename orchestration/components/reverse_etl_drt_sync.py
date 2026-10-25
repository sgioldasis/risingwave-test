import dagster as dg

from ..assets.reverse_etl_drt import build_drt_defs


class ReverseEtlDrtSync(dg.Component, dg.Model, dg.Resolvable):
    """The same Databricks source table as a ReverseEtlCdfSync, synced with drt straight into Postgres.

    `name` is the label of an existing ReverseEtlCdfSync instance (for example "orders"); its
    config says which source table and key to use. Builds the asset
    reverse_etl_<name>_drt_to_postgres, a setup job and a reset job (see
    orchestration/assets/reverse_etl_drt.py). The drt sync file for the label must exist as
    orchestration/drt_demo/syncs/reverse_etl_<name>_drt.yml. For a comparison with the Debezium
    pipeline, see docs/poc/REVERSE_ETL_DRT_COMPARISON.md.
    """

    name: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Imported here: that module imports the other component, not the other way round.
        from ..assets.reverse_etl_notebook_job import _load_sync_config

        return build_drt_defs(_load_sync_config(self.name))
