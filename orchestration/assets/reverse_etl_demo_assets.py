"""The demo's own assets: the source-table setup and the demo seed. They create
infrastructure -- the source table (when the demo configures source_columns), the
Postgres watermark table, the demo rows -- so they belong to the demo, not to the
reusable sync component (decoupling plan step 4): a team pointing the sync at an
existing table brings their own table and their own state, and gets neither asset.
"""

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from .reverse_etl_cdf_setup import _cost_metadata
from .reverse_etl_state import DuckDBSyncState, PostgresSyncState
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_resources import DatabricksResource, PostgresResource


def build_table_setup_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.table_setup_asset,
        group_name=cfg.group_name,
        description=(
            f"Create the reverse-ETL source table {cfg.source_fqn} (Change Data Feed enabled "
            "at creation) and its watermark bookkeeping table, if absent. "
            "See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
        ),
    )
    def table_setup(context: AssetExecutionContext, postgres: PostgresResource, databricks: DatabricksResource):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()

        columns = ",\n            ".join(
            [f"{cfg.key_column} BIGINT GENERATED ALWAYS AS IDENTITY", *cfg.source_columns]
        )
        if cfg.uniform:
            # UniForm (Iceberg compat) so other engines — StarRocks via UC's Iceberg
            # REST catalog — can read this table. Column mapping is an Iceberg-compat
            # requirement; deletion vectors must stay off for the same reason.
            properties = (
                "'delta.enableChangeDataFeed' = 'true',\n"
                "            'delta.columnMapping.mode' = 'name',\n"
                "            'delta.enableDeletionVectors' = 'false',\n"
                "            'delta.enableIcebergCompatV2' = 'true',\n"
                "            'delta.universalFormat.enabledFormats' = 'iceberg'"
            )
            ready = f"{cfg.source_fqn} ready (CDF + UniForm Iceberg enabled)"
        else:
            # Plain Delta on purpose: this sync shows the pipeline is
            # UniForm-agnostic. Column mapping is on anyway — it is NOT a
            # UniForm feature, and without it Delta refuses DROP COLUMN (and
            # renames) outright, which the drop-propagation demo needs. Note:
            # such a table still will NOT appear in StarRocks' databricks_uc
            # catalog (that needs UniForm's Iceberg metadata). Deletion vectors
            # are OFF on purpose too: with DVs enabled, a 1-row DELETE makes the
            # Change Data Feed emit the whole touched file (verified live: two
            # single-row deletes on 100k-row files produced 200000 delete +
            # 100000 insert CDF rows, and the targets lost 100000 legitimate
            # rows) — the sync would relay that verbatim.
            properties = (
                "'delta.enableChangeDataFeed' = 'true',\n"
                "            'delta.columnMapping.mode' = 'name',\n"
                "            'delta.enableDeletionVectors' = 'false'"
            )
            ready = f"{cfg.source_fqn} ready (plain Delta: CDF + column mapping, no UniForm)"
        if cfg.source_columns:
            databricks.run_sql(
                token,
                f"""
        CREATE TABLE IF NOT EXISTS {cfg.source_fqn} (
            {columns}
        ) USING DELTA
        TBLPROPERTIES (
            {properties}
        )
        """,
            )
            context.log.info(ready)
        else:
            # No source_columns: an existing table. Never create it from the key alone;
            # the UC lookup raises if it is missing (control plane, no warehouse).
            databricks.table_location(token, cfg, cfg.source_fqn)
            context.log.info(f"{cfg.source_fqn} exists; left untouched (no source_columns configured)")

        # The sync's watermark state lives outside the lakehouse (private bookkeeping
        # nothing in Databricks reads), in the backend the config chose: the shared
        # Postgres table (the team creates it in production; the demo setup creates it
        # here) or a local DuckDB file, which appears on first run (D15). Same columns
        # and semantics either way: append-only, freshest-row-wins.
        if cfg.state_backend == "duckdb":
            DuckDBSyncState(cfg.state_path).create()
            state_desc = f"{cfg.state_path} (DuckDB)"
        else:
            PostgresSyncState(postgres, cfg.state_table).create()
            state_desc = f"{cfg.state_table} (Postgres)"
        context.log.info(f"{state_desc} ready (watermark + last_schema per sync)")

        context.add_output_metadata({
            "source_table": MetadataValue.text(cfg.source_fqn),
            "state_table": MetadataValue.text(state_desc),
            **_cost_metadata(),
        })

    return table_setup


def build_seed_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.seed_asset,
        group_name=cfg.group_name,
        # After everything else, so the rows are not picked up by the setup job's
        # first sync and the demo can show them arriving.
        deps=[
            AssetKey(cfg.table_setup_asset),
            AssetKey(cfg.sync_asset),
            AssetKey(cfg.risingwave_asset),
            AssetKey(cfg.sink_asset),
        ],
        description=(
            f"Run the demo seed statements against {cfg.source_fqn} (insert, update, delete). "
            f"Not synced until {cfg.sync_asset} runs."
        ),
    )
    def seed(context: AssetExecutionContext, databricks: DatabricksResource):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()
        for statement in cfg.seed_statements:
            databricks.run_sql(token, statement.replace("{source_table}", cfg.source_fqn))
        context.log.info(f"Ran {len(cfg.seed_statements)} seed statement(s) against {cfg.source_fqn}")
        context.add_output_metadata({
            "statements": MetadataValue.int(len(cfg.seed_statements)),
            **_cost_metadata(),
        })

    return seed
