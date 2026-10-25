"""The demo's own assets: the source-table setup and the demo seed. They create
infrastructure -- the source table (when the demo configures source_columns), the
Postgres watermark table, the demo rows -- so they belong to the demo, not to the
reusable sync component (decoupling plan step 4): a team pointing the sync at an
existing table brings their own table and their own state, and gets neither asset.
"""

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from .reverse_etl_cdf_setup import STATE_TABLE, STATE_TABLE_DDL, _cost_metadata
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

        # The sync's watermark state lives in Postgres (reverse_etl_sync_state):
        # private bookkeeping nothing in Databricks reads, so it moved out of the
        # lakehouse entirely. That deletes the sync's LAST warehouse touch (the
        # state read + the watermark INSERT) and, with it, the whole plain-Delta
        # constraint dance the Delta state table forced — no deletion vectors, no
        # column mapping, single-writer v7 nuances — all of it obsolete. Same
        # semantics as the Delta table had: append-only, freshest-row-wins.
        with postgres.connect() as conn:
            with conn.cursor() as cur:
                # The same DDL the sync's precondition error carries, so the demo and a
                # team without the demo create exactly the same table.
                cur.execute(STATE_TABLE_DDL)
            conn.commit()
        context.log.info(f"{STATE_TABLE} ready (Postgres: watermark + last_schema per sync)")

        context.add_output_metadata({
            "source_table": MetadataValue.text(cfg.source_fqn),
            "state_table": MetadataValue.text(f"{STATE_TABLE} (Postgres)"),
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
