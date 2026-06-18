{{ config(
    materialized='sink',
    tags=['casino_uc1', 'local_infra']
) }}

CREATE SINK IF NOT EXISTS sink_casino_real_bet
FROM {{ ref('mv_casino_real_bet') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'upsert',
    force_compaction                     = 'true',
    primary_key                          = 'customer_id,currency_id,event_ts',
    enable_compaction                    = 'true',
    compaction_interval_sec              = '60',
    enable_snapshot_expiration           = 'true',
    connection                           = lakekeeper_catalog_conn,
    database.name                        = 'public',
    table.name                           = 'rw_managed_casino_real_bet',
    create_table_if_not_exists           = 'true',
    commit_checkpoint_interval           = 20,
    compaction.trigger_snapshot_count      = '5',
    compaction.write_parquet_compression = 'zstd'
)
