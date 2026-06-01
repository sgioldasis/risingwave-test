{{ config(
    materialized='sink',
    tags=['casino_uc2']
) }}

CREATE SINK IF NOT EXISTS sink_turnover_percentage
FROM {{ ref('mv_turnover_percentage') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'upsert',
    force_compaction                     = 'true',
    primary_key                          = 'customer_id',
    enable_compaction                    = 'true',
    compaction_interval_sec              = '60',
    enable_snapshot_expiration           = 'true',
    connection                           = lakekeeper_catalog_conn,
    database.name                        = 'public',
    table.name                           = 'rw_managed_turnover_percentage',
    create_table_if_not_exists           = 'true',
    commit_checkpoint_interval           = 20,
    compaction.trigger_snapshot_count      = '5',
    compaction.write_parquet_compression = 'zstd'
)
