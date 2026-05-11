{#
  Model: sink_funnel_to_rw_iceberg
  Purpose: Creates an upsert sink from funnel_for_iceberg to an Iceberg table
  registered in Lakekeeper. This table is auto-compacted by dedicated compactor-1
  service running in --compactor-mode dedicated-iceberg, making it RW-managed
  in terms of file maintenance.
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg', 'funnel', 'rw-managed']
) }}

CREATE SINK IF NOT EXISTS rw_managed_funnel_sink
FROM {{ ref('funnel_for_iceberg') }}
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    connection = lakekeeper_catalog_conn,
    database.name = 'public',
    table.name = 'rw_managed_funnel',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 30,
    -- Parquet compression for sink writes AND RW-managed compaction output.
    -- zstd: ~2-3x smaller files than snappy for low-cardinality funnel rows.
    -- Requires RisingWave >= 2.9.0; supports dynamic updates via ALTER SINK.
    compaction.write_parquet_compression = 'zstd'
)
