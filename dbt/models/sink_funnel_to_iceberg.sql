{#
  Model: sink_funnel_to_iceberg
  Purpose: Creates an upsert sink from funnel to Iceberg using the Iceberg connector
  This writes funnel analytics data to Iceberg with upsert semantics
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg', 'funnel']
) }}

-- Upsert sink to Iceberg using the Iceberg connector
-- Requires primary_key for upsert mode
CREATE SINK IF NOT EXISTS iceberg_funnel_sink
FROM {{ ref('funnel_for_iceberg') }}
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start',
    database.name = 'public',
    table.name = 'iceberg_funnel',
    connection = lakekeeper_catalog_conn,
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 1
)
