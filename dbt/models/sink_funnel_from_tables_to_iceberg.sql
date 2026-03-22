{#
  Model: sink_funnel_from_tables_to_iceberg
  Purpose: Creates an upsert sink from funnel_from_tables to Iceberg

  This demonstrates how UPDATE/DELETE operations on the source tables (tbl_*)
  propagate through to Iceberg via the sink. Key behaviors:

  1. UPDATE on tbl_* -> Updates funnel_from_tables -> Upserts to Iceberg
  2. DELETE on tbl_* -> Updates funnel_from_tables -> Deletes from Iceberg

  Use this to demo: "What happens in Iceberg when I modify source data?"

  Query in Trino to see Iceberg changes:
    SELECT * FROM iceberg.public.funnel_from_tables ORDER BY window_start DESC LIMIT 10;
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg', 'funnel', 'tables', 'demo']
) }}

-- Create the Iceberg connection if it doesn't exist
{{ create_iceberg_connection() }}

-- Upsert sink to Iceberg from funnel_from_tables
-- Uses window_start as primary key for upsert semantics
CREATE SINK IF NOT EXISTS iceberg_funnel_from_tables_sink
FROM {{ ref('funnel_from_tables') }}
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start',
    database.name = 'public',
    table.name = 'funnel_from_tables',
    connection = lakekeeper_catalog_conn,
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 1
)
