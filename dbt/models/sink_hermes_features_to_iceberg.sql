{#
  Model: sink_hermes_features_to_iceberg
  Purpose: Creates an upsert sink from hermes_features to Iceberg with multiple intervals

  This demonstrates how UPDATE/DELETE operations on the source tables (tbl_*)
  propagate through to Iceberg via the sink. Key behaviors:

    1. UPDATE on tbl_* -> Updates hermes_features -> Upserts to Iceberg
    2. DELETE on tbl_* -> Updates hermes_features -> Deletes from Iceberg

  The sink now captures funnel metrics at multiple intervals (10 SECONDS, 30 SECONDS, 60 SECONDS)
  with a composite primary key of (window_start, time_interval) for proper upsert semantics.

  Use this to demo: "What happens in Iceberg when I modify source data?"

  Query in Trino to see Iceberg changes:
    SELECT * FROM iceberg.public.iceberg_hermes_features ORDER BY window_start DESC LIMIT 10;
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg', 'funnel', 'tables', 'demo']
) }}

-- Create the Iceberg connection if it doesn't exist
{{ create_iceberg_connection() }}

-- Upsert sink to Iceberg from hermes_features
-- Uses window_start and time_interval as composite primary key for upsert semantics
CREATE SINK IF NOT EXISTS sink_hermes_features_to_iceberg
FROM {{ ref('hermes_features') }}
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start,time_interval',
    database.name = 'public',
    table.name = 'iceberg_hermes_features',
    connection = lakekeeper_catalog_conn,
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 2
)
