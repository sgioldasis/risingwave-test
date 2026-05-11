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
    -- RW-managed file maintenance via dedicated compactor-1 service.
    -- The external Spark `iceberg_compaction_job` is retained for DEMO only;
    -- in steady state RW handles rewrite + snapshot expiration on this table.
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    -- Raised from 2s to 30s to reduce small-file pressure and commit churn
    -- now that compaction is RW-managed (matches rw_managed_funnel cadence).
    commit_checkpoint_interval = 30,
    -- zstd: ~2-3x smaller files than snappy; supports dynamic updates via ALTER SINK.
    compaction.write_parquet_compression = 'zstd'
)
