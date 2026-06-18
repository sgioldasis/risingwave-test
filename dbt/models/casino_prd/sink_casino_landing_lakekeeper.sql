{{ config(
    materialized='sink',
    tags=['lakekeeper', 'local_infra']
) }}

CREATE SINK IF NOT EXISTS sink_casino_landing_lakekeeper
FROM {{ ref('mv_casino_landing') }}
WITH (
    connector                  = 'iceberg',
    type                       = 'append-only',
    force_append_only          = 'true',
    connection                 = lakekeeper_catalog_conn,
    database.name              = 'public',
    table.name                 = 'rw_casino_landing',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 5
)
