{{ config(
    materialized='sink',
    tags=['lakekeeper']
) }}

CREATE SINK IF NOT EXISTS sink_sportsbook_landing_lakekeeper
FROM {{ ref('mv_sportsbook_landing') }}
WITH (
    connector                  = 'iceberg',
    type                       = 'append-only',
    force_append_only          = 'true',
    connection                 = lakekeeper_catalog_conn,
    database.name              = 'public',
    table.name                 = 'rw_sportsbook_landing',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 5
)
