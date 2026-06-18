{#
  Model: sink_funnel_to_postgres
  Purpose: Creates a sink from funnel_summary MV to local PostgreSQL
  This allows querying funnel data from DBeaver or other PostgreSQL clients

  IMPORTANT: Requires postgres_funnel_table Dagster asset to run first
  to create the target table in PostgreSQL.
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['postgres', 'funnel', 'jdbc'],
    meta={
        'dagster': {
            'auto_materialize_policy': {'type': 'eager'},
            'deps': ['postgres_funnel_table']
        }
    }
) }}

-- Exports funnel_summary data to local PostgreSQL.
-- Note: Default to host.docker.internal, mapped via extra_hosts in docker-compose.
-- Override with HOST_POSTGRES_URL when running in environments with a different host mapping.
CREATE SINK IF NOT EXISTS funnel_postgres_sink
FROM {{ ref('funnel_summary') }}
WITH (
    connector = 'jdbc',
    jdbc.url = '{{ env_var("HOST_POSTGRES_URL", "jdbc:postgresql://host.docker.internal:5432/postgres") }}',
    user = '{{ env_var("USER", "postgres") }}',
    password = '',
    table.name = 'funnel_summary',
    type = 'upsert',
    primary_key = 'window_start,country'
)
