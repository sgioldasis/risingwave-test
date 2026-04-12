{#
  Model: sink_funnel_to_postgres
  Purpose: Creates a sink from funnel_summary_with_country view to local PostgreSQL
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

-- This sink exports funnel_summary_with_country data to local PostgreSQL
-- Requires the target table to exist in PostgreSQL before running dbt
CREATE SINK IF NOT EXISTS funnel_postgres_sink
FROM {{ ref('funnel_summary_with_country') }}
WITH (
    connector = 'jdbc',
    jdbc.url = 'jdbc:postgresql://host.docker.internal:5432/postgres',
    user = '{{ env_var("USER", "postgres") }}',
    password = '',
    table.name = 'funnel_summary_with_country',
    type = 'upsert',
    primary_key = 'window_start,country'
)
