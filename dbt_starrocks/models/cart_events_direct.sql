{{
  config(
    materialized='table',
    on_table_exists='ignore',
    table_type='DUPLICATE',
    keys=['user_id', 'item_id'],
    distributed_by=['user_id']
  )
}}

-- dbt-owned shell for the Routine-Load-fed raw funnel table (migrated from
-- orchestration/assets/funnel_direct_kafka_setup.py, 2026-11-17). Same
-- pattern as page_views_direct: on_table_exists='ignore' keeps dbt from
-- ever dropping a table a Routine Load is writing into; the empty SELECT
-- defines only the schema.
SELECT
    CAST(NULL AS BIGINT) AS user_id,
    CAST(NULL AS VARCHAR(32)) AS item_id,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
