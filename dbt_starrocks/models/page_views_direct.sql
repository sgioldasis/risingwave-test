{{
  config(
    materialized='table',
    on_table_exists='ignore',
    table_type='DUPLICATE',
    keys=['user_id', 'page_id'],
    distributed_by=['user_id']
  )
}}

-- dbt-owned shell for the Routine-Load-fed raw funnel table (migrated from
-- orchestration/assets/funnel_direct_kafka_setup.py, 2026-11-17).
-- on_table_exists='ignore' is the load-bearing config: dbt creates the table
-- once on a fresh cluster and never touches it afterwards, so the Routine
-- Load job keeps its target (a drop-recreate would break the load and
-- double-count the replayed topic -- see the recreation caveat in the
-- setup asset). The empty SELECT defines only the schema; data arrives
-- exclusively via the page_views Routine Load.
SELECT
    CAST(NULL AS BIGINT) AS user_id,
    CAST(NULL AS VARCHAR(32)) AS page_id,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
