{{
  config(
    materialized='table',
    on_table_exists='ignore',
    table_type='DUPLICATE',
    keys=['transaction_id'],
    distributed_by=['transaction_id']
  )
}}

-- dbt-owned shell for the Duplicate Key wallet log table (keeps every
-- event, no upsert/collapsing; migrated from
-- orchestration/assets/wallet_sync_mv_setup.py, 2026-11-17).
-- on_table_exists='ignore' keeps dbt from ever dropping a table a Routine
-- Load is writing into; the empty SELECT defines only the schema. The
-- synchronous rollup MV over this table (mv_wallet_type_rollup) is still
-- created by the setup asset -- sync MVs have no dbt materialization.
SELECT
    CAST(NULL AS VARCHAR(64)) AS transaction_id,
    CAST(NULL AS VARCHAR(32)) AS account_id,
    CAST(NULL AS VARCHAR(16)) AS type,
    CAST(NULL AS DECIMAL(38, 9)) AS amount,
    CAST(NULL AS VARCHAR(16)) AS status,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
