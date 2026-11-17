{{
  config(
    materialized='table',
    on_table_exists='ignore',
    table_type='PRIMARY',
    keys=['transaction_id'],
    distributed_by=['transaction_id']
  )
}}

-- dbt-owned shell for the Routine-Load-fed Primary Key wallet table
-- (migrated from orchestration/assets/wallet_direct_kafka_setup.py,
-- 2026-11-17). Same pattern as wallet_transactions.sql -- the adapter
-- makes PRIMARY KEY columns NOT NULL in the emitted DDL (confirmed live)
-- -- with one crucial difference: on_table_exists='ignore', because this
-- table's writer is a Routine Load job, not a dbt-time sink creation; a
-- drop-recreate on `dbt build` would break the load. Data arrives
-- exclusively via wallet_direct_kafka_load (and wallet_status_update_load's
-- partial_update).
SELECT
    CAST(NULL AS VARCHAR(64)) AS transaction_id,
    CAST(NULL AS VARCHAR(32)) AS account_id,
    CAST(NULL AS VARCHAR(16)) AS type,
    CAST(NULL AS DECIMAL(38, 9)) AS amount,
    CAST(NULL AS VARCHAR(16)) AS status,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
