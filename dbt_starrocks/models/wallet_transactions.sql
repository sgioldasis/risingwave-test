{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['transaction_id'],
    distributed_by=['transaction_id']
  )
}}

-- Native StarRocks Primary Key table for the upsert/point-lookup demo
-- (see docs/SR_POC_WALLET_UPSERT_DEMO.md). This is a genuine base table,
-- not a view/MV over a federated source -- it's what RisingWave's
-- sink_wallet_transactions_to_starrocks sink (dbt/models/) writes into
-- directly via connector='starrocks', type='upsert'. Confirmed via the
-- dbt-starrocks adapter source (materializations/adapters/relation_helpers.sql)
-- that table_type='PRIMARY' + keys produces a real `PRIMARY KEY (...)`
-- DDL clause -- this is a normal dbt-managed table, unlike the external
-- catalogs (risingwave/databricks_uc/lakekeeper_local) which live in
-- starrocks/init_catalog.sh because dbt can't create those.
--
-- This table must exist before the RisingWave sink can be created against
-- it (RisingWave's starrocks connector targets an existing table by name),
-- so the sink model's dagster deps reference this asset explicitly.
-- NOTE: both `CAST(NULL AS DOUBLE)` and `CAST(0.0 AS DOUBLE)` get widened to
-- `decimal(38,9)` by StarRocks's CTAS type inference regardless of the CAST
-- (confirmed live 2026-09-12: caused a genuine sink failure, "starrocks type
-- is decimal(38, 9) risingwave type is Float64") -- StarRocks appears to
-- always type plain numeric literals as DECIMAL in a CTAS context. A
-- scientific-notation literal (`1e0`) is conventionally typed as DOUBLE in
-- MySQL-compatible engines and avoids this.
SELECT
    CAST(NULL AS VARCHAR(64)) AS transaction_id,
    CAST(NULL AS VARCHAR(64)) AS account_id,
    CAST(NULL AS VARCHAR(16)) AS type,
    CAST(1e0 AS DOUBLE) AS amount,
    CAST(NULL AS VARCHAR(16)) AS status,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
