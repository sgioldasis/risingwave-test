{{
  config(
    materialized='view',
    enabled=false
  )
}}

-- DEFERRED: This table was intended to read live from RisingWave via JDBC catalog,
-- but StarRocks 4.1.4 JDBC external catalog creation is not working with the expected syntax.
-- For now, the unified MV reads only from Databricks UC (cold path).
-- TODO: Revisit JDBC catalog once a working path is confirmed (StarRocks 5.0+ or configuration fix).

SELECT 1 -- Placeholder; this model is disabled

