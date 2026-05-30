{{ config(
    materialized='iceberg_table',
    tags=['casino_uc2']
) }}

CREATE TABLE IF NOT EXISTS rw_managed_turnover_percentage (
    customer_id          INT,
    casino_turnover      NUMERIC,
    sportsbook_turnover  NUMERIC,
    total_turnover       NUMERIC,
    casino_ratio         NUMERIC,
    sportsbook_ratio     NUMERIC,
    PRIMARY KEY (customer_id)
) ENGINE = iceberg
