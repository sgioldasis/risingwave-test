{{ config(
    materialized='iceberg_table',
    tags=['casino_uc1']
) }}

CREATE TABLE IF NOT EXISTS rw_managed_casino_real_bet (
    customer_id                  INT,
    currency_id                  INT,
    event_ts                     TIMESTAMPTZ,
    rolling_14d_real_bet_amount  NUMERIC,
    PRIMARY KEY (customer_id, currency_id, event_ts)
) ENGINE = iceberg
