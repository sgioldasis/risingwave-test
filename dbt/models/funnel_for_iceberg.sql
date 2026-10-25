{#
  Model: funnel_for_iceberg
  Purpose: Casts funnel_summary data to types compatible with Iceberg sink
  Converts NUMERIC rates to DOUBLE for Iceberg compatibility

  window_date added 2026-09-07: funnel_summary_historical was migrated to a
  day-partitioned Iceberg table (see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md).
  Databricks' managed-Iceberg CREATE TABLE only accepts PARTITIONED BY on a
  plain column, not an expression transform like day(window_start) --
  confirmed via a rejected CTAS ("Partitioning by expressions is not
  supported for Delta tables") -- so the partition key has to be a real,
  visible column computed here rather than a hidden Iceberg transform.
#}

{{ config(
    materialized='materialized_view',
    schema='public',
    tags=['iceberg', 'funnel']
) }}

SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate::DOUBLE as view_to_cart_rate,
    cart_to_buy_rate::DOUBLE as cart_to_buy_rate,
    window_start::DATE as window_date
FROM {{ ref('funnel_summary') }}
