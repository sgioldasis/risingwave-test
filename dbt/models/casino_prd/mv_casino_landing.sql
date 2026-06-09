{{ config(
    materialized='materialized_view',
    tags=['casino_prd_setup']
) }}

SELECT
    payload,
    kafka_key,
    kafka_timestamp,
    kafka_partition,
    kafka_offset,
    TO_CHAR(kafka_timestamp AT TIME ZONE 'UTC', 'YYYY-MM') AS year_month
FROM {{ ref('src_casino_prd_landing') }}
