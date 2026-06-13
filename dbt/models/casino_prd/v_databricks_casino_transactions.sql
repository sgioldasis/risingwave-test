{{ config(
    materialized='view',
    tags=['databricks'],
    schema='public',
    pre_hook="SET enable_datafusion_engine = false"
) }}

SELECT * FROM {{ ref('src_databricks_casino_transactions') }}
