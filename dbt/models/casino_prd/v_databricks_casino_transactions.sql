{{ config(
    materialized='view',
    tags=['databricks'],
    schema='public'
) }}

SELECT * FROM {{ ref('src_databricks_casino_transactions') }}
