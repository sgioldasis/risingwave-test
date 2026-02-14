{#
  Model: iceberg_users
  Purpose: Creates the Iceberg table for users
  This stores user data from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_users (
    user_id BIGINT,
    full_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    signup_time VARCHAR,
    marketing_opt_in BOOLEAN,
    ingested_at VARCHAR
) ENGINE = iceberg
