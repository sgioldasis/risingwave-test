{{ config(materialized='source') }}

CREATE SOURCE {{ this }} (
    user_id int,
    page_id varchar,
    event_time timestamptz,
    produced_at timestamptz
) WITH (
    connector = 'kafka',
    topic = 'page_views',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON