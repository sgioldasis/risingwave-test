{{ config(materialized='source') }}

CREATE SOURCE {{ this }} (
    user_id int,
    amount DOUBLE,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON