{{ config(
    materialized='kafka_table',
    topic='page_views',
    primary_key='user_id',
    bootstrap_servers='redpanda:9092',
    scan_startup_mode='earliest'
) }}

{#
  Kafka Table: Page Views (Modifiable)

  Unlike src_page (which is a CREATE SOURCE), this table:
  - Stores data internally in RisingWave
  - Supports UPDATE and DELETE operations
  - Has PRIMARY KEY for upserts on duplicate user_id

  Demo Operations:
    -- Update a user's page view timestamp
    UPDATE tbl_page
    SET event_time = NOW()
    WHERE user_id = 123;

    -- Delete test user data
    DELETE FROM tbl_page WHERE user_id = 99999;

    -- Insert manual page view
    INSERT INTO tbl_page (user_id, page_id, event_time)
    VALUES (777, 'manual-page', NOW());
#}

CREATE TABLE IF NOT EXISTS {{ this }} (
    user_id int,
    page_id varchar,
    event_time timestamptz,
    PRIMARY KEY (user_id)
) WITH (
    connector = 'kafka',
    topic = 'page_views',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON
