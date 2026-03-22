{{ config(
    materialized='kafka_table',
    topic='cart_events',
    primary_key='user_id',
    bootstrap_servers='redpanda:9092',
    scan_startup_mode='earliest'
) }}

{#
  Kafka Table: Cart Events (Modifiable)

  Unlike src_cart (which is a CREATE SOURCE), this table:
  - Stores data internally in RisingWave
  - Supports UPDATE and DELETE operations
  - Has PRIMARY KEY for upserts on duplicate user_id

  Demo Operations:
    -- Update a user's cart item
    UPDATE tbl_cart
    SET item_id = 'premium-widget', event_time = NOW()
    WHERE user_id = 123;

    -- Delete a user's cart
    DELETE FROM tbl_cart WHERE user_id = 456;

    -- Insert a new cart event
    INSERT INTO tbl_cart (user_id, item_id, event_time)
    VALUES (999, 'demo-item', NOW());
#}

CREATE TABLE IF NOT EXISTS {{ this }} (
    user_id int,
    item_id varchar,
    event_time timestamptz,
    PRIMARY KEY (user_id)
) WITH (
    connector = 'kafka',
    topic = 'cart_events',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON
