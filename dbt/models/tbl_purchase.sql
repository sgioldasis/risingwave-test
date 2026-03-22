{{ config(
    materialized='kafka_table',
    topic='purchases',
    primary_key='user_id',
    bootstrap_servers='redpanda:9092',
    scan_startup_mode='earliest'
) }}

{#
  Kafka Table: Purchases (Modifiable)

  Unlike src_purchase (which is a CREATE SOURCE), this table:
  - Stores data internally in RisingWave
  - Supports UPDATE and DELETE operations
  - Has PRIMARY KEY for upserts on duplicate user_id

  Demo Operations:
    -- Correct a purchase amount
    UPDATE tbl_purchase
    SET amount = 99.99
    WHERE user_id = 123 AND amount = 999.99;

    -- Refund/delete a purchase
    DELETE FROM tbl_purchase WHERE user_id = 456;

    -- Add a manual purchase
    INSERT INTO tbl_purchase (user_id, amount, event_time)
    VALUES (888, 150.00, NOW());
#}

CREATE TABLE IF NOT EXISTS {{ this }} (
    user_id int,
    amount DOUBLE,
    event_time timestamptz,
    PRIMARY KEY (user_id)
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON
