-- Databricks Unity Catalog — one-time table setup for the RisingWave PoC.
--
-- Run this in Databricks SQL before the first dbt build. RisingWave's Iceberg
-- sink does NOT create UC tables automatically (no create_table_if_not_exists);
-- the sink will error with "Table ... not found [ErrorCode: 3000]" if the table
-- is missing when the dbt build runs.
--
-- Catalog / schema context:
--   catalog : de_dev
--   schema  : rw_poc  (created with MANAGED LOCATION pointing to PoC ADLS account
--                       stkznneurwpoccdddevstd — see DATABRICKS_ICEBERG_SINK.md §15)
--
-- TBLPROPERTIES:
--   history.expire.min-snapshots-to-keep = 50
--     UC sets this to 100 by default. Lowered to 50 so that manual or scheduled
--     VACUUM / expire_snapshots calls prune more aggressively. Note: gc.enabled
--     is forced to false by UC, so expiration never runs automatically — this
--     property only takes effect when expire_snapshots is called explicitly.

CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_transactions (
    customer_id              INT           NOT NULL,
    message_type_id          INT           NOT NULL,
    account_id               INT           NOT NULL,
    currency_id              INT           NOT NULL,
    transaction_created_at   TIMESTAMP     NOT NULL,
    amount_abs               DECIMAL(20,8),
    properties               STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_bets (
    bet_id               BIGINT        NOT NULL,
    customer_id          INT           NOT NULL,
    customer_segment_id  INT,
    bet_type_id          INT,
    bet_status_id        INT,
    channel_id           INT,
    currency_id          INT,
    placed_at            TIMESTAMP     NOT NULL,
    stake_euro           DECIMAL(20,8),
    stake_local          DECIMAL(20,8),
    properties           STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

-- Per-event rolling-turnover snapshots (sink_casino_turnover_90d_databricks).
-- Append-only; "latest per customer" collapse happens in the read-side view
-- v_casino_turnover_latest (QUALIFY ROW_NUMBER) — see DATABRICKS_ICEBERG_READ.md §6.
-- rolling_7d_turnover DECIMAL(38,8): scale 8 matches amount_abs, precision 38
-- prevents overflow when summing across a 90-day window.
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_turnover_90d (
    customer_id          INT           NOT NULL,
    event_ts             TIMESTAMP     NOT NULL,
    rolling_7d_turnover  DECIMAL(38,8)
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

-- Raw Protobuf landing tables (sink_casino_landing_databricks, sink_sportsbook_landing_databricks).
-- Payload is binary (undecoded Protobuf bytes); decoding to typed bronze happens via the
-- casino_landing_to_bronze Databricks notebook (casino_landing_to_bronze_job).
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');
