-- =============================================================================
-- Prod casino transactions pipeline (slim demo build)
--
-- Source table : src_casino_prd  (TABLE, see sql/casino_prd_source.sql)
--
-- Materialized views built here:
--   1. mv_casino_transactions               -- one row per transaction
--   2. mv_casino_real_bet_events            -- bet filter (Spark PoC parity)
--   3. mv_casino_real_bet_hourly_per_customer
--                                           -- TUMBLE 1h totals per customer
--
-- NOTE: This file used to also build mv_casino_prd_rounds_flat,
-- mv_casino_prd_volume_by_company_game, mv_casino_prd_funnel and four
-- managed Iceberg sinks. They were dropped from the demo. The DROPs below
-- are kept so re-runs on an old environment clean those objects up first.
-- =============================================================================

-- --- Compatibility drops (old demo objects) ---------------------------------
-- Old "_prd_" managed iceberg objects from the previous demo build, plus any
-- legacy MVs whose names changed.
\set ON_ERROR_STOP off
DROP SINK              IF EXISTS rw_managed_casino_prd_rounds_sink;
DROP SINK              IF EXISTS rw_managed_casino_prd_volume_sink;
DROP SINK              IF EXISTS rw_managed_casino_prd_funnel_sink;
DROP SINK              IF EXISTS rw_managed_casino_prd_transactions_sink;
DROP TABLE             IF EXISTS rw_managed_casino_prd_rounds;
DROP TABLE             IF EXISTS rw_managed_casino_prd_volume;
DROP TABLE             IF EXISTS rw_managed_casino_prd_funnel;
DROP TABLE             IF EXISTS rw_managed_casino_prd_transactions;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_rounds_flat            CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_volume_by_company_game CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_funnel                 CASCADE;
\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1. Transaction-level flat view (one row per transaction inside each message)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_transactions CASCADE;

CREATE MATERIALIZED VIEW mv_casino_transactions AS
WITH msgs AS (
    SELECT
        s."CustomerId"          AS customer_id,
        "MessageTypeId"         AS message_type_id,
        "Created"               AS message_created_struct,
        "Transactions"          AS txs
    FROM src_casino_prd AS s,
         UNNEST((s."RoundInfo")."Messages")
)
SELECT
    customer_id,
    message_type_id,
    to_timestamp(
        (message_created_struct)."seconds"
      + (message_created_struct)."nanos" / 1e9
    )                                                     AS message_created_at,
    "TransactionId"                                       AS transaction_id,
    "AccountId"                                           AS account_id,
    "CurrencyId"                                          AS currency_id,
    to_timestamp(
        (("Created"))."seconds"
      + (("Created"))."nanos" / 1e9
    )                                                     AS transaction_created_at,
    ABS(NULLIF("Amount", '')::numeric)                    AS amount_abs,
    "Amount"                                              AS amount_raw
FROM msgs, UNNEST(txs);

-- ---------------------------------------------------------------------------
-- 2. Real Bet Amount events — business-logic filter on transactions.
--    Mirrors getCasinoRealBetAmountEvents() from the Spark PoC:
--      MessageTypeId = 1   (bet)
--      AccountId     = 1   (real account, not bonus)
--      Amount        IS NOT NULL  (proto string, empty-string treated as null)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_events CASCADE;

CREATE MATERIALIZED VIEW mv_casino_real_bet_events AS
SELECT
    customer_id,
    transaction_id,
    currency_id,
    amount_abs                                  AS real_bet_amount,
    transaction_created_at                      AS event_ts,
    DATE_TRUNC('hour', transaction_created_at)  AS event_hour
FROM mv_casino_transactions
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- ---------------------------------------------------------------------------
-- 3. Hourly bucketed real-bet totals per customer.
--    Mirrors the mapGroupsWithState bucketed accumulator from the Spark PoC:
--    one row per (customer, 1-hour tumbling window) with running totals.
--
--    A strict rolling 14-day total is then a cheap aggregate on top of these
--    buckets (e.g. SUM ... WHERE window_start > now() - INTERVAL '14 days').
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_14d_per_customer    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_hourly_per_customer CASCADE;

CREATE MATERIALIZED VIEW mv_casino_real_bet_hourly_per_customer AS
SELECT
    customer_id,
    window_start,
    window_end,
    SUM(real_bet_amount) AS total_real_bet_amount,
    COUNT(*)             AS bet_count
FROM TUMBLE(mv_casino_real_bet_events, event_ts, INTERVAL '1 HOUR')
GROUP BY customer_id, window_start, window_end;

-- =============================================================================
-- Managed Iceberg sinks (Lakekeeper REST catalog + MinIO S3).
-- Requires lakekeeper-* services to be up (see bin/3_run_casino_prd_demo.sh).
--
-- Note: a faithful nested raw archive of src_casino_prd is created
-- separately in sql/casino_prd_raw_iceberg.sql (mv_casino_raw +
-- rw_managed_casino_raw). The sinks below cover analytical projections.
-- =============================================================================
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn WITH (
    type                  = 'iceberg',
    catalog.type          = 'rest',
    catalog.uri           = 'http://lakekeeper:8181/catalog/',
    warehouse.path        = 'risingwave-warehouse',
    s3.access.key         = 'hummockadmin',
    s3.secret.key         = 'hummockadmin',
    s3.path.style.access  = 'true',
    s3.endpoint           = 'http://minio-0:9301',
    s3.region             = 'us-east-1'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- --- Transactions ------------------------------------------------------------
DROP SINK  IF EXISTS rw_managed_casino_transactions_sink;
DROP TABLE IF EXISTS rw_managed_casino_transactions;

CREATE TABLE rw_managed_casino_transactions (
    customer_id              INT,
    message_type_id          INT,
    message_created_at       TIMESTAMPTZ,
    transaction_id           BIGINT,
    account_id               INT,
    currency_id              INT,
    transaction_created_at   TIMESTAMPTZ,
    amount_abs               NUMERIC,
    amount_raw               VARCHAR,
    PRIMARY KEY (transaction_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_transactions_sink
INTO rw_managed_casino_transactions
FROM mv_casino_transactions
WITH (
    type                        = 'upsert',
    primary_key                 = 'transaction_id',
    commit_checkpoint_interval  = 5
);

-- --- Real bet events ---------------------------------------------------------
DROP SINK  IF EXISTS rw_managed_casino_real_bet_events_sink;
DROP TABLE IF EXISTS rw_managed_casino_real_bet_events;

CREATE TABLE rw_managed_casino_real_bet_events (
    customer_id        INT,
    transaction_id     BIGINT,
    currency_id        INT,
    real_bet_amount    NUMERIC,
    event_ts           TIMESTAMPTZ,
    event_hour         TIMESTAMPTZ,
    PRIMARY KEY (transaction_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_real_bet_events_sink
INTO rw_managed_casino_real_bet_events
FROM mv_casino_real_bet_events
WITH (
    type                        = 'upsert',
    primary_key                 = 'transaction_id',
    commit_checkpoint_interval  = 5
);

-- --- Hourly per-customer bet totals ------------------------------------------
DROP SINK  IF EXISTS rw_managed_casino_real_bet_hourly_sink;
DROP TABLE IF EXISTS rw_managed_casino_real_bet_hourly;

CREATE TABLE rw_managed_casino_real_bet_hourly (
    customer_id              INT,
    window_start             TIMESTAMPTZ,
    window_end               TIMESTAMPTZ,
    total_real_bet_amount    NUMERIC,
    bet_count                BIGINT,
    PRIMARY KEY (customer_id, window_start)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_real_bet_hourly_sink
INTO rw_managed_casino_real_bet_hourly
FROM mv_casino_real_bet_hourly_per_customer
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id,window_start',
    commit_checkpoint_interval  = 5
);

\echo ''
\echo '=== Casino MVs ==='
SELECT name FROM rw_catalog.rw_materialized_views
WHERE name LIKE 'mv_casino_%' ORDER BY name;

\echo ''
\echo '=== Casino Iceberg sinks ==='
SELECT name FROM rw_catalog.rw_sinks
WHERE name LIKE 'rw_managed_casino_%' ORDER BY name;
