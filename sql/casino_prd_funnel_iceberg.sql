-- =============================================================================
-- Prod casino + sportsbook pipeline
--
-- Sources (created separately):
--   src_casino_prd  → sql/casino_prd_source.sql     (prd2, cronus.casino.out.gh)
--   src_bets_gh     → sql/casino_prd_bets_source.sql (prd4, bets-out-gh)
--
-- UC1 — Real Bet Amount:
--   mv_casino_transactions          one row per transaction (flattened)
--   mv_casino_real_bet_events       bet filter (MessageTypeId=1, AccountId=1)
--   mv_casino_real_bet              rolling 14-day real bet total per customer/currency
--
-- UC2 — Casino Turnover Percentage:
--   mv_casino_turnover_events       casino payout events (MessageTypeId=2)
--   mv_sportsbook_turnover_events   sportsbook placement events (from src_bets_gh)
--   mv_casino_turnover_90d          rolling 90-day casino turnover per customer
--   mv_sportsbook_turnover_90d      rolling 90-day sportsbook turnover per customer
--   mv_casino_turnover_latest       latest rolling sum per customer (deduped)
--   mv_sportsbook_turnover_latest   latest rolling sum per customer (deduped)
--   mv_turnover_percentage          casino vs sportsbook ratio per customer
--
-- Iceberg sinks (Lakekeeper REST catalog + MinIO S3):
--   rw_managed_casino_transactions
--   rw_managed_casino_real_bet_events
--   rw_managed_casino_real_bet
--   rw_managed_turnover_percentage
--
-- Idempotent: safe to re-run. DROPs cascade to dependents.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Drop in dependency order so CASCADE doesn't fail on cross-MV dependencies
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_turnover_percentage       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_latest CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_latest    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_90d   CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_90d       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_events CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_events    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet           CASCADE;
-- legacy names from previous schema
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_14d_per_customer    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_hourly_per_customer CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_events   CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_transactions      CASCADE;

-- =============================================================================
-- UC1 — Real Bet Amount
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Transaction-level flat view.
--    Two chained UNNESTs with explicit row aliases resolve the "Created" field
--    collision between CasinoMessageInformation and TransactionInformation.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_transactions AS
SELECT
    s."CustomerId"                                        AS customer_id,
    msg."MessageTypeId"                                   AS message_type_id,
    TO_TIMESTAMP((msg."Created").seconds)                 AS message_created_at,
    txn."TransactionId"                                   AS transaction_id,
    txn."AccountId"                                       AS account_id,
    txn."CurrencyId"                                      AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                 AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::numeric)                AS amount_abs,
    txn."Amount"                                          AS amount_raw,
    txn."BonusAction"                                     AS bonus_action,
    (s."GameInfo")."GameId"                               AS game_id,
    (s."GameInfo")."GameType"                             AS game_type,
    (s."GameInfo")."IsLive"                               AS is_live,
    s."CompanyId"                                         AS company_id
FROM
    src_casino_prd                             AS s,
    UNNEST((s."RoundInfo")."Messages")         AS msg,
    UNNEST(msg."Transactions")                 AS txn;

-- ---------------------------------------------------------------------------
-- 2. Real Bet Amount events (UC1 filter).
--    Mirrors getCasinoRealBetAmountEvents from the Spark PoC:
--      MessageTypeId = 1  (bet placed)
--      AccountId     = 1  (real money, not bonus)
--      Amount        IS NOT NULL AND <> '' (proto3 strings decode as '' when absent)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_real_bet_events AS
SELECT
    customer_id,
    transaction_id,
    currency_id,
    game_id,
    game_type,
    is_live,
    company_id,
    amount_abs                                    AS real_bet_amount,
    transaction_created_at                        AS event_ts
FROM mv_casino_transactions
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- ---------------------------------------------------------------------------
-- 3. Rolling 14-day real bet total per customer/currency.
--    Mirrors Spark mapGroupsWithState: one output row per event with the
--    updated sum over the preceding 14 days (1 209 600 s).
--    Consumers key on (customer_id, currency_id) and keep the latest row.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_real_bet AS
SELECT
    customer_id,
    currency_id,
    event_ts,
    SUM(real_bet_amount) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '1209600 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_14d_real_bet_amount
FROM mv_casino_real_bet_events;

-- =============================================================================
-- UC2 — Casino Turnover Percentage
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 4. Casino turnover events.
--    MessageTypeId = 2 (withdraw/payout), AccountId IN (1, 4) (real + bonus).
--    Mirrors getCasinoTurnover from the Spark PoC.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_turnover_events AS
SELECT
    customer_id,
    currency_id,
    amount_abs               AS turnover,
    transaction_created_at   AS event_ts
FROM mv_casino_transactions
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- ---------------------------------------------------------------------------
-- 5. Sportsbook turnover events (from src_bets_gh).
--    TotalStake.Euro uses DecimalValue encoding (units + nanos/1e9).
--    Euro-normalised amounts make casino and sportsbook comparable across
--    currencies when computing the ratio.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_sportsbook_turnover_events AS
SELECT
    ("CustomerInfo")."Id"                                                        AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                           AS event_ts,
    (("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000                 AS turnover
FROM src_bets_gh
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 6. 90-day rolling totals (7 776 000 s = 90 days).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_turnover_90d AS
SELECT
    customer_id,
    event_ts,
    SUM(turnover) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM mv_casino_turnover_events;

CREATE MATERIALIZED VIEW mv_sportsbook_turnover_90d AS
SELECT
    customer_id,
    event_ts,
    SUM(turnover) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM mv_sportsbook_turnover_events;

-- ---------------------------------------------------------------------------
-- 7. Latest rolling sum per customer (one row per customer for ratio join).
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_turnover_latest AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    rolling_90d_turnover    AS casino_turnover,
    event_ts
FROM mv_casino_turnover_90d
ORDER BY customer_id, event_ts DESC;

CREATE MATERIALIZED VIEW mv_sportsbook_turnover_latest AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    rolling_90d_turnover    AS sportsbook_turnover,
    event_ts
FROM mv_sportsbook_turnover_90d
ORDER BY customer_id, event_ts DESC;

-- ---------------------------------------------------------------------------
-- 8. Turnover percentage — updated whenever either side gets a new event.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_turnover_percentage AS
SELECT
    COALESCE(c.customer_id, s.customer_id)      AS customer_id,
    COALESCE(c.casino_turnover, 0)              AS casino_turnover,
    COALESCE(s.sportsbook_turnover, 0)          AS sportsbook_turnover,
    COALESCE(c.casino_turnover, 0)
        + COALESCE(s.sportsbook_turnover, 0)    AS total_turnover,
    CASE
        WHEN COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0) = 0 THEN 0
        ELSE COALESCE(c.casino_turnover, 0)
             / (COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0))
    END                                         AS casino_ratio,
    CASE
        WHEN COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0) = 0 THEN 0
        ELSE COALESCE(s.sportsbook_turnover, 0)
             / (COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0))
    END                                         AS sportsbook_ratio
FROM mv_casino_turnover_latest      c
FULL OUTER JOIN mv_sportsbook_turnover_latest s USING (customer_id);

-- =============================================================================
-- Managed Iceberg sinks (Lakekeeper REST catalog + MinIO S3).
-- Requires lakekeeper-* services to be up.
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

-- --- Rolling 14-day real bet (UC1) -------------------------------------------
DROP SINK  IF EXISTS rw_managed_casino_real_bet_sink;
DROP TABLE IF EXISTS rw_managed_casino_real_bet;

CREATE TABLE rw_managed_casino_real_bet (
    customer_id                  INT,
    currency_id                  INT,
    event_ts                     TIMESTAMPTZ,
    rolling_14d_real_bet_amount  NUMERIC,
    PRIMARY KEY (customer_id, currency_id, event_ts)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_real_bet_sink
INTO rw_managed_casino_real_bet
FROM mv_casino_real_bet
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id,currency_id,event_ts',
    commit_checkpoint_interval  = 5
);

-- --- Turnover percentage (UC2) -----------------------------------------------
DROP SINK  IF EXISTS rw_managed_turnover_percentage_sink;
DROP TABLE IF EXISTS rw_managed_turnover_percentage;

CREATE TABLE rw_managed_turnover_percentage (
    customer_id          INT,
    casino_turnover      NUMERIC,
    sportsbook_turnover  NUMERIC,
    total_turnover       NUMERIC,
    casino_ratio         NUMERIC,
    sportsbook_ratio     NUMERIC,
    PRIMARY KEY (customer_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_turnover_percentage_sink
INTO rw_managed_turnover_percentage
FROM mv_turnover_percentage
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id',
    commit_checkpoint_interval  = 5
);

\echo ''
\echo '=== MVs ==='
SELECT name FROM rw_catalog.rw_materialized_views
WHERE name LIKE 'mv_casino_%' OR name LIKE 'mv_sportsbook_%' OR name LIKE 'mv_turnover_%'
ORDER BY name;

\echo ''
\echo '=== Iceberg sinks ==='
SELECT name FROM rw_catalog.rw_sinks
WHERE name LIKE 'rw_managed_%' ORDER BY name;
