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
SET client_min_messages = WARNING;
DROP MATERIALIZED VIEW IF EXISTS mv_turnover_percentage       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_latest CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_latest    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_90d   CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_90d       CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_sportsbook_turnover_events CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_turnover_events    CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet           CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_real_bet_events   CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_transactions      CASCADE;

-- =============================================================================
-- Session settings
--
-- streaming_use_shared_source: src_casino_prd feeds mv_casino_transactions,
--   mv_casino_raw (raw_iceberg.sql), and potentially others. Without this, each
--   MV spawns its own Kafka SourceExecutor — multiplying broker connections and
--   network bandwidth. A shared source fans out a single consumer to all MVs.
--
-- NOTE: background_ddl is intentionally NOT set here. When background_ddl=true,
--   CREATE MATERIALIZED VIEW returns before the MV is visible in the catalog.
--   Because this file creates a chain of dependent MVs (mv_casino_transactions
--   → mv_casino_real_bet_events → mv_casino_real_bet, etc.), each step must see
--   the previous MV in the catalog before it can be created. background_ddl
--   breaks that dependency chain. Use it only for isolated, top-level MVs with
--   no downstream dependents in the same session.
-- =============================================================================
SET streaming_use_shared_source = true;

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
--
-- Uses ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)
-- rather than DISTINCT ON (customer_id) ORDER BY …. In RisingWave streaming
-- mode ORDER BY at the SELECT level is not honoured by the streaming executor
-- (it applies only at creation time, not to ongoing result updates). DISTINCT
-- ON relies on that ORDER BY to pick the "latest" row — in a live streaming
-- context it becomes arbitrary. The ROW_NUMBER() Top-1 pattern is the
-- RisingWave-idiomatic way to maintain a per-key maximum: it compiles to a
-- stateful TopN operator that correctly handles retract+insert pairs emitted
-- by the upstream sliding-window MVs.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_casino_turnover_latest AS
SELECT customer_id, casino_turnover, event_ts
FROM (
    SELECT
        customer_id,
        rolling_90d_turnover                                                     AS casino_turnover,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)      AS rn
    FROM mv_casino_turnover_90d
) t
WHERE rn = 1;

CREATE MATERIALIZED VIEW mv_sportsbook_turnover_latest AS
SELECT customer_id, sportsbook_turnover, event_ts
FROM (
    SELECT
        customer_id,
        rolling_90d_turnover                                                     AS sportsbook_turnover,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)      AS rn
    FROM mv_sportsbook_turnover_90d
) t
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- 8. Turnover percentage — updated whenever either side gets a new event.
--
-- Implemented as UNION ALL + GROUP BY (a pivot), NOT a FULL OUTER JOIN.
-- The upstream _latest MVs use DISTINCT ON (customer_id), so their stream key
-- IS the join key. A FULL OUTER JOIN on that key keeps its hash-join state
-- with an *empty* extra pk; while the DISTINCT ON sides emit retract+insert
-- pairs during backfill/live updates, two rows for the same customer_id can
-- briefly reach the join state and panic with `double inserting a join state
-- entry`, which (with DatabaseFailureIsolation license-disabled) resets the
-- whole database. A hash aggregation keeps one state row per group and applies
-- +/- deltas, so it consumes the same update stream without that failure mode.
-- A customer present on only one side simply gets 0 from the other branch,
-- reproducing the FULL OUTER JOIN's COALESCE(..., 0) semantics.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW mv_turnover_percentage AS
SELECT
    customer_id,
    SUM(casino_turnover)                             AS casino_turnover,
    SUM(sportsbook_turnover)                         AS sportsbook_turnover,
    SUM(casino_turnover) + SUM(sportsbook_turnover)  AS total_turnover,
    CASE
        WHEN SUM(casino_turnover) + SUM(sportsbook_turnover) = 0 THEN 0
        ELSE SUM(casino_turnover)
             / (SUM(casino_turnover) + SUM(sportsbook_turnover))
    END                                              AS casino_ratio,
    CASE
        WHEN SUM(casino_turnover) + SUM(sportsbook_turnover) = 0 THEN 0
        ELSE SUM(sportsbook_turnover)
             / (SUM(casino_turnover) + SUM(sportsbook_turnover))
    END                                              AS sportsbook_ratio
FROM (
    SELECT customer_id, casino_turnover, 0::NUMERIC AS sportsbook_turnover
    FROM mv_casino_turnover_latest
    UNION ALL
    SELECT customer_id, 0::NUMERIC AS casino_turnover, sportsbook_turnover
    FROM mv_sportsbook_turnover_latest
) u
GROUP BY customer_id;

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

-- background_ddl is safe from here to end-of-file: sinks and indexes are all
-- terminal — nothing in this session depends on them being fully built before
-- the session exits. Without it, each CREATE SINK/INDEX blocks until the full
-- initial snapshot or index build completes over the backfilled MV history.
SET background_ddl = true;

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
    commit_checkpoint_interval  = 5,
    force_compaction            = true
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
    commit_checkpoint_interval  = 5,
    force_compaction            = true
);

-- =============================================================================
-- Indexes on serving MVs
--
-- Terminal MVs queried by Grafana or application code do full heap scans
-- without indexes. customer_id is the primary lookup key for both UC1 and UC2;
-- (customer_id, currency_id) is the natural compound key for real-bet queries.
-- RisingWave indexes are maintained incrementally by the streaming engine —
-- they add write overhead but pay off immediately on point lookups.
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_casino_real_bet_customer
    ON mv_casino_real_bet (customer_id, currency_id);

CREATE INDEX IF NOT EXISTS idx_turnover_percentage_customer
    ON mv_turnover_percentage (customer_id);

\echo ''
\echo '=== MVs ==='
SELECT name FROM rw_catalog.rw_materialized_views
WHERE name LIKE 'mv_casino_%' OR name LIKE 'mv_sportsbook_%' OR name LIKE 'mv_turnover_%'
ORDER BY name;

\echo ''
\echo '=== Iceberg sinks ==='
SELECT name FROM rw_catalog.rw_sinks
WHERE name LIKE 'rw_managed_%' ORDER BY name;
