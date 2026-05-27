-- =============================================================================
-- Prod casino rounds → MVs + Iceberg sinks
--
-- Source: src_casino_prd (already created, reads cronus.casino.out.gh from
--         prd2 Kafka, schema from /proto/casinoroundinfodto.pb).
--
-- Builds:
--   1. mv_casino_prd_rounds_flat            -- one row per round (flattened)
--   2. mv_casino_prd_volume_by_company_game -- bet volume rollup
--   3. mv_casino_prd_funnel                 -- per-round message-stage funnel
--   4. Iceberg sinks (Lakekeeper REST catalog, MinIO storage)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Round-level flat view (one row per round)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_rounds_flat CASCADE;

CREATE MATERIALIZED VIEW mv_casino_prd_rounds_flat AS
SELECT
    r."UniqueId"                          AS unique_id,
    r."CustomerId"                        AS customer_id,
    r."CompanyId"                         AS company_id,
    r."CasinoProviderId"                  AS casino_provider_id,
    r."ExternalProviderId"                AS external_provider_id,
    (r."GameInfo")."GameId"               AS game_id,
    (r."GameInfo")."ProviderGameCode"     AS provider_game_code,
    (r."GameInfo")."GameType"             AS game_type,
    (r."GameInfo")."IsLive"               AS is_live,
    (r."RoundInfo")."GameRoundRef"        AS game_round_ref,
    to_timestamp(
        ((r."RoundInfo")."RoundCreated")."seconds"
      + ((r."RoundInfo")."RoundCreated")."nanos" / 1e9
    )                                     AS round_created,
    to_timestamp(
        ((r."RoundInfo")."RoundEnded")."seconds"
      + ((r."RoundInfo")."RoundEnded")."nanos" / 1e9
    )                                     AS round_ended,
    array_length((r."RoundInfo")."Messages") AS num_messages,
    r."IsBonusLockedOnFatMessageCreation" AS is_bonus_locked,
    r."IsBonusCampaignWagering"           AS is_bonus_campaign_wagering
FROM src_casino_prd AS r;

-- ---------------------------------------------------------------------------
-- 2. Bet-volume rollup per (company, game)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_volume_by_company_game CASCADE;

CREATE MATERIALIZED VIEW mv_casino_prd_volume_by_company_game AS
WITH exploded_msgs AS (
    SELECT
        r."UniqueId"               AS unique_id,
        r."CompanyId"              AS company_id,
        (r."GameInfo")."GameId"    AS game_id,
        (r."GameInfo")."GameType"  AS game_type,
        "Transactions"             AS txs
    FROM src_casino_prd AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id,
    game_id,
    game_type,
    COUNT(DISTINCT unique_id)                          AS rounds,
    COUNT(*)                                           AS transactions,
    ROUND(SUM(("Amount")::numeric), 4)                 AS total_amount,
    ROUND(SUM(("TokenAmount")::numeric), 4)            AS total_token_amount,
    ROUND(AVG(("CurrencyRateToEuro")::numeric), 6)     AS avg_fx_to_eur
FROM exploded_msgs, UNNEST(txs)
GROUP BY 1, 2, 3;

-- ---------------------------------------------------------------------------
-- 3. Per-round message-stage funnel
--    Each round emits N messages; funnel = stage counts + close-status per
--    (company, game). Stage = message ordinal within the round.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_prd_funnel CASCADE;

CREATE MATERIALIZED VIEW mv_casino_prd_funnel AS
WITH stages AS (
    SELECT
        r."CompanyId"                       AS company_id,
        (r."GameInfo")."GameId"             AS game_id,
        r."UniqueId"                        AS unique_id,
        "MessageTypeId"                     AS msg_type,
        "IsRoundClosed"                     AS round_closed
    FROM src_casino_prd AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id,
    game_id,
    msg_type,
    COUNT(*)                                          AS messages,
    COUNT(DISTINCT unique_id)                         AS rounds_with_stage,
    COUNT(*) FILTER (WHERE round_closed)              AS round_closed_count
FROM stages
GROUP BY 1, 2, 3;

-- ---------------------------------------------------------------------------
-- 4. Iceberg sinks (Lakekeeper REST + MinIO)
-- ---------------------------------------------------------------------------
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    warehouse.path = 'risingwave-warehouse',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- --- Round-level fact ---------------------------------------------------------
DROP SINK IF EXISTS rw_managed_casino_prd_rounds_sink;
DROP TABLE IF EXISTS rw_managed_casino_prd_rounds;

CREATE TABLE rw_managed_casino_prd_rounds (
    unique_id                  VARCHAR,
    customer_id                INT,
    company_id                 INT,
    casino_provider_id         INT,
    external_provider_id       INT,
    game_id                    INT,
    provider_game_code         VARCHAR,
    game_type                  INT,
    is_live                    BOOLEAN,
    game_round_ref             VARCHAR,
    round_created              TIMESTAMPTZ,
    round_ended                TIMESTAMPTZ,
    num_messages               INT,
    is_bonus_locked            BOOLEAN,
    is_bonus_campaign_wagering BOOLEAN,
    PRIMARY KEY (unique_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_prd_rounds_sink
INTO rw_managed_casino_prd_rounds
FROM mv_casino_prd_rounds_flat
WITH (
    type = 'upsert',
    primary_key = 'unique_id',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

-- --- Volume rollup ------------------------------------------------------------
DROP SINK IF EXISTS rw_managed_casino_prd_volume_sink;
DROP TABLE IF EXISTS rw_managed_casino_prd_volume;

CREATE TABLE rw_managed_casino_prd_volume (
    company_id          INT,
    game_id             INT,
    game_type           INT,
    rounds              BIGINT,
    transactions        BIGINT,
    total_amount        NUMERIC,
    total_token_amount  NUMERIC,
    avg_fx_to_eur       NUMERIC,
    PRIMARY KEY (company_id, game_id, game_type)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_prd_volume_sink
INTO rw_managed_casino_prd_volume
FROM mv_casino_prd_volume_by_company_game
WITH (
    type = 'upsert',
    primary_key = 'company_id,game_id,game_type',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

-- --- Funnel rollup ------------------------------------------------------------
DROP SINK IF EXISTS rw_managed_casino_prd_funnel_sink;
DROP TABLE IF EXISTS rw_managed_casino_prd_funnel;

CREATE TABLE rw_managed_casino_prd_funnel (
    company_id          INT,
    game_id             INT,
    msg_type            INT,
    messages            BIGINT,
    rounds_with_stage   BIGINT,
    round_closed_count  BIGINT,
    PRIMARY KEY (company_id, game_id, msg_type)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_prd_funnel_sink
INTO rw_managed_casino_prd_funnel
FROM mv_casino_prd_funnel
WITH (
    type = 'upsert',
    primary_key = 'company_id,game_id,msg_type',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

\echo ''
\echo '=== MVs + Iceberg sinks created ==='
SELECT name, connector, sink_type
FROM rw_catalog.rw_sinks
WHERE name LIKE 'rw_managed_casino_prd_%';
