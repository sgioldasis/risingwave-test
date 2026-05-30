-- =============================================================================
-- Faithful raw-source iceberg sink: mirror src_casino_prd's nested
-- protobuf struct 1:1 into a managed Iceberg table. No flattening.
--
-- A thin pass-through MV (mv_casino_raw) only renames top-level columns to
-- snake_case so the iceberg sink's `primary_key` config (which doesn't
-- handle quoted identifiers) can reference `unique_id`. The struct/array
-- column types flow through unchanged.
-- =============================================================================

\set ON_ERROR_STOP off
DROP SINK              IF EXISTS rw_managed_casino_raw_sink;
DROP TABLE             IF EXISTS rw_managed_casino_raw;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_raw CASCADE;
\set ON_ERROR_STOP on

SET client_min_messages = WARNING;

-- Session settings (see casino_prd_funnel_iceberg.sql for rationale)
-- NOTE: background_ddl is NOT set — mv_casino_raw must be in the catalog
--   before the Iceberg sink DDL can reference it.
SET streaming_use_shared_source = true;

-- --- Pass-through MV: rename top-level cols, keep nested structs intact ----
CREATE MATERIALIZED VIEW mv_casino_raw AS
SELECT
    s."UniqueId"                          AS unique_id,
    s."CustomerId"                        AS customer_id,
    s."CompanyId"                         AS company_id,
    s."CasinoProviderId"                  AS casino_provider_id,
    s."ExternalProviderId"                AS external_provider_id,
    s."GameInfo"                          AS game_info,
    s."RoundInfo"                         AS round_info,
    s."IsBonusLockedOnFatMessageCreation" AS is_bonus_locked_on_fat_message_creation,
    s."IsBonusCampaignWagering"           AS is_bonus_campaign_wagering
FROM src_casino_prd AS s;

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

CREATE TABLE rw_managed_casino_raw (
    unique_id              VARCHAR,
    customer_id            INT,
    company_id             INT,
    casino_provider_id     INT,
    external_provider_id   INT,
    game_info STRUCT<
        "GameId"                            INT,
        "ProviderGameCode"                  VARCHAR,
        "IsLive"                            BOOLEAN,
        "ProviderTableCode"                 VARCHAR,
        "GameType"                          INT,
        "IsJackpotContributionsFromOperator" BOOLEAN
    >,
    round_info STRUCT<
        "GameRoundRef" VARCHAR,
        "RoundCreated" STRUCT<seconds BIGINT, nanos INT>,
        "RoundEnded"   STRUCT<seconds BIGINT, nanos INT>,
        "Messages" STRUCT<
            "MessageId"                 BIGINT,
            "TransactionRef"            VARCHAR,
            "SessionId"                 VARCHAR,
            "MessageTypeId"             INT,
            "TokenTypeId"               INT,
            "ProviderBonusTokenCode"    VARCHAR,
            "Created"                   STRUCT<seconds BIGINT, nanos INT>,
            "JackpotWinAmount"          VARCHAR,
            "JackpotContributionAmount" VARCHAR,
            "JackpotInfo"               VARCHAR,
            "IsRoundClosed"             BOOLEAN,
            "Transactions" STRUCT<
                "TransactionId"          BIGINT,
                "Created"                STRUCT<seconds BIGINT, nanos INT>,
                "CurrencyId"             INT,
                "AccountId"              INT,
                "BaseBalanceBefore"      VARCHAR,
                "BonusBalanceBefore"     VARCHAR,
                "Amount"                 VARCHAR,
                "CurrencyRateToEuro"     VARCHAR,
                "TokenAmount"            VARCHAR,
                "CustomerCampaignId"     INT,
                "CampaignId"             INT,
                "CampaignTypeId"         INT,
                "PandoraJourneyId"       BIGINT,
                "SourceId"               INT,
                "BonusAction"            VARCHAR,
                "IsDepositFromToken"     BOOLEAN,
                "RawBaseBalanceBefore"   VARCHAR,
                "RawBonusBalanceBefore"  VARCHAR,
                "TransactionTypeId"      INT
            >[],
            "CommissionAmount"      VARCHAR,
            "P2PGameMode"           INT,
            "JackpotRealWinAmount"  VARCHAR,
            "JackpotBonusWinAmount" VARCHAR,
            "BetCoverage"           VARCHAR
        >[]
    >,
    is_bonus_locked_on_fat_message_creation BOOLEAN,
    is_bonus_campaign_wagering              BOOLEAN,
    PRIMARY KEY (unique_id)
) ENGINE = iceberg;

-- background_ddl is safe here: CREATE SINK is the last statement in this file;
-- nothing downstream depends on it in this session. Without it, psql blocks
-- until the full initial Iceberg snapshot (424k+ rows) is committed.
SET background_ddl = true;
CREATE SINK rw_managed_casino_raw_sink
INTO rw_managed_casino_raw
FROM mv_casino_raw
WITH (
    type                        = 'upsert',
    primary_key                 = 'unique_id',
    commit_checkpoint_interval  = 5,
    force_compaction            = true
);

\echo ''
SELECT name FROM rw_catalog.rw_sinks WHERE name = 'rw_managed_casino_raw_sink';
