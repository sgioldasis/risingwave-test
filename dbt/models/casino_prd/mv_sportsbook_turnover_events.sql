{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    ("CustomerInfo")."Id"                                                        AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                           AS event_ts,
    (("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000                 AS turnover
FROM {{ ref('src_bets_gh') }}
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL
