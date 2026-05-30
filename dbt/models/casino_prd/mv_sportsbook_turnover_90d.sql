{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    ("CustomerInfo")."Id"                                                        AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                           AS event_ts,
    SUM((("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000) OVER (
        PARTITION BY ("CustomerInfo")."Id"
        ORDER BY TO_TIMESTAMP(("PlacedAt").seconds)
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    )                                                                            AS rolling_90d_turnover
FROM {{ ref('src_bets_gh') }}
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL
