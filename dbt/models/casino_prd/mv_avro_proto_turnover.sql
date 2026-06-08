{{ config(
    materialized='materialized_view',
    tags=['casino_avro', 'casino_uc2']
) }}

-- UC2 (Turnover) combining two encoding paths: casino side from the Avro round-trip
-- topic, sportsbook side from the native Protobuf pipeline. Demonstrates cross-format
-- joins — Avro-decoded casino data joined with Protobuf-decoded sportsbook data.
WITH casino AS (
    SELECT
        s."CustomerId"                            AS customer_id,
        SUM(ABS(NULLIF(txn."Amount", '')::numeric)) AS casino_turnover
    FROM {{ ref('src_casino_avro') }}             AS s,
        UNNEST((s."RoundInfo")."Messages")        AS msg,
        UNNEST(msg."Transactions")                AS txn
    WHERE msg."MessageTypeId" = 1
    GROUP BY s."CustomerId"
),
sportsbook AS (
    SELECT customer_id, sportsbook_turnover
    FROM {{ ref('mv_sportsbook_turnover_latest') }}
)
SELECT
    COALESCE(c.customer_id, s.customer_id)              AS customer_id,
    COALESCE(c.casino_turnover,     0)                   AS casino_turnover,
    COALESCE(s.sportsbook_turnover, 0)                   AS sportsbook_turnover,
    COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0) AS total_turnover
FROM casino c
FULL OUTER JOIN sportsbook s ON c.customer_id = s.customer_id
