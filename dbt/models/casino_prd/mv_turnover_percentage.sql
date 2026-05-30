{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

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
    FROM {{ ref('mv_casino_turnover_latest') }}
    UNION ALL
    SELECT customer_id, 0::NUMERIC AS casino_turnover, sportsbook_turnover
    FROM {{ ref('mv_sportsbook_turnover_latest') }}
) u
GROUP BY customer_id
