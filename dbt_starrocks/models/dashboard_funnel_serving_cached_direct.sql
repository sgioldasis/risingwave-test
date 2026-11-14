{{
  config(
    materialized='view',
    alias='dashboard_funnel_serving_cached_direct'
  )
}}

-- RW-free variant of dashboard_funnel_serving_cached (2026-11-14): the
-- original's live branch reads risingwave.public.funnel_summary over the
-- JDBC catalog, so with the RW containers stopped the whole view fails
-- ANALYSIS ("Unknown database 'public.'") and both cached charts error --
-- a SQL view cannot catch that, because the failure happens before any
-- row is read. This variant swaps the live JDBC branch for the
-- Direct-from-Kafka path (funnel_summary_direct, computed over the
-- Routine-Load tables that StarRocks owns itself), so no object in the
-- query touches an external catalog at query time. Verified live with the
-- RW containers stopped: the view resolved and served continuous
-- 2026-09-05 -> now data (frozen MV rows + fresh Routine-Load rows).
--
-- No double-counting by construction: the cutoff is
-- `window_start > (SELECT MAX(window_start) FROM mv_unified_funnel_summary)`
-- -- the MV branch contributes every window it has, the direct branch only
-- windows strictly newer, so no window is served twice no matter when the
-- MV was last refreshed (if the MV is refreshed later, it takes over those
-- windows and the direct branch shrinks accordingly). The COALESCE handles
-- an empty MV (fresh stack): its MAX is NULL, and the direct branch serves
-- everything.
--
-- funnel_summary_direct is NOT a dbt model -- it's built by the
-- funnel_events_direct_kafka Dagster asset (orchestration/assets/
-- funnel_direct_kafka_setup.py), same as the Routine Load jobs feeding it;
-- referenced here by literal name, with a Dagster-side dep added in
-- orchestration/definitions.py so the asset always runs first.
WITH unified_funnel AS (
  SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate
  FROM {{ ref('mv_unified_funnel_summary') }}

  UNION ALL

  SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    CAST(carters AS DOUBLE) / NULLIF(viewers, 0) AS view_to_cart_rate,
    CAST(purchasers AS DOUBLE) / NULLIF(carters, 0) AS cart_to_buy_rate
  FROM sr_local_db_sr_local_db.funnel_summary_direct
  WHERE window_start > COALESCE(
    (SELECT MAX(window_start) FROM {{ ref('mv_unified_funnel_summary') }}),
    CAST('0000-01-01 00:00:00' AS DATETIME)
  )
)

SELECT *
FROM unified_funnel
