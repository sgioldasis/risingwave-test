{#
  Demo Operations - UPDATE/DELETE Examples for Kafka Tables

  This model provides documentation and example SQL for demonstrating
  UPDATE/DELETE operations on the tbl_* Kafka tables.

  RUN THESE MANUALLY via psql or a SQL client to see real-time effects
  on the funnel_from_tables materialized view.
#}

{{ config(materialized='ephemeral') }}

--
-- ============================================================================
-- DEMO 1: Update Cart Item
-- ============================================================================
--
-- Update a specific user's cart item and timestamp.
-- The funnel_from_tables MV will automatically reflect this change.
--
-- SQL:
--   UPDATE tbl_cart
--   SET item_id = 'premium-widget', event_time = NOW()
--   WHERE user_id = 123;
--

--
-- ============================================================================
-- DEMO 2: Delete Test Data
-- ============================================================================
--
-- Remove test user data from all tables.
-- Useful for cleaning up test scenarios.
--
-- SQL:
--   DELETE FROM tbl_cart WHERE user_id = 99999;
--   DELETE FROM tbl_page WHERE user_id = 99999;
--   DELETE FROM tbl_purchase WHERE user_id = 99999;
--

--
-- ============================================================================
-- DEMO 3: Correct Purchase Amount
-- ============================================================================
--
-- Fix an erroneous purchase amount (e.g., decimal point error).
--
-- SQL:
--   UPDATE tbl_purchase
--   SET amount = 15.99
--   WHERE user_id = 456 AND amount = 1599.00;
--

--
-- ============================================================================
-- DEMO 4: Insert Manual Records
-- ============================================================================
--
-- Add manual records for testing or demo purposes.
--
-- SQL:
--   -- Add a manual page view
--   INSERT INTO tbl_page (user_id, page_id, event_time)
--   VALUES (777, 'demo-landing-page', NOW());
--
--   -- Add a manual cart event
--   INSERT INTO tbl_cart (user_id, item_id, event_time)
--   VALUES (777, 'demo-product', NOW());
--
--   -- Add a manual purchase
--   INSERT INTO tbl_purchase (user_id, amount, event_time)
--   VALUES (777, 49.99, NOW());
--

--
-- ============================================================================
-- DEMO 5: Batch Update Campaign
-- ============================================================================
--
-- Update multiple records based on a condition.
--
-- SQL:
--   -- Mark VIP users (assuming we have a vip_users reference)
--   UPDATE tbl_cart
--   SET item_id = item_id || '-vip'
--   WHERE user_id IN (100, 200, 300);
--

--
-- ============================================================================
-- DEMO 6: View Funnel Changes in Real-Time
-- ============================================================================
--
-- Query to monitor the funnel after making changes.
--
-- SQL:
--   -- View latest funnel metrics from tables
--   SELECT * FROM funnel_from_tables
--   ORDER BY window_start DESC
--   LIMIT 5;
--
--   -- Compare source vs table-based funnel
--   SELECT
--       COALESCE(s.window_start, t.window_start) as window_start,
--       s.viewers as source_viewers,
--       t.viewers as table_viewers,
--       s.carters as source_carters,
--       t.carters as table_carters,
--       s.purchasers as source_purchasers,
--       t.purchasers as table_purchasers
--   FROM funnel s
--   FULL OUTER JOIN funnel_from_tables t
--       ON s.window_start = t.window_start
--   ORDER BY COALESCE(s.window_start, t.window_start) DESC
--   LIMIT 10;
--

--
-- ============================================================================
-- DEMO 7: Check Table Contents
-- ============================================================================
--
-- Inspect the current state of the modifiable tables.
--
-- SQL:
--   -- Count records per table
--   SELECT 'page_views' as table_name, count(*) as record_count FROM tbl_page
--   UNION ALL
--   SELECT 'cart_events', count(*) FROM tbl_cart
--   UNION ALL
--   SELECT 'purchases', count(*) FROM tbl_purchase;
--
--   -- Find recent activity for a specific user
--   SELECT 'page' as event_type, page_id as detail, event_time
--   FROM tbl_page WHERE user_id = 123
--   UNION ALL
--   SELECT 'cart', item_id, event_time
--   FROM tbl_cart WHERE user_id = 123
--   UNION ALL
--   SELECT 'purchase', amount::varchar, event_time
--   FROM tbl_purchase WHERE user_id = 123
--   ORDER BY event_time DESC;
--

--
-- ============================================================================
-- DEMO 8: Iceberg Sink Demo - UPDATE/DELETE Propagation
-- ============================================================================
--
-- This demonstrates how changes propagate from tbl_* tables through
-- funnel_from_tables MV to the Iceberg sink.
--
-- Setup:
--   1. Ensure sink_funnel_from_tables_to_iceberg.sql is deployed
--   2. Connect to Trino: psql -h localhost -p 8080 -d risingwave
--
-- Demo Steps:
--
-- Step 1: Check current state in RisingWave
--   SELECT * FROM funnel_from_tables ORDER BY window_start DESC LIMIT 5;
--
-- Step 2: Check current state in Iceberg (via Trino)
--   SELECT * FROM iceberg.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
--
-- Step 3: Modify source data in RisingWave
--   UPDATE tbl_purchase SET amount = 999.99 WHERE user_id = 100;
--
-- Step 4: Watch funnel_from_tables update (in RisingWave)
--   SELECT * FROM funnel_from_tables ORDER BY window_start DESC LIMIT 5;
--
-- Step 5: Watch Iceberg update (in Trino)
--   SELECT * FROM iceberg.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
--
-- The upsert sink ensures Iceberg stays in sync with the MV changes!
--

-- Ephemeral model just returns a placeholder
SELECT
    1 as demo_placeholder,
    'See comments above for UPDATE/DELETE examples' as instructions
