-- WHAT: the per-job template for the THREE Routine Load jobs feeding
--   funnel_daily_totals_agg (one each for page_views, cart_events,
--   purchases).
-- FEATURE: Routine Load -- StarRocks's continuous Kafka consumer (JSON
--   batches committed every 5s here).
-- WHY: three independent jobs incrementing only their own counter column
--   into the SAME AGGREGATE row is the demo point: parallel writers merge
--   into one key's row without clobbering each other's column.
-- WHERE: rendered per job by the funnel_daily_totals_agg Dagster asset
--   (which owns the placeholder values); the table itself is
--   funnel_daily_totals_agg.sql.

-- Template for the three Routine Load jobs feeding funnel_daily_totals_agg
-- (see funnel_daily_totals_agg.sql for the AGGREGATE KEY merge story).
-- One job per raw event topic; each explicitly sets ALL three counter
-- columns (1 for the one it owns, 0 literal for the other two) rather than
-- omitting columns and relying on a DEFAULT -- simpler and more certain to
-- work, confirmed live. `day` is derived from the raw ISO 8601 event_time
-- via LEFT(...,10) (the date portion is always the first 10 characters
-- regardless of what follows) then STR_TO_DATE. country is hardcoded 'GR'
-- (the raw topics carry no country; dim_users holds the synthetic mapping).
-- Placeholders are filled per job by
-- orchestration/assets/funnel_agg_key_setup.py: {job_name}, {columns},
-- {jsonpaths}, {topic}.
CREATE ROUTINE LOAD {schema}.{job_name} ON funnel_daily_totals_agg
COLUMNS({columns})
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[{jsonpaths}]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "{topic}",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
