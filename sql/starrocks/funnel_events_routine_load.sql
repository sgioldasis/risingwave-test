-- WHAT: the per-table template for the Routine Loads feeding
--   page_views_direct / cart_events_direct / purchases_direct straight
--   from the three funnel Kafka topics.
-- FEATURE: Routine Load -- StarRocks's continuous Kafka consumer (JSON
--   batches committed every 5s here).
-- WHY: the "hot-only" contrast of the Funnel Dashboard -- the same traffic
--   ingested with no RisingWave in the loop, powering the Direct-from-Kafka
--   chart next to the RisingWave-JDBC chart.
-- WHERE: rendered per table by the funnel_events_direct_kafka Dagster
--   asset (which owns the placeholder values); the tables and the
--   funnel_summary_direct views over them are dbt models.

-- Template for the Routine Load jobs feeding the dbt-managed DUPLICATE KEY
-- shell tables page_views_direct / cart_events_direct / purchases_direct
-- (dbt_starrocks/models/*_direct.sql, on_table_exists='ignore') straight
-- from the three funnel Kafka topics -- no RisingWave in the loop. One
-- placeholder-filled job per topic; wired by
-- orchestration/assets/funnel_direct_kafka_setup.py: {table}, {cols},
-- {jsonpaths}, {topic} (the table name minus its _direct suffix).
--
-- event_time is parsed from the producer's ISO 8601 UTC string at load
-- time via a derived column (SUBSTR drops the fixed 6-char "+00:00"
-- suffix), same str_to_date trick as the wallet loaders.
--
-- max_batch_interval = 5 is the StarRocks-enforced floor ("3" is rejected
-- with "max_batch_interval should >= 5"). Consequence for any side-by-side
-- comparison against a RisingWave-fed table: the RW sink lands rows per
-- checkpoint while this job commits only every 5s, so the _direct tables
-- trail by up to ~5s / a few rows at any instant -- expected pipeline
-- latency, not missing data (verified 2026-11-14 on the wallet twin of
-- this comparison: exactly 5 rows / 5.0s behind at 1 TPS, converging).
--
-- Recreation caveat: the tables are append-only with no unique key (the
-- producer emits no event id), so a Routine Load recreation re-reads the
-- topic from OFFSET_BEGINNING and double-counts everything it re-reads.
CREATE ROUTINE LOAD {schema}.{table}_load ON {table}
COLUMNS({cols}, event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "{jsonpaths}",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "{topic}",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
