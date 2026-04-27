#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# SPDX-License-Identifier: MIT
"""Track Kafka head produced_at per topic and persist it to Postgres.

This script reads the tail of configured Kafka topics, tracks the maximum observed
`produced_at` per topic, and upserts those values into a Postgres table:
`kafka_head_produced_at`.

It can optionally mirror the same table into RisingWave to preserve existing
Grafana SQL dashboards that join against RisingWave sources.

The table is used by Grafana dashboards and ad hoc queries to compare Kafka head
produce-time against RisingWave visibility and freshness metrics.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg2
from confluent_kafka import Consumer, TopicPartition

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_ERROR = 2

TOPICS = ["page_views", "cart_events", "purchases"]

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure process logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def parse_payload_timestamp(value: Any) -> datetime | None:
    """Parse payload timestamp value into UTC datetime.

    Args:
        value: Event time value from Kafka payload.

    Returns:
        Parsed timezone-aware UTC datetime, or None when parsing fails.
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Heuristic: treat large values as milliseconds since epoch.
        ts = float(value)
        if ts > 10_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=UTC)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        if s.isdigit():
            ts = float(s)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=UTC)

        # Normalize common ISO format variants.
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    return None


def parse_message_produced_at(raw_value: bytes | str | None) -> datetime | None:
    """Extract produced_at from a Kafka message payload.

    Args:
        raw_value: Raw message value.

    Returns:
        Parsed produced_at in UTC if present and valid. Falls back to
        event_time for backwards compatibility during rollout.
    """
    if raw_value is None:
        return None

    payload_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return None

    return parse_payload_timestamp(payload.get("produced_at")) or parse_payload_timestamp(payload.get("event_time"))


def connect_db(dsn: str):
    """Create a PostgreSQL-compatible connection with autocommit enabled."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def ensure_table(conn) -> None:
    """Create storage table for Kafka head produced_at if missing."""
    ddl = """
    CREATE TABLE IF NOT EXISTS kafka_head_produced_at (
        topic VARCHAR PRIMARY KEY,
        max_produced_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def upsert_topic_head(conn, topic: str, max_produced_at: datetime) -> None:
    """Upsert topic head produced_at into target database."""
    delete_sql = "DELETE FROM kafka_head_produced_at WHERE topic = %s;"
    insert_sql = """
    INSERT INTO kafka_head_produced_at (topic, max_produced_at, updated_at)
    VALUES (%s, %s, NOW());
    """
    with conn.cursor() as cur:
        cur.execute(delete_sql, (topic,))
        cur.execute(insert_sql, (topic, max_produced_at))


def persist_topic_head(
    primary_conn,
    primary_dsn: str,
    topic: str,
    max_produced_at: datetime,
    mirror_conn=None,
    mirror_dsn: str | None = None,
):
    """Persist topic head to primary database and optionally mirror to RisingWave."""
    try:
        upsert_topic_head(primary_conn, topic, max_produced_at)
    except Exception:
        logger.exception("Primary DB upsert failed, attempting reconnect")
        try:
            primary_conn.close()
        except Exception:
            pass
        primary_conn = connect_db(primary_dsn)
        ensure_table(primary_conn)
        upsert_topic_head(primary_conn, topic, max_produced_at)

    if mirror_conn is not None and mirror_dsn is not None:
        try:
            upsert_topic_head(mirror_conn, topic, max_produced_at)
        except Exception:
            logger.exception("Mirror RisingWave upsert failed, attempting reconnect")
            try:
                mirror_conn.close()
            except Exception:
                pass
            mirror_conn = connect_db(mirror_dsn)
            ensure_table(mirror_conn)
            upsert_topic_head(mirror_conn, topic, max_produced_at)

    return primary_conn, mirror_conn


def get_partition_tail_produced_at(consumer: Consumer, topic: str, partition_id: int) -> datetime | None:
    """Read the tail message of one topic partition and return produced_at.

    Args:
        consumer: Kafka consumer used for partition assignment and polling.
        topic: Topic name.
        partition_id: Partition index.

    Returns:
        Parsed tail produced_at or None.
    """
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10, cached=False)
    if high <= low:
        return None

    tail_offset = high - 1
    consumer.assign([TopicPartition(topic, partition_id, tail_offset)])

    deadline = time.time() + 5
    while time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            logger.debug("Tail poll error for %s[%s]: %s", topic, partition_id, msg.error())
            continue
        if msg.topic() == topic and msg.partition() == partition_id and msg.offset() == tail_offset:
            return parse_message_produced_at(msg.value())

    return None


def bootstrap_topic_heads(bootstrap_servers: str) -> dict[str, datetime]:
    """Bootstrap max produced_at from the current tail of each Kafka topic.

    Args:
        bootstrap_servers: Kafka bootstrap servers.

    Returns:
        Topic-to-max-produced_at map for topics where a value was found.
    """
    result: dict[str, datetime] = {}
    temp_group = f"kafka-head-bootstrap-{uuid4()}"
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": temp_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )

    try:
        for topic in TOPICS:
            md = consumer.list_topics(topic=topic, timeout=10)
            topic_md = md.topics.get(topic)
            if topic_md is None or topic_md.error is not None:
                logger.warning("Topic metadata unavailable for %s", topic)
                continue

            max_dt: datetime | None = None
            for partition_id in topic_md.partitions:
                dt = get_partition_tail_produced_at(consumer, topic, partition_id)
                if dt is None:
                    continue
                if max_dt is None or dt > max_dt:
                    max_dt = dt

            if max_dt is not None:
                result[topic] = max_dt
                logger.info("Bootstrapped %s max_produced_at=%s", topic, max_dt.isoformat())
    finally:
        consumer.close()

    return result


def main() -> int:
    """Main entry point."""
    configure_logging()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    # Primary persistence target: Postgres metadata DB in this compose stack.
    primary_dsn = os.getenv("OBSERVER_DB_DSN", "postgresql://postgres@postgres-0:5432/metadata")
    # Optional mirror for existing RisingWave-based Grafana panel compatibility.
    mirror_dsn = os.getenv("MIRROR_RISINGWAVE_DSN", "postgresql://root:root@frontend-node-0:4566/dev")
    mirror_enabled = os.getenv("MIRROR_RISINGWAVE_ENABLED", "false").lower() == "true"
    poll_timeout = float(os.getenv("KAFKA_OBSERVER_POLL_TIMEOUT", "1.0"))

    try:
        primary_conn = connect_db(primary_dsn)
    except Exception as exc:
        logger.exception("Failed to connect to primary observer DB: %s", exc)
        return EXIT_ERROR

    try:
        ensure_table(primary_conn)
    except Exception as exc:
        logger.exception("Failed to ensure kafka_head_produced_at table in primary DB: %s", exc)
        return EXIT_ERROR

    mirror_conn = None
    if mirror_enabled:
        try:
            mirror_conn = connect_db(mirror_dsn)
            ensure_table(mirror_conn)
        except Exception as exc:
            logger.warning("Could not initialize RisingWave mirror connection, continuing without mirror: %s", exc)
            mirror_conn = None

    topic_max: dict[str, datetime] = {}

    try:
        bootstrapped = bootstrap_topic_heads(bootstrap_servers)
        for topic, dt in bootstrapped.items():
            primary_conn, mirror_conn = persist_topic_head(
                primary_conn,
                primary_dsn,
                topic,
                dt,
                mirror_conn=mirror_conn,
                mirror_dsn=mirror_dsn if mirror_enabled else None,
            )
            topic_max[topic] = dt
    except Exception as exc:
        logger.warning("Bootstrap tail scan failed, continuing with live stream only: %s", exc)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "kafka-head-observer",
            "enable.auto.commit": True,
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(TOPICS)
    logger.info("Kafka head observer started for topics: %s", ", ".join(TOPICS))

    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            dt = parse_message_produced_at(msg.value())
            if dt is None:
                continue

            previous = topic_max.get(topic)
            if previous is None or dt > previous:
                topic_max[topic] = dt
                primary_conn, mirror_conn = persist_topic_head(
                    primary_conn,
                    primary_dsn,
                    topic,
                    dt,
                    mirror_conn=mirror_conn,
                    mirror_dsn=mirror_dsn if mirror_enabled else None,
                )
                logger.debug("Updated %s max_produced_at=%s", topic, dt.isoformat())

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except BrokenPipeError:
        return EXIT_FAILURE
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        return EXIT_FAILURE
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            primary_conn.close()
        except Exception:
            pass
        if mirror_conn is not None:
            try:
                mirror_conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
