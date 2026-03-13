#!/usr/bin/env python3
"""
Diagnostic script to check the entire data pipeline.
"""
import json
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_kafka_connection():
    """Check if Kafka is accessible."""
    logger.info("=" * 60)
    logger.info("1. Checking Kafka Connection...")
    logger.info("=" * 60)
    try:
        from confluent_kafka import Consumer, KafkaException

        conf = {
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'check-pipeline',
            'auto.offset.reset': 'latest',
        }
        consumer = Consumer(conf)
        
        # Get cluster metadata (includes topics)
        metadata = consumer.list_topics(timeout=5)
        topics = [t.topic for t in iter(metadata.topics.values())]
        
        logger.info(f"✅ Kafka is reachable")
        logger.info(f"   Available topics: {sorted(topics) if topics else 'None'}")

        if 'funnel' in topics:
            logger.info(f"✅ 'funnel' topic exists")
        else:
            logger.warning(f"❌ 'funnel' topic NOT found")
        consumer.close()
        return True
    except KafkaException as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        return False


def check_dashboard_api():
    """Check if dashboard API is serving data."""
    logger.info("\n" + "=" * 60)
    logger.info("2. Checking Dashboard API...")
    logger.info("=" * 60)
    try:
        import requests

        # Check funnel endpoint
        try:
            resp = requests.get('http://localhost:8000/api/funnel', timeout=5)
            logger.info(f"   /api/funnel status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    logger.info(f"✅ Got {len(data)} funnel records")
                    if data:
                        logger.info(f"   Latest record: {data[-1]}")
                else:
                    logger.warning(f"⚠️  Unexpected response: {data}")
        except Exception as e:
            logger.error(f"❌ /api/funnel failed: {e}")

        # Check stats endpoint
        try:
            resp = requests.get('http://localhost:8000/api/stats', timeout=5)
            logger.info(f"   /api/stats status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"   Stats: {json.dumps(data, indent=2)}")
        except Exception as e:
            logger.error(f"❌ /api/stats failed: {e}")

    except Exception as e:
        logger.error(f"❌ Dashboard API check failed: {e}")


def check_risingwave():
    """Check if RisingWave has funnel data."""
    logger.info("\n" + "=" * 60)
    logger.info("3. Checking RisingWave...")
    logger.info("=" * 60)
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://root:root@localhost:4566/dev")
        with engine.connect() as conn:
            # Check funnel MV
            result = conn.execute(text("SELECT COUNT(*) FROM funnel"))
            count = result.fetchone()[0]
            logger.info(f"✅ Funnel MV has {count} records")

            # Check sinks
            result = conn.execute(text("SHOW SINKS"))
            sinks = result.fetchall()
            logger.info(f"   Available sinks: {[s[0] for s in sinks]}")

            # Check if funnel_kafka_sink exists
            if any('funnel_kafka' in str(s) for s in sinks):
                logger.info(f"✅ funnel_kafka_sink exists")
            else:
                logger.warning(f"❌ funnel_kafka_sink NOT found")

    except Exception as e:
        logger.error(f"❌ RisingWave check failed: {e}")


def main():
    logger.info("\n🔍 Pipeline Diagnostic Tool\n")

    check_kafka_connection()
    check_dashboard_api()
    check_risingwave()

    logger.info("\n" + "=" * 60)
    logger.info("Diagnostic complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
