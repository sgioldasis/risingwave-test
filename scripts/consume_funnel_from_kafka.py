#!/usr/bin/env python3
"""
Test script to consume funnel data from Kafka topic.
Usage: python scripts/consume_funnel_from_kafka.py
"""

import json
import logging
import sys

from confluent_kafka import Consumer, KafkaException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def consume_funnel():
    """Consume funnel data from Kafka and print to console."""
    conf = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'test-consumer',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
    }

    consumer = Consumer(conf)
    consumer.subscribe(['funnel'])

    logger.info("Consuming from 'funnel' topic...")
    logger.info("Press Ctrl+C to stop")
    logger.info("-" * 60)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # Deserialize the message value
            data = json.loads(msg.value().decode('utf-8'))

            logger.info(f"\nReceived at {msg.timestamp()}:")
            logger.info(f"  Window: {data.get('window_start')} → {data.get('window_end')}")
            logger.info(f"  Viewers: {data.get('viewers')}")
            logger.info(f"  Carters: {data.get('carters')}")
            logger.info(f"  Purchasers: {data.get('purchasers')}")
            logger.info(f"  View→Cart Rate: {data.get('view_to_cart_rate')}")
            logger.info(f"  Cart→Buy Rate: {data.get('cart_to_buy_rate')}")
            logger.info("-" * 60)
    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_funnel()
