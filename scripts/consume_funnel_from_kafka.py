#!/usr/bin/env python3
"""
Test script to consume funnel data from Kafka topic.
Usage: python scripts/consume_funnel_from_kafka.py
"""

import json
import sys

from confluent_kafka import Consumer, KafkaException


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

    print("Consuming from 'funnel' topic...")
    print("Press Ctrl+C to stop")
    print("-" * 60)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Deserialize the message value
            data = json.loads(msg.value().decode('utf-8'))

            print(f"\nReceived at {msg.timestamp()}:")
            print(f"  Window: {data.get('window_start')} → {data.get('window_end')}")
            print(f"  Viewers: {data.get('viewers')}")
            print(f"  Carters: {data.get('carters')}")
            print(f"  Purchasers: {data.get('purchasers')}")
            print(f"  View→Cart Rate: {data.get('view_to_cart_rate')}")
            print(f"  Cart→Buy Rate: {data.get('cart_to_buy_rate')}")
            print("-" * 60)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_funnel()
