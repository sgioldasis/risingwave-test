#!/usr/bin/env python3
"""
Test script to consume funnel data from Kafka topic.
Usage: python scripts/consume_funnel_from_kafka.py
"""

from kafka import KafkaConsumer
import json
import sys

def consume_funnel():
    """Consume funnel data from Kafka and print to console."""
    consumer = KafkaConsumer(
        'funnel',
        bootstrap_servers=['localhost:19092'],  # External port for host access
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='test-consumer'
    )
    
    print("Consuming from 'funnel' topic...")
    print("Press Ctrl+C to stop")
    print("-" * 60)
    
    try:
        for message in consumer:
            data = message.value
            print(f"\nReceived at {message.timestamp}:")
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
