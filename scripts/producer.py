#!/usr/bin/env python3
"""
Kafka Event Producer for RisingWave Demo

Generates synthetic events (page views, cart events, purchases) and sends them to Kafka.
"""
import json
import time
import random
import argparse
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

TOPICS = ['page_views', 'cart_events', 'purchases']


def get_timestamp():
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err:
        print(f'Delivery failed: {err}')


def main():
    parser = argparse.ArgumentParser(description="Kafka Event Producer with configurable TPS")
    parser.add_argument("--tps", type=float, default=1.0, help="Transactions per second (default: 1.0)")
    args = parser.parse_args()

    bootstrap_servers = 'localhost:19092'

    print("Starting producer pre-flight checks...")

    # 1. Connect and Check Kafka Connectivity
    try:
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'event-producer',
            'request.timeout.ms': 5000,
        }
        producer = Producer(conf)
    except KafkaException as e:
        print(f"❌ Error: Could not connect to Kafka at {bootstrap_servers}.")
        print(f"👉 Please make sure services are started with './bin/1_up.sh'")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: Unexpected error connecting to Kafka: {str(e)}")
        sys.exit(1)

    # 2. Check Topic Availability
    # Get metadata to check available topics
    try:
        metadata = producer.list_topics(timeout=5)
        available_topics = [t.topic for t in iter(metadata.topics.values())]
        
        for topic in TOPICS:
            if topic not in available_topics:
                print(f"❌ Error: Required topic '{topic}' does not exist.")
                print(f"👉 Please make sure initialization scripts have run (e.g., './bin/3_run_dbt.sh')")
                sys.exit(1)
    except Exception as e:
        print(f"❌ Error checking topics: {e}")
        sys.exit(1)

    print(f"✅ Pre-flight checks passed. Kafka is reachable and all topics exist.")
    print(f"Starting data generation at {args.tps} TPS... Press Ctrl+C to stop.")

    interval = 1.0 / args.tps if args.tps > 0 else 0
    
    # Counters and reporting state
    views_count = 0
    carts_count = 0
    purchases_count = 0
    last_report_time = time.time()

    # Track target time for precise TPS control
    target_time = time.time()
    
    try:
        while True:
            current_loop_time = time.time()
            
            # Check if it's time to report (every second)
            if current_loop_time - last_report_time >= 1.0:
                total_count = views_count + carts_count + purchases_count
                print(f"[{get_timestamp()}] Views: {views_count}, Carts: {carts_count}, Purchases: {purchases_count}, Total: {total_count}")
                # Reset counters
                views_count = 0
                carts_count = 0
                purchases_count = 0
                last_report_time = current_loop_time

            # Produce events if TPS > 0
            if interval > 0:
                user_id = random.randint(1, 1000000)
                event_time_str = get_timestamp()

                # 1. Page Views (Always)
                view_data = {
                    "user_id": user_id,
                    "page_id": f"page_{random.randint(1, 100)}",
                    "event_time": event_time_str
                }
                producer.produce(
                    'page_views',
                    value=json.dumps(view_data).encode('utf-8'),
                    callback=delivery_report
                )
                views_count += 1

                # 2. Add to Cart (Medium volume)
                # Only users who viewed can cart
                cart_created = False
                if random.random() < 0.3:
                    cart_data = {
                        "user_id": user_id,
                        "item_id": f"item_{random.randint(100, 1000)}",
                        "event_time": event_time_str
                    }
                    producer.produce(
                        'cart_events',
                        value=json.dumps(cart_data).encode('utf-8'),
                        callback=delivery_report
                    )
                    carts_count += 1
                    cart_created = True

                # 3. Purchases (Low volume)
                # Only users who carted can purchase
                if cart_created and random.random() < 0.33:
                    purchase_data = {
                        "user_id": user_id,
                        "amount": round(random.uniform(10, 500), 2),
                        "event_time": event_time_str
                    }
                    producer.produce(
                        'purchases',
                        value=json.dumps(purchase_data).encode('utf-8'),
                        callback=delivery_report
                    )
                    purchases_count += 1

                # Poll for delivery reports
                producer.poll(0)

                # Control execution rate using target time for precise TPS
                target_time += interval
                sleep_time = target_time - time.time()
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                # If TPS is 0, just sleep until next report
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping data generation.")
        producer.flush()


if __name__ == "__main__":
    main()
