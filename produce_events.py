#!/usr/bin/env python3

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Producer


def get_random_device():
    """Generates a random device type for an event."""
    return random.choice(["mobile", "desktop", "tablet"])


def generate_message(user_id: int, schema_version: int) -> dict:
    """Generates a single user event message based on the schema version."""
    event_types = ["login", "click", "view_page", "add_to_cart", "logout", "purchase"]

    # Base message for all schema versions
    message = {
        "user_id": user_id,
        "event_type": random.choice(event_types),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Add fields specific to schema version 2
    if schema_version == 2:
        message["device_type"] = get_random_device()

    return message


def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


def main():
    """Main function to parse arguments and produce messages."""
    parser = argparse.ArgumentParser(
        description="Produce JSON messages to the 'user_events' Redpanda topic."
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=1,
        help="Number of messages to produce (default: 1).",
    )
    parser.add_argument(
        "--schema_version",
        type=int,
        default=1,
        choices=[1, 2],
        help="Schema version of the message to produce (1 or 2, default: 1).",
    )
    args = parser.parse_args()

    # Kafka configuration
    kafka_topic = "user_events"
    kafka_bootstrap_servers = "localhost:19092"

    try:
        # Create a Kafka producer
        conf = {
            "bootstrap.servers": kafka_bootstrap_servers,
            "client.id": "event-producer",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
        }
        producer = Producer(conf)

        print(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
        print(
            f"Producing {args.count} message(s) of schema version {args.schema_version} to topic '{kafka_topic}'...\n"
        )

        for i in range(1, args.count + 1):
            # Pass the schema_version to the generate_message function
            message = generate_message(i, args.schema_version)
            print(f"-> Sending: {message}")

            # Send the message to the topic
            producer.produce(
                kafka_topic,
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  # Trigger delivery reports

            # Optional: add a small delay between messages
            time.sleep(0.5)

        print("\nAll messages sent. Flushing producer...")
        # Ensure all buffered messages are sent before exiting
        producer.flush()
        print("Done.")

    except KafkaError as e:
        print(
            f"Error: Could not connect to Kafka. Is Redpanda running? Details: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
