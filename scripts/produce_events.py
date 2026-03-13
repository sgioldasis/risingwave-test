#!/usr/bin/env python3

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Producer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}]")


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

        logger.info(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
        logger.info(
            f"Producing {args.count} message(s) of schema version {args.schema_version} to topic '{kafka_topic}'..."
        )

        for i in range(1, args.count + 1):
            # Pass the schema_version to the generate_message function
            message = generate_message(i, args.schema_version)
            logger.info(f"-> Sending: {message}")

            # Send the message to the topic
            producer.produce(
                kafka_topic,
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  # Trigger delivery reports

            # Optional: add a small delay between messages
            time.sleep(0.1)

        logger.info("All messages sent. Flushing producer...")
        # Ensure all buffered messages are sent before exiting
        producer.flush()
        logger.info("Done.")

    except KafkaError as e:
        logger.error(
            f"Error: Could not connect to Kafka. Is Redpanda running? Details: {e}"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
