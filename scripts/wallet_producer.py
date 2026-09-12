#!/usr/bin/env python3
"""
Synthetic Wallet Transaction Producer (StarRocks Upsert Demo)

Emits synthetic wallet transactions to the 'wallet_transactions' Kafka
topic, then a short time later emits a reversal event carrying the SAME
transaction_id -- this is the mechanism the demo exists to show: a StarRocks
Primary Key table sink should reflect the reversed state via upsert, not
duplicate the row. All data is synthetic (no real accounts/amounts/PII).

See docs/SR_POC_WALLET_UPSERT_DEMO.md.
"""
import json
import time
import random
import argparse
import sys
import uuid
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

TOPIC = 'wallet_transactions'
TYPES = ['bet', 'win', 'deposit']


def get_timestamp():
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')


def main():
    parser = argparse.ArgumentParser(description="Synthetic wallet transaction producer with reversal simulation")
    parser.add_argument("--tps", type=float, default=1.0, help="Transactions per second (default: 1.0)")
    parser.add_argument("--reversal-delay", type=float, default=5.0,
                         help="Seconds after a transaction before its reversal is emitted (default: 5.0)")
    parser.add_argument("--reversal-rate", type=float, default=0.2,
                         help="Fraction of transactions that get reversed (default: 0.2)")
    args = parser.parse_args()

    bootstrap_servers = 'localhost:19092'

    print("Starting wallet producer pre-flight checks...")
    try:
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'wallet-producer',
            'request.timeout.ms': 5000,
        }
        producer = Producer(conf)
    except KafkaException as e:
        print(f"❌ Error: Could not connect to Kafka at {bootstrap_servers}.")
        print(f"👉 Please make sure services are started with './bin/1_up.sh'")
        sys.exit(1)

    try:
        metadata = producer.list_topics(timeout=5)
        available_topics = [t.topic for t in iter(metadata.topics.values())]
        if TOPIC not in available_topics:
            print(f"❌ Error: Topic '{TOPIC}' does not exist.")
            print(f"👉 Please make sure services are started with './bin/1_up.sh' (creates it via redpanda-init)")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error checking topics: {e}")
        sys.exit(1)

    print("✅ Pre-flight checks passed. Kafka is reachable and topic exists.")
    print(f"Starting wallet transaction generation at {args.tps} TPS "
          f"(reversal_rate={args.reversal_rate}, reversal_delay={args.reversal_delay}s)... Press Ctrl+C to stop.")

    interval = 1.0 / args.tps if args.tps > 0 else 1.0
    target_time = time.time()

    # Pending reversals: (fire_at_epoch_seconds, event_dict)
    pending_reversals = deque()
    tx_count = 0
    reversal_count = 0

    def emit(event):
        producer.produce(TOPIC, value=json.dumps(event).encode('utf-8'), callback=delivery_report)

    try:
        while True:
            now = time.time()

            # Fire any due reversals first, regardless of TPS pacing.
            while pending_reversals and pending_reversals[0][0] <= now:
                _, rev_event = pending_reversals.popleft()
                rev_event["event_time"] = get_timestamp()
                emit(rev_event)
                reversal_count += 1
                print(f"[{get_timestamp()}] REVERSAL transaction_id={rev_event['transaction_id']}")

            if args.tps > 0:
                account_id = f"acct_{random.randint(1, 50)}"
                transaction_id = str(uuid.uuid4())
                event_time = get_timestamp()
                amount = round(random.uniform(1, 200), 2)
                tx_type = random.choice(TYPES)

                tx_event = {
                    "transaction_id": transaction_id,
                    "account_id": account_id,
                    "type": tx_type,
                    "amount": amount,
                    "status": "settled",
                    "event_time": event_time,
                }
                emit(tx_event)
                tx_count += 1

                if random.random() < args.reversal_rate:
                    # Carries the NEGATIVE of the original transaction's
                    # amount -- a reversal removes that value, it doesn't
                    # repeat it. Since this is a StarRocks Primary Key
                    # table, the reversal upserts over the original row, so
                    # this is the only place the original amount's
                    # magnitude survives past the reversal, and the sign
                    # flip makes SUM(amount) net out correctly (settled +X,
                    # reversed -X) instead of double-counting +X twice.
                    reversal_event = {
                        "transaction_id": transaction_id,
                        "account_id": account_id,
                        "type": "reversal",
                        "amount": -amount,
                        "status": "reversed",
                        "event_time": None,  # set at emit time below
                    }
                    pending_reversals.append((now + args.reversal_delay, reversal_event))

                if tx_count % 20 == 0:
                    print(f"[{get_timestamp()}] Transactions: {tx_count}, Reversals: {reversal_count}, "
                          f"Pending: {len(pending_reversals)}")

                producer.poll(0)

                target_time += interval
                now2 = time.time()
                sleep_time = target_time - now2
                if sleep_time > 0:
                    time.sleep(sleep_time)
                elif -sleep_time > max(interval * 5, 1.0):
                    print(f"[{get_timestamp()}] ⚠️  Detected {-sleep_time:.1f}s lag "
                          f"(likely host/container suspend); resyncing pacing.")
                    target_time = now2
            else:
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping wallet transaction generation.")
        producer.flush()


if __name__ == "__main__":
    main()
