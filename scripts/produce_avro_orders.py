#!/usr/bin/env python3
"""Produce Avro-encoded `Order` events into Redpanda for the RisingWave demo.

Companion to ``scripts/produce_protobuf_orders.py`` — same payload shape, same
topic ergonomics, but Avro on the wire.

- Loads the Avro schema from ``avro/orders.avsc``.
- Registers the schema under subject ``orders_avro-value`` via Redpanda's
  Schema Registry using ``AvroSerializer`` (Confluent wire format: magic byte
  + 4-byte schema id + Avro body).
- Emits nested orders (customer.address, repeated items, map attributes, union
  payment, logical-type timestamp) so the queries in ``sql/avro_demo.sql`` have
  something to chew on.

Usage:
    uv run python scripts/produce_avro_orders.py --count 50 --tps 5

Env overrides:
    KAFKA_BOOTSTRAP    default localhost:19092
    SCHEMA_REGISTRY    default http://localhost:8081
    TOPIC              default orders_avro
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
AVRO_FILE = REPO_ROOT / "avro" / "orders.avsc"


def build_order(i: int) -> dict:
    """Build a richly-nested Order dict matching the Avro schema."""
    name = random.choice(["Alice", "Bob", "Carol", "Dan", "Eve", "Frank"])
    is_vip = (i % 7 == 0)

    customer = {
        "id": 1000 + (i % 25),
        "email": f"user{i % 25}@example.com",
        "name": name,
        "is_vip": is_vip,
        "address": {
            "street": f"{random.randint(1, 999)} Main St",
            "city": random.choice(["Berlin", "Paris", "Tokyo", "NYC", "Sydney"]),
            "country": random.choice(["DE", "FR", "JP", "US", "AU"]),
            "postal_code": f"{random.randint(10000, 99999)}",
        },
    }

    categories = ["books", "electronics", "apparel", "grocery"]
    items = []
    for _ in range(random.randint(1, 4)):
        cat = random.choice(categories)
        items.append(
            {
                "product": {
                    "id": f"SKU-{random.randint(1000, 9999)}",
                    "name": f"{cat.title()} item {random.randint(1, 99)}",
                    "category": cat,
                },
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(5.0, 250.0), 2),
                "discount_pct": random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 20.0]),
            }
        )

    total = round(
        sum(it["quantity"] * it["unit_price"] * (1 - it["discount_pct"] / 100.0) for it in items),
        2,
    )

    attributes = {
        "source": random.choice(["web", "mobile", "kiosk"]),
        "campaign": random.choice(["none", "spring_sale", "newsletter"]),
    }
    if is_vip:
        attributes["tier"] = "gold"

    # Protobuf-oneof analogue: a `Payment` record with three nullable
    # sub-records. RisingWave doesn't yet support Avro unions of named record
    # types (issue #17632), so we set exactly one sub-field per row and leave
    # the other two as null.
    payment = {"card": None, "wallet": None, "crypto": None}
    choice = random.choice(["card", "wallet", "crypto"])
    if choice == "card":
        payment["card"] = {
            "brand": random.choice(["visa", "mastercard", "amex"]),
            "last4": f"{random.randint(0, 9999):04d}",
            "holder": name,
        }
    elif choice == "wallet":
        payment["wallet"] = {
            "provider": random.choice(["apple_pay", "google_pay", "paypal"]),
            "wallet_id": f"w-{random.randint(10**9, 10**10 - 1)}",
        }
    else:
        payment["crypto"] = {
            "chain": random.choice(["eth", "btc", "sol"]),
            "tx_hash": f"0x{random.randint(0, 16**16 - 1):016x}",
        }

    return {
        "order_id": f"ord-{int(time.time()*1000)}-{i:05d}",
        "customer": customer,
        "items": items,
        "attributes": attributes,
        "status": random.choice(
            [
                "ORDER_STATUS_PENDING",
                "ORDER_STATUS_PAID",
                "ORDER_STATUS_PAID",
                "ORDER_STATUS_SHIPPED",
                "ORDER_STATUS_CANCELLED",
            ]
        ),
        # Avro logicalType timestamp-millis → epoch millis (datetime also works
        # with fastavro, but ints sidestep tz ambiguity).
        "event_time": datetime.now(timezone.utc),
        "total": total,
        "currency": random.choice(["USD", "EUR", "JPY"]),
        "payment": payment,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=50, help="Total messages to send (0 = run forever)")
    parser.add_argument("--tps", type=float, default=5.0, help="Throughput cap, messages per second")
    args = parser.parse_args()

    from confluent_kafka import SerializingProducer  # noqa: WPS433
    from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: WPS433
    from confluent_kafka.schema_registry.avro import AvroSerializer  # noqa: WPS433
    from confluent_kafka.serialization import StringSerializer  # noqa: WPS433

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    sr_url = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
    topic = os.environ.get("TOPIC", "orders_avro")

    print(f"[producer] bootstrap={bootstrap}  schema_registry={sr_url}  topic={topic}")

    schema_str = AVRO_FILE.read_text()
    # Validate it parses before handing to SR.
    json.loads(schema_str)

    sr_client = SchemaRegistryClient({"url": sr_url})

    # to_dict: the serializer hands us each value and asks for a plain dict;
    # `_obj` is already a dict here, but the indirection lets us hook custom
    # objects if needed.
    def to_dict(obj, ctx):
        return obj

    avro_serializer = AvroSerializer(
        sr_client,
        schema_str,
        to_dict=to_dict,
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "linger.ms": 50,
        }
    )

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"[producer] delivery FAILED: {err}", file=sys.stderr)

    interval = 1.0 / args.tps if args.tps > 0 else 0
    sent = 0
    i = 0
    try:
        while args.count == 0 or sent < args.count:
            order = build_order(i)
            producer.produce(
                topic=topic,
                key=order["order_id"],
                value=order,
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            i += 1
            if sent % 25 == 0:
                print(f"[producer] sent {sent} orders (last total={order['total']} {order['currency']})")
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[producer] interrupted")
    finally:
        print("[producer] flushing...")
        producer.flush(10)
        print(f"[producer] done, total sent = {sent}")


if __name__ == "__main__":
    main()
