#!/usr/bin/env python3
"""Produce protobuf-encoded `Order` events into Redpanda for the RisingWave demo.

- Auto-compiles `proto/events.proto` -> `scripts/_pb/events_pb2.py` on first run
  (uses `grpc_tools.protoc`, which ships with `grpcio-tools`).
- Registers the schema under subject `orders-value` via Redpanda's Schema Registry
  (Confluent-compatible) using `ProtobufSerializer`.
- Emits nested orders (customer.address, repeated items, map attributes, oneof
  payment) so the RisingWave queries in sql/protobuf_demo.sql have something to
  chew on.

Usage:
    uv run python scripts/produce_protobuf_orders.py --count 50 --tps 5

Env overrides:
    KAFKA_BOOTSTRAP    default localhost:19092
    SCHEMA_REGISTRY    default http://localhost:8081
    TOPIC              default orders
"""
from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto"
GEN_DIR = REPO_ROOT / "scripts" / "_pb"
PROTO_FILE = PROTO_DIR / "events.proto"
GEN_FILE = GEN_DIR / "events_pb2.py"


def ensure_pb2() -> None:
    """Compile events.proto -> _pb/events_pb2.py if missing or stale."""
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    init_py = GEN_DIR / "__init__.py"
    if not init_py.exists():
        init_py.write_text("")

    if GEN_FILE.exists() and GEN_FILE.stat().st_mtime >= PROTO_FILE.stat().st_mtime:
        return

    print(f"[protoc] compiling {PROTO_FILE.relative_to(REPO_ROOT)} -> {GEN_FILE.relative_to(REPO_ROOT)}")
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={GEN_DIR}",
        str(PROTO_FILE),
    ]
    subprocess.run(cmd, check=True)


def build_order(pb, i: int):
    """Build a richly-nested Order proto message."""
    from google.protobuf.timestamp_pb2 import Timestamp  # noqa: WPS433

    customer = pb.Customer(
        id=1000 + (i % 25),
        email=f"user{i % 25}@example.com",
        name=random.choice(["Alice", "Bob", "Carol", "Dan", "Eve", "Frank"]),
        is_vip=(i % 7 == 0),
        address=pb.Address(
            street=f"{random.randint(1, 999)} Main St",
            city=random.choice(["Berlin", "Paris", "Tokyo", "NYC", "Sydney"]),
            country=random.choice(["DE", "FR", "JP", "US", "AU"]),
            postal_code=f"{random.randint(10000, 99999)}",
        ),
    )

    categories = ["books", "electronics", "apparel", "grocery"]
    items = []
    for _ in range(random.randint(1, 4)):
        cat = random.choice(categories)
        items.append(
            pb.LineItem(
                product=pb.Product(
                    id=f"SKU-{random.randint(1000, 9999)}",
                    name=f"{cat.title()} item {random.randint(1, 99)}",
                    category=cat,
                ),
                quantity=random.randint(1, 5),
                unit_price=round(random.uniform(5.0, 250.0), 2),
                discount_pct=random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 20.0]),
            )
        )

    total = round(
        sum(it.quantity * it.unit_price * (1 - it.discount_pct / 100.0) for it in items),
        2,
    )

    ts = Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))

    order = pb.Order(
        order_id=f"ord-{int(time.time()*1000)}-{i:05d}",
        customer=customer,
        items=items,
        status=random.choice(
            [
                pb.ORDER_STATUS_PENDING,
                pb.ORDER_STATUS_PAID,
                pb.ORDER_STATUS_PAID,
                pb.ORDER_STATUS_SHIPPED,
                pb.ORDER_STATUS_CANCELLED,
                # --- schema-evolution demo: uncomment on the 2nd run ---
                # pb.ORDER_STATUS_REFUNDED,
            ]
        ),
        event_time=ts,
        total=total,
        currency=random.choice(["USD", "EUR", "JPY"]),
        # --- schema-evolution demo: uncomment on the 2nd run ---
        # shipping_method=random.choice(["standard", "standard", "express", "pickup"]),
    )

    # map<string, string>
    order.attributes["source"] = random.choice(["web", "mobile", "kiosk"])
    order.attributes["campaign"] = random.choice(["none", "spring_sale", "newsletter"])
    if customer.is_vip:
        order.attributes["tier"] = "gold"

    # oneof payment
    choice = random.choice(["card", "wallet", "crypto"])
    if choice == "card":
        order.card.CopyFrom(
            pb.CardPayment(
                brand=random.choice(["visa", "mastercard", "amex"]),
                last4=f"{random.randint(0, 9999):04d}",
                holder=customer.name,
            )
        )
    elif choice == "wallet":
        order.wallet.CopyFrom(
            pb.WalletPayment(
                provider=random.choice(["apple_pay", "google_pay", "paypal"]),
                wallet_id=f"w-{random.randint(10**9, 10**10 - 1)}",
            )
        )
    else:
        order.crypto.CopyFrom(
            pb.CryptoPayment(
                chain=random.choice(["eth", "btc", "sol"]),
                tx_hash=f"0x{random.randint(0, 16**16 - 1):016x}",
            )
        )

    return order


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=50, help="Total messages to send (0 = run forever)")
    parser.add_argument("--tps", type=float, default=5.0, help="Throughput cap, messages per second")
    args = parser.parse_args()

    ensure_pb2()

    # Make generated module importable.
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from _pb import events_pb2 as pb  # type: ignore  # noqa: WPS433

    from confluent_kafka import SerializingProducer  # noqa: WPS433
    from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: WPS433
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer  # noqa: WPS433
    from confluent_kafka.serialization import StringSerializer  # noqa: WPS433

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    sr_url = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
    topic = os.environ.get("TOPIC", "orders")

    print(f"[producer] bootstrap={bootstrap}  schema_registry={sr_url}  topic={topic}")

    sr_client = SchemaRegistryClient({"url": sr_url})
    proto_serializer = ProtobufSerializer(
        pb.Order,
        sr_client,
        {"use.deprecated.format": False},
    )
    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": proto_serializer,
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
            order = build_order(pb, i)
            producer.produce(
                topic=topic,
                key=order.order_id,
                value=order,
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            i += 1
            if sent % 25 == 0:
                print(f"[producer] sent {sent} orders (last total={order.total} {order.currency})")
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
