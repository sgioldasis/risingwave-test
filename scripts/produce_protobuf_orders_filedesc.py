#!/usr/bin/env python3
"""Produce raw protobuf `Order` events into Redpanda — NO Schema Registry.

Companion to ``produce_protobuf_orders.py`` (which uses Confluent Schema
Registry). This variant exercises RisingWave's ``ENCODE PROTOBUF
(schema.location = 'file:///...')`` code path:

  1. Compile ``proto/events.proto`` -> ``scripts/_pb/events_pb2.py`` (for
     building messages in Python) AND a FileDescriptorSet at
     ``proto/events.pb`` (consumed by RisingWave at CREATE SOURCE time).
  2. Publish raw ``order.SerializeToString()`` bytes to Kafka. Unlike the SR
     variant there is no 5-byte Confluent magic-byte/schema-id prefix; the
     value is a plain protobuf wire-format payload.

Usage:
    uv run python scripts/produce_protobuf_orders_filedesc.py --count 50 --tps 5

Env overrides:
    KAFKA_BOOTSTRAP    default localhost:19092
    TOPIC              default orders_filedesc
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto"
GEN_DIR = REPO_ROOT / "scripts" / "_pb"
PROTO_FILE = PROTO_DIR / "events.proto"
GEN_PY = GEN_DIR / "events_pb2.py"
DESCRIPTOR_FILE = PROTO_DIR / "events.pb"  # FileDescriptorSet for RisingWave


def ensure_pb2_and_descriptor() -> None:
    """Compile both the Python module and the FileDescriptorSet."""
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    init_py = GEN_DIR / "__init__.py"
    if not init_py.exists():
        init_py.write_text("")

    proto_mtime = PROTO_FILE.stat().st_mtime

    if not GEN_PY.exists() or GEN_PY.stat().st_mtime < proto_mtime:
        print(f"[protoc] compiling {PROTO_FILE.relative_to(REPO_ROOT)} -> {GEN_PY.relative_to(REPO_ROOT)}")
        subprocess.run(
            [
                sys.executable, "-m", "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={GEN_DIR}",
                str(PROTO_FILE),
            ],
            check=True,
        )

    if not DESCRIPTOR_FILE.exists() or DESCRIPTOR_FILE.stat().st_mtime < proto_mtime:
        print(
            f"[protoc] building FileDescriptorSet "
            f"{PROTO_FILE.relative_to(REPO_ROOT)} -> {DESCRIPTOR_FILE.relative_to(REPO_ROOT)}"
        )
        # --include_imports pulls in google/protobuf/timestamp.proto, which
        # RisingWave needs to fully resolve `event_time`.
        subprocess.run(
            [
                sys.executable, "-m", "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--descriptor_set_out={DESCRIPTOR_FILE}",
                "--include_imports",
                str(PROTO_FILE),
            ],
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=50, help="Total messages to send (0 = run forever)")
    parser.add_argument("--tps", type=float, default=5.0, help="Throughput cap, messages per second")
    args = parser.parse_args()

    ensure_pb2_and_descriptor()

    # Make generated module importable, then reuse build_order() from the SR
    # variant so both demos send semantically identical payloads.
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from _pb import events_pb2 as pb  # type: ignore  # noqa: WPS433
    from produce_protobuf_orders import build_order  # type: ignore  # noqa: WPS433

    from confluent_kafka import Producer  # noqa: WPS433

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    topic = os.environ.get("TOPIC", "orders_filedesc")

    print(f"[producer] bootstrap={bootstrap}  topic={topic}  (raw protobuf, no SR)")

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
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
                key=order.order_id.encode("utf-8"),
                value=order.SerializeToString(),
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
