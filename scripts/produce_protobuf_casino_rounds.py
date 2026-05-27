#!/usr/bin/env python3
"""Produce protobuf-encoded `CasinoRoundInfoDto` events into Kafka/Redpanda.

Mirrors scripts/produce_protobuf_orders.py but for the Cronus casino schema in
proto/casinoroundinfodto.proto.

- Auto-compiles proto/casinoroundinfodto.proto -> scripts/_pb/casinoroundinfodto_pb2.py.
- Registers the schema under subject `<topic>-value` via a
  Confluent-compatible Schema Registry endpoint (works against Redpanda's SR
  locally and against Apicurio's `/apis/ccompat/v7/` endpoint in staging/prod).
- Emits realistic nested CasinoRoundInfoDto rounds with multiple Messages, each
  carrying multiple Transactions, so the demo SQL can exercise nested struct
  access + double UNNEST + map-less aggregations.
- Writes Confluent magic-byte wire format (5-byte id prefix + varint message
  index), which RisingWave can decode.

Usage:
    uv run python scripts/produce_protobuf_casino_rounds.py --count 200 --tps 0

Local (Redpanda) defaults:
    KAFKA_BOOTSTRAP            localhost:19092
    SCHEMA_REGISTRY            http://localhost:8081
    TOPIC                      casino_rounds

Apicurio staging example (point at ccompat endpoint, reuse existing artifact):
    KAFKA_BOOTSTRAP=<kaizen-bootstrap>:9092 \\
    SCHEMA_REGISTRY=http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7 \\
    SCHEMA_SUBJECT=bigdata:casinoroundinfo \\
    SCHEMA_AUTO_REGISTER=false \\
    TOPIC=casino_rounds \\
    uv run python scripts/produce_protobuf_casino_rounds.py --count 100 --tps 5

Env reference:
    KAFKA_BOOTSTRAP            Kafka bootstrap server(s).
    SCHEMA_REGISTRY            Schema-Registry-compatible URL.
                               For Apicurio MUST end in /apis/ccompat/v7 (or v6).
    SCHEMA_REGISTRY_USERNAME   Basic-auth username (optional).
    SCHEMA_REGISTRY_PASSWORD   Basic-auth password (optional).
    SCHEMA_SUBJECT             Override subject lookup name.
                               Default: <topic>-value (Confluent TopicNameStrategy).
                               For Apicurio with a non-default group, use
                               '<groupId>:<artifactId>' if ccompat is configured
                               with legacy-id-mode=false. E.g. 'bigdata:casinoroundinfo'.
    SCHEMA_AUTO_REGISTER       'true' (default) or 'false'. Set false in
                               environments where you pre-register schemas
                               and want the producer to only look up by hash.
    KAFKA_SECURITY_PROTOCOL    e.g. SASL_SSL (optional).
    KAFKA_SASL_MECHANISM       e.g. PLAIN, SCRAM-SHA-512 (optional).
    KAFKA_SASL_USERNAME        SASL username (optional).
    KAFKA_SASL_PASSWORD        SASL password (optional).
    TOPIC                      Kafka topic. Default: casino_rounds.
"""
from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto"
GEN_DIR = REPO_ROOT / "scripts" / "_pb"
PROTO_FILE = PROTO_DIR / "casinoroundinfodto.proto"
GEN_FILE = GEN_DIR / "casinoroundinfodto_pb2.py"


def ensure_pb2() -> None:
    """Compile casinoroundinfodto.proto -> _pb/casinoroundinfodto_pb2.py if stale."""
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


# Small lookup pools — keep cardinality low so aggregations actually aggregate.
COMPANIES = [1, 2, 3, 7, 12]
PROVIDERS = [101, 102, 103, 104]
EXT_PROVIDERS = [9001, 9002, 9003]
GAME_TYPES = [1, 2, 3]                       # 1=slot, 2=live, 3=table (illustrative)
PROVIDER_GAME_CODES = [f"GAME_{n:03d}" for n in range(1, 11)]
PROVIDER_TABLE_CODES = ["", "TABLE_A", "TABLE_B", "TABLE_C"]
CURRENCIES = [1, 2, 3, 4]                    # 1=EUR 2=USD 3=GBP 4=JPY (illustrative)
MESSAGE_TYPES = [10, 20, 30]                 # bet / win / refund
TOKEN_TYPES = [0, 1, 2]
SOURCES = [1, 2]
BONUS_ACTIONS = ["", "WAGER", "RELEASE", "FORFEIT"]


def _amount(lo: float = 0.1, hi: float = 500.0) -> str:
    """Decimal amounts are strings in this schema."""
    return f"{random.uniform(lo, hi):.4f}"


def _ts_pb(when: datetime):
    from google.protobuf.timestamp_pb2 import Timestamp  # noqa: WPS433
    ts = Timestamp()
    ts.FromDatetime(when)
    return ts


def build_transaction(pb, when: datetime, idx: int):
    return pb.TransactionInformation(
        TransactionId=random.randint(10**12, 10**13 - 1),
        Created=_ts_pb(when),
        CurrencyId=random.choice(CURRENCIES),
        AccountId=random.randint(10**6, 10**7 - 1),
        BaseBalanceBefore=_amount(0.0, 5000.0),
        BonusBalanceBefore=_amount(0.0, 200.0),
        Amount=_amount(),
        CurrencyRateToEuro=f"{random.uniform(0.5, 1.5):.6f}",
        TokenAmount=_amount(0.0, 50.0),
        CustomerCampaignId=random.choice([0, 0, 0, 555, 777]),
        CampaignId=random.choice([0, 0, 100, 200]),
        CampaignTypeId=random.choice([0, 1, 2]),
        PandoraJourneyId=random.randint(0, 10**12),
        SourceId=random.choice(SOURCES),
        BonusAction=random.choice(BONUS_ACTIONS),
        # optionals — set ~30% of the time
        **(
            {"IsDepositFromToken": bool(random.getrandbits(1))}
            if random.random() < 0.3 else {}
        ),
        **(
            {"TransactionTypeId": random.choice([1, 2, 3])}
            if random.random() < 0.3 else {}
        ),
    )


def build_message(pb, base_time: datetime, msg_idx: int, total_msgs: int):
    when = base_time + timedelta(seconds=msg_idx)
    is_last = msg_idx == total_msgs - 1
    n_tx = random.randint(1, 3)
    msg = pb.CasinoMessageInformation(
        MessageId=random.randint(10**11, 10**12 - 1),
        TransactionRef=str(uuid.uuid4()),
        SessionId=str(uuid.uuid4()),
        MessageTypeId=random.choice(MESSAGE_TYPES),
        TokenTypeId=random.choice(TOKEN_TYPES),
        ProviderBonusTokenCode=random.choice(["", "TKN_A", "TKN_B"]),
        Created=_ts_pb(when),
        JackpotWinAmount=_amount(0.0, 10.0),
        JackpotContributionAmount=_amount(0.0, 1.0),
        JackpotInfo="",
        IsRoundClosed=is_last,
        Transactions=[build_transaction(pb, when, i) for i in range(n_tx)],
    )
    # Sprinkle some optionals.
    if random.random() < 0.2:
        msg.CommissionAmount = _amount(0.0, 5.0)
    if random.random() < 0.1:
        msg.P2PGameMode = random.choice([1, 2])
    return msg


def build_round(pb, i: int):
    company_id = random.choice(COMPANIES)
    provider_id = random.choice(PROVIDERS)
    ext_provider = random.choice(EXT_PROVIDERS)
    game_type = random.choice(GAME_TYPES)
    is_live = game_type == 2

    round_created = datetime.now(timezone.utc) - timedelta(seconds=random.randint(1, 300))
    n_messages = random.randint(1, 4)
    round_ended = round_created + timedelta(seconds=n_messages)

    game_info = pb.GameInformation(
        GameId=random.randint(1, 50),
        ProviderGameCode=random.choice(PROVIDER_GAME_CODES),
        IsLive=is_live,
        ProviderTableCode=random.choice(PROVIDER_TABLE_CODES),
        GameType=game_type,
    )
    if random.random() < 0.3:
        game_info.IsJackpotContributionsFromOperator = bool(random.getrandbits(1))

    round_info = pb.RoundInformation(
        GameRoundRef=f"GR-{uuid.uuid4()}",
        RoundCreated=_ts_pb(round_created),
        RoundEnded=_ts_pb(round_ended),
        Messages=[
            build_message(pb, round_created, m, n_messages) for m in range(n_messages)
        ],
    )

    dto = pb.CasinoRoundInfoDto(
        UniqueId=str(uuid.uuid4()),
        CustomerId=random.randint(10**5, 10**6 - 1),
        CompanyId=company_id,
        CasinoProviderId=provider_id,
        ExternalProviderId=ext_provider,
        GameInfo=game_info,
        RoundInfo=round_info,
        IsBonusLockedOnFatMessageCreation=bool(random.getrandbits(1)),
    )
    if random.random() < 0.3:
        dto.IsBonusCampaignWagering = bool(random.getrandbits(1))
    return dto


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=200, help="Total messages to send (0 = run forever)")
    parser.add_argument("--tps", type=float, default=0.0, help="Throughput cap (msgs/sec); 0 = unlimited")
    args = parser.parse_args()

    ensure_pb2()

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from _pb import casinoroundinfodto_pb2 as pb  # type: ignore  # noqa: WPS433

    from confluent_kafka import SerializingProducer  # noqa: WPS433
    from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: WPS433
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer  # noqa: WPS433
    from confluent_kafka.serialization import StringSerializer  # noqa: WPS433

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    sr_url = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
    topic = os.environ.get("TOPIC", "casino_rounds")
    sr_user = os.environ.get("SCHEMA_REGISTRY_USERNAME")
    sr_pass = os.environ.get("SCHEMA_REGISTRY_PASSWORD")
    subject_override = os.environ.get("SCHEMA_SUBJECT")
    auto_register = os.environ.get("SCHEMA_AUTO_REGISTER", "true").lower() != "false"

    effective_subject = subject_override or f"{topic}-value"
    print(
        f"[producer] bootstrap={bootstrap}  schema_registry={sr_url}  topic={topic}\n"
        f"           subject={effective_subject!r}  auto_register={auto_register}"
    )

    sr_conf: dict[str, object] = {"url": sr_url}
    if sr_user and sr_pass:
        sr_conf["basic.auth.user.info"] = f"{sr_user}:{sr_pass}"
    sr_client = SchemaRegistryClient(sr_conf)

    def _fixed_subject(_topic, _record):  # noqa: WPS430
        return effective_subject

    proto_serializer = ProtobufSerializer(
        pb.CasinoRoundInfoDto,
        sr_client,
        {
            "use.deprecated.format": False,
            "auto.register.schemas": auto_register,
            # `use.latest.version` is mutually exclusive with auto-register; when
            # auto_register is False we want the serializer to look up the
            # latest registered schema by subject instead of trying to register.
            "use.latest.version": not auto_register,
            "subject.name.strategy": _fixed_subject,
        },
    )

    kafka_conf: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": proto_serializer,
        "linger.ms": 50,
    }
    sec_proto = os.environ.get("KAFKA_SECURITY_PROTOCOL")
    if sec_proto:
        kafka_conf["security.protocol"] = sec_proto
    sasl_mech = os.environ.get("KAFKA_SASL_MECHANISM")
    if sasl_mech:
        kafka_conf["sasl.mechanism"] = sasl_mech
    sasl_user = os.environ.get("KAFKA_SASL_USERNAME")
    if sasl_user:
        kafka_conf["sasl.username"] = sasl_user
    sasl_pass = os.environ.get("KAFKA_SASL_PASSWORD")
    if sasl_pass:
        kafka_conf["sasl.password"] = sasl_pass

    producer = SerializingProducer(kafka_conf)

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"[producer] delivery FAILED: {err}", file=sys.stderr)

    interval = 1.0 / args.tps if args.tps > 0 else 0
    sent = 0
    i = 0
    try:
        while args.count == 0 or sent < args.count:
            dto = build_round(pb, i)
            producer.produce(
                topic=topic,
                key=dto.UniqueId,
                value=dto,
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            i += 1
            if sent % 25 == 0:
                print(
                    f"[producer] sent {sent} rounds "
                    f"(last company={dto.CompanyId} game={dto.GameInfo.GameId} "
                    f"messages={len(dto.RoundInfo.Messages)})"
                )
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
