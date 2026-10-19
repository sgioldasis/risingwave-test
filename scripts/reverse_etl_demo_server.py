"""Helper for the reverse-ETL animation (docs/poc/reverse_etl_data_flow.html) and the
Jobotron producer animation (docs/poc/jobotron_data_flow.html): serves both pages and
lets their buttons launch the Dagster runs behind each step, plus read-only checks
(Kafka Connect and RisingWave, Postgres, the Jobotron operator topics, Apicurio
schemas). Standard library plus psycopg2, requests and confluent-kafka. Binds to
localhost only.

  POST /api/launch          {"sync": "avro|cdf|orders|delta|jobotron", "action": "change|sync|reset|setup", "change": ...}
  GET  /api/run/<run_id>    status and duration of a launched run
  GET  /api/check/<sync>/readers | postgres     (CDF syncs)
  GET  /api/check/jobotron/topics            (everything on the operator topics)
  GET  /api/schema/<sync>

Needs the stack up (Dagster on DAGSTER_URL). The Postgres check reads POSTGRES_USER and POSTGRES_PASSWORD
from the environment (a devbox shell sets them) and never returns them; the Jobotron
topics check needs the KAFKA_OUTPUT_* credentials from .env (the bin script sources it).
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg2
import requests

DAGSTER_URL = os.environ.get("DAGSTER_URL", "http://localhost:3000")
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL_HOST", "http://localhost:8083")
PORT = int(os.environ.get("PORT", "4002"))
PAGE = Path(__file__).resolve().parent.parent / "docs" / "poc" / "reverse_etl_data_flow.html"
JOBOTRON_PAGE = PAGE.parent / "jobotron_data_flow.html"
SYNCS = ("avro", "cdf", "orders", "delta", "jobotron")
CHANGES = ("insert", "update", "delete", "add_column", "rename_column", "drop_column", "bulk_1m")
JOBOTRON_CHANGES = ("add_customer", "verify_customer", "deactivate_customer")
JOBOTRON_TOPICS = ("jobotron_demo_gr", "jobotron_demo_br")
SCHEMA_REGISTRY = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7"
)


def gql(query: str, variables: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{DAGSTER_URL}/graphql",
        json.dumps({"query": query, "variables": variables or {}}).encode(),
        {"content-type": "application/json"},
    )
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))["data"]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        # The definitions code server kills itself when heartbeats stop, which
        # happens whenever the Mac sleeps with the stack up; launches then fail
        # until the services are restarted. Say so instead of a bare 500.
        if "user code server" in body or "UserCodeUnreachable" in body:
            raise RuntimeError(
                "Dagster's code server is unreachable (this happens after the Mac sleeps with the stack up)."
                " Fix: docker compose restart dagster-webserver dagster-daemon"
            ) from e
        raise RuntimeError(f"Dagster GraphQL {e.code}: {body[:300]}") from e


def launch(sync: str, action: str, change: str | None, rows: int | None = None) -> dict:
    if sync not in SYNCS:
        raise ValueError(f"unknown sync {sync!r}")
    allowed_changes = JOBOTRON_CHANGES if sync == "jobotron" else CHANGES
    repo = gql("{repositoriesOrError{... on RepositoryConnection{nodes{name location{name}}}}}")[
        "repositoriesOrError"
    ]["nodes"][0]
    selector = {"repositoryLocationName": repo["location"]["name"], "repositoryName": repo["name"]}
    job_name = f"reverse_etl_{sync}_{'make_change' if action == 'change' else action}_job"
    params = {"selector": {**selector, "jobName": job_name}}
    if action == "sync":
        # The publish asset of the Jobotron producer; the sync asset of a CDF sync.
        asset = "reverse_etl_jobotron_publish" if sync == "jobotron" else f"reverse_etl_{sync}_to_kafka"
        params["selector"] = {**selector, "jobName": "__ASSET_JOB", "assetSelection": [{"path": [asset]}]}
    elif action == "consume":
        # The Jobotron demo consumer: operator topics -> per-operator Postgres tables.
        if sync != "jobotron":
            raise ValueError("consume is only available for the jobotron demo")
        params["selector"] = {
            **selector,
            "jobName": "__ASSET_JOB",
            "assetSelection": [{"path": ["reverse_etl_jobotron_consume"]}],
        }
    elif action == "change":
        if change not in allowed_changes:
            raise ValueError(f"change for {sync} must be one of {allowed_changes}")
        if change == "bulk_1m" and sync != "delta":
            raise ValueError("bulk_1m is only available for the delta sync (its 1M-row load generator)")
        config: dict = {"change": change}
        if change == "bulk_1m":
            # Optional total row count for the bulk generator (default 1,000,000).
            try:
                config["rows"] = int(rows) if rows is not None else 1_000_000
            except (TypeError, ValueError):
                raise ValueError(f"rows must be an integer, got {rows!r}") from None
        params["runConfigData"] = {"ops": {f"reverse_etl_{sync}_make_change": {"config": config}}}
    elif action not in ("reset", "setup"):
        raise ValueError(f"unknown action {action!r}")
    result = gql(
        "mutation($p:ExecutionParams!){launchRun(executionParams:$p)"
        "{__typename ... on LaunchRunSuccess{run{runId}} ... on PythonError{message} ... on RunConfigValidationInvalid{errors{message}}}}",
        {"p": params},
    )["launchRun"]
    if result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(result.get("message") or json.dumps(result))
    return {"runId": result["run"]["runId"], "url": f"{DAGSTER_URL}/runs/{result['run']['runId']}"}


def _avro_field_type(field_type) -> str:
    """Human-readable type for one Avro field: unwrap null-unions, map Debezium's
    logical timestamp marker to 'timestamp'."""
    if isinstance(field_type, list):
        field_type = next(t for t in field_type if t != "null")
    if isinstance(field_type, dict):
        logical = field_type.get("connect.name", "")
        if "ZonedTimestamp" in logical:
            return "timestamp"
        return str(field_type.get("type", "record"))
    return str(field_type)


def _envelope_row_columns(value_schema: dict) -> dict[str, str]:
    """The row columns the envelope's before/after carry (the named Value record)."""
    for field in value_schema.get("fields", []):
        if field.get("name") == "before":
            row = next((t for t in field["type"] if isinstance(t, dict)), None)
            if row:
                return {f["name"]: _avro_field_type(f["type"]) for f in row.get("fields", [])}
    return {}


def schema_info(sync: str) -> dict:
    """The latest registered Avro schemas for the sync's topic, reduced to what a
    demo audience needs: column names and types (+ the envelope field list)."""
    subject_base = f"reverse_etl_{sync}_topic"
    out = {}
    for role in ("key", "value"):
        resp = requests.get(f"{SCHEMA_REGISTRY}/subjects/{subject_base}-{role}/versions/latest", timeout=10)
        if resp.status_code == 404:
            out[role] = None
            continue
        resp.raise_for_status()
        info = resp.json()
        schema = json.loads(info["schema"])
        entry = {"version": info["version"], "id": info["id"]}
        if role == "key":
            entry["columns"] = {f["name"]: _avro_field_type(f["type"]) for f in schema.get("fields", [])}
        else:
            entry["columns"] = _envelope_row_columns(schema)
            entry["envelope"] = [f["name"] for f in schema.get("fields", [])]
        out[role] = entry
    return out


def run_status(run_id: str) -> dict:
    if not re.fullmatch(r"[0-9a-f-]{36}", run_id):
        raise ValueError("bad run id")
    info = gql(
        "query($i:ID!){runOrError(runId:$i){... on Run{status startTime endTime "
        "assetMaterializations{metadataEntries{label "
        "... on TextMetadataEntry{text} ... on FloatMetadataEntry{floatValue} ... on IntMetadataEntry{intValue} "
        "... on JsonMetadataEntry{jsonString}}}}}}",
        {"i": run_id},
    )["runOrError"]
    end = info["endTime"] or time.time()
    out = {"status": info["status"], "seconds": round(end - (info["startTime"] or end))}

    # While the run is live, hand the page its step's log lines: the sync's
    # per-chunk heartbeats ("Chunk 2/6: versions 2-2, 2 rows, 1 events (total
    # 4)") let the animation show REAL progress instead of a timed narration.
    # Only for running runs — done runs get their summary from the metadata.
    if info["status"] in ("RUNNING", "STARTED"):
        logs = gql(
            "query($i:ID!){runOrError(runId:$i){... on Run{eventConnection{events{"
            "... on MessageEvent{message stepKey}}}}}}",
            {"i": run_id},
        )["runOrError"]
        events = ((logs.get("eventConnection") or {}).get("events")) or []
        out["log_tail"] = [
            (event or {}).get("message") or ""
            for event in events
            if event and event.get("stepKey")
        ][-10:]

    # Databricks cost, from the meter metadata the assets emit
    # (orchestration/assets/databricks_cost_meter.py). The delta-rs sync reads
    # for free; only the watermark write meters a second or two.
    warehouse, cluster, compute = 0.0, 0.0, []
    change = None
    for mat in info.get("assetMaterializations") or []:
        for entry in mat.get("metadataEntries") or []:
            if entry["label"] == "databricks_warehouse_seconds" and entry.get("floatValue") is not None:
                warehouse += entry["floatValue"]
            elif entry["label"] == "databricks_cluster_seconds" and entry.get("floatValue") is not None:
                cluster += entry["floatValue"]
            elif entry["label"] == "databricks_compute" and entry.get("text"):
                compute.append(entry["text"])
            elif entry["label"] == "databricks_change_sql" and entry.get("jsonString"):
                try:
                    change = json.loads(entry["jsonString"])
                except ValueError:
                    pass
    out["cost"] = {
        "warehouse_seconds": round(warehouse, 2),
        "cluster_seconds": round(cluster, 2),
        "compute": compute,
    }
    out["change"] = change
    return out


def _rows(conn, sql: str) -> list[list[str]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return [[str(v) for v in row] for row in cur.fetchall()]


def check_readers(sync: str) -> dict:
    table, connector = f"reverse_etl_{sync}_target", f"reverse_etl_{sync}_sink"
    out = {}
    try:
        status = requests.get(f"{CONNECT_URL}/connectors/{connector}/status", timeout=5).json()
        out["kafka_connect"] = {"connector": status["connector"]["state"], "tasks": [t["state"] for t in status.get("tasks", [])]}
    except Exception as e:
        out["kafka_connect"] = {"error": type(e).__name__}
    try:
        conn = psycopg2.connect(host="localhost", port=4566, user="root", dbname="dev", connect_timeout=5)
        out["risingwave_rows"] = _rows(conn, f"select count(*) from {table}")[0][0]
        conn.close()
    except Exception as e:
        out["risingwave_rows"] = {"error": type(e).__name__}
    return out


def _kafka_inspect_conf() -> dict[str, str]:
    """Consumer config for the Jobotron topics check: the same KAFKA_OUTPUT_* SASL
    convention the producer uses. Needs the creds in the helper's environment --
    bin/3_run_reverse_etl_demo.sh sources .env for this."""
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError(
            "KAFKA_OUTPUT_BOOTSTRAP is not set -- start the helper with "
            "bin/3_run_reverse_etl_demo.sh (it sources .env)"
        )
    conf: dict[str, str] = {
        "bootstrap.servers": bootstrap,
        "group.id": "reverse-etl-demo-inspect",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
    username = os.environ.get("KAFKA_OUTPUT_SASL_USERNAME", "")
    if username:
        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512"),
                "sasl.username": username,
                "sasl.password": os.environ.get("KAFKA_OUTPUT_SASL_PASSWORD", ""),
            }
        )
    return conf


def check_jobotron_topics() -> dict:
    """Everything currently on the operator topics: assigned from the earliest
    offset, consumed to exhaustion, values decoded through the registry. This is
    the Jobotron demo's 'target check' -- the topics ARE the endpoint (the worker
    that would consume them is the RFC's scope, not this demo's)."""
    # Imported lazily: only this check needs confluent-kafka, and the helper
    # should still serve the CDF pages if the host venv lacks it.
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    deserializer = AvroDeserializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY}))
    consumer = Consumer(_kafka_inspect_conf())
    out: dict[str, list] = {topic: [] for topic in JOBOTRON_TOPICS}
    try:
        consumer.assign([TopicPartition(topic, 0) for topic in JOBOTRON_TOPICS])
        # Drain fully, verified against the broker's watermarks -- a plain "break on
        # the first quiet window" loses the race on a cold consumer and reports an
        # empty topic that actually holds messages (same bug the consume asset had).
        for _ in range(10):
            while True:
                batch = consumer.consume(num_messages=500, timeout=5.0)
                if not batch:
                    break
                for message in batch:
                    if message.error() or message.topic() not in out:
                        continue
                    try:
                        key = deserializer(message.key(), SerializationContext(message.topic(), MessageField.KEY))
                        value = deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
                    except Exception as e:  # noqa: BLE001 - surface decode failures per message
                        out[message.topic()].append({"error": f"{type(e).__name__}: {e}"})
                        continue
                    out[message.topic()].append({"key": key, "value": value})
            lagging = []
            for t in JOBOTRON_TOPICS:
                lo, hi = consumer.get_watermark_offsets(TopicPartition(t, 0), timeout=10.0, cached=False)
                if hi - lo > len(out[t]):
                    lagging.append(t)
            if not lagging:
                break
        # Keep the payload bounded no matter what a run published.
        return {topic: {"count": len(rows), "messages": rows[:12]} for topic, rows in out.items()}
    finally:
        consumer.close()


def check_jobotron_postgres() -> dict:
    """Rows currently in the per-operator Postgres tables the demo consumer
    maintains (table = topic name). Same credential convention as the CDF
    demo's postgres check: POSTGRES_USER/POSTGRES_PASSWORD from the environment."""
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="postgres", connect_timeout=5,
        user=os.environ.get("POSTGRES_USER", "postgres"), password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    try:
        out: dict[str, list] = {}
        with conn.cursor() as cur:
            for topic in JOBOTRON_TOPICS:
                cur.execute(f"SELECT customer_id, email, locale, run_id FROM {topic} ORDER BY customer_id")
                out[topic] = [[str(v) for v in row] for row in cur.fetchall()]
        return out
    finally:
        conn.close()


def jobotron_schema_info() -> dict:
    """The latest registered Avro schemas for every operator topic's subjects."""
    out = {}
    for topic in JOBOTRON_TOPICS:
        entry = {}
        for role in ("key", "value"):
            subject = f"{topic}-{role}"
            try:
                resp = requests.get(f"{SCHEMA_REGISTRY}/subjects/{subject}/versions/latest", timeout=10)
                if resp.status_code == 404:
                    entry[role] = None
                    continue
                resp.raise_for_status()
                info = resp.json()
                schema = json.loads(info["schema"])
                entry[role] = {
                    "version": info["version"],
                    "id": info["id"],
                    "columns": {f["name"]: _avro_field_type(f["type"]) for f in schema.get("fields", [])},
                }
            except Exception as e:  # noqa: BLE001 - report registry errors per subject
                entry[role] = {"error": type(e).__name__}
        out[topic] = entry
    return out


def check_postgres(sync: str) -> dict:
    table = f"reverse_etl_{sync}_target"
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, dbname="postgres", connect_timeout=5,
            user=os.environ.get("POSTGRES_USER", "postgres"), password=os.environ.get("POSTGRES_PASSWORD", ""),
        )
        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {table}")
            count = cur.fetchall()[0][0]
            cur.execute(f"select * from {table} order by 1 desc limit 5")
            latest = [[str(v) for v in row] for row in cur.fetchall()]
            # The column set moves with the demo's schema changes (add/rename),
            # so the names come from the query itself, not a hardcoded list.
            columns = [d.name for d in cur.description]
        conn.close()
        return {"rows": count, "latest": latest, "columns": columns}
    except Exception as e:
        return {"error": type(e).__name__}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, ctype: str = "application/json") -> None:
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        # The pages and checks are live demo state — never let the browser
        # serve a stale copy from heuristic cache.
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload: dict) -> None:
        self._send(code, json.dumps(payload).encode())

    def do_GET(self):
        try:
            if self.path.split("?")[0] in ("/", "/reverse_etl_data_flow.html"):
                return self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            if self.path.split("?")[0] in ("/jobotron", "/jobotron_data_flow.html"):
                return self._send(200, JOBOTRON_PAGE.read_bytes(), "text/html; charset=utf-8")
            if self.path.startswith("/img/"):
                name = urllib.parse.unquote(self.path[len("/img/"):])
                if not name or "/" in name or ".." in name or name.startswith("."):
                    raise ValueError("bad path")
                f = PAGE.parent / "img" / name
                ctype = {"png": "image/png", "svg": "image/svg+xml"}.get(f.suffix.lstrip("."))
                if not ctype or not f.is_file():
                    raise ValueError("not found")
                return self._send(200, f.read_bytes(), ctype)
            if self.path == "/api/ping":
                return self._json(200, {"ok": True})
            if m := re.fullmatch(r"/api/run/([^/]+)", self.path):
                return self._json(200, run_status(m.group(1)))
            if m := re.fullmatch(r"/api/check/jobotron/(topics|postgres)", self.path):
                check = check_jobotron_topics if m.group(1) == "topics" else check_jobotron_postgres
                return self._json(200, check())
            if m := re.fullmatch(r"/api/check/(\w+)/(readers|postgres)", self.path):
                if m.group(1) not in SYNCS:
                    raise ValueError("unknown sync")
                return self._json(200, (check_readers if m.group(2) == "readers" else check_postgres)(m.group(1)))
            if m := re.fullmatch(r"/api/schema/(\w+)", self.path):
                if m.group(1) not in SYNCS:
                    raise ValueError("unknown sync")
                info = jobotron_schema_info() if m.group(1) == "jobotron" else schema_info(m.group(1))
                return self._json(200, info)
            self._json(404, {"error": "not found"})
        except Exception as e:
            self._json(400, {"error": str(e)})

    def do_POST(self):
        try:
            if self.path != "/api/launch":
                return self._json(404, {"error": "not found"})
            body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
            self._json(200, launch(body.get("sync", ""), body.get("action", ""), body.get("change"), body.get("rows")))
        except Exception as e:
            self._json(400, {"error": str(e)})

    def log_message(self, fmt, *args):
        print(self.address_string(), fmt % args, file=sys.stderr, flush=True)


if __name__ == "__main__":
    print(
        f"Reverse-ETL demo at http://localhost:{PORT}/?sync=avro — Jobotron demo at "
        f"http://localhost:{PORT}/jobotron (Dagster: {DAGSTER_URL})",
        flush=True,
    )
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
