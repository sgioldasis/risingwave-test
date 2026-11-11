"""Helper for the reverse-ETL animation (docs/poc/reverse_etl_data_flow.html) and the
Jobotron producer panel (docs/poc/jobotron_data_flow.html): serves both pages and
lets their buttons launch the Dagster runs behind each step, plus read-only checks
(Kafka Connect and RisingWave, Postgres, the Jobotron topic and mirror, Apicurio
schemas). Standard library plus psycopg2, requests and confluent-kafka. Binds to
localhost only.

  POST /api/launch          {"sync": "avro|cdf|orders|delta", "action": "change|sync|reset|setup", "change": ...}
                            {"sync": "jobotron", "action": "setup|publish|consume", "market": "cy|gr|ro", "dry_run": bool}
  GET  /api/run/<run_id>    status and duration of a launched run
  GET  /api/check/<sync>/readers | postgres | starrocks   (CDF syncs; starrocks = delta only)
  GET  /api/check/<sync>/federation   (delta only: StarRocks reading the source in place)
  GET  /api/check/<sync>/flight       (delta only: Arrow Flight SQL vs MySQL benchmark)
  GET  /api/check/<sync>/duckdb       (delta only: DuckDB over Flight, zero-copy Arrow)
  GET  /api/check/jobotron/topics|postgres?market=<m>   (the instance's topic / mirror)
  GET  /api/schema/<sync>

Needs the stack up (Dagster on DAGSTER_URL). The Postgres check reads POSTGRES_USER and POSTGRES_PASSWORD
from the environment (a devbox shell sets them) and never returns them; the Jobotron
topic check needs the KAFKA_OUTPUT_* credentials from .env (the bin script sources it).

(The Jobotron demo was rebuilt on 2026-10-26 around the team's real contract — three
per-market instances, JSON envelopes, a per-run dry-run switch in the launch config; see
docs/poc/JOBOTRON_CONTRACT_PARITY.md and REVERSE_ETL_JOBOTRON.md §12-§13. The previous
animation page, with its demo tables and change ops, was retired with the routing layer
it demonstrated; this panel drives the new instances directly.)
"""

import concurrent.futures
import json
import os
import re
import subprocess
import sys
import threading
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
SYNCS = ("avro", "cdf", "orders", "delta", "company")
CHANGES = ("insert", "update", "delete", "add_column", "rename_column", "drop_column", "bulk_1m")
MARKETS = ("cy", "gr", "ro")
# action -> the instance asset's suffix ("" is the publish asset itself); "reset" is the
# instance's reset JOB, not an asset
# Suffixes of the local demo-plumbing assets per action; "publish" is special (the
# library instance's asset, handled in launch_jobotron) and "reset" is a job.
JOBOTRON_ASSETS = {"setup": "_setup", "publish": None, "consume": "_consume", "reset": None}
SCHEMA_REGISTRY = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7"
)


def _dagster_unreachable(text: str | None) -> bool:
    """True when the failure is the dead code server (the Mac slept with the stack
    up: on wake the server's heartbeat watchdog fires -- `heartbeat=True` is
    hardcoded for managed gRPC locations -- and `watch_server=False` means nothing
    respawns it, so every launch/poll fails until the services restart)."""
    t = text or ""
    return (
        "user code server" in t
        or "UserCodeUnreachable" in t
        or "Could not reach user code server" in t
    )


_DAGSTER_RESTART_LOCK = threading.Lock()
_DAGSTER_LAST_RESTART = 0.0
_DAGSTER_RESTART_COOLDOWN = 300  # seconds: one self-heal attempt per window


def _restart_dagster() -> None:
    """Self-heal: restart the dagster services and wait for the code location to
    load. Throttled so a persistently broken stack fails fast instead of
    restarting in a loop; the caller retries the original request exactly once."""
    global _DAGSTER_LAST_RESTART
    with _DAGSTER_RESTART_LOCK:
        if time.time() - _DAGSTER_LAST_RESTART < _DAGSTER_RESTART_COOLDOWN:
            return  # a restart already ran inside the cooldown; let it land
        _DAGSTER_LAST_RESTART = time.time()
    subprocess.run(
        ["docker", "restart", "dagster-webserver", "dagster-daemon"],
        check=False,
        timeout=180,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            ws = gql("{ workspaceOrError { ... on Workspace { locationEntries { loadStatus } } } }")
            entries = (ws["workspaceOrError"].get("locationEntries")) or []
            if entries and all(entry.get("loadStatus") == "LOADED" for entry in entries):
                return
        except Exception:
            pass  # the webserver is still coming up
        time.sleep(3)


_UNREACHABLE_HINT = (
    "Dagster's code server is unreachable (this happens after the Mac sleeps with the stack up)."
    " Fix: docker compose restart dagster-webserver dagster-daemon"
)

_CODE_SERVER_PROBE = (
    "import glob, socket\n"
    "alive = 0\n"
    "for p in glob.glob('/tmp/tmp*'):\n"
    "    try:\n"
    "        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n"
    "        s.settimeout(1)\n"
    "        s.connect(p)\n"
    "        s.close()\n"
    "        alive += 1\n"
    "    except OSError:\n"
    "        pass\n"
    "print(alive)\n"
)


def dagster_health() -> str:
    """The launch path's view of Dagster health, for the UI's status pill:
    'unreachable' (webserver down), 'loading' (up, location not ready), 'dead'
    (the location claims LOADED but the code server's unix socket refuses
    connections — the post-sleep state), or 'LOADED'. The socket connect probe
    is the honest check: every read API is served from the webserver's cache,
    which stays green while the code server is dead. Note this rides on gql(),
    so a ping during an unreachable window also triggers the auto-heal."""
    try:
        ws = gql("{ workspaceOrError { ... on Workspace { locationEntries { loadStatus } } } }")
        entries = (ws["workspaceOrError"].get("locationEntries")) or []
        if not entries or any(entry.get("loadStatus") != "LOADED" for entry in entries):
            return "loading"
    except Exception:
        return "unreachable"
    try:
        probe = subprocess.run(
            ["docker", "exec", "dagster-webserver", "python", "-c", _CODE_SERVER_PROBE],
            capture_output=True,
            text=True,
            timeout=15,
        )
        return "LOADED" if probe.stdout.strip() == "1" else "dead"
    except Exception:
        return "unknown"  # probe itself failed — don't cry wolf


def gql(query: str, variables: dict | None = None, _retried: bool = False) -> dict:
    req = urllib.request.Request(
        f"{DAGSTER_URL}/graphql",
        json.dumps({"query": query, "variables": variables or {}}).encode(),
        {"content-type": "application/json"},
    )
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))["data"]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        if _dagster_unreachable(body):
            if not _retried:
                _restart_dagster()
                return gql(query, variables, _retried=True)
            raise RuntimeError(_UNREACHABLE_HINT + " (auto-restart did not recover it)") from e
        raise RuntimeError(f"Dagster GraphQL {e.code}: {body[:300]}") from e


def launch(
    sync: str, action: str, change: str | None, rows: int | None = None,
    market: str | None = None, dry_run: bool | None = None, _retried: bool = False,
) -> dict:
    if sync == "jobotron":
        return launch_jobotron(action, market, dry_run, _retried=_retried)
    if sync not in SYNCS:
        raise ValueError(f"unknown sync {sync!r}")
    repo = gql("{repositoriesOrError{... on RepositoryConnection{nodes{name location{name}}}}}")[
        "repositoriesOrError"
    ]["nodes"][0]
    selector = {"repositoryLocationName": repo["location"]["name"], "repositoryName": repo["name"]}
    job_name = f"reverse_etl_{sync}_{'make_change' if action == 'change' else action}_job"
    params = {"selector": {**selector, "jobName": job_name}}
    if action == "sync":
        params["selector"] = {**selector, "jobName": "__ASSET_JOB", "assetSelection": [{"path": [f"reverse_etl_{sync}_to_kafka"]}]}
    elif action == "change":
        if sync == "company":
            # company's source is an existing table (source_columns unset): the component
            # defines no make_change assets or jobs for it — change the table with SQL.
            raise ValueError("company's source is an existing table: change it with SQL in Databricks (no make-change jobs)")
        if change not in CHANGES:
            raise ValueError(f"change for {sync} must be one of {CHANGES}")
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
    return _launch_run(params, _retried)


def _assert_topic_exists(topic: str) -> None:
    """Raise a clear error unless the topic exists (the publish preflight)."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_kafka_inspect_conf())
    if topic not in admin.list_topics(timeout=10).topics:
        raise ValueError(f"topic {topic} does not exist -- run setup first")


def launch_jobotron(action: str, market: str | None, dry_run: bool | None, _retried: bool = False) -> dict:
    """One instance's run: the market's setup/publish/consume asset, with the per-run mode
    switch in the run config (the checkbox; the YAML value governs when it is not sent).
    Reset is the instance's job, not an asset selection. The publish asset is the vendored
    team component's (asset key jobotron/unverified_customers_<market>); setup and consume
    are the local demo plumbing."""
    if market not in MARKETS:
        raise ValueError(f"market must be one of {MARKETS}")
    if action not in JOBOTRON_ASSETS:
        raise ValueError(f"action must be one of {sorted(JOBOTRON_ASSETS)}")
    # An omitted mode is a DRY run, not a real publish: bool(None) is False, so a
    # bare {"action": "publish"} would otherwise publish for real while the
    # docstring promises the YAML default (true). The UI always sends the
    # checkbox; this guards every other caller.
    dry_run = True if dry_run is None else bool(dry_run)
    if action == "publish" and not dry_run:
        # Fail fast on a missing topic: the library's producer would otherwise
        # retry UNKNOWN_TOPIC_OR_PART for 120s before surfacing it. Setup creates
        # the topic; the library deliberately does not.
        _assert_topic_exists(f"jobotron_unverified_customers_{market}")
    repo = gql("{repositoriesOrError{... on RepositoryConnection{nodes{name location{name}}}}}")[
        "repositoriesOrError"
    ]["nodes"][0]
    selector = {"repositoryLocationName": repo["location"]["name"], "repositoryName": repo["name"]}
    if action == "reset":
        return _launch_run({"selector": {**selector, "jobName": f"jobotron_unverified_customers_{market}_reset_job"}}, _retried)
    if action == "publish":
        # The library instance's publish asset: key prefix [jobotron], name
        # unverified_customers_<market> (its own per-run dry_run switch). The
        # run-config op key is the FULL op name: the prefix folds in with a
        # double underscore (jobotron__unverified_customers_<market>) — keyed
        # by the bare name, Dagster silently ignores the config.
        path = ["jobotron", f"unverified_customers_{market}"]
        op_name = "__".join(path)
        params: dict = {"selector": {**selector, "jobName": "__ASSET_JOB", "assetSelection": [{"path": path}]}}
        params["runConfigData"] = {"ops": {op_name: {"config": {"dry_run": bool(dry_run)}}}}
        return _launch_run(params, _retried)
    asset = f"jobotron_unverified_customers_{market}{JOBOTRON_ASSETS[action]}"
    params = {"selector": {**selector, "jobName": "__ASSET_JOB", "assetSelection": [{"path": [asset]}]}}
    if action != "consume":  # the mode switch exists on setup; consume reads only
        params["runConfigData"] = {"ops": {asset: {"config": {"dry_run": bool(dry_run)}}}}
    return _launch_run(params, _retried)


def _launch_run(params: dict, _retried: bool = False) -> dict:
    result = gql(
        "mutation($p:ExecutionParams!){launchRun(executionParams:$p)"
        "{__typename ... on LaunchRunSuccess{run{runId}} ... on PythonError{message} "
        "... on InvalidSubsetError{message} ... on PipelineNotFoundError{message} "
        "... on RunConflict{message} ... on RunConfigValidationInvalid{errors{message}}}}",
        {"p": params},
    )["launchRun"]
    if result["__typename"] != "LaunchRunSuccess":
        # A dead code server surfaces here as InvalidSubsetError wrapping the
        # UserCodeUnreachable message (the launch needs the live gRPC server, not
        # the webserver's cached snapshot). Self-heal once, then fail loudly.
        message = result.get("message") or json.dumps(result)
        if _dagster_unreachable(message):
            if not _retried:
                _restart_dagster()
                return _launch_run(params, _retried=True)
            raise RuntimeError(_UNREACHABLE_HINT + " (auto-restart did not recover it)")
        raise RuntimeError(message)
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

    # A failed run must fail LOUDLY in the animation: surface the root cause (the
    # raise-from chain's innermost exception, which otherwise only shows in the
    # Dagster UI's error viewer) as a red sub-line on the page. Best-effort — the
    # FAILURE state itself still renders if this lookup hiccups.
    if out["status"] in ("FAILURE", "CANCELED"):
        try:
            errs = gql(
                "query($i:ID!){runOrError(runId:$i){... on Run{eventConnection{events{"
                "... on ExecutionStepFailureEvent{error{causes{className message}}} "
                "... on RunFailureEvent{error{causes{className message}}}}}}}}",
                {"i": run_id},
            )["runOrError"]
            causes = [
                cause
                for event in ((errs.get("eventConnection") or {}).get("events")) or []
                if event and event.get("error")
                for cause in (event["error"].get("causes") or [])
            ]
            if causes:
                root = causes[-1]  # the innermost cause is the actionable one
                text = f"[{root.get('className')}] {root.get('message') or ''}".strip()
                out["error"] = ("…" + text[-260:]) if len(text) > 260 else text
        except Exception:
            pass
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
    """Consumer config for the Jobotron topic check: the same KAFKA_OUTPUT_* SASL
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


def check_jobotron_topics(market: str) -> dict:
    """Everything currently on the instance's topic: assigned from the earliest offset,
    consumed to exhaustion, values decoded as the JSON they are (no registry -- the
    validated contract is plain JSON). This is the panel's 'target check'."""
    if market not in MARKETS:
        raise ValueError(f"market must be one of {MARKETS}")
    topic = f"jobotron_unverified_customers_{market}"
    # Imported lazily: only this check needs confluent-kafka, and the helper
    # should still serve the CDF pages if the host venv lacks it.
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(_kafka_inspect_conf())
    messages: list = []
    try:
        consumer.assign([TopicPartition(topic, 0)])
        # Drain fully, verified against the broker's watermarks -- a plain "break on
        # the first quiet window" loses the race on a cold consumer and reports an
        # empty topic that actually holds messages (the bug the consume asset hit).
        for _ in range(10):
            while True:
                batch = consumer.consume(num_messages=500, timeout=5.0)
                if not batch:
                    break
                for message in batch:
                    if message.error() or message.topic() != topic:
                        continue
                    try:
                        messages.append(json.loads(message.value()))
                    except Exception as e:  # noqa: BLE001 - surface decode failures per message
                        messages.append({"error": f"{type(e).__name__}: {e}"})
            lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10.0, cached=False)
            if hi - lo <= len(messages):
                break
        # Keep the payload bounded no matter what a run published.
        return {"topic": topic, "count": len(messages), "messages": messages[:12]}
    finally:
        consumer.close()


def check_jobotron_postgres(market: str) -> dict:
    """Rows currently in the instance's Postgres mirror table (table = topic name; one
    row per CustomerId, the Kafka key). Same credential convention as the CDF demo's
    postgres check: POSTGRES_USER/POSTGRES_PASSWORD from the environment."""
    if market not in MARKETS:
        raise ValueError(f"market must be one of {MARKETS}")
    topic = f"jobotron_unverified_customers_{market}"
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="postgres", connect_timeout=5,
        user=os.environ.get("POSTGRES_USER", "postgres"), password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT customer_id, company_id, run_id, processed_at "
                f"FROM {topic} ORDER BY customer_id"
            )
            rows = [[str(v) for v in row] for row in cur.fetchall()]
        return {"table": topic, "count": len(rows), "rows": rows[:12]}
    finally:
        conn.close()


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


def check_starrocks(sync: str) -> dict:
    """The StarRocks serving layer on the sync's target: an async rollup
    materialized view that the planner reaches transparently — a query written
    against the base table scans the MV, and EXPLAIN shows MaterializedView: true.
    Only the delta sync wires a StarRocks target (Avro encoding is a target
    requirement). The MV is demo-owned here rather than in the target asset
    because the setup asset drops and recreates the target table on every setup,
    which turns any MV over it inactive — this check recreates the MV when
    needed and refreshes it synchronously, so what it reports is always current.
    Needs pymysql in the helper's environment (the devbox venv has it)."""
    table = f"reverse_etl_{sync}_target"
    mv = f"mv_{sync}_target_rollup"
    if sync != "delta":
        return {"error": f"no StarRocks target for sync '{sync}' — only delta wires one (Avro encoding is a target requirement)"}
    try:
        import pymysql

        conn = pymysql.connect(
            host=os.environ.get("STARROCKS_HOST", "127.0.0.1"),
            port=int(os.environ.get("STARROCKS_PORT", "9030")),
            user=os.environ.get("STARROCKS_USER", "root"), database="reverse_etl", connect_timeout=5,
        )
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT QUERY_REWRITE_STATUS FROM information_schema.materialized_views "
                "WHERE TABLE_SCHEMA = 'reverse_etl' AND TABLE_NAME = %s", (mv,)
            )
            row = cur.fetchone()
            if not row or row[0] != "VALID":
                cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv}")
                # Plain SPJG rollup over the synced Primary Key table: the shape
                # transparent rewrite is eligible for (no volatile predicates).
                cur.execute(
                    f"CREATE MATERIALIZED VIEW {mv} DISTRIBUTED BY HASH(description) "
                    f"REFRESH ASYNC EVERY (INTERVAL 10 SECOND) AS "
                    f"SELECT description, COUNT(*) AS n, MAX(updated_at) AS last_change "
                    f"FROM {table} GROUP BY description"
                )
            cur.execute(f"REFRESH MATERIALIZED VIEW {mv} WITH SYNC MODE")
            cur.execute(f"SELECT COUNT(*) FROM {mv}")
            mv_count = cur.fetchone()[0]
            cur.execute(f"SELECT description, n, last_change FROM {mv} ORDER BY description LIMIT 12")
            mv_rows = [[str(v) for v in r] for r in cur.fetchall()]
            # The MV's own DDL, read back from the engine rather than echoed
            # from the constant above, so what the card shows is what exists.
            cur.execute(f"SHOW CREATE MATERIALIZED VIEW {mv}")
            mv_definition = cur.fetchone()[1]
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            target_rows = cur.fetchone()[0]
            # The rewrite proof: a query naming only the base table. If the
            # planner substitutes the MV, the plan scans it instead.
            cur.execute(f"EXPLAIN SELECT description, COUNT(*) FROM {table} GROUP BY description")
            plan = "\n".join(r[0] for r in cur.fetchall())
            scanned = re.search(r"TABLE: (\S+)", plan)
            return {
                "mv": mv,
                "mv_definition": mv_definition,
                "target_rows": target_rows,
                "mv_count": mv_count,
                "mv_rows": mv_rows,
                "rewritten": "MaterializedView: true" in plan,
                "scanned": scanned.group(1) if scanned else "",
                # Echo the exact query the rewrite proof ran, so the card can
                # show both sides: what was asked vs what StarRocks scanned,
                # and the query it is answered by (the planner substitutes the
                # MV's precomputed columns for the aggregates).
                "query": f"SELECT description, COUNT(*) FROM {table} GROUP BY description",
                "rewritten_query": f"SELECT description, n FROM {mv}",
            }
        finally:
            conn.close()
    except ImportError:
        return {"error": "pymysql not available in the helper's environment"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# UC mints a new storage path per table only on a rebuild; between rebuilds
# the `databricks tables get` answer is stable, so it is cached briefly (the
# five CLI invocations cost ~0.7s of process startup even in parallel). A
# stale cache after a rebuild shows up as COUNT failures — bust and re-run.
_UC_CACHE_TTL_S = 30
_uc_cache: dict = {"at": 0.0, "locs": None}


def _uc_locations() -> dict:
    import time

    now = time.monotonic()
    if _uc_cache["locs"] is None or now - _uc_cache["at"] > _UC_CACHE_TTL_S:
        import setup_delta_federation as fed

        with concurrent.futures.ThreadPoolExecutor(len(fed.SOURCES)) as pool:
            locs = dict(zip(fed.SOURCES, pool.map(fed.table_location, fed.SOURCES)))
        _uc_cache.update(at=now, locs=locs)
    return _uc_cache["locs"]


def check_federation(sync: str) -> dict:
    """The Delta federation (serving-layer doc §13): StarRocks reads the
    Databricks Delta source in place through an HMS-backed external catalog —
    no pipeline, no compute, just the HMS shim and the sync's service
    principal. Verifies the chain live and self-heals what the setup script
    would fix: if UC has re-pointed the source (a rebuild mints a new UUID
    path), the HMS registration is refreshed before querying. Delta sync only —
    the federation reads that source table."""
    if sync != "delta":
        return {"error": f"no federation for sync '{sync}' — only the delta source is registered in HMS"}
    try:
        import pymysql

        import setup_delta_federation as fed
    except ImportError as e:
        return {"error": f"imports unavailable: {e}"}

    def connect():
        return pymysql.connect(
            host=os.environ.get("STARROCKS_HOST", "127.0.0.1"),
            port=int(os.environ.get("STARROCKS_PORT", "9030")),
            user=os.environ.get("STARROCKS_USER", "root"), connect_timeout=5,
        )

    def refresh_and_count(table_name: str) -> int:
        """REFRESH + COUNT on one source, on its own connection — the per-source
        work runs in parallel (~0.6s wall) instead of serially (~2.8s)."""
        conn = connect()
        try:
            cur = conn.cursor()
            cur.execute(f"REFRESH EXTERNAL TABLE {fed.CATALOG}.{fed.HMS_DATABASE}.{table_name}")
            cur.execute(f"SELECT COUNT(*) FROM {fed.CATALOG}.{fed.HMS_DATABASE}.{table_name}")
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    def run(locations: dict, uc_location: str) -> dict:
        # Compare-then-fix every registered source: re-register only when UC's
        # path moved (a drop + create on every click would be harmless but
        # pointless). The delta source's state drives the card's message, but
        # a stale registration never hides behind the others. HMS read-backs
        # are cheap (~0.1s for all five), so this stays serial.
        hms_matches = True
        for fqn, location in locations.items():
            name = fqn.rpartition(".")[2]
            if fed.registered_location(name) != location:
                fed.register_in_hms(location, name)
                if name == fed.SOURCE_TABLE:
                    hms_matches = False
        conn = connect()
        try:
            cur = conn.cursor()
            cur.execute(f"SHOW CATALOGS LIKE '{fed.CATALOG}'")
            catalog_exists = bool(cur.fetchone())
            if not catalog_exists:
                fed.create_catalog(fed.load_env())
        finally:
            conn.close()
        # The FE refreshes external-catalog metadata in the background every
        # hive_meta_cache_refresh_interval_s, so a Delta commit younger than
        # that is not yet visible. Each source's REFRESH + COUNT runs in its
        # own connection/thread, so the button always shows current state and
        # the wall cost is one table's, not five's. A table that fails (never
        # set up, or a moved path the cache didn't see) reports None.
        names = [fqn.rpartition(".")[2] for fqn in fed.SOURCES]
        with concurrent.futures.ThreadPoolExecutor(len(names)) as pool:
            counts = dict(zip(names, pool.map(refresh_and_count, names)))
        tables: dict[str, int | None] = {n: c for n, c in counts.items() if n != fed.SOURCE_TABLE}
        rows = counts[fed.SOURCE_TABLE]
        # The delta source's detail queries: the in-place proof (a plan
        # scanning Delta storage directly), a taste of the data, and the
        # cache evidence — two back-to-back counts, showing that after the
        # first ADLS scan the CN's block cache answers repeats.
        conn = connect()
        try:
            cur = conn.cursor()
            table = f"{fed.CATALOG}.{fed.HMS_DATABASE}.{fed.SOURCE_TABLE}"
            cur.execute(f"EXPLAIN SELECT description, COUNT(*) FROM {table} GROUP BY description")
            plan = "\n".join(r[0] for r in cur.fetchall())
            cur.execute(f"SELECT * FROM {table} ORDER BY rid DESC LIMIT 5")
            latest = [[str(v) for v in row] for row in cur.fetchall()]
            # pymysql's description rows are plain tuples (name first) — unlike
            # psycopg2's attribute-carrying ones the Postgres check reads.
            columns = [d[0] for d in cur.description]
            cache_ms = []
            for _ in range(2):
                t0 = time.perf_counter()
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                cur.fetchall()
                cache_ms.append(round((time.perf_counter() - t0) * 1000))
        finally:
            conn.close()
        return {
            "location": uc_location,
            "hms_matches": hms_matches,
            "repointed": not hms_matches,
            "catalog_exists": catalog_exists,
            "rows": rows,
            "tables": tables,
            "plan_scanned": "DeltaLakeScanNode" if "DeltaLakeScanNode" in plan else "",
            "latest": latest,
            "columns": columns,
            "query": f"SELECT COUNT(*) FROM {table}",
            "cache_ms": cache_ms,
        }

    try:
        fed.load_env()
        result = run(_uc_locations(), _uc_locations()[fed.DEFAULT_SOURCE_FQN])
        # A COUNT failure on a cache-served location means UC moved since the
        # cache was filled (a rebuild) — bust and re-run once.
        if any(v is None for v in result["tables"].values()):
            _uc_cache.update(at=0.0, locs=None)
            result = run(_uc_locations(), _uc_locations()[fed.DEFAULT_SOURCE_FQN])
        return result
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def check_flight(sync: str) -> dict:
    """Arrow Flight SQL vs MySQL protocol, live (SR_POC_ARROW_FLIGHT_SQL_DEMO.md).
    Runs scripts/starrocks_flight_benchmark.py --json inside the dagster-webserver
    container: the FE advertises its Flight endpoint as grpc+tcp://starrocks:8010,
    which only resolves on the compose network, and the container already has
    pymysql + pyarrow (the ADBC driver is (re)installed into its writable layer
    here, same as bin/5_starrocks_flight_benchmark.sh). ~40s: 3 streamed pulls of
    the 420k-row federated source + 200 point lookups each way."""
    if sync != "delta":
        return {"error": f"no flight benchmark for sync '{sync}' — the benchmark pulls the delta source"}
    cmd = [
        "docker", "exec", "dagster-webserver", "python", "/workspace/scripts/starrocks_flight_benchmark.py", "--json",
    ]
    try:
        subprocess.run(
            ["docker", "exec", "-u", "root", "dagster-webserver",
             "python", "-m", "pip", "install", "-q", "adbc-driver-flightsql"],
            check=True, capture_output=True, timeout=120,
        )
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
    except FileNotFoundError:
        return {"error": "docker CLI not available to the demo server — it shells out to docker exec"}
    except subprocess.CalledProcessError as e:
        return {"error": f"benchmark failed: {e.stderr.strip().splitlines()[-1] if e.stderr else e}"}
    except subprocess.TimeoutExpired:
        return {"error": "benchmark timed out"}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"error": f"unparseable benchmark output: {proc.stdout[:200]}"}


def check_duckdb(sync: str) -> dict:
    """DuckDB over Arrow Flight SQL, live (SR_POC_ARROW_FLIGHT_SQL_DEMO.md,
    'Querying through DuckDB'). Unlike check_flight this runs in-process: the
    demo server's host venv already has duckdb + adbc-driver-flightsql, and
    the /etc/hosts 'starrocks' entry makes the FE's advertised endpoint
    resolvable from the host. If the venv lacks a dependency, the error says
    so (uv sync / uv pip install adbc-driver-flightsql)."""
    if sync != "delta":
        return {"error": f"no DuckDB demo for sync '{sync}' — it pulls the delta source"}
    try:
        import starrocks_duckdb_demo
    except ImportError as e:
        return {"error": f"{e.name} missing in the demo server's venv — pip install adbc-driver-flightsql duckdb"}
    except SystemExit as e:
        # starrocks_duckdb_demo sys.exits at import time on a missing dependency;
        # uncaught, that kills the handler thread before any response and the
        # browser reports "Failed to fetch".
        return {"error": str(e)}
    try:
        return starrocks_duckdb_demo.report_json()
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


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
                return self._json(200, {"ok": True, "dagster": dagster_health()})
            if m := re.fullmatch(r"/api/run/([^/]+)", self.path):
                return self._json(200, run_status(m.group(1)))
            if m := re.fullmatch(r"/api/check/jobotron/(topics|postgres)", self.path.split("?")[0]):
                market = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("market", [""])[0]
                check = check_jobotron_topics if m.group(1) == "topics" else check_jobotron_postgres
                return self._json(200, check(market))
            if m := re.fullmatch(r"/api/check/(\w+)/(readers|postgres|starrocks|federation|flight|duckdb)", self.path):
                if m.group(1) not in SYNCS:
                    raise ValueError("unknown sync")
                if m.group(2) == "starrocks":
                    return self._json(200, check_starrocks(m.group(1)))
                if m.group(2) == "federation":
                    return self._json(200, check_federation(m.group(1)))
                if m.group(2) == "flight":
                    return self._json(200, check_flight(m.group(1)))
                if m.group(2) == "duckdb":
                    return self._json(200, check_duckdb(m.group(1)))
                return self._json(200, (check_readers if m.group(2) == "readers" else check_postgres)(m.group(1)))
            if m := re.fullmatch(r"/api/schema/(\w+)", self.path):
                if m.group(1) not in SYNCS:
                    raise ValueError("unknown sync")
                return self._json(200, schema_info(m.group(1)))
            self._json(404, {"error": "not found"})
        except Exception as e:
            self._json(400, {"error": str(e)})

    def do_POST(self):
        try:
            if self.path != "/api/launch":
                return self._json(404, {"error": "not found"})
            body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
            self._json(
                200,
                launch(
                    body.get("sync", ""), body.get("action", ""), body.get("change"), body.get("rows"),
                    market=body.get("market"), dry_run=body.get("dry_run"),
                ),
            )
        except Exception as e:
            self._json(400, {"error": str(e)})

    def log_message(self, fmt, *args):
        print(self.address_string(), fmt % args, file=sys.stderr, flush=True)


if __name__ == "__main__":
    print(
        f"Reverse-ETL demo at http://localhost:{PORT}/?sync=delta — Jobotron panel at "
        f"http://localhost:{PORT}/jobotron (Dagster: {DAGSTER_URL})",
        flush=True,
    )
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
