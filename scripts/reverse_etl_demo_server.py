"""Helper for the reverse-ETL animation (docs/poc/reverse_etl_data_flow.html): serves the page and lets its
buttons launch the Dagster runs behind each step, plus two read-only checks (Kafka Connect and RisingWave,
Postgres). Standard library plus psycopg2. Binds to localhost only.

  POST /api/launch          {"sync": "avro", "action": "change|sync|reset|setup", "change": "insert|update|delete|add_column"}
  GET  /api/run/<run_id>    status and duration of a launched run
  GET  /api/check/<sync>/readers | postgres

Needs the stack up (Dagster on DAGSTER_URL). The Postgres check reads POSTGRES_USER and POSTGRES_PASSWORD
from the environment (a devbox shell sets them) and never returns them.
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
SYNCS = ("avro", "cdf", "orders")
CHANGES = ("insert", "update", "delete", "add_column")


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


def launch(sync: str, action: str, change: str | None) -> dict:
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
        if change not in CHANGES:
            raise ValueError(f"change must be one of {CHANGES}")
        params["runConfigData"] = {"ops": {f"reverse_etl_{sync}_make_change": {"config": {"change": change}}}}
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

    # Databricks cost, from the meter metadata the assets emit
    # (orchestration/assets/databricks_cost_meter.py). The oss_spark sync
    # reports 0.0 structurally: it touches no Databricks compute.
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


def check_postgres(sync: str) -> dict:
    table = f"reverse_etl_{sync}_target"
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, dbname="postgres", connect_timeout=5,
            user=os.environ.get("POSTGRES_USER", "postgres"), password=os.environ.get("POSTGRES_PASSWORD", ""),
        )
        count = _rows(conn, f"select count(*) from {table}")[0][0]
        latest = _rows(conn, f"select * from {table} order by 1 desc limit 5")
        conn.close()
        return {"rows": count, "latest": latest}
    except Exception as e:
        return {"error": type(e).__name__}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, ctype: str = "application/json") -> None:
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload: dict) -> None:
        self._send(code, json.dumps(payload).encode())

    def do_GET(self):
        try:
            if self.path.split("?")[0] in ("/", "/reverse_etl_data_flow.html"):
                return self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
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
            if m := re.fullmatch(r"/api/check/(\w+)/(readers|postgres)", self.path):
                if m.group(1) not in SYNCS:
                    raise ValueError("unknown sync")
                return self._json(200, (check_readers if m.group(2) == "readers" else check_postgres)(m.group(1)))
            self._json(404, {"error": "not found"})
        except Exception as e:
            self._json(400, {"error": str(e)})

    def do_POST(self):
        try:
            if self.path != "/api/launch":
                return self._json(404, {"error": "not found"})
            body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
            self._json(200, launch(body.get("sync", ""), body.get("action", ""), body.get("change")))
        except Exception as e:
            self._json(400, {"error": str(e)})

    def log_message(self, fmt, *args):
        print(self.address_string(), fmt % args, file=sys.stderr, flush=True)


if __name__ == "__main__":
    print(f"Reverse-ETL demo at http://localhost:{PORT}/?sync=avro (Dagster: {DAGSTER_URL})", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
