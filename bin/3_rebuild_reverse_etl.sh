#!/bin/bash

# Reset and set up the reverse-ETL syncs, one after the other per sync but all syncs at the same time.
# Each sync's objects are separate, so this takes about 2 minutes instead of about 4.
#
# Usage:
#   ./bin/3_rebuild_reverse_etl.sh                # avro cdf orders delta
#   ./bin/3_rebuild_reverse_etl.sh avro           # one sync
#   DAGSTER_URL=http://localhost:3000 ./bin/3_rebuild_reverse_etl.sh
#
# Needs the stack up (Dagster on port 3000) and the VPN on (the staging Kafka and registry).
# This only resets and sets up. The first sync and its Kafka messages come from the sync asset,
# for example reverse_etl_avro_to_kafka (not the Databricks notebook job, which starts a cluster).

set -e

SYNCS=("$@")
if [ ${#SYNCS[@]} -eq 0 ]; then
    SYNCS=(avro cdf orders delta)
fi

echo "=== Rebuilding reverse-ETL syncs in parallel: ${SYNCS[*]} ==="
echo ""

exec python3 - "${SYNCS[@]}" <<'PYEOF'
import json
import os
import sys
import threading
import time
import urllib.request

URL = os.environ.get("DAGSTER_URL", "http://localhost:3000") + "/graphql"
SYNCS = sys.argv[1:]


def gql(query, variables=None):
    req = urllib.request.Request(
        URL,
        json.dumps({"query": query, "variables": variables or {}}).encode(),
        {"content-type": "application/json"},
    )
    return json.load(urllib.request.urlopen(req, timeout=120))["data"]


repo = gql("{repositoriesOrError{... on RepositoryConnection{nodes{name location{name}}}}}")[
    "repositoriesOrError"
]["nodes"][0]


def run(job):
    selector = {
        "repositoryLocationName": repo["location"]["name"],
        "repositoryName": repo["name"],
        "jobName": job,
    }
    launched = gql(
        "mutation($s:JobOrPipelineSelector!){launchRun(executionParams:{selector:$s})"
        "{__typename ... on LaunchRunSuccess{run{runId}} ... on PythonError{message}}}",
        {"s": selector},
    )["launchRun"]
    if launched["__typename"] != "LaunchRunSuccess":
        return "FAILURE", 0, launched.get("message", launched["__typename"])
    run_id = launched["run"]["runId"]
    while True:
        info = gql(
            "query($i:ID!){runOrError(runId:$i){... on Run{status startTime endTime}}}",
            {"i": run_id},
        )["runOrError"]
        if info["status"] in ("SUCCESS", "FAILURE", "CANCELED"):
            return info["status"], info["endTime"] - info["startTime"], run_id
        time.sleep(2)


results = {}


def chain(sync):
    for kind in ("reset", "setup"):
        job = f"reverse_etl_{sync}_{kind}_job"
        status, seconds, detail = run(job)
        results[job] = (status, seconds, detail)
        print(f"  {status:8} {seconds:5.0f}s  {job}", flush=True)
        if status != "SUCCESS":
            return


start = time.time()
threads = [threading.Thread(target=chain, args=(s,)) for s in SYNCS]
for t in threads:
    t.start()
for t in threads:
    t.join()

failed = [j for j, (s, _, _) in results.items() if s != "SUCCESS"]
expected = len(SYNCS) * 2
print(f"\nDone in {time.time() - start:.0f}s")
if failed or len(results) < expected:
    print("FAILED or skipped:", failed or "a setup job did not run after a failed reset")
    sys.exit(1)
PYEOF
