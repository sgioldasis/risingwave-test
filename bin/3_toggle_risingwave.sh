#!/bin/bash

# Non-destructive pause/resume of the RisingWave cluster, for the "what
# happens in Superset without RisingWave" demo moment.
#
#   stop  = docker compose stop -- containers killed cleanly; container
#           definitions AND all state (Hummock in MinIO) preserved.
#   start = docker compose start -- no recreate, no volumes touched; sources
#           resume from their committed offsets and replay everything the
#           producers wrote while RW was down (verified live 2026-11-14:
#           funnel_summary came back continuous, zero gap).
#
# Deliberately never `docker compose down` (removes containers) and never
# `down -v` (wipes state -- see bin/6_down.sh for the full teardown).
#
# Safe to click during a demo: the producers keep buffering into Redpanda
# the whole time, and on start RW catches up from its offsets.

set -e

SERVICES=(meta-node-0 compute-node-0 frontend-node-0 compactor-0 compactor-1)

running=0
for svc in "${SERVICES[@]}"; do
    state=$(docker inspect -f '{{.State.Running}}' "$svc" 2>/dev/null || echo missing)
    [ "$state" = "true" ] && running=$((running + 1))
done

if [ "$running" -eq "${#SERVICES[@]}" ]; then
    echo "=== RisingWave is UP -- stopping (non-destructive pause) ==="
    echo "    Producers keep buffering into Redpanda; state preserved in MinIO."
    docker compose stop "${SERVICES[@]}"
    echo ""
    echo "=== RisingWave stopped. ==="
    echo "    Superset: RW-dependent charts fail fast; the RW-free cached pair"
    echo "    (Async MV + Direct-from-Kafka) keeps working -- that's the demo."
elif [ "$running" -eq 0 ]; then
    echo "=== RisingWave is DOWN -- starting (resume) ==="
    docker compose start "${SERVICES[@]}"
    echo ""
    echo "=== RisingWave started. ==="
    echo "    Sources replay from their committed offsets and catch up with"
    echo "    everything the producers wrote while RW was down (expect a short"
    echo "    backfill before the live charts fill in)."
else
    echo "=== RisingWave in a mixed state ($running/${#SERVICES[@]} running) -- starting the stopped ones ==="
    docker compose start "${SERVICES[@]}"
fi
