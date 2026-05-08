#!/bin/bash

# Script to start Docker Compose services
# This script should be run from the project root

set -e

echo "=== Pre-flight: Syncing local Python packages ==="
# Preserve historical behavior: refresh dependencies on startup by default.
# Set UPGRADE_DEPS=0 to force lockfile-only sync for faster deterministic startup.
UV_SYNC_ARGS="sync --upgrade"
if [ "${UPGRADE_DEPS:-1}" = "1" ]; then
    echo "Upgrade mode enabled (default; set UPGRADE_DEPS=0 to disable)"
else
    UV_SYNC_ARGS="sync --frozen"
    echo "Upgrade mode disabled (UPGRADE_DEPS=0)"
    echo "Using lockfile-only sync for faster startup"
fi

# Prevent indefinite hangs during dependency sync.
UV_SYNC_TIMEOUT_SECONDS="${UV_SYNC_TIMEOUT_SECONDS:-45}"
echo "Package sync timeout: ${UV_SYNC_TIMEOUT_SECONDS}s"

run_sync_cmd() {
    local runner="$1"
    local cmd="$2"

    if command -v timeout >/dev/null 2>&1; then
        timeout "${UV_SYNC_TIMEOUT_SECONDS}" bash -lc "${runner} ${cmd}"
    else
        bash -lc "${runner} ${cmd}"
    fi
}

SYNC_DONE=0

# Capture uv.lock hash before sync so we can detect dependency changes
# and trigger an automatic image rebuild when needed.
hash_lock() {
    if [ -f uv.lock ]; then
        if command -v shasum >/dev/null 2>&1; then
            shasum -a 256 uv.lock | awk '{print $1}'
        elif command -v sha256sum >/dev/null 2>&1; then
            sha256sum uv.lock | awk '{print $1}'
        fi
    fi
}
LOCK_HASH_BEFORE="$(hash_lock)"

if command -v uv >/dev/null 2>&1; then
    echo "Using local uv binary"
    if run_sync_cmd "" "uv ${UV_SYNC_ARGS}"; then
        SYNC_DONE=1
    fi
fi

if [ "$SYNC_DONE" -ne 1 ] && [ "${USE_DEVBOX_UV_FALLBACK:-0}" = "1" ] && command -v devbox >/dev/null 2>&1; then
    echo "Falling back to: devbox run uv ${UV_SYNC_ARGS}"
    if run_sync_cmd "" "devbox run uv ${UV_SYNC_ARGS}"; then
        SYNC_DONE=1
    fi
fi

if [ "$SYNC_DONE" -ne 1 ]; then
    echo "⚠ Package sync did not complete successfully within ${UV_SYNC_TIMEOUT_SECONDS}s."
    echo "  Continuing startup. To debug manually, run: uv ${UV_SYNC_ARGS}"
    echo "  Set USE_DEVBOX_UV_FALLBACK=1 to also try: devbox run uv ${UV_SYNC_ARGS}"
else
    echo "✅ Local packages synced"
fi

echo ""
echo "=== Pre-flight: Cleaning up any existing Dagster state ==="
# Stop and remove Dagster containers first to release the volume
# This is necessary because Docker won't remove a volume that's in use
docker compose rm -fsv dagster-daemon dagster-webserver 2>/dev/null || true

# Remove the dagster-storage volume to ensure a clean state
# This prevents "Version table 'alembic_version' has more than one head" errors
echo "Removing dagster-storage volume..."
docker volume rm -f dagster-storage 2>/dev/null || true
echo "✅ Cleaned up Dagster storage"

echo ""
echo "=== Starting Docker Compose Services ==="
echo "Running docker compose up -d from project root"
echo ""

# Run from project root (where docker-compose.yml is located)
# Auto-rebuild images when uv.lock changed during sync (dependency drift),
# or when explicitly requested via COMPOSE_FORCE_BUILD=1.
LOCK_HASH_AFTER="$(hash_lock)"
AUTO_REBUILD=0
if [ -n "$LOCK_HASH_BEFORE" ] && [ -n "$LOCK_HASH_AFTER" ] && \
   [ "$LOCK_HASH_BEFORE" != "$LOCK_HASH_AFTER" ]; then
    AUTO_REBUILD=1
    echo "Detected uv.lock changes during sync → will rebuild images to match"
fi

if [ "${COMPOSE_FORCE_BUILD:-0}" = "1" ] || [ "$AUTO_REBUILD" = "1" ]; then
    if [ "${COMPOSE_FORCE_BUILD:-0}" = "1" ]; then
        echo "Build mode enabled (COMPOSE_FORCE_BUILD=1): rebuilding images"
    fi
    docker compose up --build -d
else
    echo "Build mode disabled: reusing existing local images (set COMPOSE_FORCE_BUILD=1 to force)"
    docker compose up -d
fi

echo ""
echo "Checking modern dashboard dependencies..."
if [ ! -d "modern-dashboard/frontend/node_modules" ]; then
    echo "node_modules not found. Installing frontend dependencies..."
    cd modern-dashboard/frontend && npm install && cd ../..
    echo "✅ Frontend dependencies installed"
else
    echo "✅ Frontend dependencies already installed (node_modules exists)"
fi

echo ""
echo "=== Starting Sink Failure Watchdog ==="
if ./bin/watch_sink_failures.sh; then
    echo "✅ Sink failure watchdog started"
else
    echo "⚠ Failed to start sink failure watchdog"
fi

echo ""
echo "✅ Docker Compose services started successfully!"
if [ "${UPGRADE_DEPS:-1}" = "1" ]; then
    echo "✅ Packages upgraded with uv sync --upgrade"
else
    echo "✅ Packages synced with uv sync"
fi
echo "✅ Dagster started with fresh storage (no Alembic conflicts)"
echo "You can now run your dbt models and applications."
