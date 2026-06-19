#!/bin/bash

# Script to start Docker Compose services
# This script should be run from the project root
#
# Flags:
#   --offline | --skip-dagster-build
#       Reuse the locally cached Dagster image instead of (re)building it,
#       and skip any network-touching pre-flight (uv --upgrade, image pulls).
#       Equivalent to exporting SKIP_DAGSTER_BUILD=1 before invocation.
#       Use when the network blocks Docker Hub / Adoptium / Maven / PyPI
#       (e.g. VPN). Requires the image 'risingwave-test/dagster:local' to
#       already exist; build it once when off-VPN:
#           docker compose build dagster-webserver

set -e

# Parse CLI flags (the script runner forwards its params field as positional
# args, so this lets users toggle offline mode per-run from the UI).
for arg in "$@"; do
    case "$arg" in
        --offline|--skip-dagster-build)
            export SKIP_DAGSTER_BUILD=1
            echo "Flag '$arg' → SKIP_DAGSTER_BUILD=1 for this run"
            ;;
    esac
done

# Offline mode implies "fully offline": don't refresh deps from PyPI, don't
# pull container images. Callers can still opt back in by explicitly setting
# UPGRADE_DEPS=1 / COMPOSE_PULL=always alongside SKIP_DAGSTER_BUILD=1.
if [ "${SKIP_DAGSTER_BUILD:-0}" = "1" ]; then
    : "${UPGRADE_DEPS:=0}"
    : "${COMPOSE_PULL:=never}"
    export UPGRADE_DEPS COMPOSE_PULL
    echo "Offline mode active → UPGRADE_DEPS=${UPGRADE_DEPS}, COMPOSE_PULL=${COMPOSE_PULL}"

    # Fail fast with an actionable message before we touch anything else.
    DAGSTER_IMAGE_REF="${DAGSTER_IMAGE:-risingwave-test/dagster:local}"
    if ! docker image inspect "${DAGSTER_IMAGE_REF}" >/dev/null 2>&1; then
        echo "❌ Offline mode requires '${DAGSTER_IMAGE_REF}' to be present locally."
        echo "   Build it once off-VPN: docker compose build dagster-webserver"
        exit 1
    fi
fi

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
DAGSTER_BUILD_HASH_FILE=".dagster_build_hash"

# Use git's built-in object hashing — available wherever git is.
hash_lockfile() {
    [ -f uv.lock ] && git hash-object uv.lock
}

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

# Determine whether the Dagster image needs rebuilding by comparing the current
# uv.lock hash against the hash recorded after the last successful build.
CURRENT_LOCK_HASH="$(hash_lockfile)"
SAVED_LOCK_HASH="$(cat "${DAGSTER_BUILD_HASH_FILE}" 2>/dev/null || true)"
if [ -z "${CURRENT_LOCK_HASH}" ] || [ "${CURRENT_LOCK_HASH}" != "${SAVED_LOCK_HASH}" ]; then
    NEEDS_BUILD=1
    echo "uv.lock changed or no previous build recorded → Dagster image will be rebuilt"
else
    NEEDS_BUILD=0
    echo "uv.lock unchanged since last build → skipping Dagster image rebuild"
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
# Build behaviour is controlled solely by the Offline checkbox (SKIP_DAGSTER_BUILD):
#   checkbox unchecked → always rebuild the Dagster image (default)
#   checkbox checked   → skip build, reuse the locally cached image (VPN / offline)
COMPOSE_ARGS=()
UP_ARGS=(-d)

if [ "${SKIP_DAGSTER_BUILD:-0}" = "1" ]; then
    echo "Offline mode (SKIP_DAGSTER_BUILD=1): reusing local image '${DAGSTER_IMAGE:-risingwave-test/dagster:local}'"
    COMPOSE_ARGS+=(-f docker-compose.yml -f docker-compose.dagster-prebuilt.yml)
fi

# Honor COMPOSE_PULL (defaults to "missing", offline mode sets it to "never").
if [ -n "${COMPOSE_PULL:-}" ]; then
    UP_ARGS+=(--pull "${COMPOSE_PULL}")
fi

if [ "${SKIP_DAGSTER_BUILD:-0}" = "1" ]; then
    docker compose "${COMPOSE_ARGS[@]}" up "${UP_ARGS[@]}"
elif [ "${NEEDS_BUILD}" = "1" ]; then
    docker compose "${COMPOSE_ARGS[@]}" up --build "${UP_ARGS[@]}"
    echo "${CURRENT_LOCK_HASH}" > "${DAGSTER_BUILD_HASH_FILE}"
else
    docker compose "${COMPOSE_ARGS[@]}" up "${UP_ARGS[@]}"
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
echo "✅ Docker Compose services started successfully!"
if [ "${UPGRADE_DEPS:-1}" = "1" ]; then
    echo "✅ Packages upgraded with uv sync --upgrade"
else
    echo "✅ Packages synced with uv sync"
fi
echo "✅ Dagster started with fresh storage (no Alembic conflicts)"
echo "You can now run your dbt models and applications."
