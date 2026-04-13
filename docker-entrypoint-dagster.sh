#!/bin/bash
set -e

# Use the user's home directory for dbt config (works for both root and dagster user)
DBT_HOME="${HOME}/.dbt"

# Generate dbt manifest if it doesn't exist
if [ ! -f "/workspace/dbt/target/manifest.json" ]; then
    echo "Generating dbt manifest..."
    cd /workspace/dbt
    # Create minimal profiles.yml if it doesn't exist
    mkdir -p "$DBT_HOME"
    if [ ! -f "$DBT_HOME/profiles.yml" ]; then
        cat > "$DBT_HOME/profiles.yml" << 'EOF'
funnel_profile:
  target: dev
  outputs:
    dev:
      type: risingwave
      host: frontend-node-0
      port: 4566
      user: root
      database: dev
      schema: public
EOF
    fi
    dbt parse || echo "dbt parse completed with warnings"
    cd /workspace
fi

# Ensure dagster_storage directory exists
mkdir -p /workspace/dagster_storage/logs

# Clean up any existing SQLite databases to prevent Alembic migration issues
# This is necessary because Dagster's SQLite storage can get into a corrupted
# state with "Version table 'alembic_version' has more than one head" errors
RUNS_DB="/workspace/dagster_storage/runs.db"
EVENT_LOGS_DB="/workspace/dagster_storage/event_logs.db"
SCHEDULES_DB="/workspace/dagster_storage/schedules.db"
RUNS_DIR="/workspace/dagster_storage/runs"
SCHEDULES_DIR="/workspace/dagster_storage/schedules"

if [ -f "$RUNS_DB" ] || [ -f "$EVENT_LOGS_DB" ] || [ -f "$SCHEDULES_DB" ] || [ -d "$RUNS_DIR" ] || [ -d "$SCHEDULES_DIR" ]; then
    echo "Removing existing Dagster databases to ensure clean state..."
    rm -rf "$RUNS_DB" "$EVENT_LOGS_DB" "$SCHEDULES_DB" "$RUNS_DIR" "$SCHEDULES_DIR"
    echo "✅ Cleaned up existing Dagster databases"
fi

# Resolve HOST_POSTGRES_URL for RisingWave JDBC sink
# RisingWave's embedded connector doesn't read /etc/hosts, so host.docker.internal
# won't resolve. We resolve the IP here and pass it as an env var for dbt.
if [ -z "$HOST_POSTGRES_URL" ]; then
    HOST_IP=$(getent hosts host.docker.internal | awk '{print $1}')
    if [ -n "$HOST_IP" ]; then
        export HOST_POSTGRES_URL="jdbc:postgresql://${HOST_IP}:5432/postgres"
        echo "Resolved HOST_POSTGRES_URL=${HOST_POSTGRES_URL}"
    else
        echo "WARNING: Could not resolve host.docker.internal, using default"
        export HOST_POSTGRES_URL="jdbc:postgresql://host.docker.internal:5432/postgres"
    fi
fi

# Start the requested command
exec "$@"
