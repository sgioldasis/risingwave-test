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

if [ -f "$RUNS_DB" ] || [ -f "$EVENT_LOGS_DB" ] || [ -f "$SCHEDULES_DB" ]; then
    echo "Removing existing Dagster databases to ensure clean state..."
    rm -f "$RUNS_DB" "$EVENT_LOGS_DB" "$SCHEDULES_DB"
    echo "✅ Cleaned up existing Dagster databases"
fi

# Start the requested command
exec "$@"
