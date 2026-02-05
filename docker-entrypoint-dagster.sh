#!/bin/bash
set -e

# Generate dbt manifest if it doesn't exist
if [ ! -f "/workspace/dbt/target/manifest.json" ]; then
    echo "Generating dbt manifest..."
    cd /workspace/dbt
    # Create minimal profiles.yml if it doesn't exist
    mkdir -p /root/.dbt
    if [ ! -f "/root/.dbt/profiles.yml" ]; then
        cat > /root/.dbt/profiles.yml << 'EOF'
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

# Start the requested command
exec "$@"
