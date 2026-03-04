#!/bin/bash
# Create funnel views directly in RisingWave
# Use this if dbt fails

set -e

echo "Creating funnel views in RisingWave..."

psql postgresql://root@localhost:4566/dev -f sql/create_funnel_views.sql

echo ""
echo "Views created. Checking counts..."
psql postgresql://root@localhost:4566/dev -c "SELECT COUNT(*) as funnel_count FROM funnel;"
psql postgresql://root@localhost:4566/dev -c "SELECT COUNT(*) as training_count FROM funnel_training;"

echo ""
echo "Done!"
