#!/bin/bash

# Script to run dbt commands with specific profiles directory
# This script should be run from the dbt folder

set -e

echo "=== Running dbt with custom profiles directory ==="
echo "Command: dbt run --profiles-dir ."
echo ""

# Run dbt with profiles directory set to current directory
dbt run --profiles-dir .