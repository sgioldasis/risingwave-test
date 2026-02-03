#!/bin/bash

# Script to run dbt commands with specific profiles directory
# This script should be run from the project root

set -e

echo "=== Running dbt with custom profiles directory ==="
echo "Command: dbt run --profiles-dir ."
echo ""

# Change to dbt directory and run dbt
cd dbt
dbt run --profiles-dir .
