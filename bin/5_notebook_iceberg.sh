#!/bin/bash
set -e

# Script to run the Marimo Notebook for Iceberg User Activity Flow
# This script should be run from the project root

echo "🚀 Starting Marimo Notebook for Iceberg User Activity Flow"
echo "============================================================"

# Run the marimo notebook in edit mode
uv run marimo edit scripts/user_activity_flow.py
