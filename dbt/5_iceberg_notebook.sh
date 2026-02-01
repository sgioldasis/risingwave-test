#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "🚀 Starting Marimo Notebook for Iceberg User Activity Flow"
echo "============================================================"

# Run the marimo notebook in edit mode
uv run marimo edit user_activity_flow.py
