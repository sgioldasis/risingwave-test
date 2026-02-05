#!/bin/bash

# Script to run Dagster for the realtime funnel project
# This script can run Dagster either locally or via Docker

# Get the project root (where this script is located from bin/)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse command line arguments
USE_DOCKER=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --docker|-d)
            USE_DOCKER=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --docker, -d    Run Dagster in Docker container (uses docker-compose)"
            echo "  --help, -h      Show this help message"
            echo ""
            echo "Without any options, runs Dagster locally using uv."
            echo ""
            echo "Examples:"
            echo "  $0              # Run locally"
            echo "  $0 --docker     # Run in Docker"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [ "$USE_DOCKER" = true ]; then
    echo "Starting Dagster in Docker..."
    echo "Dagster UI will be available at: http://localhost:3000"
    echo ""
    
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo "Error: docker-compose is not installed"
        exit 1
    fi
    
    # Use the appropriate docker compose command
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    # Build and start dagster webserver
    echo "Building and starting Dagster webserver..."
    $DOCKER_COMPOSE up -d dagster-webserver
    
    echo ""
    echo "Dagster webserver started!"
    echo "Webserver: http://localhost:3000"
    echo ""
    echo "To view logs: $DOCKER_COMPOSE logs -f dagster-webserver"
    echo "To stop: $DOCKER_COMPOSE stop dagster-webserver"
else
    echo "Starting Dagster locally..."
    echo "Dagster UI will be available at: http://localhost:3000"
    echo ""
    echo "Tip: Use '$0 --docker' to run in a container instead."
    echo ""

    # Check if orchestration directory exists
    if [ ! -d "orchestration" ]; then
        echo "Error: orchestration directory not found at $PROJECT_ROOT/orchestration"
        exit 1
    fi

    # Create dagster storage directory
    mkdir -p dagster_storage/logs

    # Check if dbt manifest exists, if not run dbt parse
    if [ ! -f "dbt/target/manifest.json" ]; then
        echo "dbt manifest not found. Running dbt parse to generate manifest..."
        cd dbt && dbt parse && cd "$PROJECT_ROOT"
    fi

    # Set environment variable for dagster home to use our config
    export DAGSTER_HOME="$PROJECT_ROOT"
    # Add project root to PYTHONPATH so orchestration module can be found
    export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

    # Check if uv is available
    if command -v uv &> /dev/null; then
        echo "Using uv to run Dagster..."
        uv run dagster-webserver -m orchestration.definitions --host 0.0.0.0 --port 3000
    else
        echo "Using dagster-webserver directly..."
        dagster-webserver -m orchestration.definitions --host 0.0.0.0 --port 3000
    fi
fi
