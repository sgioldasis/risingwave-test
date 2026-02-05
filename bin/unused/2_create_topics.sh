#!/bin/bash

# Script to create Redpanda topics
# DEPRECATED: Topics are now created automatically by the redpanda-init
# container during docker-compose startup. This script is kept for
# reference or manual topic management if needed.
# This script should be run from the project root

set -e

# Configuration
REDPANDA_CONTAINER="redpanda"
TOPICS=("page_views" "cart_events" "purchases")

echo "=== Redpanda Topic Creator ==="
echo "Creating topics using rpk in Redpanda container..."

# Function to check if container is running
check_container_running() {
    if ! docker-compose ps -q "$REDPANDA_CONTAINER" | grep -q .; then
        echo "Error: Redpanda container is not running."
        echo "Please start your Docker Compose setup first: ./bin/1_up.sh"
        exit 1
    fi
}

# Function to create a topic
create_topic() {
    local topic_name=$1
    echo "Creating topic: $topic_name"
    
    # Try to create the topic directly
    if docker-compose exec -T "$REDPANDA_CONTAINER" rpk topic create "$topic_name" \
        --partitions 1 \
        --replicas 1 \
        2>/dev/null; then
        echo "Topic '$topic_name' created successfully"
    else
        # If topic already exists, show a message but don't error
        echo "Topic '$topic_name' already exists, skipping..."
    fi
}

# Function to verify topics
verify_topics() {
    echo "Verifying created topics..."
    docker-compose exec -T "$REDPANDA_CONTAINER" rpk topic list
}

# NOTE: Lakekeeper namespace creation is now handled automatically by the
# lakekeeper-bootstrap container in docker-compose.yml. The analytics
# namespace is created during the initial startup process.
#
# Function to create Lakekeeper namespace (deprecated - kept for reference)
# create_lakekeeper_namespace() {
#     echo ""
#     echo "=== Lakekeeper Namespace Setup ==="
#
#     # Wait for Lakekeeper to be ready
#     echo "Waiting for Lakekeeper to be ready..."
#     local attempts=0
#     while [ $attempts -lt 30 ]; do
#         if curl -s "$LAKEKEEPER_URL/management/v1/warehouse" > /dev/null 2>&1; then
#             break
#         fi
#         attempts=$((attempts + 1))
#         sleep 2
#     done
#
#     if [ $attempts -eq 30 ]; then
#         echo "⚠️  Lakekeeper is not responding. Skipping namespace creation."
#         echo "   You may need to create the namespace manually later."
#         return 0
#     fi
#
#     echo "Lakekeeper is ready. Creating namespace..."
#
#     # Get Warehouse ID
#     local warehouse_id
#     warehouse_id=$(curl -s "$LAKEKEEPER_URL/management/v1/warehouse" | grep -o '"warehouse-id":"[^"]*"' | cut -d'"' -f4)
#
#     if [ -z "$warehouse_id" ]; then
#         echo "⚠️  Could not retrieve Warehouse ID. Skipping namespace creation."
#         return 0
#     fi
#
#     echo "Using Warehouse ID: $warehouse_id"
#
#     # Create namespace
#     local http_code
#     http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
#         -H "Content-Type: application/json" \
#         -d "{\"namespace\": [\"$NAMESPACE\"]}" \
#         "$LAKEKEEPER_URL/catalog/v1/$warehouse_id/namespaces")
#
#     case "$http_code" in
#         200|201)
#             echo "✅ Namespace '$NAMESPACE' created successfully"
#             ;;
#         409)
#             echo "✅ Namespace '$NAMESPACE' already exists"
#             ;;
#         *)
#             echo "⚠️  Failed to create namespace (HTTP $http_code)"
#             ;;
#     esac
# }

# Main execution
main() {
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null; then
        echo "Error: docker-compose is not installed or not in PATH"
        exit 1
    fi
    
    # Check if Redpanda container is running
    check_container_running
    
    echo "Redpanda container is running. Proceeding with topic creation..."
    echo ""
    
    # Create each topic
    for topic in "${TOPICS[@]}"; do
        create_topic "$topic"
        echo ""
    done
    
    # Verify all topics were created
    echo ""
    echo "=== Verification ==="
    verify_topics
    
    # Lakekeeper namespace creation is now handled by docker-compose
    # The lakekeeper-bootstrap container creates the analytics namespace automatically
    
    echo ""
    echo "✅ Topic creation completed successfully!"
    echo "You can now run your producer and dashboard."
}

# Run main function
main "$@"
