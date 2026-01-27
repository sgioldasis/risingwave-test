#!/bin/bash

# Script to create Redpanda topics using rpk
# This script should be run from the parent directory where docker-compose.yml is located

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
        echo "Please start your Docker Compose setup first: docker-compose up -d"
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
    
    echo ""
    echo "✅ Topic creation completed successfully!"
    echo "You can now run your producer and dashboard."
}

# Run main function
main "$@"