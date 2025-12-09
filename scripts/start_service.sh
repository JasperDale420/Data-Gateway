#!/bin/bash

# Ensure script is run from project root or scripts dir
cd "$(dirname "$0")/.."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "Starting Data Ingestion Service..."
docker-compose up -d --build

echo "Service started in background."
echo "Checking logs for 5 seconds..."
timeout 5s docker-compose logs -f || true

echo "Done. Service is running with restart policy 'always'."
echo "To view logs: docker-compose logs -f"
echo "To stop: docker-compose down"
