#!/bin/bash

# Start script for ABAP to PySpark migrated project (Linux/Mac)
# This script starts the Docker environment and runs the pipeline

set -e

echo "=================================================="
echo "ABAP to PySpark Migration - Startup Script"
echo "=================================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH"
    echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "ERROR: Docker Compose is not installed"
    echo "Please install Docker Compose"
    exit 1
fi

# Determine docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    DOCKER_COMPOSE="docker compose"
fi

echo "Step 1: Creating required directories..."
mkdir -p data logs checkpoints output

echo "Step 2: Starting Docker containers..."
$DOCKER_COMPOSE up -d

echo "Step 3: Waiting for Spark to be ready..."
sleep 10

echo "Step 4: Installing Python dependencies..."
docker exec abap_test_pyspark pip install --no-cache-dir -r /app/requirements.txt

echo "Step 5: Running the PySpark pipeline..."
echo ""
docker exec abap_test_pyspark python /app/main.py

echo ""
echo "=================================================="
echo "Pipeline execution completed!"
echo "=================================================="
echo ""
echo "Useful commands:"
echo "  View logs:        docker logs abap_test_pyspark"
echo "  Stop containers:  $DOCKER_COMPOSE down"
echo "  Restart:          $DOCKER_COMPOSE restart"
echo "  Shell access:     docker exec -it abap_test_pyspark bash"
echo "  Spark UI:         http://localhost:8080"
echo ""