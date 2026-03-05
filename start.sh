#!/bin/bash

# start.sh - Launch script for ABAP to PySpark Migration (Linux/Mac)

set -e

echo "================================================"
echo "ABAP to PySpark Migration - Startup Script"
echo "================================================"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p data logs output config

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker daemon is not running. Please start Docker first.${NC}"
    exit 1
fi

# Stop any existing containers
echo -e "${YELLOW}Stopping any existing containers...${NC}"
docker-compose down 2>/dev/null || true

# Build and start the Docker environment
echo -e "${YELLOW}Starting Docker environment...${NC}"
docker-compose up -d

# Wait for the container to be ready
echo -e "${YELLOW}Waiting for PySpark environment to be ready...${NC}"
sleep 10

# Check if container is running
if ! docker ps | grep -q pyspark_abap_migration; then
    echo -e "${RED}Error: PySpark container failed to start${NC}"
    docker-compose logs
    exit 1
fi

echo -e "${GREEN}PySpark environment is ready!${NC}"

# Execute the main pipeline
echo -e "${YELLOW}Running the migration pipeline...${NC}"
docker exec -it pyspark_abap_migration python main.py

# Check exit code
if [ $? -eq 0 ]; then
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}Pipeline completed successfully!${NC}"
    echo -e "${GREEN}================================================${NC}"
else
    echo -e "${RED}================================================${NC}"
    echo -e "${RED}Pipeline execution failed!${NC}"
    echo -e "${RED}================================================${NC}"
    exit 1
fi

# Display logs location
echo -e "${YELLOW}Logs available in: ./logs/${NC}"
echo -e "${YELLOW}Output available in: ./output/${NC}"

# Optionally keep container running or stop it
echo ""
echo "Options:"
echo "  - To view logs: docker-compose logs -f"
echo "  - To stop environment: docker-compose down"
echo "  - To access PySpark shell: docker exec -it pyspark_abap_migration pyspark"
echo "  - To access container: docker exec -it pyspark_abap_migration bash"
echo ""

# Keep container running for inspection
echo -e "${GREEN}Container is still running for inspection. Use 'docker-compose down' to stop.${NC}"