#!/bin/bash

# ABAP to PySpark Pipeline - Linux/Mac Startup Script

set -e

echo "=========================================="
echo "ABAP to PySpark ETL Pipeline"
echo "=========================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed or not in PATH${NC}"
    echo "Please install Docker Compose"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker daemon is not running${NC}"
    echo "Please start Docker Desktop"
    exit 1
fi

echo -e "${GREEN}✓ Docker is installed and running${NC}"
echo ""

# Create necessary directories
echo "Creating required directories..."
mkdir -p data logs output
echo -e "${GREEN}✓ Directories created${NC}"
echo ""

# Stop any existing containers
echo "Stopping existing containers (if any)..."
docker-compose down 2>/dev/null || true
echo ""

# Build and start containers
echo "Starting PySpark environment..."
echo -e "${YELLOW}This may take a few minutes on first run...${NC}"
docker-compose up -d

# Wait for services to be ready
echo ""
echo "Waiting for services to be ready..."
sleep 10

# Check if containers are running
if ! docker-compose ps | grep -q "Up"; then
    echo -e "${RED}Error: Containers failed to start${NC}"
    echo "Check logs with: docker-compose logs"
    exit 1
fi

echo -e "${GREEN}✓ PySpark environment is ready${NC}"
echo ""

# Display service URLs
echo "=========================================="
echo "Service URLs:"
echo "=========================================="
echo "Spark Master UI: http://localhost:8080"
echo "Spark Worker UI: http://localhost:8081"
echo "Spark Application UI: http://localhost:4040"
echo ""

# Execute the pipeline
echo "=========================================="
echo "Executing ETL Pipeline..."
echo "=========================================="
echo ""

docker-compose exec -T pyspark python /app/main.py

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Pipeline execution completed successfully!${NC}"
else
    echo -e "${RED}Pipeline execution failed with exit code $EXIT_CODE${NC}"
    echo "Check logs in pipeline.log or run: docker-compose logs"
fi
echo "=========================================="
echo ""

# Ask if user wants to stop containers
read -p "Do you want to stop the containers? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping containers..."
    docker-compose down
    echo -e "${GREEN}✓ Containers stopped${NC}"
else
    echo "Containers are still running. To stop them later, run: docker-compose down"
fi

exit $EXIT_CODE