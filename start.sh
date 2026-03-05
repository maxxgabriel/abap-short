#!/bin/bash

echo "=========================================="
echo "ABAP Short - PySpark Pipeline Launcher"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

echo "✓ Docker is running"
echo ""

# Start Docker Compose services
echo "Starting PySpark environment..."
docker-compose up -d

# Wait for container to be ready
echo "Waiting for container to be ready..."
sleep 10

# Check if container is running
if ! docker ps | grep -q abap_short_pyspark; then
    echo "Error: Container failed to start"
    docker-compose logs
    exit 1
fi

echo "✓ PySpark environment is ready"
echo ""

# Install dependencies
echo "Installing dependencies..."
docker exec abap_short_pyspark bash -c "pip install --upgrade pip && pip install -r requirements.txt"

if [ $? -ne 0 ]; then
    echo "Error: Failed to install dependencies"
    docker-compose logs
    exit 1
fi

echo "✓ Dependencies installed"
echo ""

# Run the pipeline
echo "=========================================="
echo "Running Pipeline"
echo "=========================================="
echo ""

docker exec -it abap_short_pyspark python main.py

PIPELINE_EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
    echo "✓ Pipeline completed successfully"
else
    echo "✗ Pipeline failed with exit code $PIPELINE_EXIT_CODE"
fi
echo "=========================================="
echo ""

# Show logs location
echo "Logs are available in: ./logs"
echo "Output is available in: ./output"
echo ""

# Ask if user wants to stop the environment
read -p "Stop the Docker environment? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping Docker environment..."
    docker-compose down
    echo "✓ Environment stopped"
else
    echo "Environment is still running. To stop it later, run: docker-compose down"
    echo "To view logs: docker-compose logs -f"
    echo "To access container shell: docker exec -it abap_short_pyspark bash"
fi

exit $PIPELINE_EXIT_CODE